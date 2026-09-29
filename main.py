#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import base64
from pathlib import Path
from io import BytesIO

import pyodbc
import mysql.connector
from PIL import Image

# 1. Определение корневой директории проекта
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.append(str(SRC_DIR))

# Импорт локальных модулей из src/
from logger_setup import setup_logger
from backup_manager import backup_and_rotate_aif
from sql_loader import SQLLoader

# 2. Загрузка конфигурации
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"

def load_config(config_file):
    if not config_file.exists():
        sys.stderr.write(f"Критическая ошибка: Файл конфигурации не найден ({config_file})\n")
        sys.exit(1)
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config(CONFIG_PATH)

# Преобразуем относительный путь логов в абсолютный от PROJECT_ROOT
log_file_setting = CONFIG.get("logging", {}).get("log_file", "log/artix_etl.log")
if not os.path.isabs(log_file_setting):
    CONFIG["logging"]["log_file"] = str(PROJECT_ROOT / log_file_setting)

logger = setup_logger(CONFIG.get("logging", {}))
sql_loader = SQLLoader(PROJECT_ROOT / "sql")

# ==============================================================================
# УПРАВЛЕНИЕ КЭШЕМ MYSQL
# ==============================================================================
class LocalCacheMySQL:
    def __init__(self, db_config):
        self.config = db_config

    def _get_connection(self):
        return mysql.connector.connect(
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            password=self.config["password"],
            database=self.config["database"],
            charset=self.config.get("charset", "utf8mb4"),
            autocommit=True
        )

    def get_image(self, binary_id: int):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT base64_data FROM image_cache WHERE binary_id = %s", (binary_id,))
            row = cursor.fetchone()
            cursor.close()
            return row if row else None
        except Exception as e:
            logger.error(f"Ошибка чтения кэша картинок (binary_id={binary_id}): {e}", exc_info=True)
            return None
        finally:
            if conn and conn.is_connected():
                conn.close()

    def save_image(self, binary_id: int, base64_data: str):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
            INSERT INTO image_cache (binary_id, base64_data) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE base64_data = VALUES(base64_data);
            """
            cursor.execute(sql, (binary_id, base64_data))
            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка сохранения картинки (binary_id={binary_id}): {e}", exc_info=True)
        finally:
            if conn and conn.is_connected():
                conn.close()

    def update_sync_state(self, entity_name: str, records_count: int, status: str = "SUCCESS"):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
            INSERT INTO sync_state (entity_name, last_sync, records_processed, status)
            VALUES (%s, NOW(), %s, %s)
            ON DUPLICATE KEY UPDATE 
                last_sync = NOW(), 
                records_processed = VALUES(records_processed),
                status = VALUES(status);
            """
            cursor.execute(sql, (entity_name, records_count, status))
            cursor.close()
            logger.info(f"Статус синхронизации '{entity_name}' записан: records={records_count}, status={status}")
        except Exception as e:
            logger.error(f"Ошибка фиксации статуса синхронизации: {e}", exc_info=True)
        finally:
            if conn and conn.is_connected():
                conn.close()

# ==============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================================================================
def get_mssql_connection(retries=3, delay=5):
    mssql_conf = CONFIG["mssql"]
    encrypt_val = "yes" if mssql_conf.get("encrypt", True) else "no"
    trust_cert_val = "yes" if mssql_conf.get("trust_server_certificate", True) else "no"

    conn_str = (
        f"DRIVER={mssql_conf['driver']};"
        f"SERVER={mssql_conf['server']};"
        f"DATABASE={mssql_conf['database']};"
        f"UID={mssql_conf['user']};"
        f"PWD={mssql_conf['password']};"
        f"Encrypt={encrypt_val};"
        f"TrustServerCertificate={trust_cert_val};"
        f"Timeout={mssql_conf['timeout']};"
    )
    for attempt in range(1, retries + 1):
        try:
            conn = pyodbc.connect(conn_str)
            logger.info("Соединение с MS SQL Server успешно установлено.")
            return conn
        except Exception as e:
            logger.warning(f"Попытка {attempt}/{retries} подключения к MS SQL не удалась: {e}")
            if attempt == retries:
                raise
            time.sleep(delay)

def compress_image_to_base64(image_bytes, cache_mgr, binary_id, stats_dict):
    if not image_bytes:
        return ""

    cached_b64 = cache_mgr.get_image(binary_id)
    if cached_b64:
        stats_dict["cache_hits"] += 1
        return cached_b64

    try:
        stats_dict["compressed_new"] += 1
        img = Image.open(BytesIO(image_bytes))
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        try:
            resample_filter = Image.Resampling.LANCZOS
        except AttributeError:
            resample_filter = getattr(Image, 'LANCZOS', getattr(Image, 'ANTIALIAS', 1))

        img.thumbnail((CONFIG["image"]["max_dimension"], CONFIG["image"]["max_dimension"]), resample_filter)

        quality = 85
        output = BytesIO()

        while True:
            output.seek(0)
            output.truncate()
            img.save(output, format="JPEG", optimize=True, quality=quality)
            if output.tell() <= CONFIG["image"]["max_size_kb"] * 1024 or quality <= 20:
                break
            quality -= 10

        b64_str = base64.b64encode(output.getvalue()).decode("utf-8")
        cache_mgr.save_image(binary_id, b64_str)
        return b64_str
    except Exception as e:
        logger.error(f"Ошибка сжатия картинки binary_id={binary_id}: {e}", exc_info=True)
        return ""

# ==============================================================================
# ФУНКЦИИ ВЫГРУЗКИ С ИСПОЛЬЗОВАНИЕМ ВЫНЕСЕННЫХ SQL-ФАЙЛОВ
# ==============================================================================
def fetch_units(cursor):
    logger.info("Извлечение единиц измерения (addUnit)...")
    sql = sql_loader.get_query("01_add_unit.sql")
    cursor.execute(sql)
    rows = cursor.fetchall()
    return [{"command": "addUnit", "unit": {"unitcode": r.unitcode, "name": r.name, "fractional": bool(r.fractional)}} for r in rows]

def fetch_groups(cursor):
    logger.info("Извлечение групп товаров (addInventGroup)...")
    sql = sql_loader.get_query("02_add_invent_group.sql")
    cursor.execute(sql)
    rows = cursor.fetchall()
    return [{"command": "addInventGroup", "inventGroup": {"groupCode": str(r.groupcode), "groupname": r.groupname, "parentGroupCode": str(r.parentgroupcode) if r.parentgroupcode else None}} for r in rows]

def fetch_items(cursor):
    logger.info("Извлечение карточек товаров (addInventItem)...")
    sql = sql_loader.get_query("03_add_invent_item.sql")
    cursor.execute(sql, (CONFIG["exchange"]["dept_code"],))
    rows = cursor.fetchall()
    return [{
        "command": "addInventItem",
        "invent": {
            "inventcode": str(r.inventcode),
            "measurecode": str(r.measurecode),
            "isInventItem": True,
            "name": r.name,
            "inventgroup": str(r.inventgroup) if r.inventgroup else None,
            "options": {"quantityoptions": {"requirequantityscales": bool(r.requirequantityscales)}, "inventitemoptions": {"ageverify": 1 if r.age > 0 else 0}},
            "opmode": r.opmode,
            "age": r.age
        }
    } for r in rows]

def fetch_barcodes(cursor):
    logger.info("Извлечение штрих-кодов (addBarcode)...")
    sql = sql_loader.get_query("04_add_barcode.sql")
    cursor.execute(sql, (CONFIG["exchange"]["dept_code"],))
    rows = cursor.fetchall()
    return [{"command": "addBarcode", "barcode": {"code": str(r.code), "barcode": str(r.barcode), "name": r.name, "measure": int(r.measure), "tmctype": int(r.tmctype), "quantdefault": float(r.quantdefault)}} for r in rows]

def fetch_prices(cursor):
    logger.info("Извлечение розничных цен (addPrice)...")
    sql = sql_loader.get_query("05_add_price.sql")
    cursor.execute(sql, (CONFIG["exchange"]["dept_code"],))
    rows = cursor.fetchall()
    return [{"command": "addPrice", "price": {"barcode": str(r.barcode), "price": str(r.price), "minprice": str(r.price), "pricetype": 3, "doctype": 1, "documentid": str(CONFIG["exchange"]["dept_code"])}} for r in rows]

def fetch_additional_prices(cursor):
    logger.info("Извлечение дополнительных цен/уценки (addAdditionalPrice)...")
    sql = sql_loader.get_query("06_add_additional_price.sql")
    cursor.execute(sql, (CONFIG["exchange"]["dept_code"],))
    rows = cursor.fetchall()
    return [{"command": "addAdditionalPrice", "additionalPrice": {"barcode": str(r.barcode), "price": str(r.additional_price), "pricecode": 1, "pricename": "Уценка"}} for r in rows]

def fetch_picklist(cursor, cache_mgr):
    logger.info("Извлечение пиклиста (addPicklist)...")
    stats = {"cache_hits": 0, "compressed_new": 0}
    commands = []

    sql_cats = sql_loader.get_query("07_picklist_categories.sql")
    cursor.execute(sql_cats)
    for r in cursor.fetchall():
        b64 = compress_image_to_base64(r.image_bytes, cache_mgr, r.binary_id, stats)
        commands.append({"command": "addPicklist", "picklist": {"code": str(r.code), "name": r.name, "image": b64, "parent": None, "tmccode": None, "barcode": None, "is_category": True}})

    sql_items = sql_loader.get_query("08_picklist_items.sql")
    cursor.execute(sql_items)
    for r in cursor.fetchall():
        b64 = compress_image_to_base64(r.image_bytes, cache_mgr, r.binary_id, stats)
        commands.append({"command": "addPicklist", "picklist": {"code": str(r.raw_code).lstrip('0'), "name": r.name, "image": b64, "parent": str(r.parent) if r.parent else None, "tmccode": str(r.tmccode).lstrip('0'), "barcode": str(r.barcode) if r.barcode else None, "is_category": False}})

    logger.info(f"Картинки пиклиста: из кэша={stats['cache_hits']}, сжато заново={stats['compressed_new']}")
    return commands

# ==============================================================================
# ПУБЛИКАЦИЯ ФАЙЛОВ ОБМЕНА
# ==============================================================================
def publish_aif_package(commands_list):
    target_dir = CONFIG["exchange"]["target_dir"]
    os.makedirs(target_dir, exist_ok=True)

    tmp_filepath = os.path.join(target_dir, f"{CONFIG['exchange']['aif_filename']}.tmp")
    aif_filepath = os.path.join(target_dir, CONFIG['exchange']['aif_filename'])
    flz_filepath = os.path.join(target_dir, CONFIG['exchange']['flag_filename'])

    logger.info(f"Формирование пакета AIF ({len(commands_list)} команд)...")
    with open(tmp_filepath, "w", encoding="utf-8") as f:
        for cmd in commands_list:
            f.write(json.dumps(cmd, ensure_ascii=False) + "\n" + "---" + "\n")

    os.replace(tmp_filepath, aif_filepath)
    
    # Бэкап создается в директории, указанной в config.json (или ./backup по умолчанию)
    backup_conf = CONFIG.get("backup", {})
    if backup_conf.get("enabled", True) and not os.path.isabs(backup_conf.get("backup_dir", "")):
        backup_conf["backup_dir"] = str(PROJECT_ROOT / backup_conf.get("backup_dir", "backup"))

    backup_and_rotate_aif(aif_filepath, backup_conf)

    with open(flz_filepath, "w", encoding="utf-8") as f:
        pass
    logger.info("Пакет опубликован и флаг готовности создан.")

# ==============================================================================
# MAIN
# ==============================================================================
def main():
    start_time = time.time()
    logger.info("=== Старт выгрузки справочников Artix SCO ===")
    cache_mgr = LocalCacheMySQL(CONFIG["mysql"])
    
    try:
        conn = get_mssql_connection()
        cursor = conn.cursor()

        all_commands = []
        all_commands.extend(fetch_units(cursor))
        all_commands.extend(fetch_groups(cursor))
        all_commands.extend(fetch_items(cursor))
        all_commands.extend(fetch_barcodes(cursor))
        all_commands.extend(fetch_prices(cursor))
        all_commands.extend(fetch_additional_prices(cursor))
        all_commands.extend(fetch_picklist(cursor, cache_mgr))

        cursor.close()
        conn.close()

        if all_commands:
            publish_aif_package(all_commands)
            cache_mgr.update_sync_state("full_dictionary_sync", len(all_commands), "SUCCESS")
            elapsed = time.time() - start_time
            logger.info(f"=== Выгрузка завершена за {elapsed:.2f} сек. Команд: {len(all_commands)} ===")
        else:
            logger.warning("Данные для выгрузки не найдены.")

    except Exception as e:
        elapsed = time.time() - start_time
        cache_mgr.update_sync_state("full_dictionary_sync", 0, "ERROR")
        logger.critical(f"Критический сбой ETL спустя {elapsed:.2f} сек: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()