#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Двухэтапный ETL-сервис обмена между MS SQL Express и Artix Control Center (SCO)
через локальную промежуточную базу данных MySQL (martin_etl).

Поддерживает режимы:
- full: Полная выгрузка с командами clear* и заменой справочников
- incremental: Дельта-выгрузка с генерацией команд delete* для удаленных записей
"""

import os
import sys
import json
import time
import base64
import argparse
from pathlib import Path
from io import BytesIO
from datetime import datetime, timedelta

import pyodbc
import mysql.connector
from PIL import Image

# ==============================================================================
# 1. РЕЗОЛВИНГ ПУТЕЙ И ИМПОРТ МОДУЛЕЙ
# ==============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.append(str(SRC_DIR))

from logger_setup import setup_logger
from backup_manager import backup_and_rotate_aif
from sql_loader import SQLLoader

# ==============================================================================
# 2. ПАРСИНГ АРГУМЕНТОВ КОМАНДНОЙ СТРОКИ И КОНФИГУРАЦИИ
# ==============================================================================
def parse_cli_args():
    parser = argparse.ArgumentParser(description="ETL-сервис выгрузки справочников Artix SCO")
    parser.add_argument("--mode", choices=["full", "incremental"], default=None, help="Режим выгрузки: full или incremental")
    parser.add_argument("--config", default=None, help="Путь к файлу config.json")
    parser.add_argument("--dept", type=int, default=None, help="Код отдела/департамента (например, 410)")
    parser.add_argument("--days-back", type=int, default=None, help="Глубина дней для пиклиста")
    parser.add_argument("--entities", type=str, default=None, help="Список сущностей через запятую (например: items,prices,picklist)")
    return parser.parse_args()

CLI_ARGS = parse_cli_args()

CONFIG_PATH = Path(CLI_ARGS.config) if CLI_ARGS.config else (PROJECT_ROOT / "config" / "config.json")

def load_config(config_file):
    if not config_file.exists():
        sys.stderr.write(f"Критическая ошибка: Файл конфигурации не найден ({config_file})\n")
        sys.exit(1)
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config(CONFIG_PATH)

# Переопределение параметров из CLI (командной строки)
if CLI_ARGS.mode:
    CONFIG.setdefault("exchange", {})["mode"] = CLI_ARGS.mode
if CLI_ARGS.dept:
    CONFIG.setdefault("exchange", {})["dept_code"] = CLI_ARGS.dept
if CLI_ARGS.days_back:
    CONFIG.setdefault("picklist", {})["days_back"] = CLI_ARGS.days_back
if CLI_ARGS.entities:
    selected_entities = [e.strip() for e in CLI_ARGS.entities.split(",")]
    if "entities" not in CONFIG:
        CONFIG["entities"] = {}
    for key in CONFIG["entities"]:
        CONFIG["entities"][key] = (key in selected_entities)

# Корректировка относительных путей
log_file_setting = CONFIG.get("logging", {}).get("log_file", "log/artix_etl.log")
if not os.path.isabs(log_file_setting):
    CONFIG["logging"]["log_file"] = str(PROJECT_ROOT / log_file_setting)

logger = setup_logger(CONFIG.get("logging", {}))
sql_loader = SQLLoader(PROJECT_ROOT / "sql")

# ==============================================================================
# 3. КЛАСС РАБОТЫ С ЛОКАЛЬНОЙ БД MYSQL (martin_etl)
# ==============================================================================
class LocalCacheMySQL:
    """Управление кэшем и промежуточными таблицами в MySQL (martin_etl)."""
    
    def __init__(self, db_config):
        self.config = db_config

    def get_connection(self):
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
            conn = self.get_connection()
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
            conn = self.get_connection()
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
            conn = self.get_connection()
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
            logger.info(f"Статус синхронизации '{entity_name}' записан: count={records_count}, status={status}")
        except Exception as e:
            logger.error(f"Ошибка фиксации статуса синхронизации: {e}", exc_info=True)
        finally:
            if conn and conn.is_connected():
                conn.close()

# ==============================================================================
# 4. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
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

        # Если есть прозрачность (RGBA/P), накладываем на белый фон и переводим в RGB
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            background.paste(img, mask=img.split()[3]) # Используем альфа-канал как маску
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")

        # Ресемплинг
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

def build_restricted_categories_set(group_rows, root_category_ids):
    parent_to_children = {}
    all_category_ids = set()

    for row in group_rows:
        code = str(row.groupcode)
        parent = str(row.parentgroupcode) if row.parentgroupcode else None
        all_category_ids.add(code)
        if parent:
            if parent not in parent_to_children:
                parent_to_children[parent] = []
            parent_to_children[parent].append(code)

    restricted_set = set()
    stack = [str(root_id) for root_id in root_category_ids if str(root_id) in all_category_ids]

    while stack:
        curr_id = stack.pop()
        restricted_set.add(curr_id)
        if curr_id in parent_to_children:
            stack.extend(parent_to_children[curr_id])

    logger.info(f"Определено {len(restricted_set)} подкатегорий 18+ из корневых: {root_category_ids}")
    return restricted_set

# ==============================================================================
# 5. ЭТАП 1: СИНХРОНИЗАЦИЯ С DIFFING (MS SQL -> MYSQL martin_etl)
# ==============================================================================
def sync_mssql_to_mysql(mssql_conn, mysql_cache):
    logger.info(">>> СТАРТ ЭТАПА 1: Синхронизация MS SQL -> MySQL (martin_etl) с фиксацией среза")
    mssql_cursor = mssql_conn.cursor()
    mysql_conn = mysql_cache.get_connection()
    mysql_cursor = mysql_conn.cursor()

    entities = CONFIG.get("entities", {})
    dept_code = CONFIG["exchange"]["dept_code"]

    try:
        # 1. Единицы измерения
        if entities.get("units", True):
            logger.info("Синхронизация units...")
            mysql_cursor.execute("UPDATE units SET is_present = 0;")
            sql_units = sql_loader.get_query("01_add_unit.sql")
            mssql_cursor.execute(sql_units)
            unit_rows = mssql_cursor.fetchall()
            
            sql_ins = """
            INSERT INTO units (unitcode, name, fractional, is_present) 
            VALUES (%s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE name=VALUES(name), fractional=VALUES(fractional), is_present=1;
            """
            mysql_cursor.executemany(sql_ins, [(r.unitcode, r.name, int(r.fractional)) for r in unit_rows])

        # 2. Группы товаров
        if entities.get("groups", True):
            logger.info("Синхронизация invent_groups...")
            mysql_cursor.execute("UPDATE invent_groups SET is_present = 0;")
            sql_groups = sql_loader.get_query("02_add_invent_group.sql")
            mssql_cursor.execute(sql_groups)
            group_rows = mssql_cursor.fetchall()

            root_cats = CONFIG.get("age_restrictions", {}).get("root_categories", [])
            restricted_cats = build_restricted_categories_set(group_rows, root_cats)

            sql_ins = """
            INSERT INTO invent_groups (group_code, group_name, parent_group_code, is_age_restricted, is_present) 
            VALUES (%s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE 
                group_name=VALUES(group_name), 
                parent_group_code=VALUES(parent_group_code),
                is_age_restricted=VALUES(is_age_restricted),
                is_present=1;
            """
            group_data = [
                (str(r.groupcode), r.groupname, str(r.parentgroupcode) if r.parentgroupcode else None, 1 if str(r.groupcode) in restricted_cats else 0) 
                for r in group_rows
            ]
            mysql_cursor.executemany(sql_ins, group_data)

        # 3. Товары (items)
        if entities.get("items", True):
            logger.info("Синхронизация items...")
            mysql_cursor.execute("UPDATE items SET is_present = 0;")
            sql_items = sql_loader.get_query("03_add_invent_item.sql")
            mssql_cursor.execute(sql_items, (dept_code,))
            item_rows = mssql_cursor.fetchall()

            sql_ins = """
            INSERT INTO items (inventcode, name, measurecode, inventgroup, effectivedate, is_present) 
            VALUES (%s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE 
                name=VALUES(name), 
                measurecode=VALUES(measurecode),
                inventgroup=VALUES(inventgroup),
                is_present=1;
            """
            item_data = [(str(r.inventcode), r.name, r.measurecode, str(r.inventgroup) if r.inventgroup else None, r.effectivedate) for r in item_rows]
            mysql_cursor.executemany(sql_ins, item_data)

        # 4. Штрих-коды (barcodes)
        if entities.get("barcodes", True):
            logger.info("Синхронизация barcodes...")
            mysql_cursor.execute("UPDATE barcodes SET is_present = 0;")
            sql_barcodes = sql_loader.get_query("04_add_barcode.sql")
            mssql_cursor.execute(sql_barcodes, (dept_code,))
            barcode_rows = mssql_cursor.fetchall()

            sql_ins = """
            INSERT INTO barcodes (barcode, inventcode, name, measure, tmctype, quantdefault, is_present) 
            VALUES (%s, %s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE 
                inventcode=VALUES(inventcode), 
                name=VALUES(name), 
                measure=VALUES(measure),
                tmctype=VALUES(tmctype),
                quantdefault=VALUES(quantdefault),
                is_present=1;
            """
            barcode_data = [(str(r.barcode), str(r.code), r.name, int(r.measure), int(r.tmctype), float(r.quantdefault)) for r in barcode_rows]
            mysql_cursor.executemany(sql_ins, barcode_data)

        # 5. Розничные цены (prices)
        if entities.get("prices", True):
            logger.info("Синхронизация prices...")
            mysql_cursor.execute("UPDATE prices SET is_present = 0;")
            sql_prices = sql_loader.get_query("05_add_price.sql")
            mssql_cursor.execute(sql_prices, (dept_code,))
            price_rows = mssql_cursor.fetchall()

            sql_ins = """
            INSERT INTO prices (barcode, price, minprice, pricetype, doctype, documentid, effectivedate, is_present) 
            VALUES (%s, %s, %s, 3, 1, %s, %s, 1)
            ON DUPLICATE KEY UPDATE 
                price=VALUES(price), 
                minprice=VALUES(minprice),
                documentid=VALUES(documentid),
                is_present=1;
            """
            price_data = [(str(r.barcode), r.price, r.price, str(dept_code), r.effectivedate) for r in price_rows]
            mysql_cursor.executemany(sql_ins, price_data)

        # 6. Уценка (additional_prices)
        if entities.get("additional_prices", True):
            logger.info("Синхронизация additional_prices...")
            mysql_cursor.execute("UPDATE additional_prices SET is_present = 0;")
            sql_add_prices = sql_loader.get_query("06_add_additional_price.sql")
            mssql_cursor.execute(sql_add_prices, (dept_code,))
            add_price_rows = mssql_cursor.fetchall()

            sql_ins = """
            INSERT INTO additional_prices (barcode, pricecode, additional_price, pricename, effectivedate, is_present) 
            VALUES (%s, 1, %s, 'Уценка', %s, 1)
            ON DUPLICATE KEY UPDATE 
                additional_price=VALUES(additional_price),
                is_present=1;
            """
            add_price_data = [(str(r.barcode), r.additional_price, r.effectivedate) for r in add_price_rows]
            mysql_cursor.executemany(sql_ins, add_price_data)

        # 7. Товары СЗТ (items_social)
        if entities.get("items_social", True):
            logger.info("Синхронизация items_social...")
            mysql_cursor.execute("UPDATE items_social SET is_present = 0;")
            sql_social = sql_loader.get_query("09_add_item_social.sql")
            mssql_cursor.execute(sql_social)
            social_rows = mssql_cursor.fetchall()

            sql_ins = """
            INSERT INTO items_social (inventcode, is_present) 
            VALUES (%s, 1)
            ON DUPLICATE KEY UPDATE is_present=1;
            """
            social_data = [(str(r.inventcode),) for r in social_rows]
            mysql_cursor.executemany(sql_ins, social_data)

        # 8. Пиклист (picklist)
        if entities.get("picklist", True):
            logger.info("Синхронизация picklist...")
            mysql_cursor.execute("UPDATE picklist SET is_present = 0;")
            stats = {"cache_hits": 0, "compressed_new": 0}

            sql_pick_cats = sql_loader.get_query("07_picklist_categories.sql")
            mssql_cursor.execute(sql_pick_cats)
            pick_cats = mssql_cursor.fetchall()

            pick_data = []
            for r in pick_cats:
                compress_image_to_base64(r.image_bytes, mysql_cache, r.binary_id, stats)
                pick_data.append((str(r.code), r.name, r.binary_id, None, None, None, 1))

            # Параметризованные даты для товаров пиклиста
            now = datetime.now()
            days_back = CONFIG.get("picklist", {}).get("days_back", 30)
            start_date = now - timedelta(days=days_back)
            end_date = now

            sql_pick_items = sql_loader.get_query("08_picklist_items.sql")
            mssql_cursor.execute(sql_pick_items, (start_date, end_date))
            pick_items = mssql_cursor.fetchall()

            for r in pick_items:
                compress_image_to_base64(r.image_bytes, mysql_cache, r.binary_id, stats)
                pick_data.append((
                    str(r.raw_code).lstrip('0'), 
                    r.name, 
                    r.binary_id, 
                    str(r.parent) if r.parent else None,
                    str(r.tmccode).lstrip('0'),
                    str(r.barcode) if r.barcode else None,
                    0
                ))

            sql_ins = """
            INSERT INTO picklist (code, name, binary_id, parent, tmccode, barcode, is_category, is_present) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE 
                name=VALUES(name), 
                binary_id=VALUES(binary_id),
                parent=VALUES(parent),
                tmccode=VALUES(tmccode),
                barcode=VALUES(barcode),
                is_present=1;
            """
            mysql_cursor.executemany(sql_ins, pick_data)
            logger.info(f"Пиклист обработан. Кэш: hits={stats['cache_hits']}, new_compressed={stats['compressed_new']}")

    finally:
        mssql_cursor.close()
        mysql_cursor.close()
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()

    logger.info(">>> ЭТАП 1 ЗАВЕРШЕН: Срез данных зафиксирован в martin_etl.")

# ==============================================================================
# 6. ЭТАП 2: ВЫГРУЗКА ИЗ MYSQL И АВТО-ОЧИСТКА (martin_etl -> pos.aif)
# ==============================================================================
def generate_aif_from_local_db(mysql_cache):
    mode = CONFIG.get("exchange", {}).get("mode", "full").lower()
    entities = CONFIG.get("entities", {})
    logger.info(f">>> СТАРТ ЭТАПА 2: Формирование pos.aif в режиме [{mode.upper()}]")
    
    conn = mysql_cache.get_connection()
    cursor = conn.cursor(dictionary=True)
    commands = []

    try:
        # A. В режиме FULL первым делом пишем команды очистки clear*
        if mode == "full":
            if entities.get("units", True): commands.append({"command": "clearUnit"})
            if entities.get("groups", True): commands.append({"command": "clearInventGroup"})
            if entities.get("items", True): commands.append({"command": "clearInventory"})
            if entities.get("barcodes", True): commands.append({"command": "clearBarcode"})
            if entities.get("prices", True): commands.append({"command": "clearPrice"})
            if entities.get("additional_prices", True): commands.append({"command": "clearAdditionalPrice"})
            if entities.get("picklist", True): commands.append({"command": "clearPicklist"})

        # B. В режиме INCREMENTAL вычисляем удаленные записи (is_present = 0) и шлем delete*
        elif mode == "incremental":
            if entities.get("items", True):
                cursor.execute("SELECT inventcode FROM items WHERE is_present = 0;")
                for r in cursor.fetchall():
                    commands.append({"command": "deleteInventItem", "inventcode": r["inventcode"]})

            if entities.get("barcodes", True):
                cursor.execute("SELECT barcode FROM barcodes WHERE is_present = 0;")
                for r in cursor.fetchall():
                    commands.append({"command": "deleteBarcode", "barcode": r["barcode"]})

            if entities.get("prices", True):
                cursor.execute("SELECT barcode, documentid FROM prices WHERE is_present = 0;")
                for r in cursor.fetchall():
                    commands.append({"command": "deletePrice", "price": {"barcode": r["barcode"], "documentid": r["documentid"], "doctype": 1}})

            if entities.get("additional_prices", True):
                cursor.execute("SELECT barcode, pricecode FROM additional_prices WHERE is_present = 0;")
                for r in cursor.fetchall():
                    commands.append({"command": "deleteAdditionalPrice", "additionalprice": {"barcode": r["barcode"], "pricecode": r["pricecode"]}})

            if entities.get("picklist", True):
                cursor.execute("SELECT code FROM picklist WHERE is_present = 0;")
                for r in cursor.fetchall():
                    commands.append({"command": "deletePicklist", "code": r["code"]})

        # C. Добавляем команды add* для актуальных записей (is_present = 1)
        if entities.get("units", True):
            cursor.execute("SELECT unitcode, name, fractional FROM units WHERE is_present = 1;")
            for r in cursor.fetchall():
                commands.append({"command": "addUnit", "unit": {"unitcode": r["unitcode"], "name": r["name"], "fractional": bool(r["fractional"])}})

        if entities.get("groups", True):
            cursor.execute("SELECT group_code, group_name, parent_group_code FROM invent_groups WHERE is_present = 1;")
            for r in cursor.fetchall():
                commands.append({"command": "addInventGroup", "inventGroup": {"groupCode": r["group_code"], "groupname": r["group_name"], "parentGroupCode": r["parent_group_code"]}})

        if entities.get("items", True):
            sql_items_aif = """
            SELECT 
                i.inventcode, i.name, i.measurecode, i.inventgroup,
                CASE WHEN u.unitcode = 1 THEN 0 ELSE 1 END AS requirequantityscales,
                CASE WHEN g.is_age_restricted = 1 THEN 18 ELSE 0 END AS age,
                CASE WHEN g.is_age_restricted = 1 THEN 32 ELSE 0 END AS opmode,
                CASE WHEN g.is_age_restricted = 1 THEN 1 ELSE 0 END AS ageverify
            FROM items i
            LEFT JOIN units u ON i.measurecode = u.unitcode
            LEFT JOIN invent_groups g ON i.inventgroup = g.group_code
            WHERE i.is_present = 1;
            """
            cursor.execute(sql_items_aif)
            for r in cursor.fetchall():
                commands.append({
                    "command": "addInventItem",
                    "invent": {
                        "inventcode": r["inventcode"],
                        "measurecode": str(r["measurecode"]),
                        "isInventItem": True,
                        "name": r["name"],
                        "inventgroup": r["inventgroup"],
                        "options": {
                            "quantityoptions": {"requirequantityscales": bool(r["requirequantityscales"])},
                            "inventitemoptions": {"ageverify": r["ageverify"]}
                        },
                        "opmode": r["opmode"],
                        "age": r["age"]
                    }
                })

        if entities.get("barcodes", True):
            cursor.execute("SELECT inventcode, barcode, name, measure, tmctype, quantdefault FROM barcodes WHERE is_present = 1;")
            for r in cursor.fetchall():
                commands.append({"command": "addBarcode", "barcode": {"code": r["inventcode"], "barcode": r["barcode"], "name": r["name"], "measure": r["measure"], "tmctype": r["tmctype"], "quantdefault": float(r["quantdefault"])}})

        if entities.get("prices", True):
            cursor.execute("SELECT barcode, price, minprice, pricetype, doctype, documentid FROM prices WHERE is_present = 1;")
            for r in cursor.fetchall():
                commands.append({"command": "addPrice", "price": {"barcode": r["barcode"], "price": str(r["price"]), "pricetype": r["pricetype"], "doctype": r["doctype"], "documentid": r["documentid"]}})

        if entities.get("additional_prices", True):
            cursor.execute("SELECT barcode, pricecode, additional_price, pricename FROM additional_prices WHERE is_present = 1;")
            for r in cursor.fetchall():
                commands.append({"command": "addAdditionalPrice", "additionalPrice": {"barcode": r["barcode"], "price": str(r["additional_price"]), "pricecode": r["pricecode"], "pricename": r["pricename"]}})

        if entities.get("picklist", True):
            sql_picklist_aif = """
            SELECT 
                p.code, p.name, p.parent, p.tmccode, p.barcode, p.is_category,
                c.base64_data AS image
            FROM picklist p
            LEFT JOIN image_cache c ON p.binary_id = c.binary_id
            WHERE p.is_present = 1;
            """
            cursor.execute(sql_picklist_aif)
            for r in cursor.fetchall():
                commands.append({
                    "command": "addPicklist",
                    "picklist": {
                        "code": r["code"],
                        "name": r["name"],
                        "image": r["image"] if r["image"] else "",
                        "parent": r["parent"],
                        "tmccode": r["tmccode"],
                        "barcode": r["barcode"],
                        "is_category": bool(r["is_category"])
                    }
                })

        # D. Физическая очистка устаревших записей (is_present = 0) из MySQL
        logger.info("Очистка устаревших строк (is_present = 0) из локальной MySQL...")
        tables_to_cleanup = ["units", "invent_groups", "items", "barcodes", "prices", "additional_prices", "picklist", "items_social"]
        for tbl in tables_to_cleanup:
            cursor.execute(f"DELETE FROM {tbl} WHERE is_present = 0;")

    finally:
        cursor.close()
        if conn and conn.is_connected():
            conn.close()

    logger.info(f">>> ЭТАП 2 ЗАВЕРШЕН: Сформировано {len(commands)} команд для AIF-пакета.")
    return commands

# ==============================================================================
# 7. ПУБЛИКАЦИЯ ПАКЕТА И БЭКАП
# ==============================================================================
def publish_aif_package(commands_list):
    target_dir = CONFIG["exchange"]["target_dir"]
    os.makedirs(target_dir, exist_ok=True)

    tmp_filepath = os.path.join(target_dir, f"{CONFIG['exchange']['aif_filename']}.tmp")
    aif_filepath = os.path.join(target_dir, CONFIG['exchange']['aif_filename'])
    flz_filepath = os.path.join(target_dir, CONFIG['exchange']['flag_filename'])

    logger.info(f"Запись пакета AIF ({len(commands_list)} команд) во временный файл: {tmp_filepath}")
    with open(tmp_filepath, "w", encoding="utf-8") as f:
        for cmd in commands_list:
            f.write(json.dumps(cmd, ensure_ascii=False) + "\n" + "---" "\n")

    os.replace(tmp_filepath, aif_filepath)
    logger.info(f"Файл AIF переименован и опубликован: {aif_filepath}")

    # Бэкапирование с ротацией
    backup_conf = CONFIG.get("backup", {})
    if backup_conf.get("enabled", True) and not os.path.isabs(backup_conf.get("backup_dir", "")):
        backup_conf["backup_dir"] = str(PROJECT_ROOT / backup_conf.get("backup_dir", "backup"))

    backup_and_rotate_aif(aif_filepath, backup_conf)

    # Создание файла-флага
    with open(flz_filepath, "w", encoding="utf-8") as f:
        pass
    logger.info(f"Файл-флаг активации импорта создан: {flz_filepath}")

# ==============================================================================
# 8. ОСНОВНАЯ ТОЧКА ВХОДА
# ==============================================================================
def main():
    start_time = time.time()
    logger.info("=== Запуск ETL-сервиса обмена Artix SCO ===")
    cache_mgr = LocalCacheMySQL(CONFIG["mysql"])
    
    try:
        # Этап 1: Синхронизация данных из MS SQL в локальную MySQL с фиксацией среза
        mssql_conn = get_mssql_connection()
        sync_mssql_to_mysql(mssql_conn, cache_mgr)
        mssql_conn.close()

        # Этап 2: Генерация AIF-пакета (clear*/delete*/add*) из MySQL и авто-очистка
        aif_commands = generate_aif_from_local_db(cache_mgr)

        if aif_commands:
            publish_aif_package(aif_commands)
            cache_mgr.update_sync_state("full_dictionary_sync", len(aif_commands), "SUCCESS")
            elapsed = time.time() - start_time
            logger.info(f"=== Процесс успешно завершен за {elapsed:.2f} сек. Всего выгружено команд: {len(aif_commands)} ===")
        else:
            logger.warning("Команды для выгрузки не сформированы.")

    except Exception as e:
        elapsed = time.time() - start_time
        cache_mgr.update_sync_state("full_dictionary_sync", 0, "ERROR")
        logger.critical(f"Критический сбой процесса ETL спустя {elapsed:.2f} сек: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()