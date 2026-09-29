#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import shutil
import logging
from datetime import datetime

logger = logging.getLogger("ArtixETL")

def backup_and_rotate_aif(aif_filepath, backup_config):
    """
    Создание резервной копии AIF-файла и ротация старых бэкапов.
    """
    if not backup_config.get("enabled", True):
        return

    backup_dir = backup_config.get("backup_dir", "/obmen/dict/410/backup")
    retention_days = backup_config.get("retention_days", 30)

    try:
        os.makedirs(backup_dir, exist_ok=True)

        # 1. Создание бэкапа с меткой времени
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"pos_{timestamp_str}.aif"
        backup_path = os.path.join(backup_dir, backup_filename)

        shutil.copy2(aif_filepath, backup_path)
        logger.info(f"Создана резервная копия AIF-файла: {backup_path}")

        # 2. Очистка старых бэкапов
        rotate_backups(backup_dir, retention_days)

    except Exception as e:
        logger.error(f"Ошибка при создании резервной копии AIF: {e}", exc_info=True)

def rotate_backups(backup_dir, retention_days):
    """
    Удаление бэкап-файлов старше retention_days дней.
    """
    if retention_days <= 0:
        return

    now = time.time()
    cutoff_time = now - (retention_days * 86400)
    removed_count = 0

    for filename in os.listdir(backup_dir):
        filepath = os.path.join(backup_dir, filename)
        if os.path.isfile(filepath) and filename.startswith("pos_") and filename.endswith(".aif"):
            file_mtime = os.path.getmtime(filepath)
            if file_mtime < cutoff_time:
                try:
                    os.remove(filepath)
                    removed_count += 1
                    logger.debug(f"Удален устаревший бэкап: {filename}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить устаревший бэкап {filename}: {e}")

    if removed_count > 0:
        logger.info(f"Ротация бэкапов завершена: удалено {removed_count} файлов старше {retention_days} дней.")