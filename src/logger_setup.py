#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(log_config):
    """
    Настройка логирования с выгрузкой в консоль и файл с ротацией по размеру.
    """
    log_file = log_config.get("log_file", "/var/log/artix_etl.log")
    log_level_str = log_config.get("level", "INFO").upper()
    max_bytes = log_config.get("max_bytes", 10 * 1024 * 1024)  # 10 MB по умолчанию
    backup_count = log_config.get("backup_count", 5)

    log_level = getattr(logging, log_level_str, logging.INFO)

    log_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    logger = logging.getLogger("ArtixETL")
    logger.setLevel(log_level)
    logger.handlers.clear()  # Очистка хэндлеров от предыдущих запусков

    # 1. Файловый хэндлер с автоматической ротацией
    try:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)
    except Exception as e:
        sys.stderr.write(f"Ошибка настройки файлового логирования ({log_file}): {e}\n")

    # 2. Консольный хэндлер
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    return logger