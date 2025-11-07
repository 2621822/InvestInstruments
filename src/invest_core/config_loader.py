"""config_loader.py

Утилита загрузки конфигурации из config.ini.
Приоритет источников для настроек:
  1. Переменные окружения (если заданы явно)
  2. Файл config.ini (секция -> ключ)
  3. Жёстко прошитые значения по умолчанию

Использование:
    from invest_core.config_loader import get_config, cfg_val
    cfg = get_config()
    host = cfg_val('database', 'host', 'localhost')

Файл ищется по переменной окружения INVEST_CONFIG_FILE или имени 'config.ini' в корне.
"""
from __future__ import annotations
import os
import configparser
from functools import lru_cache

_DEFAULT_PATH = os.getenv('INVEST_CONFIG_FILE', 'config.ini')

@lru_cache(maxsize=1)
def get_config() -> configparser.ConfigParser:
    cp = configparser.ConfigParser()
    if os.path.exists(_DEFAULT_PATH):
        cp.read(_DEFAULT_PATH, encoding='utf-8')
    return cp

def cfg_val(section: str, key: str, default: str | int | None = None):
    """Получить значение из секции/ключа с fallback на default.
    Приведение типов делается просто: если default int -> пытаемся int().
    """
    cfg = get_config()
    if not cfg or section not in cfg or key not in cfg[section]:
        return default
    raw = cfg[section][key]
    if isinstance(default, int):
        try:
            return int(raw)
        except ValueError:
            return default
    return raw

__all__ = ["get_config", "cfg_val"]
