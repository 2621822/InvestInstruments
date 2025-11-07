"""Утилиты нормализации значений и дат (общие для forecasts/potentials).

Функции:
  to_number(val)          -> float | None   (поддержка dict с units/nano, строк с запятой)
  normalize_date(value)   -> YYYY-MM-DD строка или исходное значение
  float_equal(a,b,eps=1e-9) -> bool          (сравнение с абсолютной погрешностью)
"""
from __future__ import annotations
from typing import Any
import datetime as _dt

__all__ = ["to_number", "normalize_date", "float_equal"]

def to_number(val: Any) -> float | None:
    """Привести произвольное значение к float.

    Поддерживаемые варианты:
      - int/float
      - dict с ключами units/nano (MoneyValue) -> units + nano/1e9
      - dict с любыми числовыми значениями (берём первое подходящее)
      - строка с точкой или запятой
      - объекты с атрибутами units/nano
    Возвращает None если преобразование невозможно.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        if "units" in val and "nano" in val:
            try:
                return int(val.get("units") or 0) + int(val.get("nano") or 0) / 1_000_000_000
            except Exception:
                return None
        for v in val.values():
            if isinstance(v, (int, float)):
                return float(v)
        return None
    if hasattr(val, "units") and hasattr(val, "nano"):
        try:
            return int(getattr(val, "units") or 0) + int(getattr(val, "nano") or 0) / 1_000_000_000
        except Exception:
            return None
    if isinstance(val, str):
        s = val.strip().replace(',', '.')
        try:
            return float(s)
        except Exception:
            return None
    return None


def normalize_date(value: str) -> str:
    """Обрезать строку даты до формата YYYY-MM-DD если возможно.

    Если строка >= 10 символов и первые 10 укладываются в ISO дату – возвращаем обрезанный вариант.
    Иначе возвращаем исходный trimmed.
    """
    if not value:
        return value
    s = str(value).strip()
    if len(s) >= 10:
        cand = s[:10]
        try:
            _dt.datetime.strptime(cand, "%Y-%m-%d")
            return cand
        except Exception:
            pass
    return s


def float_equal(a: float | None, b: float | None, eps: float = 1e-9) -> bool:
    """Сравнить два float с абсолютной погрешностью eps. None всегда не равны ничему кроме взаимного None.
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) < eps
