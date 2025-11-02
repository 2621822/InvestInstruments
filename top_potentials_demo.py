"""Простой демонстрационный скрипт вывода топ-10 потенциалов.

Запуск:
  python top_potentials_demo.py

Предполагает, что ранее выполнены загрузка истории, консенсусов и расчёт потенциалов.
"""
from __future__ import annotations
import json
import sys
sys.path.append('d:/#Work/#Invest/Project/src')  # добавляем путь к пакету
from invest_core import db  # noqa: E402
from invest_core.potentials import GetTopSharePotentials  # noqa: E402


def main():
    db.init_schema()
    res = GetTopSharePotentials(limit=10)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()