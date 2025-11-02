#!/usr/bin/env python
"""CLI: вывести избранные акции из профиля Tinkoff Invest.

Использование:
  python list_favorites.py [--add]

Опции:
  --add  Добавить отсутствующие избранные акции в perspective_shares

Требуется токен: INVEST_TOKEN / INVEST_TINKOFF_TOKEN / tinkoff_token.txt
"""
from __future__ import annotations
import argparse
import json
import sys
sys.path.append('d:/#Work/#Invest/Project/src')
from invest_core.favorites import list_favorite_shares, ensure_perspective_from_favorites  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description='Вывод избранных акций из Tinkoff профиля')
    parser.add_argument('--add', action='store_true', help='Добавить отсутствующие в perspective_shares')
    args = parser.parse_args()
    shares = list_favorite_shares()
    if not shares:
        print('Нет избранных акций или отсутствует доступ (проверьте токен).')
        return
    # Табличный вывод
    print(f"Всего избранных акций: {len(shares)}")
    print("ticker\tuid\tfigi\tname")
    for s in shares:
        print(f"{s.get('ticker')}\t{s.get('uid')}\t{s.get('figi')}\t{s.get('name')}")
    if args.add:
        stats = ensure_perspective_from_favorites()
        print('\nДобавление в perspective_shares:')
        print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
