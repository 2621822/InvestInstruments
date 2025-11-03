"""revert_perspective_insert.py

Удаляет все записи из perspective_shares кроме заданного списка сохранения.
По умолчанию сохраняет только тикеры из KEEP_TICKERS.

Запуск:
  python revert_perspective_insert.py          # превью
  python revert_perspective_insert.py --apply  # применить
  python revert_perspective_insert.py --keep TTLK,YDEX,SBER  # расширить список сохраняемых
  python revert_perspective_insert.py --apply --yes          # без подтверждения
"""
from __future__ import annotations
import argparse
import sys
from typing import Set, List
from src.invest_core import db as db_layer

DEFAULT_KEEP = ["TTLK", "YDEX"]


def get_all() -> List[tuple[str,str,str]]:
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT ticker, uid, name FROM perspective_shares")
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def parse_keep(arg: str | None) -> Set[str]:
    if not arg:
        return set(DEFAULT_KEEP)
    out: Set[str] = set()
    for t in arg.split(','):
        t = t.strip().upper()
        if t:
            out.add(t)
    return out


def preview(keep: Set[str]):
    rows = get_all()
    del_rows = [r for r in rows if r[0] not in keep]
    print(f"Всего строк: {len(rows)}")
    print(f"Сохраняем: {', '.join(sorted(keep)) or '(пусто)'}")
    print(f"Будет удалено: {len(del_rows)}")
    print("ticker | uid | name")
    for t,u,n in sorted(del_rows):
        print(f"{t:<6} | {u} | {n}")
    return del_rows


def apply_delete(del_rows: List[tuple[str,str,str]]):
    if not del_rows:
        print("Нет строк для удаления.")
        return 0
    tickers = {r[0] for r in del_rows}
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        q_marks = ','.join('?' for _ in tickers)
        sql = f"DELETE FROM perspective_shares WHERE ticker IN ({q_marks})"
        cur = conn.execute(sql, tuple(tickers))
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else None
        if db_layer.BACKEND == 'sqlite':
            conn.commit()
    print("Удалено записей:", deleted)
    return deleted or 0


def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Revert perspective_shares bulk insert by deleting all except keep list")
    ap.add_argument('--keep', help='Список тикеров для сохранения через запятую (по умолчанию TTLK,YDEX)')
    ap.add_argument('--apply', action='store_true', help='Применить удаление')
    ap.add_argument('--yes', action='store_true', help='Подтвердить без интерактивного вопроса')
    args = ap.parse_args(argv)
    keep = parse_keep(args.keep)
    del_rows = preview(keep)
    if not args.apply:
        print('\nРежим превью. Чтобы удалить используйте --apply')
        return
    if not args.yes:
        ans = input('Подтвердите удаление (y/N): ').strip().lower()
        if ans != 'y':
            print('Отмена удаления.')
            return
    apply_delete(del_rows)

if __name__ == '__main__':
    main(sys.argv[1:])
