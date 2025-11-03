"""remove_perspective_batch.py

Скрипт отката (удаления) ранее пакетно добавленных бумаг из таблицы perspective_shares.

По умолчанию выполняет только PREVIEW: показывает какие строки (uid, ticker, name) будут удалены.
Чтобы применить удаления нужно указать флаг --apply.

Можно ограничить множество тикеров через --tickers (через запятую) или --file с
перечнем тикеров (один в строке). Если ничего не задано – используется встроенный список
(совпадающий с add_perspective_batch.py, после нормализаций/коррекций).

Пример:
  python remove_perspective_batch.py                # превью всего
  python remove_perspective_batch.py --apply        # удалить все из списка
  python remove_perspective_batch.py --tickers AFKS,SBER # превью только AFKS и SBER
  python remove_perspective_batch.py --tickers AFKS,SBER --apply
"""
from __future__ import annotations
import argparse
import sys
from typing import List, Set
from src.invest_core import db as db_layer

# Базовый список тикеров (после коррекции YDEX->YNDX и исключения ошибочной строки WUSH|Лента)
BASE_TICKERS = [
    "AFKS","IVAT","BELU","GEMC","WUSH","ALRS","AFLT","VSEH","GAZP","SMLT","DATA","ASTR","POSI","FESH","DIAS","LEAS","IRAO","X5","LENT","LKOH","MTLR","MOEX","MSNG","MTSS","MBNK","NVTK","GMKN","OZPH","PLZL","RENI","ROSN","RTKM","RTKMP","RAGR","RUAL","SBER","SBERP","CHMF","SVCB","SOFL","SNGS","SNGSP","TATN","TRMK","T","PHOR","HEAD","SFIN","UGLD","YNDX"
]

# -- вспомогательные функции -------------------------------------------------

def parse_tickers(arg: str | None, file: str | None) -> Set[str]:
    ticks: Set[str] = set()
    if arg:
        for t in arg.split(','):
            t = t.strip().upper()
            if t:
                ticks.add(t)
    if file:
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                t = line.strip().upper()
                if t:
                    ticks.add(t)
    if not ticks:
        ticks = set(BASE_TICKERS)
    return ticks


def preview(tickers: Set[str]):
    db_layer.init_schema()
    rows = []
    with db_layer.get_connection() as conn:
        q_marks = ','.join('?' for _ in tickers)
        sql = f"SELECT uid, ticker, name FROM perspective_shares WHERE ticker IN ({q_marks}) ORDER BY ticker"
        cur = conn.execute(sql, tuple(tickers))
        rows = cur.fetchall()
    print("Найдено к удалению строк: ", len(rows))
    print("ticker | uid | name")
    for uid, ticker, name in rows:
        print(f"{ticker:<6} | {uid} | {name}")
    return rows


def apply_delete(tickers: Set[str]):
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        q_marks = ','.join('?' for _ in tickers)
        sql = f"DELETE FROM perspective_shares WHERE ticker IN ({q_marks})"
        cur = conn.execute(sql, tuple(tickers))
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else None
        if db_layer.BACKEND == 'sqlite':
            conn.commit()
    print("Удалено записей:", deleted)
    return deleted

# -- main --------------------------------------------------------------------

def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Rollback batch-added perspective shares")
    ap.add_argument('--tickers', help='Список тикеров через запятую (если не задан – базовый список)')
    ap.add_argument('--file', help='Файл со списком тикеров (один в строке)')
    ap.add_argument('--apply', action='store_true', help='Выполнить удаление (без флага только превью)')
    ap.add_argument('--yes', action='store_true', help='Подтвердить удаление без интерактивного запроса (для автоматизации)')
    args = ap.parse_args(argv)
    tickers = parse_tickers(args.tickers, args.file)
    rows = preview(tickers)
    if not args.apply:
        print('\nРежим превью. Для удаления добавьте --apply')
        return
    # apply mode
    if not rows:
        print('Нет строк для удаления.')
        return
    if args.yes:
        apply_delete(tickers)
        return
    confirm = input('Подтвердите удаление (y/N): ').strip().lower()
    if confirm == 'y':
        apply_delete(tickers)
    else:
        print('Отмена удаления (ответ не y)')

if __name__ == '__main__':
    main(sys.argv[1:])
