"""add_perspective_rest_exact.py

Вставка перспективных акций, используя ТОЛЬКО REST функцию GetUidInstrument
для точного поиска UID по тикеру (share + classCode=TQBR).

Шаги:
  1. Нормализация списка тикеров (коррекция YDEX->YNDX, удаление дублей).
  2. Для каждого тикера вызываем GetUidInstrument.
  3. Если UID найден – вставляем минимальную запись (ticker, name=ticker, uid, secid=ticker).
  4. После всех вставок запускаем обогащение enrich_all_perspective.

Dry-run: только отображение плана (UIDы), без вставок.

"""
from __future__ import annotations
import argparse
import sys
from typing import List, Set, Dict

from src.invest_core.rest_instruments import GetUidInstrument
from src.invest_core.instruments import fill_all_perspective_shares
from src.invest_core import db as db_layer

RAW_TICKERS = [
    "AFKS","AFLT","ALRS","ASTR","BELU","CHMF","DATA","DIAS","FESH","GAZP","GEMC","GMKN","HEAD","IRAO","IVAT","LEAS","LENT","LKOH","LKOH","MBNK","MOEX","MSNG","MTLR","MTSS","NVTK","OZPH","PHOR","PLZL","PLZL","POSI","RAGR","RENI","ROSN","RTKM","RTKMP","RUAL","SBER","SBERP","SFIN","SMLT","SNGS","SNGSP","SOFL","SVCB","T","TATN","TRMK","UGLD","VSEH","WUSH","X5","YDEX"
]
CORRECTIONS: Dict[str,str] = {}  # Больше не преобразуем YDEX -> YNDX

def normalize(tickers: List[str]) -> List[str]:
    # Теперь не удаляем дубликаты намеренно (например двойные LKOH, PLZL) и не делаем коррекции.
    out: List[str] = []
    for t in tickers:
        out.append(t.strip().upper())
    return out

def minimal_insert(uid: str, ticker: str) -> str:
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid FROM perspective_shares WHERE uid = ?", (uid,))
        if cur.fetchone():
            return "exists"
        sql = ("INSERT INTO perspective_shares(ticker, name, uid, secid, isin, figi, classCode, instrumentType, assetUid) "
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        conn.execute(sql, (ticker, ticker, uid, ticker, None, None, None, None, None))
        if db_layer.BACKEND == 'sqlite':
            conn.commit()
        return "inserted"

def process(limit: int | None, dry_run: bool):
    ticks = normalize(RAW_TICKERS)
    if limit is not None:
        ticks = ticks[:limit]
    stats = {"inserted":0, "exists":0, "not_found":0, "errors":0}
    rows: List[Dict[str,str]] = []
    for t in ticks:
        uid = GetUidInstrument(t)
        if not uid:
            stats["not_found"] += 1
            rows.append({"ticker": t, "uid": "", "status": "not-found"})
            continue
        if dry_run:
            rows.append({"ticker": t, "uid": uid, "status": "ready"})
            continue
        st = minimal_insert(uid, t)
        if st == "inserted": stats["inserted"] += 1
        elif st == "exists": stats["exists"] += 1
        else: stats["errors"] += 1
        rows.append({"ticker": t, "uid": uid, "status": st})

    print("ticker | status    | uid")
    print("-------|-----------|----")
    for r in rows:
        print(f"{r['ticker']:<6} | {r['status']:<9} | {r['uid']}")
    if CORRECTIONS:
        print("\nКоррекции:")
        for s,d in CORRECTIONS.items():
            print(f"  {s} -> {d}")
    print("\nСводка:")
    for k,v in stats.items():
        print(f"  {k}: {v}")
    if dry_run:
        ready = sum(1 for r in rows if r['status'] == 'ready')
        print(f"  (dry-run) ready-to-insert: {ready}")
    else:
        enrich = fill_all_perspective_shares()
        print("\nЗаполнение атрибутов (fill_all_perspective_shares):")
        for k,v in enrich.items():
            print(f"  {k}: {v}")

def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Insert perspective shares via REST exact ticker -> UID")
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args(argv)
    process(limit=args.limit, dry_run=args.dry_run)

if __name__ == '__main__':
    main(sys.argv[1:])
