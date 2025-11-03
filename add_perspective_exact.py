"""add_perspective_exact.py

Добавляет ТОЛЬКО акции по точному совпадению тикера: для каждого тикера выполняется
поиск, выбирается инструмент типа share с тем же ticker (регистронезависимо), берётся UID.

Далее выполняется минимальная вставка (ticker, name, uid, secid=ticker) — остальные
поля будут дополнены вызовом обогащения.

После вставок вызывается enrich_all_perspective чтобы заполнить figi, isin, assetUid и т.д.

Использование:
  python add_perspective_exact.py               # полный список
  python add_perspective_exact.py --limit 10    # первые 10
  python add_perspective_exact.py --dry-run     # только показать план, без вставок / обогащения

"""
from __future__ import annotations
import argparse
import sys
from typing import List, Dict, Set

from src.invest_core.instruments import get_uid_instrument, enrich_all_perspective
from src.invest_core import db as db_layer

RAW_TICKERS = [
    "AFKS","SVCB","SNGS","POSI","RTKMP","T","GEMC","DATA","BELU","LEAS","RENI","RAGR","SFIN","ASTR","IRAO","LKOH","SNGSP","MBNK","TRMK","X5","FESH","DIAS","MTLR","WUSH","LENT","OZPH","TATN","YDEX","MSNG","UGLD","ALRS","CHMF","PHOR","PLZL","GMKN","SBER","IVAT","RTKM","ROSN","RUAL","SBERP","AFLT","NVTK","GAZP","MOEX","MTSS","LKOH","HEAD","PLZL","SMLT","SOFL","VSEH"
]

CORRECTIONS = {"YDEX":"YNDX"}


def normalize(tickers: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in tickers:
        t1 = CORRECTIONS.get(t.strip().upper(), t.strip().upper())
        if t1 not in seen:
            seen.add(t1)
            out.append(t1)
    return out


def ensure_minimal_insert(uid: str, ticker: str) -> str:
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid FROM perspective_shares WHERE uid = ?", (uid,))
        row = cur.fetchone()
        if row:
            return "exists"
        # Вставка минимального набора.
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
        uid = get_uid_instrument(t)
        if not uid:
            stats["not_found"] += 1
            rows.append({"ticker": t, "uid": "", "status": "not-found"})
            continue
        if dry_run:
            rows.append({"ticker": t, "uid": uid, "status": "ready"})
            continue
        st = ensure_minimal_insert(uid, t)
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
        # Обогащение атрибутов после вставки
        enrich_stats = enrich_all_perspective()
        print("\nОбогащение:")
        for k,v in enrich_stats.items():
            print(f"  {k}: {v}")


def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Insert perspective shares by exact ticker -> UID lookup")
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args(argv)
    process(limit=args.limit, dry_run=args.dry_run)

if __name__ == '__main__':
    main(sys.argv[1:])
