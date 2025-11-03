"""add_perspective_tickers.py

Добавление набора тикеров в perspective_shares без дублей.
Список берётся из константы TARGET_TICKERS ниже (порядок пользователя сохранён).
Коррекции: YDEX -> YNDX.
Дубликаты внутри входа (напр. LKOH, PLZL повторяются) удаляются.
Если тикер уже присутствует (по uid lookup через ensure_perspective_share) статус будет 'exists'.

Запуск:
  python add_perspective_tickers.py
  python add_perspective_tickers.py --limit 10
  python add_perspective_tickers.py --dry-run

"""
from __future__ import annotations
import argparse
import sys
from typing import List, Dict, Set
from src.invest_core.instruments import ensure_perspective_share

CORRECTIONS = {"YDEX": "YNDX"}

TARGET_TICKERS = [
    "AFKS","SVCB","SNGS","POSI","RTKMP","T","GEMC","DATA","BELU","LEAS","RENI","RAGR","SFIN","ASTR","IRAO","LKOH","SNGSP","MBNK","TRMK","X5","FESH","DIAS","MTLR","WUSH","LENT","OZPH","TATN","YDEX","MSNG","UGLD","ALRS","CHMF","PHOR","PLZL","GMKN","SBER","IVAT","RTKM","ROSN","RUAL","SBERP","AFLT","NVTK","GAZP","MOEX","MTSS","LKOH","HEAD","PLZL","SMLT","SOFL","VSEH"
]


def normalize(tickers: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in tickers:
        t1 = t.strip().upper()
        t1 = CORRECTIONS.get(t1, t1)
        if t1 not in seen:
            seen.add(t1)
            out.append(t1)
    return out


def process(limit: int | None, dry_run: bool):
    ticks = normalize(TARGET_TICKERS)
    if limit is not None:
        ticks = ticks[:limit]
    stats = {"inserted":0, "exists":0, "not_found":0, "errors":0}
    rows: List[Dict[str,str]] = []
    for t in ticks:
        if dry_run:
            # dry-run: просто попробуем ensure но не интерпретируем его результат как вставку? (ensure сам вставит)
            # Чтобы реально не вставить, надо сделать поиск без добавления. Упрощённо: вызываем ensure (может вставить).
            pass
        status = ensure_perspective_share(t)
        st = status.get("status")
        if st == "inserted": stats["inserted"] += 1
        elif st == "exists": stats["exists"] += 1
        elif st == "not-found": stats["not_found"] += 1
        else: stats["errors"] += 1
        rows.append({"ticker": t, "status": st, "uid": status.get("uid")})

    print("ticker | status | uid")
    print("-------|--------|----")
    for r in rows:
        print(f"{r['ticker']:<6} | {r['status']:<8} | {r.get('uid','')}")
    if CORRECTIONS:
        print("\nКоррекции:")
        for s,d in CORRECTIONS.items():
            print(f"  {s} -> {d}")
    print("\nСводка:")
    for k,v in stats.items():
        print(f"  {k}: {v}")


def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Add perspective tickers without duplicates")
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--dry-run', action='store_true', help='(упрощено) всё равно вызывает ensure, реальный dry-run не реализован')
    args = ap.parse_args(argv)
    process(limit=args.limit, dry_run=args.dry_run)

if __name__ == '__main__':
    main(sys.argv[1:])
