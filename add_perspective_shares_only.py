"""add_perspective_shares_only.py

Добавляет ТОЛЬКО инструменты типа акция (instrumentType == 'INSTRUMENT_TYPE_SHARE').
Берёт список тикеров из константы RAW_TICKERS (из запроса пользователя), нормализует,
убирает дубликаты и выполняет поиск через search_share. Перед вставкой проверяет тип.

Вставка использует существующую функцию ensure_perspective_share, но только после
того как проверено что инструмент является акцией. Если тип не акция – помечается
как 'not-share'. Если поиск не дал результата – 'not-found'.

Коррекция: YDEX -> YNDX.

Запуск:
  python add_perspective_shares_only.py                # полный набор
  python add_perspective_shares_only.py --limit 10      # первые 10
  python add_perspective_shares_only.py --dry-run       # без вставок

Выводит таблицу: ticker | status | instrumentType | uid
Сводная статистика: inserted / exists / not-found / not-share / errors.
"""
from __future__ import annotations
import argparse
import sys
from typing import List, Dict, Set

from src.invest_core.instruments import search_share, ensure_perspective_share

CORRECTIONS = {"YDEX": "YNDX"}

RAW_TICKERS = [
    "AFKS","SVCB","SNGS","POSI","RTKMP","T","GEMC","DATA","BELU","LEAS","RENI","RAGR","SFIN","ASTR","IRAO","LKOH","SNGSP","MBNK","TRMK","X5","FESH","DIAS","MTLR","WUSH","LENT","OZPH","TATN","YDEX","MSNG","UGLD","ALRS","CHMF","PHOR","PLZL","GMKN","SBER","IVAT","RTKM","ROSN","RUAL","SBERP","AFLT","NVTK","GAZP","MOEX","MTSS","LKOH","HEAD","PLZL","SMLT","SOFL","VSEH"
]

SHARE_TYPE = "INSTRUMENT_TYPE_SHARE"  # ожидаемое значение instrumentType для акций


def normalize(tickers: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in tickers:
        t1 = CORRECTIONS.get(t.strip().upper(), t.strip().upper())
        if t1 not in seen:
            seen.add(t1)
            out.append(t1)
    return out


def process(limit: int | None, dry_run: bool):
    ticks = normalize(RAW_TICKERS)
    if limit is not None:
        ticks = ticks[:limit]
    stats = {"inserted":0, "exists":0, "not_found":0, "not_share":0, "errors":0}
    rows: List[Dict[str,str]] = []
    for t in ticks:
        inst = search_share(t)
        if not inst:
            stats["not_found"] += 1
            rows.append({"ticker": t, "status": "not-found", "type": "", "uid": ""})
            continue
        itype = inst.get("instrumentType") or inst.get("instrument_type") or ""
        if itype != SHARE_TYPE:
            stats["not_share"] += 1
            rows.append({"ticker": t, "status": "not-share", "type": itype, "uid": inst.get("uid","")})
            continue
        if dry_run:
            # dry-run: НЕ вставляем, просто помечаем как ready
            rows.append({"ticker": t, "status": "ready", "type": itype, "uid": inst.get("uid","")})
            continue
        res = ensure_perspective_share(t)
        st = res.get("status")
        if st == "inserted": stats["inserted"] += 1
        elif st == "exists": stats["exists"] += 1
        elif st == "not-found": stats["not_found"] += 1
        else: stats["errors"] += 1
        rows.append({"ticker": t, "status": st, "type": itype, "uid": res.get("uid","")})

    print("ticker | status     | instrumentType          | uid")
    print("-------|------------|--------------------------|----")
    for r in rows:
        print(f"{r['ticker']:<6} | {r['status']:<10} | {r['type']:<24} | {r['uid']}")
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


def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Add ONLY share instruments to perspective_shares")
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--dry-run', action='store_true', help='Показать какие акции будут вставлены без изменений БД')
    args = ap.parse_args(argv)
    process(limit=args.limit, dry_run=args.dry_run)


if __name__ == '__main__':
    main(sys.argv[1:])
