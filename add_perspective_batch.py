"""add_perspective_batch.py

Скрипт пакетного добавления эмитентов в таблицу perspective_shares без дублей.

Использование:
  python add_perspective_batch.py [--dry-run] [--limit N]

Источники входных данных: встроенный список (NAME, TICKER). Если ticker указан
несколько раз или встречается потенциальная опечатка, это фиксируется в отчёте.

Алгоритм:
  1. Нормализуем пары (strip, collapse whitespace).
  2. Группируем по тикеру: если один тикер соответствует нескольким именам – помечаем.
  3. Для каждого тикера вызываем ensure_perspective_share(ticker) – SDK поиск.
  4. Собираем статистику: inserted / exists / not-found / duplicates.
  5. При --dry-run не выполняем фактическую вставку (только проверка поиска).

Выход: печатает таблицу и сводную статистику.
"""
from __future__ import annotations
import argparse
import sys
import logging
from collections import defaultdict
from typing import List, Tuple, Dict

from src.invest_core.instruments import ensure_perspective_share

log = logging.getLogger("add_perspective_batch")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

RAW_PAIRS = [
    ("АФК Система", "AFKS"),
    ("IVA Technologies", "IVAT"),
    ("Novabev Group", "BELU"),
    ("United medical group", "GEMC"),
    ("Whoosh", "WUSH"),
    ("WUSH", "Лента"),  # Строка пользователя: порядок инвертирован? Выглядит как ошибка.
    ("АЛРОСА", "ALRS"),
    ("Аэрофлот", "AFLT"),
    ("ВИ.РУ", "VSEH"),
    ("Газпром", "GAZP"),
    ("ГК Самолет", "SMLT"),
    ("Группа Аренадата", "DATA"),
    ("Группа Астра", "ASTR"),
    ("Группа Позитив", "POSI"),
    ("ДВМП", "FESH"),
    ("Диасофт", "DIAS"),
    ("Европлан", "LEAS"),
    ("Интер РАО ЕЭС", "IRAO"),
    ("Корпоративный Центр Икс 5", "X5"),
    ("Лента", "LENT"),
    ("ЛУКОЙЛ", "LKOH"),
    ("Мечел", "MTLR"),
    ("Московская Биржа", "MOEX"),
    ("Мосэнерго", "MSNG"),
    ("МТС", "MTSS"),
    ("МТС-Банк", "MBNK"),
    ("НОВАТЭК", "NVTK"),
    ("Норильский никель", "GMKN"),
    ("Озон Фармацевтика", "OZPH"),
    ("Полюс", "PLZL"),
    ("Ренессанс Страхование", "RENI"),
    ("Роснефть", "ROSN"),
    ("Ростелеком", "RTKM"),
    ("Ростелеком - Привилегированные акции", "RTKMP"),
    ("РусАгро", "RAGR"),
    ("РУСАЛ", "RUAL"),
    ("Сбер Банк", "SBER"),
    ("Сбер Банк ап", "SBERP"),
    ("Северсталь", "CHMF"),
    ("Совкомбанк", "SVCB"),
    ("Софтлайн", "SOFL"),
    ("Сургутнефтегаз", "SNGS"),
    ("Сургутнефтегаз - привилегированные акции", "SNGSP"),
    ("Татнефть", "TATN"),
    ("Трубная Металлургическая Компания", "TRMK"),
    ("Т-Технологии", "T"),
    ("ФосАгро", "PHOR"),
    ("Хэдхантер", "HEAD"),
    ("ЭсЭфАй", "SFIN"),
    ("Южуралзолото ГК", "UGLD"),
    ("Яндекс", "YDEX"),  # Возможная опечатка тикера (ожидаемо YNDX на MOEX, тикер может отличаться для другой площадки).
]

# Коррекции явных опечаток тикеров (если потребуется). Ключ = исходный, значение = исправленный.
CORRECTIONS = {
    "YDEX": "YNDX",  # Яндекс
    # "T": "TTLK",  # Пример: если понадобится скорректировать "T" -> реальный тикер (закомментировано пока).
}


def normalize_pairs(pairs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for name, ticker in pairs:
        name_n = " ".join(name.strip().split())
        tick_n = ticker.strip().upper()
        tick_n = CORRECTIONS.get(tick_n, tick_n)
        out.append((name_n, tick_n))
    return out


def group_by_ticker(pairs: List[Tuple[str, str]]):
    by: Dict[str, List[str]] = defaultdict(list)
    for name, ticker in pairs:
        by[ticker].append(name)
    return by


def process(limit: int | None, dry_run: bool):
    pairs = normalize_pairs(RAW_PAIRS)
    if limit is not None:
        pairs = pairs[:limit]
    grouped = group_by_ticker(pairs)
    duplicates = {t: names for t, names in grouped.items() if len(names) > 1}

    stats = {
        "inserted": 0,
        "exists": 0,
        "not_found": 0,
        "duplicates": len(duplicates),
        "errors": 0,
    }
    rows = []

    for name, ticker in pairs:
        status = {"status": "skipped"}
        if dry_run:
            # В режиме dry-run просто проверим возможность поиска.
            status = ensure_perspective_share(ticker)
        else:
            status = ensure_perspective_share(ticker)
        st = status.get("status")
        if st == "inserted":
            stats["inserted"] += 1
        elif st == "exists":
            stats["exists"] += 1
        elif st == "not-found":
            stats["not_found"] += 1
        else:
            if st not in ("skipped",):
                stats["errors"] += 1
        rows.append({"name": name, "ticker": ticker, "status": st, "uid": status.get("uid")})

    # Печать отчёта.
    print("Тикер | Имя | Статус | UID")
    print("------|-----|--------|----")
    for r in rows:
        print(f"{r['ticker']:<6} | {r['name']:<35} | {r['status']:<10} | {r.get('uid','')}")

    if duplicates:
        print("\nДубликаты по тикеру (несколько имён):")
        for t, names in duplicates.items():
            print(f"  {t}: {', '.join(names)}")

    if CORRECTIONS:
        print("\nАвтокоррекции тикеров:")
        for src, dst in CORRECTIONS.items():
            print(f"  {src} -> {dst}")

    print("\nСводка:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


def main(argv: List[str]):
    ap = argparse.ArgumentParser(description="Batch add perspective shares without duplicates")
    ap.add_argument("--dry-run", action="store_true", help="Только проверка поиска (не влияет на БД)")
    ap.add_argument("--limit", type=int, default=None, help="Ограничить количество пар к обработке")
    args = ap.parse_args(argv)
    process(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main(sys.argv[1:])
