"""screen_bonds.py

Скрипт:
 1. Загружает список облигаций с выбранных досок MOEX (по умолчанию корпоративные TQOB и офз TQCB/TQIR при желании).
 2. Сохраняет сырые данные в таблицу moex_bonds_raw (upsert по SECID+BOARDID).
 3. Отбирает облигации, торгующиеся на 20% и более ниже номинала (PREVPRICE <= 80) И с годовой доходностью >= 20% (YIELD >= 20).
 4. Группирует результат по группам рейтингов эмитента (issuer_ratings.rating_group). Если рейтинга нет — группа 'UNSPECIFIED'.

Примечания:
 - Для облигаций на MOEX поле PREVPRICE выражено в процентах от номинала, поэтому сравнение PREVPRICE <= 80 эквивалентно «ниже номинала >=20%».
 - Поле YIELD (или YIELDCLOSE) — доходность к погашению, % годовых (берём YIELD, если есть; fallback YIELDCLOSE).
 - Источник рейтингов не предоставлен; создаётся вспомогательная таблица issuer_ratings(issuer TEXT PRIMARY KEY, rating_group TEXT). Заполните её вручную для более осмысленной группировки.

Запуск:
  python screen_bonds.py

Параметры окружения:
  BONDS_BOARDS=TQOB,TQCB  (список досок, запятая)
  BONDS_MIN_YIELD=20
        BONDS_MAX_PRICE_PCT=90   (максимальная цена в % от номинала, чтобы считалось >=10% ниже номинала)
    BONDS_EXPORT_CSV=1       (экспорт результата в bonds_screen.csv)
    BONDS_EXPORT_XLSX=1      (экспорт результата в bonds_screen.xlsx)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import requests

DB_PATH = Path("GorbunovInvestInstruments.db")
ISS_BASE = "https://iss.moex.com/iss"

DEFAULT_BOARDS = ["TQOB"]  # корпоративные облигации основная доска. Можно добавить TQCB (офз?) при необходимости.

@dataclass
class BondRow:
    secid: str
    boardid: str
    shortname: str | None
    secname: str | None
    isin: str | None
    facevalue: float | None
    prevprice: float | None  # как возвращает ISS (часто % от номинала, но иногда абсолют)
    yield_pct: float | None
    matdate: str | None
    couponvalue: float | None
    couponpercent: float | None
    issuer: str | None
    couponperiod: float | None  # дни между купонами (для расчёта годовой)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def ensure_tables() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS moex_bonds_raw (
                secid TEXT,
                boardid TEXT,
                shortname TEXT,
                secname TEXT,
                isin TEXT,
                facevalue REAL,
                prevprice REAL,
                yield_pct REAL,
                matdate TEXT,
                couponvalue REAL,
                couponpercent REAL,
                couponperiod REAL,
                issuer TEXT,
                updatedAt TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (secid, boardid)
            )"""
        )
        # миграция: добавить couponperiod если отсутствует
        try:
            c.execute("PRAGMA table_info(moex_bonds_raw)")
            cols = {r[1] for r in c.fetchall()}
            if 'couponperiod' not in cols:
                c.execute("ALTER TABLE moex_bonds_raw ADD COLUMN couponperiod REAL")
        except Exception:  # noqa: BLE001
            pass
        c.execute(
            """CREATE TABLE IF NOT EXISTS issuer_ratings (
                    issuer TEXT PRIMARY KEY,
                    rating_group TEXT
            )"""
        )
        conn.commit()


def fetch_board(board: str) -> List[BondRow]:
    url = f"{ISS_BASE}/engines/stock/markets/bonds/boards/{board}/securities.json"
    params = {"iss.meta": "off"}
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logging.warning("Не удалось получить данные для доски %s: %s", board, exc)
        return []
    data = resp.json()
    sec_block = data.get("securities", {})
    cols: List[str] = sec_block.get("columns", [])
    rows = sec_block.get("data", [])
    col_index = {name: i for i, name in enumerate(cols)}

    def col(row, name):
        idx = col_index.get(name)
        if idx is None:
            return None
        return row[idx]

    result: List[BondRow] = []
    for r in rows:
        try:
            prevprice = col(r, "PREVPRICE")
            if prevprice is not None:
                try:
                    prevprice = float(prevprice)
                except Exception:
                    prevprice = None
            couponperiod = col(r, "COUPONPERIOD")
            if couponperiod is not None:
                try:
                    couponperiod = float(couponperiod)
                except Exception:
                    couponperiod = None
            facevalue = col(r, "FACEVALUE")
            if facevalue is not None:
                try:
                    facevalue = float(facevalue)
                except Exception:
                    facevalue = None
            yld = col(r, "YIELD")
            if yld is None:
                yld = col(r, "YIELDCLOSE")
            if yld is not None:
                try:
                    yld = float(yld)
                except Exception:
                    yld = None
            result.append(
                BondRow(
                    secid=str(col(r, "SECID") or ""),
                    boardid=board,
                    shortname=col(r, "SHORTNAME"),
                    secname=col(r, "SECNAME"),
                    isin=col(r, "ISIN"),
                    facevalue=facevalue,
                    prevprice=prevprice,
                    yield_pct=yld,
                    matdate=col(r, "MATDATE"),
                    couponvalue=col(r, "COUPONVALUE"),
                    couponpercent=col(r, "COUPONPERCENT"),
                    issuer=col(r, "ISSUER") or col(r, "LATNAME") or col(r, "SECNAME"),
                    couponperiod=couponperiod,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logging.debug("parse row error: %s", exc)
    logging.info("Загружено %s облигаций с доски %s", len(result), board)
    return result


def upsert_bonds(rows: List[BondRow]) -> None:
    if not rows:
        return
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.executemany(
            """INSERT OR REPLACE INTO moex_bonds_raw
                (secid,boardid,shortname,secname,isin,facevalue,prevprice,yield_pct,matdate,couponvalue,couponpercent,couponperiod,issuer)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    r.secid,
                    r.boardid,
                    r.shortname,
                    r.secname,
                    r.isin,
                    r.facevalue,
                    r.prevprice,
                    r.yield_pct,
                    r.matdate,
                    r.couponvalue,
                    r.couponpercent,
                    r.couponperiod,
                    r.issuer,
                )
                for r in rows
            ],
        )
        conn.commit()
    logging.info("Сохранено/обновлено %s строк в moex_bonds_raw", len(rows))


def screen(min_yield: float, max_price_pct: float) -> List[Dict[str, Any]]:
    """Отбор с перерасчётом эффективной доходности.

    Правила:
      1. Определяем price_percent: если prevprice <=150 -> это уже % от номинала, иначе prevprice/facevalue*100.
      2. discount_from_par = 100 - price_percent.
      3. Базовая доходность: yield_pct (если не None и >0). Если отсутствует и разрешён fallback — считаем текущую (current yield)
         current_yield = annual_coupon_cash / price_abs * 100.
         annual_coupon_cash = couponvalue * (365/couponperiod) ИЛИ (facevalue * couponpercent/100)*(365/couponperiod) если couponvalue отсутствует.
      4. Отбор: price_percent <= max_price_pct И effective_yield >= min_yield.
    """
    use_fallback = os.getenv("BONDS_USE_FALLBACK_YIELD", "1").lower() in {"1","true","yes"}
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT b.secid,b.boardid,b.shortname,b.secname,b.isin,b.prevprice,b.yield_pct,b.facevalue,
                       b.matdate,b.couponpercent,b.couponvalue,b.couponperiod,b.issuer,
                       COALESCE(r.rating_group,'UNSPECIFIED') as rating_group
                FROM moex_bonds_raw b
                LEFT JOIN issuer_ratings r ON r.issuer=b.issuer
            """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    out: List[Dict[str, Any]] = []
    for row in rows:
        rec = dict(zip(cols, row))
        prevprice = rec.get("prevprice")
        face = rec.get("facevalue") or 1000
        price_percent = None
        if isinstance(prevprice, (int, float)) and prevprice is not None:
            if prevprice <= 150:
                price_percent = float(prevprice)
            else:
                try:
                    price_percent = (float(prevprice) / float(face)) * 100 if face else None
                except Exception:
                    price_percent = None
        if price_percent is None:
            continue
        discount_from_par = 100 - price_percent if price_percent is not None else None
        couponperiod = rec.get("couponperiod")
        couponvalue = rec.get("couponvalue")
        couponpercent = rec.get("couponpercent")
        yield_pct = rec.get("yield_pct")
        effective_yield = yield_pct if isinstance(yield_pct, (int, float)) and yield_pct is not None else None
        price_abs = price_percent / 100 * face if (price_percent is not None and face) else None
        if (effective_yield is None or effective_yield < min_yield) and use_fallback and price_abs and couponperiod and couponperiod > 0:
            annual_coupon_cash = None
            if isinstance(couponvalue, (int, float)) and couponvalue:
                annual_coupon_cash = couponvalue * (365 / couponperiod)
            elif isinstance(couponpercent, (int, float)) and couponpercent:
                annual_coupon_cash = face * (couponpercent / 100) * (365 / couponperiod)
            if annual_coupon_cash and price_abs > 0:
                effective_yield = annual_coupon_cash / price_abs * 100
        if price_percent <= max_price_pct and effective_yield is not None and effective_yield >= min_yield:
            rec.update({
                "price_percent": price_percent,
                "discount_from_par": discount_from_par,
                "effective_yield": effective_yield,
                "price_abs": price_abs,
            })
            out.append(rec)
    # сортировка: сначала рейтинг, потом цена
    out.sort(key=lambda r: (r.get("rating_group"), r.get("price_percent")))
    return out


def print_grouped(items: List[Dict[str, Any]]) -> None:
    if not items:
        print("Нет облигаций, удовлетворяющих критериям.")
        return
    # Группировка по rating_group
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        groups.setdefault(it["rating_group"], []).append(it)
    for grp, rows in sorted(groups.items(), key=lambda kv: kv[0]):
        print(f"\nРейтинг группа: {grp} (кол-во: {len(rows)})")
        print("SECID     Price%  EffY%  Disc%  MatDate    Cup%  Period  Issuer")
        for r in rows:
            print(f"{r.get('secid',''):<9} {r.get('price_percent',0):6.2f} {r.get('effective_yield',0):6.2f} "
                  f"{r.get('discount_from_par',0):6.2f} {str(r.get('matdate','') or '')[:10]:<10} "
                  f"{(r.get('couponpercent') or 0):6.2f} {str(r.get('couponperiod') or ''):>6}  {str(r.get('issuer') or '')[:34]}")


def export_csv(items: List[Dict[str, Any]], fname: str = "bonds_screen.csv") -> None:
    if not items:
        return
    import csv

    cols = list(items[0].keys())
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(items)
    logging.info("Экспортирован отбор облигаций -> %s (rows=%s)", fname, len(items))


def main() -> None:
    setup_logging()
    ensure_tables()
    boards = [b.strip() for b in os.getenv("BONDS_BOARDS", ",".join(DEFAULT_BOARDS)).split(",") if b.strip()]
    all_rows: List[BondRow] = []
    for b in boards:
        all_rows.extend(fetch_board(b))
    upsert_bonds(all_rows)
    min_yield = float(os.getenv("BONDS_MIN_YIELD", "20"))
    # 10% ниже номинала => цена <= 90
    max_price_pct = float(os.getenv("BONDS_MAX_PRICE_PCT", "90"))
    filtered = screen(min_yield=min_yield, max_price_pct=max_price_pct)
    print_grouped(filtered)
    if os.getenv("BONDS_EXPORT_CSV", "0").lower() in {"1","true","yes"}:
        export_csv(filtered)
    if os.getenv("BONDS_EXPORT_XLSX", "1").lower() in {"1","true","yes"}:
        try:
            from openpyxl import Workbook
            wb = Workbook(); ws = wb.active; ws.title = "Screen"
            if filtered:
                cols = list(filtered[0].keys())
                ws.append(cols)
                for row in filtered:
                    ws.append([row.get(c) for c in cols])
            wb.save("bonds_screen.xlsx")
            logging.info("Экспортирован отбор облигаций -> bonds_screen.xlsx (rows=%s)", len(filtered))
        except Exception as exc:  # noqa: BLE001
            logging.warning("Не удалось экспортировать в Excel: %s", exc)


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
