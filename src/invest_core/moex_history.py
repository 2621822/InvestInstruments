"""Загрузка исторических данных цен для перспективных акций (MOEX ISS).

Спецификация (переписано по требованию):
    Таблица хранения: moex_shares_history (PRIMARY KEY (SECID, TRADEDATE))
  Поля сохраняются:
    SECID, TRADEDATE, BOARDID, OPEN, CLOSE, HIGH, LOW, WAPRICE, SHORTNAME,
    NUMTRADES, VOLUME, VALUE, WAVAL

Функции:
  GetMoexHistoryByUid(board="TQBR", secid=None, dr_start=None, dr_end=None)
  AddMoexHistory(rows)
  FillingMoexHistory(board="TQBR") – ежедневная загрузка для всех бумаг.

Правила дат:
  dr_end default = сегодня (UTC date)
    dr_start default = следующий день после MAX(TRADEDATE) в moex_shares_history
    Если записей нет: 1100 дней назад.
  Если передан secid=None: берём все SECID из perspective_shares.

Дубликаты: строка (SECID, TRADEDATE) существует – пропуск.
"""
from __future__ import annotations
from typing import List, Dict, Any, Iterable
import datetime as dt
from datetime import UTC
import logging
import os
import requests

from . import db as db_layer

log = logging.getLogger(__name__)

BASE_URL_TMPL = "https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/{board}/securities/{secid}.json"
DEFAULT_TIMEOUT = int(os.getenv("INVEST_MOEX_TIMEOUT_SEC", "10"))
USER_AGENT = os.getenv("INVEST_MOEX_USER_AGENT", "invest-core/0.1 (+https://example.com)")
SLEEP_PAGE_SEC = float(os.getenv("INVEST_MOEX_SLEEP_SEC", "0"))

NEEDED_COLS = [
    "SECID", "TRADEDATE", "BOARDID", "OPEN", "CLOSE", "HIGH", "LOW", "WAPRICE", "SHORTNAME",
    "NUMTRADES", "VOLUME", "VALUE", "WAVAL"
]


def _iso_today() -> str:
    return dt.datetime.now(UTC).date().isoformat()


def _date_n_days_ago(days: int) -> str:
    return (dt.datetime.now(UTC).date() - dt.timedelta(days=days)).isoformat()


def _next_day(date_str: str) -> str:
    d = dt.date.fromisoformat(date_str)
    return (d + dt.timedelta(days=1)).isoformat()


def _get_last_tradedate(secid: str) -> str | None:
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT TRADEDATE FROM moex_shares_history WHERE SECID=? ORDER BY TRADEDATE DESC LIMIT 1", (secid,))
        row = cur.fetchone()
        return row[0] if row else None


def _fetch_pages(board: str, secid: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Страничная загрузка истории MOEX для одной бумаги."""
    all_rows: List[Dict[str, Any]] = []
    start = 0
    session = requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    base_url = BASE_URL_TMPL.format(board=board, secid=secid)
    params_base = {"from": start_date, "till": end_date}
    while True:
        params = {**params_base, "start": start}
        resp = session.get(base_url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"MOEX history error status={resp.status_code} secid={secid} text={resp.text[:200]}")
        data = resp.json()
        history = data.get("history", {})
        columns = history.get("columns", [])
        rows = history.get("data", [])
        if not rows:
            break
        idx_map = {col: i for i, col in enumerate(columns)}
        for r in rows:
            rec = {c: (r[idx_map[c]] if c in idx_map else None) for c in NEEDED_COLS}
            all_rows.append(rec)
        start += len(rows)
        if SLEEP_PAGE_SEC > 0:
            import time
            time.sleep(SLEEP_PAGE_SEC)
    session.close()
    return all_rows


def GetMoexHistoryByUid(board: str = "TQBR", secid: str | None = None,
                        dr_start: str | None = None, dr_end: str | None = None) -> Dict[str, Any]:
    """Получить исторические данные для одной или набора бумаг.

    Автовычисление диапазона дат, если не указаны.
    """
    db_layer.init_schema()
    if dr_end is None:
        dr_end = _iso_today()
    if secid:
        secids: Iterable[str] = [secid]
    else:
        secids = db_layer.list_perspective_secids()
    results: List[Dict[str, Any]] = []
    for s in secids:
        start_date = dr_start
        if start_date is None:
            last = _get_last_tradedate(s)
            if last:
                start_date = _next_day(last)
            else:
                start_date = _date_n_days_ago(1100)
        if start_date > dr_end:
            log.debug("Skip secid=%s start_date=%s > dr_end=%s", s, start_date, dr_end)
            continue
        rows = _fetch_pages(board, s, start_date, dr_end)
        for r in rows:
            results.append(r)
    return {"status": "ok", "rows": results, "count": len(results), "board": board, "secid": secid}


def AddMoexHistory(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Сохранить строки в moex_shares_history, пропуская дубликаты.

    Возвращает словарь: {inserted: N, duplicates: M}.
    """
    if not rows:
        return {"inserted": 0, "duplicates": 0}
    db_layer.init_schema()
    inserted = 0
    duplicates = 0
    with db_layer.get_connection() as conn:
        backend = db_layer.BACKEND
        try:
            if backend == 'sqlite':
                conn.execute("""
                CREATE TABLE IF NOT EXISTS moex_shares_history (
                    SECID TEXT NOT NULL,
                    TRADEDATE TEXT NOT NULL,
                    BOARDID TEXT,
                    OPEN REAL,
                    CLOSE REAL,
                    HIGH REAL,
                    LOW REAL,
                    WAPRICE REAL,
                    SHORTNAME TEXT,
                    NUMTRADES INTEGER,
                    VOLUME INTEGER,
                    VALUE REAL,
                    WAVAL INTEGER,
                    PRIMARY KEY (SECID, TRADEDATE)
                )
                """)
            else:
                conn.execute("""
                CREATE TABLE IF NOT EXISTS moex_shares_history (
                    SECID TEXT,
                    TRADEDATE TEXT,
                    BOARDID TEXT,
                    OPEN DOUBLE,
                    CLOSE DOUBLE,
                    HIGH DOUBLE,
                    LOW DOUBLE,
                    WAPRICE DOUBLE,
                    SHORTNAME TEXT,
                    NUMTRADES INTEGER,
                    VOLUME INTEGER,
                    VALUE DOUBLE,
                    WAVAL INTEGER
                )
                """)
        except Exception as ex:  # noqa
            log.warning("Failed to ensure moex_shares_history exists ex=%s", ex)
        for r in rows:
            secid = r.get("SECID")
            tradedate = r.get("TRADEDATE")
            if not secid or not tradedate:
                continue
            cur = conn.execute("SELECT 1 FROM moex_shares_history WHERE SECID=? AND TRADEDATE=? LIMIT 1", (secid, tradedate))
            if cur.fetchone():
                duplicates += 1
                continue
            try:
                conn.execute(
                    "INSERT INTO moex_shares_history(SECID, TRADEDATE, BOARDID, OPEN, CLOSE, HIGH, LOW, WAPRICE, SHORTNAME, NUMTRADES, VOLUME, VALUE, WAVAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        secid,
                        tradedate,
                        r.get("BOARDID"),
                        r.get("OPEN"),
                        r.get("CLOSE"),
                        r.get("HIGH"),
                        r.get("LOW"),
                        r.get("WAPRICE"),
                        r.get("SHORTNAME"),
                        r.get("NUMTRADES"),
                        r.get("VOLUME"),
                        r.get("VALUE"),
                        r.get("WAVAL"),
                    )
                )
                inserted += 1
            except Exception as ex:  # noqa
                log.warning("Insert failed SECID=%s TRADEDATE=%s ex=%s", secid, tradedate, ex)
        if backend == 'sqlite':
            conn.commit()
    return {"inserted": inserted, "duplicates": duplicates}


def FillingMoexHistory(board: str = "TQBR") -> Dict[str, Any]:
    """Ежедневная загрузка: обходит каждую бумагу из perspective_shares и догружает только её недостающие записи.

    Стратегия:
      1. Получаем список SECID из perspective_shares.
      2. Для каждой SECID вызываем GetMoexHistoryByUid(board=..., secid=<SECID>) чтобы вычислить её индивидуальный диапазон дат.
      3. Передаём полученные строки в AddMoexHistory.
      4. Агрегируем статистику вставок и дубликатов.
    Ошибки по одной бумаге логируются и не прерывают общий процесс.
    """
    secids = db_layer.list_perspective_secids()
    total_fetched = 0
    total_inserted = 0
    total_duplicates = 0
    errors: list[dict[str, Any]] = []
    invalid: list[str] = []
    for s in secids:
        try:
            # Проверим есть ли уже история в БД - если нет, пробуем получить первый пакет.
            has_existing = _get_last_tradedate(s) is not None
            data = GetMoexHistoryByUid(board=board, secid=s)
            rows = data.get("rows") or []
            if not rows and not has_existing:
                # MOEX ничего не вернуло с полной логикой диапазона -> считаем тикер потенциально невалидным.
                invalid.append(s)
                log.debug("Skip invalid or empty SECID=%s", s)
                continue
            total_fetched += len(rows)
            res = AddMoexHistory(rows)
            total_inserted += res.get("inserted", 0)
            total_duplicates += res.get("duplicates", 0)
        except Exception as ex:  # noqa
            log.warning("FillingMoexHistory error secid=%s ex=%s", s, ex)
            errors.append({"secid": s, "error": str(ex)})
    return {
        "status": "ok" if not errors else "partial",
        "secids": secids,
        "invalid_secids": invalid,
        "fetched": total_fetched,
        "inserted": total_inserted,
        "duplicates": total_duplicates,
        "errors": errors,
        "board": board,
    }


__all__ = [
    "GetMoexHistoryByUid",
    "AddMoexHistory",
    "FillingMoexHistory",
]



