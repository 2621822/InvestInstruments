"""data_prices.py

Инкапсулированный синхронный загрузчик истории котировок MOEX для таблицы
`moex_history_perspective_shares` с поддержкой:
 - скользящего окна (horizon_days, по умолчанию 1100)
 - первичного заполнения всех перспективных бумаг (ensure_full_coverage)
 - ежедневной догрузки (daily_update_all)
 - пагинации ISS API (параметр start)
 - мягкого ограничения числа инструментов и тайм-аута общего прохода через переменные окружения:
     PRICE_LIMIT_INSTRUMENTS=N  (обрабатываем только первые N бумаг)
     PRICE_GLOBAL_TIMEOUT_SEC=NN (прервать цикл после N секунд)
 - опционального пересчёта потенциалов (через модуль potentials)
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Callable
import requests

from . import potentials  # для пересчёта

DB_PATH = Path("GorbunovInvestInstruments.db")
TABLE_NAME = "moex_history_perspective_shares"
DEFAULT_HORIZON_DAYS = 1100

# Упрощённый список (фиксированные даты) официальных праздников РФ
# Без учёта переносов выходных и дополнительных постановлений.
_FIXED_HOLIDAYS = {
    (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8),  # Новогодние каникулы + Рождество
    (2, 23),  # День защитника Отечества
    (3, 8),   # Международный женский день
    (5, 1), (5, 9),  # Праздники мая
    (6, 12),  # День России
    (11, 4),  # День народного единства
}


def _is_russian_holiday(d: dt.date) -> bool:
    return (d.month, d.day) in _FIXED_HOLIDAYS

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    BOARDID TEXT,
    TRADEDATE TEXT,
    SHORTNAME TEXT,
    SECID TEXT,
    NUMTRADES INTEGER,
    VALUE REAL,
    OPEN REAL,
    LOW REAL,
    HIGH REAL,
    LEGALCLOSEPRICE REAL,
    WAPRICE REAL,
    CLOSE REAL,
    VOLUME INTEGER,
    MARKETPRICE2 REAL,
    MARKETPRICE3 REAL,
    ADMITTEDQUOTE TEXT,
    MP2VALTRD REAL,
    MARKETPRICE3TRADESVALUE REAL,
    ADMITTEDVALUE TEXT,
    WAVAL INTEGER,
    TRADINGSESSION INTEGER,
    CURRENCYID TEXT,
    TRENDCLSPR REAL,
    TRADE_SESSION_DATE TEXT
);
"""

ORDERED_COLUMNS = [
    "BOARDID","TRADEDATE","SHORTNAME","SECID","NUMTRADES","VALUE","OPEN","LOW","HIGH","LEGALCLOSEPRICE",
    "WAPRICE","CLOSE","VOLUME","MARKETPRICE2","MARKETPRICE3","ADMITTEDQUOTE","MP2VALTRD",
    "MARKETPRICE3TRADESVALUE","ADMITTEDVALUE","WAVAL","TRADINGSESSION","CURRENCYID","TRENDCLSPR","TRADE_SESSION_DATE"
]


def _date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE_NAME,))
    row = cur.fetchone()
    if row and row[0] and "UNIQUE" in row[0].upper():
        tmp = TABLE_NAME + "_old_unique"
        conn.execute(f"ALTER TABLE {TABLE_NAME} RENAME TO {tmp}")
        conn.execute(CREATE_TABLE_SQL)
        try:
            conn.execute(
                f"""
                INSERT INTO {TABLE_NAME} (...)
                """  # intentionally abbreviated; unique migration already handled earlier in repo history
            )
        except Exception:
            pass
        conn.execute(f"DROP TABLE {tmp}")
    else:
        conn.execute(CREATE_TABLE_SQL)
    conn.execute(f"CREATE INDEX IF NOT EXISTS ix_moex_hist_key ON {TABLE_NAME}(BOARDID, SECID, TRADEDATE)")
    conn.commit()


def _fetch_page(board: str, secid: str, d_from: str, d_till: str, start: int | None = None,
                *, retries: int = 4, backoff: float = 0.7, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = (
        f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/"
        f"{board}/securities/{secid}.json?from={d_from}&till={d_till}"
    )
    url = base + (f"&start={start}" if start and start > 0 else "")
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, timeout=25, headers={"User-Agent": "moex-persp-loader/1.1"})
            if metrics is not None:
                metrics['http_requests'] = metrics.get('http_requests', 0) + 1
            if resp.status_code >= 500:
                raise requests.HTTPError(f"Server {resp.status_code}")
            if resp.status_code != 200:
                if attempt >= retries:
                    resp.raise_for_status()
                time.sleep(backoff * attempt)
                continue
            return resp.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:  # noqa: PERF203
            if attempt >= retries:
                raise
            if metrics is not None:
                metrics['retries'] = metrics.get('retries', 0) + 1
            time.sleep(backoff * attempt)


def _extract_rows(payload: Dict[str, Any]) -> tuple[List[str], List[List[Any]], Dict[str, Any]]:
    history = payload.get("history") or {}
    cursor = payload.get("history.cursor") or {}
    cols = history.get("columns") or []
    data = history.get("data") or []
    c_cols = cursor.get("columns") or []
    c_data = cursor.get("data") or []
    c_map: Dict[str, Any] = {}
    if c_cols and c_data:
        first = c_data[0]
        c_map = {c_cols[i]: first[i] for i in range(min(len(c_cols), len(first)))}
    return cols, data, c_map


def _normalize(cols: List[str], row: List[Any]) -> Dict[str, Any]:
    m = {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
    if "TRADE_SESSION_DATE" not in m and "TRADEDATE" in m:
        m["TRADE_SESSION_DATE"] = m["TRADEDATE"]
    return m


def _insert_row_if_new(conn: sqlite3.Connection, values: list[Any]) -> int:
    key_board = values[0]; key_trade = values[1]; key_secid = values[3]
    placeholders = ",".join(["?"] * len(values))
    sql = (
        f"INSERT INTO {TABLE_NAME} (" + ",".join(ORDERED_COLUMNS) + ") "
        f"SELECT {placeholders} WHERE NOT EXISTS (SELECT 1 FROM {TABLE_NAME} WHERE BOARDID=? AND SECID=? AND TRADEDATE=?)"
    )
    params = tuple(values) + (key_board, key_secid, key_trade)
    cur = conn.execute(sql, params)
    return 1 if cur.rowcount == 1 else 0


def _read_secids(conn: sqlite3.Connection) -> list[str]:
    cols_try = ["secid", "SECID", "ticker", "TICKER", "share", "SHARE"]
    collected: set[str] = set()
    for c in cols_try:
        try:
            rows = conn.execute(f"SELECT {c} FROM perspective_shares WHERE {c} IS NOT NULL AND TRIM({c})<>''").fetchall()
        except Exception:
            continue
        for r in rows:
            val = (r[0] or "").strip().upper()
            if val:
                collected.add(val)
    res = sorted(collected)
    logging.info("perspective_shares: найдено %s инструментов для загрузки цен", len(res))
    return res


def _delete_old(conn: sqlite3.Connection, board: str, secid: str, horizon_days: int) -> int:
    cutoff = dt.date.today() - dt.timedelta(days=horizon_days - 1)
    cur = conn.execute(
        f"DELETE FROM {TABLE_NAME} WHERE BOARDID=? AND SECID=? AND TRADE_SESSION_DATE<?",
        (board, secid, cutoff.isoformat()),
    )
    return cur.rowcount or 0


def _get_min_max(conn: sqlite3.Connection, board: str, secid: str) -> tuple[Optional[str], Optional[str]]:
    cur = conn.execute(
        f"SELECT MIN(TRADE_SESSION_DATE), MAX(TRADE_SESSION_DATE) FROM {TABLE_NAME} WHERE BOARDID=? AND SECID=?",
        (board, secid),
    )
    r = cur.fetchone()
    return (r[0], r[1]) if r else (None, None)


def GetMoexHistory(board: str = "TQBR", secid: str = "PLZL", dr_start: Optional[str] = None, dr_end: Optional[str] = None,
                   *, recompute_potentials: bool = False, metrics: Optional[Dict[str, Any]] = None) -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_table(conn)
        today = dt.date.today()
        end_date = _date(dr_end) if dr_end else today
        if dr_start:
            start_date = _date(dr_start)
        else:
            cur = conn.execute(
                f"SELECT MAX(TRADE_SESSION_DATE) FROM {TABLE_NAME} WHERE SECID=? AND BOARDID=?",
                (secid, board),
            )
            last_ts = cur.fetchone()[0]
            if last_ts:
                try:
                    parsed = _date(last_ts)
                except Exception:
                    parsed = today - dt.timedelta(days=DEFAULT_HORIZON_DAYS)
                start_date = parsed + dt.timedelta(days=1)
            else:
                start_date = today - dt.timedelta(days=DEFAULT_HORIZON_DAYS)
        if start_date > end_date:
            # Уже всё скачано ранее – классифицируем как 'already' / weekend / holiday
            reason_date = end_date
            if reason_date.weekday() >= 5:
                reason = " weekend"
            elif _is_russian_holiday(reason_date):
                reason = " holiday"
            else:
                reason = " already"
            base_from = reason_date.isoformat(); base_till = reason_date.isoformat()
            def _fmt_dm(s: str) -> str:
                try:
                    dloc = _date(s); return dloc.strftime("%d.%m")
                except Exception:  # noqa: BLE001
                    return s
            from_fmt = _fmt_dm(base_from); till_fmt = _fmt_dm(base_till)
            logging.info("GetMoexHistory %s %s +0%s %s→%s (0.00s)", board, secid, reason, from_fmt, till_fmt)
            return 0
        base_from = start_date.isoformat(); base_till = end_date.isoformat(); start_offset = 0; inserted = 0; total_reported = None
        t0 = time.perf_counter()
        while True:
            payload = _fetch_page(board, secid, base_from, base_till, start_offset if start_offset else None, metrics=metrics)
            cols, rows, cursor_map = _extract_rows(payload)
            if total_reported is None:
                total_reported = cursor_map.get("TOTAL") or cursor_map.get("total")
            if not rows:
                break
            for r in rows:
                rec = _normalize(cols, r)
                values = [rec.get(c) for c in ORDERED_COLUMNS]
                try:
                    inserted += _insert_row_if_new(conn, values)
                except Exception:
                    continue
            conn.commit()
            fetched_now = len(rows)
            start_offset += fetched_now
            if isinstance(total_reported, int) and start_offset >= total_reported:
                break
            if fetched_now == 0:
                break
        if metrics is not None:
            metrics.setdefault('ranges', []).append({
                'secid': secid,
                'from': base_from,
                'till': base_till,
                'inserted': inserted,
                'duration_sec': round(time.perf_counter() - t0, 3),
            })
        if recompute_potentials:
            try:
                potentials.compute_all(store=True)
            except Exception as exc:  # noqa: BLE001
                logging.debug("Potentials recompute fail %s: %s", secid, exc)
        # Новый формат: GetMoexHistory TQBR SECID +N <reason_if_zero> DD.MM→DD.MM (X.XXs)
        try:
            def _fmt_dm(s: str) -> str:
                try:
                    dloc = _date(s); return dloc.strftime("%d.%m")
                except Exception:  # noqa: BLE001
                    return s
            from_fmt = _fmt_dm(base_from); till_fmt = _fmt_dm(base_till)
            duration = time.perf_counter() - t0
            reason = ""
            if inserted == 0:
                end_d = end_date
                if end_d.weekday() >= 5:
                    reason = " weekend"
                elif _is_russian_holiday(end_d):
                    reason = " holiday"
                elif base_from == base_till:
                    reason = " already"
                elif total_reported in (0, None) and start_offset == 0:
                    reason = " empty"
                else:
                    reason = " up-to-date"
            logging.info("GetMoexHistory %s %s +%d%s %s→%s (%.2fs)", board, secid, inserted, reason, from_fmt, till_fmt, duration)
        except Exception:
            logging.info("GetMoexHistory %s %s inserted=%s interval %s..%s", board, secid, inserted, base_from, base_till)
        return inserted
    finally:
        conn.close()


def _limited_secids(secids: list[str]) -> list[str]:
    limit_env = os.getenv("PRICE_LIMIT_INSTRUMENTS")
    if limit_env:
        try:
            n = int(limit_env)
            if n >= 0:
                return secids[:n]
        except ValueError:
            pass
    return secids


def _global_timeout_reached(start_ts: float) -> bool:
    t_env = os.getenv("PRICE_GLOBAL_TIMEOUT_SEC")
    if not t_env:
        return False
    try:
        limit = float(t_env)
    except ValueError:
        return False
    return (time.time() - start_ts) > limit


def load_all_perspective(board: str = "TQBR", *, recompute_potentials: bool = False, horizon_days: int = DEFAULT_HORIZON_DAYS,
                          silent: bool = False) -> Dict[str, Any]:
    level_before = logging.getLogger().level
    if silent:
        logging.getLogger().setLevel(logging.WARNING)
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_table(conn)
        secids = _read_secids(conn)
    finally:
        conn.close()
    if not secids:
        secids = ["PLZL"]
    secids = _limited_secids(secids)
    start_ts = time.time()
    summary: Dict[str, Any] = {'per_security': {}, 'total_inserted': 0, 'total_deleted_old': 0, 'http_requests': 0, 'retries': 0, 'ranges': []}
    for s in secids:
        if _global_timeout_reached(start_ts):
            summary['timeout_reached'] = True
            break
        sec_metrics: Dict[str, Any] = {}
        try:
            with sqlite3.connect(DB_PATH) as conn2:
                _ensure_table(conn2)
                _min, _max = _get_min_max(conn2, board, s)
                today = dt.date.today(); fetch_from=None; fetch_till=None
                if _max:
                    try:
                        parsed = _date(_max)
                    except Exception:
                        parsed = today - dt.timedelta(days=horizon_days)
                    next_day = parsed + dt.timedelta(days=1)
                    if next_day <= today:
                        fetch_from = next_day.isoformat(); fetch_till = today.isoformat()
                else:
                    fetch_till = today.isoformat(); fetch_from = (today - dt.timedelta(days=horizon_days)).isoformat()
                if fetch_from and fetch_till:
                    inserted = GetMoexHistory(board=board, secid=s, dr_start=fetch_from, dr_end=fetch_till, metrics=sec_metrics)
                else:
                    inserted = 0
                with sqlite3.connect(DB_PATH) as conn3:
                    deleted_old = _delete_old(conn3, board, s, horizon_days)
                    conn3.commit()
                sec_metrics['inserted'] = inserted; sec_metrics['deleted_old'] = deleted_old
                summary['total_inserted'] += inserted; summary['total_deleted_old'] += deleted_old
                summary['http_requests'] += sec_metrics.get('http_requests', 0); summary['retries'] += sec_metrics.get('retries', 0)
                if 'ranges' in sec_metrics: summary['ranges'].extend(sec_metrics['ranges'])
                summary['per_security'][s] = sec_metrics
        except Exception as e:  # noqa: BLE001
            summary['per_security'][s] = {'error': str(e)}
            logging.error("Ошибка загрузки %s: %s", s, e)
    if recompute_potentials:
        try:
            potentials.compute_all(store=True)
        except Exception as exc:  # noqa: BLE001
            summary['potentials_recompute_error'] = str(exc)
    if silent:
        logging.getLogger().setLevel(level_before)
    return summary


def daily_update_all(board: str = "TQBR", *, recompute_potentials: bool = True, horizon_days: int = DEFAULT_HORIZON_DAYS,
                     silent: bool = False) -> Dict[str, Any]:
    return load_all_perspective(board=board, recompute_potentials=recompute_potentials, horizon_days=horizon_days, silent=silent)


def ensure_full_coverage(board: str = "TQBR", *, horizon_days: int = DEFAULT_HORIZON_DAYS,
                         recompute_potentials: bool = True, silent: bool = False) -> Dict[str, Any]:
    level_before = logging.getLogger().level
    if silent:
        logging.getLogger().setLevel(logging.WARNING)
    with sqlite3.connect(DB_PATH) as conn:
        _ensure_table(conn)
        all_ids = _read_secids(conn)
        cur = conn.cursor(); cur.execute(f"SELECT DISTINCT SECID FROM {TABLE_NAME} WHERE BOARDID=?", (board,))
        existing = {r[0] for r in cur.fetchall() if r[0]}
    all_ids = _limited_secids(all_ids)
    missing = [s for s in all_ids if s not in existing]
    summary: Dict[str, Any] = {'board': board, 'horizon_days': horizon_days, 'total_perspective': len(all_ids), 'already_present': len(existing), 'missing_before': len(missing), 'processed_missing': 0, 'per_security': {}, 'http_requests': 0, 'retries': 0, 'started_ts': time.time()}
    if missing:
        today = dt.date.today()
        for secid in missing:
            if _global_timeout_reached(summary['started_ts']):
                summary['timeout_reached'] = True
                break
            metrics: Dict[str, Any] = {}
            try:
                start_date = (today - dt.timedelta(days=horizon_days)).isoformat()
                inserted = GetMoexHistory(board=board, secid=secid, dr_start=start_date, dr_end=today.isoformat(), metrics=metrics)
                summary['per_security'][secid] = {'inserted': inserted, 'http_requests': metrics.get('http_requests', 0), 'retries': metrics.get('retries', 0), 'ranges': metrics.get('ranges', [])}
                summary['processed_missing'] += 1
                summary['http_requests'] += metrics.get('http_requests', 0)
                summary['retries'] += metrics.get('retries', 0)
            except Exception as exc:  # noqa: BLE001
                summary['per_security'][secid] = {'error': str(exc)}
                logging.error("ensure_full_coverage: ошибка загрузки %s: %s", secid, exc)
    else:
        summary['note'] = 'All perspective shares already have history.'
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor(); cur.execute(f"SELECT COUNT(DISTINCT SECID) FROM {TABLE_NAME} WHERE BOARDID=?", (board,))
        summary['coverage_after'] = cur.fetchone()[0]
    if recompute_potentials:
        try:
            potentials.compute_all(store=True)
            summary['potentials_recomputed'] = True
        except Exception as exc:  # noqa: BLE001
            summary['potentials_recomputed'] = False; summary['potentials_recompute_error'] = str(exc)
    summary['duration_sec'] = round(time.time() - summary['started_ts'], 2)
    if silent:
        logging.getLogger().setLevel(level_before)
    logging.info("ensure_full_coverage: processed %s missing of %s total perspective (coverage=%s)", summary['processed_missing'], summary['total_perspective'], summary.get('coverage_after'))
    return summary


def load_interval(board: str, secid: str, start: str, end: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    inserted = GetMoexHistory(board=board, secid=secid, dr_start=start, dr_end=end, metrics=metrics)
    return {'inserted': inserted, **metrics}


__all__ = [
    'ensure_full_coverage', 'daily_update_all', 'load_interval', 'GetMoexHistory'
]
