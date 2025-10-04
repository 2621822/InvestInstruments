"""MOEX history loader for perspective shares (per updated requirements).

Требования (реализовано):
 - Таблица `moex_history_perspective_shares` в БД `GorbunovInvestInstruments.db` c ТОЛЬКО указанными полями (без UNIQUE / лишних ограничений).
 - Функция GetMoexHistory(board, secid, dr_start, dr_end) с дефолтами:
             board="TQBR", secid="PLZL", dr_start=следующий день после MAX(TRADE_SESSION_DATE) или today-1100d если нет строк, dr_end=today.
 - Поддержка пагинации API ISS (параметр start) до загрузки всех строк.
 - Первичное заполнение для всех перспективных бумаг из таблицы perspective_shares.
 - Ежедневная догрузка (incremental) всех перспективных бумаг.
 - Возможность получить данные по любой бумаге за произвольный интервал (через явные dr_start/dr_end).

Дополнительно:
 - Миграция: если ранее таблица была создана с UNIQUE(BOARDID,SECID,TRADEDATE), она будет пересоздана без UNIQUE (данные сохраняются уникализированными по ключу).
 - Избежание дублей при вставке без использования UNIQUE: вставка через NOT EXISTS.
"""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import time
import argparse
import requests
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Callable

DB_PATH = "GorbunovInvestInstruments.db"
TABLE_NAME = "moex_history_perspective_shares"
DEFAULT_HORIZON_DAYS = 1100  # sliding window horizon

# Точная схема без UNIQUE как в требованиях
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


def ensure_table(conn: sqlite3.Connection) -> None:
    """Ensure table exists with exact required schema (no UNIQUE). Migrate if needed."""
    # Check existing create SQL
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (TABLE_NAME,),
    )
    row = cur.fetchone()
    if row and row[0]:
        create_stmt = row[0].upper()
        if "UNIQUE" in create_stmt:
            # Migration path: rename, create fresh, copy distinct
            tmp = TABLE_NAME + "_old_unique"
            conn.execute(f"ALTER TABLE {TABLE_NAME} RENAME TO {tmp}")
            conn.execute(CREATE_TABLE_SQL)
            # Copy DISTINCT by key (BOARDID, SECID, TRADEDATE) picking MAX(TRADE_SESSION_DATE) etc.
            copy_sql = f"""
                INSERT INTO {TABLE_NAME} (
                  BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH,LEGALCLOSEPRICE,WAPRICE,CLOSE,
                  VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,
                  TRADINGSESSION,CURRENCYID,TRENDCLSPR,TRADE_SESSION_DATE
                )
                SELECT BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH,LEGALCLOSEPRICE,WAPRICE,CLOSE,
                       VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,
                       TRADINGSESSION,CURRENCYID,TRENDCLSPR,TRADE_SESSION_DATE
                FROM (
                   SELECT *, ROW_NUMBER() OVER (PARTITION BY BOARDID,SECID,TRADEDATE ORDER BY TRADE_SESSION_DATE DESC) AS rn
                   FROM {tmp}
                )
                WHERE rn=1
            """
            try:
                conn.execute("PRAGMA recursive_triggers=OFF")  # safety
                conn.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass
            # If SQLite version lacks window functions, fallback to DISTINCT
            try:
                conn.execute(copy_sql)
            except Exception:
                fallback = f"""
                    INSERT INTO {TABLE_NAME} (
                      BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH,LEGALCLOSEPRICE,WAPRICE,CLOSE,
                      VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,
                      TRADINGSESSION,CURRENCYID,TRENDCLSPR,TRADE_SESSION_DATE
                    )
                    SELECT DISTINCT BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH,LEGALCLOSEPRICE,WAPRICE,CLOSE,
                                    VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,
                                    TRADINGSESSION,CURRENCYID,TRENDCLSPR,TRADE_SESSION_DATE
                    FROM {tmp}
                """
                conn.execute(fallback)
            conn.execute(f"DROP TABLE {tmp}")
            conn.execute("CREATE INDEX IF NOT EXISTS ix_moex_hist_key ON moex_history_perspective_shares(BOARDID, SECID, TRADEDATE)")
            conn.commit()
            return
        # Already correct schema -> ensure index
        conn.execute("CREATE INDEX IF NOT EXISTS ix_moex_hist_key ON moex_history_perspective_shares(BOARDID, SECID, TRADEDATE)")
        conn.commit()
        return
    # No table — create
    conn.execute(CREATE_TABLE_SQL)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_moex_hist_key ON moex_history_perspective_shares(BOARDID, SECID, TRADEDATE)")
    conn.commit()


def _get_last_trade_session_date(conn: sqlite3.Connection, board: str, secid: str) -> Optional[str]:
    cur = conn.execute(
        f"SELECT MAX(TRADE_SESSION_DATE) FROM {TABLE_NAME} WHERE SECID=? AND BOARDID=?",
        (secid, board),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _iso(d: dt.date) -> str:
    return d.isoformat()


def _fetch_page(board: str, secid: str, d_from: str, d_till: str, start: int | None = None,
                *, retries: int = 4, backoff: float = 0.7, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = (
        f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/"
        f"{board}/securities/{secid}.json?from={d_from}&till={d_till}"
    )
    if start and start > 0:
        url = base + f"&start={start}"
    else:
        url = base
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
                logging.warning("HTTP %s %s %s %s-%s (attempt %s)", resp.status_code, board, secid, d_from, d_till, attempt)
                if attempt >= retries:
                    resp.raise_for_status()
                time.sleep(backoff * attempt)
                continue
            return resp.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:  # noqa: PERF203
            if attempt >= retries:
                logging.error("Fetch fail %s %s after %s attempts: %s", board, secid, attempt, exc)
                raise
            sleep_for = backoff * attempt
            logging.warning("Retry %s %s %s sleep=%.2fs: %s", attempt, board, secid, sleep_for, exc)
            if metrics is not None:
                metrics['retries'] = metrics.get('retries', 0) + 1
            time.sleep(sleep_for)


def _extract_rows(json_payload: Dict[str, Any]) -> Tuple[List[str], List[List[Any]], Dict[str, Any]]:
    history = json_payload.get("history") or {}
    cursor = json_payload.get("history.cursor") or {}
    cols: List[str] = history.get("columns") or []
    data: List[List[Any]] = history.get("data") or []
    cursor_cols = cursor.get("columns") or []
    cursor_data = cursor.get("data") or []
    cursor_map: Dict[str, Any] = {}
    if cursor_cols and cursor_data:
        first = cursor_data[0]
        cursor_map = {cursor_cols[i]: first[i] for i in range(min(len(cursor_cols), len(first)))}
    return cols, data, cursor_map


def _normalize_record(cols: List[str], row: List[Any]) -> Dict[str, Any]:
    mapping = {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
    # unify: TRADEDATE duplicated as TRADE_SESSION_DATE if second absent
    if "TRADE_SESSION_DATE" not in mapping and "TRADEDATE" in mapping:
        mapping["TRADE_SESSION_DATE"] = mapping["TRADEDATE"]
    return mapping


def _insert_row_if_new(conn: sqlite3.Connection, values: list[Any]) -> int:
    """Insert row if key (BOARDID,SECID,TRADEDATE) not present. Returns 1 if inserted."""
    key_board = values[0]
    key_trade = values[1]
    key_secid = values[3]
    placeholders = ",".join(["?"] * len(values))
    sql = (
        f"INSERT INTO {TABLE_NAME} (BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH,LEGALCLOSEPRICE,"
        "WAPRICE,CLOSE,VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,"
        "TRADINGSESSION,CURRENCYID,TRENDCLSPR,TRADE_SESSION_DATE) "
        f"SELECT {placeholders} WHERE NOT EXISTS (SELECT 1 FROM {TABLE_NAME} WHERE BOARDID=? AND SECID=? AND TRADEDATE=?)"
    )
    params = tuple(values) + (key_board, key_secid, key_trade)
    cur = conn.execute(sql, params)
    return 1 if cur.rowcount == 1 else 0

ORDERED_COLUMNS = [
    "BOARDID","TRADEDATE","SHORTNAME","SECID","NUMTRADES","VALUE","OPEN","LOW","HIGH","LEGALCLOSEPRICE",
    "WAPRICE","CLOSE","VOLUME","MARKETPRICE2","MARKETPRICE3","ADMITTEDQUOTE","MP2VALTRD",
    "MARKETPRICE3TRADESVALUE","ADMITTEDVALUE","WAVAL","TRADINGSESSION","CURRENCYID","TRENDCLSPR","TRADE_SESSION_DATE"
]


def GetMoexHistory(board: str = "TQBR", secid: str = "PLZL", dr_start: Optional[str] = None, dr_end: Optional[str] = None,
                   *, recompute_potentials: bool = False,
                   potentials_callback: Optional[Callable[[Path], None]] = None,
                   db_path: str = DB_PATH,
                   metrics: Optional[Dict[str, Any]] = None) -> int:
    """Загрузить исторические данные MOEX по бумаге.

    Параметры:
      board: торговая площадка (обычно TQBR)
      secid: тикер (SECID)
      dr_start: дата начала (YYYY-MM-DD) или None для авто-определения
      dr_end: дата окончания (YYYY-MM-DD) или None => сегодня

    Возвращает количество вставленных строк.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_table(conn)
        today = dt.date.today()
        # Определяем end
        end_date = _date(dr_end) if dr_end else today
        # Определяем start
        if dr_start:
            start_date = _date(dr_start)
        else:
            last_ts = _get_last_trade_session_date(conn, board, secid)
            if last_ts:
                parsed = None
                for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
                    try:
                        parsed = dt.datetime.strptime(last_ts, fmt).date()
                        break
                    except ValueError:
                        continue
                if not parsed:
                    parsed = today - dt.timedelta(days=1100)
                start_date = parsed + dt.timedelta(days=1)
            else:
                start_date = today - dt.timedelta(days=1100)
        if start_date > end_date:
            return 0
        base_from = _iso(start_date)
        base_till = _iso(end_date)
        start_offset = 0
        total_reported = None
        inserted = 0
        range_start_time = time.perf_counter()
        while True:
            payload = _fetch_page(board, secid, base_from, base_till, start_offset if start_offset else None, metrics=metrics)
            cols, rows, cursor_map = _extract_rows(payload)
            if total_reported is None:
                total_reported = cursor_map.get("TOTAL") or cursor_map.get("total")
            if not rows:
                break
            for r in rows:
                rec = _normalize_record(cols, r)
                values = [rec.get(c) for c in ORDERED_COLUMNS]
                try:
                    inserted += _insert_row_if_new(conn, values)
                except Exception:
                    # Если столбцы несовместимы / редкая ошибка — пропускаем строку
                    continue
            conn.commit()
            fetched_now = len(rows)
            start_offset += fetched_now
            if isinstance(total_reported, int) and start_offset >= total_reported:
                break
            if fetched_now == 0:
                break
        if metrics is not None:
            ranges = metrics.setdefault('ranges', [])
            ranges.append({
                'secid': secid,
                'from': base_from,
                'till': base_till,
                'inserted': inserted,
                'duration_sec': round(time.perf_counter() - range_start_time, 3),
            })
        # Recompute potentials if requested
        if recompute_potentials:
            try:
                if potentials_callback:
                    potentials_callback(Path(db_path))
                else:
                    # Lazy import to avoid circular costs if not needed
                    from . import main as _main  # type: ignore
                    _main.ComputePotentials(Path(db_path), store=True)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Potentials recompute failed for %s: %s", secid, exc)
        logging.info("GetMoexHistory %s %s inserted=%s interval %s..%s", board, secid, inserted, base_from, base_till)
        return inserted
    finally:
        conn.close()


def _read_perspective_secids(conn: sqlite3.Connection) -> list[str]:
    """Собрать список идентификаторов для загрузки цен.

    Ранее брали только первый непустой столбец (часто только один secid -> загружался один инструмент).
    Теперь объединяем значения всех кандидатов (secid / ticker / share), нормализуя в upper.
    Приоритет: если у строки есть SECID, используем его; иначе fallback к ticker; иначе share.
    """
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
    # Если есть и SECID и TICKER раздельно (например SECID пустой почти у всех кроме одной строки), но
    # большинство значений пришло из ticker — всё равно вернём объединение.
    result = sorted(collected)
    logging.info("perspective_shares: найдено %s инструментов для загрузки цен", len(result))
    return result


def _delete_old_rows(conn: sqlite3.Connection, board: str, secid: str, horizon_days: int) -> int:
    cutoff = dt.date.today() - dt.timedelta(days=horizon_days - 1)
    cutoff_iso = cutoff.isoformat()
    cur = conn.execute(
        f"DELETE FROM {TABLE_NAME} WHERE BOARDID=? AND SECID=? AND TRADE_SESSION_DATE<?",
        (board, secid, cutoff_iso)
    )
    return cur.rowcount if cur.rowcount is not None else 0


def _get_min_max_dates(conn: sqlite3.Connection, board: str, secid: str) -> tuple[Optional[str], Optional[str]]:
    cur = conn.execute(
        f"SELECT MIN(TRADE_SESSION_DATE), MAX(TRADE_SESSION_DATE) FROM {TABLE_NAME} WHERE BOARDID=? AND SECID=?",
        (board, secid)
    )
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def load_all_perspective(board: str = "TQBR", *, recompute_potentials: bool = False, horizon_days: int = DEFAULT_HORIZON_DAYS,
                          silent: bool = False) -> Dict[str, Any]:
    """Загрузить / обновить все перспективные бумаги с учётом скользящего окна.

    Логика:
      - Для новой бумаги: backfill horizon_days (по умолчанию 1100) дней.
      - Для существующей: догружаем только с (max_date+1 .. today).
      - После загрузки удаляем строки старше окна.
      - Пересчёт потенциалов (один раз) если запрошено.
      - Возвращаем агрегированные метрики.
    """
    level_before = logging.getLogger().level
    if silent:
        logging.getLogger().setLevel(logging.WARNING)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_table(conn)
        secids = _read_perspective_secids(conn)
    finally:
        conn.close()
    if not secids:
        secids = ["PLZL"]
    summary: Dict[str, Any] = {
        'per_security': {},
        'total_inserted': 0,
        'total_deleted_old': 0,
        'http_requests': 0,
        'retries': 0,
        'ranges': []
    }
    for s in secids:
        sec_metrics: Dict[str, Any] = {}
        try:
            with sqlite3.connect(DB_PATH) as conn2:
                ensure_table(conn2)
                min_dt, max_dt = _get_min_max_dates(conn2, board, s)
                # Decide interval
                today = dt.date.today()
                fetch_from: Optional[str] = None
                fetch_till: Optional[str] = None
                if max_dt:
                    # incremental from next day
                    try:
                        parsed = _date(max_dt)
                    except Exception:
                        parsed = today - dt.timedelta(days=horizon_days)
                    next_day = parsed + dt.timedelta(days=1)
                    if next_day <= today:
                        fetch_from = next_day.isoformat()
                        fetch_till = today.isoformat()
                else:
                    # new security full window
                    fetch_till = today.isoformat()
                    fetch_from = (today - dt.timedelta(days=horizon_days)).isoformat()
                if fetch_from and fetch_till:
                    inserted = GetMoexHistory(board=board, secid=s, dr_start=fetch_from, dr_end=fetch_till, metrics=sec_metrics)
                else:
                    inserted = 0
                with sqlite3.connect(DB_PATH) as conn3:
                    deleted_old = _delete_old_rows(conn3, board, s, horizon_days)
                    conn3.commit()
                sec_metrics['inserted'] = inserted
                sec_metrics['deleted_old'] = deleted_old
                summary['total_inserted'] += inserted
                summary['total_deleted_old'] += deleted_old
                # accumulate metrics
                summary['http_requests'] += sec_metrics.get('http_requests', 0)
                summary['retries'] += sec_metrics.get('retries', 0)
                if 'ranges' in sec_metrics:
                    summary['ranges'].extend(sec_metrics['ranges'])
                summary['per_security'][s] = sec_metrics
        except Exception as e:  # noqa: BLE001
            summary['per_security'][s] = {'error': str(e)}
            logging.error("Ошибка загрузки %s: %s", s, e)
    # batch potentials recompute once
    if recompute_potentials:
        try:
            from . import main as _main  # type: ignore
            _main.ComputePotentials(Path(DB_PATH), store=True)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Batch potentials recompute failed: %s", exc)
            summary['potentials_recompute_error'] = str(exc)
    if silent:
        logging.getLogger().setLevel(level_before)
    return summary


def daily_update_all(board: str = "TQBR", *, recompute_potentials: bool = True, horizon_days: int = DEFAULT_HORIZON_DAYS,
                     silent: bool = False) -> Dict[str, Any]:
    """Ежедневная догрузка всех бумаг с пересчётом потенциалов (один раз) и поддержанием окна."""
    return load_all_perspective(board=board, recompute_potentials=recompute_potentials, horizon_days=horizon_days, silent=silent)


def ensure_full_coverage(board: str = "TQBR", *, horizon_days: int = DEFAULT_HORIZON_DAYS,
                         recompute_potentials: bool = True, silent: bool = False) -> Dict[str, Any]:
    """Гарантировать, что история загружена *для всех* бумаг из perspective_shares.

    Логика:
      1. Собираем список идентификаторов (SECID / ticker) из perspective_shares.
      2. Определяем какие SECID уже присутствуют в moex_history_perspective_shares для указанного board.
      3. Для отсутствующих выполняем *полный* backfill окна horizon_days (независимо от того, была ли частичная история).
      4. Опционально один раз пересчитываем потенциалы.

    Возвращает словарь метрик:
        {
          'total_perspective': int,
          'already_present': int,
          'missing_before': int,
          'processed_missing': int,
          'per_security': { secid: { inserted:int, http_requests:int, retries:int, ranges:[...] } | {error: str}},
          'http_requests': int,
          'retries': int,
          'potentials_recomputed': bool,
          ...
        }
    """
    level_before = logging.getLogger().level
    if silent:
        logging.getLogger().setLevel(logging.WARNING)
    with sqlite3.connect(DB_PATH) as conn:
        ensure_table(conn)
        # Собираем идентификаторы аналогично _read_perspective_secids (используем саму функцию для консистентности)
        all_ids = _read_perspective_secids(conn)
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT SECID FROM {TABLE_NAME} WHERE BOARDID=?", (board,))
        existing = {r[0] for r in cur.fetchall() if r[0]}
    missing = [s for s in all_ids if s not in existing]
    summary: Dict[str, Any] = {
        'board': board,
        'horizon_days': horizon_days,
        'total_perspective': len(all_ids),
        'already_present': len(existing),
        'missing_before': len(missing),
        'processed_missing': 0,
        'per_security': {},
        'http_requests': 0,
        'retries': 0,
        'started_ts': time.time(),
    }
    if not missing:
        summary['note'] = 'All perspective shares already have history.'
    else:
        today = dt.date.today()
        for idx, secid in enumerate(missing, 1):
            metrics: Dict[str, Any] = {}
            try:
                start_date = (today - dt.timedelta(days=horizon_days)).isoformat()
                inserted = GetMoexHistory(board=board, secid=secid, dr_start=start_date, dr_end=today.isoformat(), metrics=metrics)
                summary['per_security'][secid] = {
                    'inserted': inserted,
                    'http_requests': metrics.get('http_requests', 0),
                    'retries': metrics.get('retries', 0),
                    'ranges': metrics.get('ranges', []),
                }
                summary['processed_missing'] += 1
                summary['http_requests'] += metrics.get('http_requests', 0)
                summary['retries'] += metrics.get('retries', 0)
            except Exception as exc:  # noqa: BLE001
                summary['per_security'][secid] = {'error': str(exc)}
                logging.error("ensure_full_coverage: ошибка загрузки %s: %s", secid, exc)
    # Coverage after
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(DISTINCT SECID) FROM {TABLE_NAME} WHERE BOARDID=?", (board,))
        summary['coverage_after'] = cur.fetchone()[0]
    if recompute_potentials:
        try:
            from . import main as _main  # type: ignore
            _main.ComputePotentials(Path(DB_PATH), store=True)
            summary['potentials_recomputed'] = True
        except Exception as exc:  # noqa: BLE001
            summary['potentials_recomputed'] = False
            summary['potentials_recompute_error'] = str(exc)
            logging.warning("ensure_full_coverage: potentials recompute failed: %s", exc)
    summary['duration_sec'] = round(time.time() - summary['started_ts'], 2)
    if silent:
        logging.getLogger().setLevel(level_before)
    logging.info("ensure_full_coverage: processed %s missing of %s total perspective (coverage=%s)",
                 summary['processed_missing'], summary['total_perspective'], summary.get('coverage_after'))
    return summary


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="MOEX history loader (perspective shares) with sliding window")
    parser.add_argument('--board', default='TQBR', help='Board ID (default TQBR)')
    parser.add_argument('--horizon-days', type=int, default=DEFAULT_HORIZON_DAYS, help='Sliding window size in days (default 1100)')
    parser.add_argument('--no-potentials', action='store_true', help='Do not recompute potentials after batch')
    parser.add_argument('--silent', action='store_true', help='Reduce logging (warnings only)')
    parser.add_argument('--log-level', default='INFO', help='Logging level (default INFO)')
    parser.add_argument('--ensure-full-coverage', action='store_true', help='Backfill missing perspective shares that have no history yet')
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s | %(levelname)s | %(message)s")
    if args.ensure_full_coverage:
        res = ensure_full_coverage(board=args.board, horizon_days=args.horizon_days, recompute_potentials=not args.no_potentials, silent=args.silent)
        print("Full coverage summary:")
        print(f"Perspective total={res['total_perspective']} already_present={res['already_present']} missing_before={res['missing_before']} processed_missing={res['processed_missing']}")
        print(f"Coverage after={res.get('coverage_after')} HTTP={res['http_requests']} Retries={res['retries']} PotentialsRecomputed={res.get('potentials_recomputed')}")
    else:
        res = daily_update_all(board=args.board, recompute_potentials=not args.no_potentials, horizon_days=args.horizon_days, silent=args.silent)
        print("Summary:")
        print(f"Inserted={res['total_inserted']} DeletedOld={res['total_deleted_old']} HTTP={res['http_requests']} Retries={res['retries']}")
        print(f"Securities processed={len(res['per_security'])}")
