"""Слой доступа к БД (SQLite / DuckDB) для хранения:
* moex_shares_history
* perspective_shares

Выбор backend через переменную окружения INVEST_DB_BACKEND (sqlite|duckdb), по умолчанию sqlite.
"""
from __future__ import annotations
import os
import sqlite3
from pathlib import Path
try:
    import duckdb  # type: ignore
except ImportError:  # duckdb опционален
    duckdb = None  # type: ignore

DB_FILE = os.getenv("INVEST_DB_FILE", "invest_data.db")
BACKEND = os.getenv("INVEST_DB_BACKEND", "sqlite").lower()
_SCHEMA_INITIALIZED = False  # глобальный флаг чтобы не пересоздавать индексы

SCHEMA_SQL = """-- reserved (нет общей служебной таблицы)"""

# Основная таблица исторических данных по акциям
MOEX_SHARES_HISTORY_SQL_SQLITE = """
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
);
"""

MOEX_SHARES_HISTORY_SQL_DUCKDB = """
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
);
"""

# Специфичная таблица исторических данных перспективных бумаг (расширенный набор полей)

# Таблица перспективных бумаг
PERSPECTIVE_SHARES_SQL = """
CREATE TABLE IF NOT EXISTS perspective_shares (
    ticker TEXT,
    name TEXT,
    uid TEXT PRIMARY KEY,
    secid TEXT,
    isin TEXT,
    figi TEXT,
    classCode TEXT,
    instrumentType TEXT,
    assetUid TEXT
);
"""

# Таблица консенсус-прогнозов (агрегированный консенсус по бумаге)
CONSENSUS_FORECASTS_SQL = """
CREATE TABLE IF NOT EXISTS consensus_forecasts (
    uid TEXT NOT NULL,
    ticker TEXT,
    recommendation TEXT,
    recommendationDate TEXT NOT NULL, -- ISO date (YYYY-MM-DD) или дата ответа
    currency TEXT,
    priceConsensus REAL,
    minTarget REAL,
    maxTarget REAL,
    currentPrice REAL,
    priceChange REAL,
    priceChangeRel REAL,
    PRIMARY KEY (uid, recommendationDate)
);
"""

# Таблица индивидуальных целевых цен (прогнозы аналитиков)
CONSENSUS_TARGETS_SQL = """
CREATE TABLE IF NOT EXISTS consensus_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid TEXT NOT NULL,
    ticker TEXT,
    company TEXT NOT NULL,
    recommendation TEXT,
    recommendationDate TEXT NOT NULL,
    currency TEXT,
    targetPrice REAL,
    showName TEXT
);
"""

# Таблица рассчитанных потенциалов инструмента (история расчётов)
INSTRUMENT_POTENTIALS_SQL = """
CREATE TABLE IF NOT EXISTS instrument_potentials (
    uid TEXT NOT NULL,
    ticker TEXT,
    computedAt TEXT NOT NULL, -- UTC ISO timestamp
    prevClose REAL,
    consensusPrice REAL,
    pricePotentialRel REAL,
    PRIMARY KEY (uid, computedAt)
);
"""

# Новая таблица расчёта потенциалов акций (shares_potentials) по спецификации
SHARES_POTENTIALS_SQL = """
CREATE TABLE IF NOT EXISTS shares_potentials (
    uid TEXT NOT NULL,
    secid TEXT NOT NULL,
    ticker TEXT,
    computedAt TEXT NOT NULL, -- UTC ISO timestamp
    prevClose REAL,
    consensusPrice REAL,
    pricePotentialRel REAL,
    PRIMARY KEY (uid, computedAt)
);
"""


def get_connection():
    if BACKEND == "sqlite":
        conn = sqlite3.connect(DB_FILE, timeout=5.0)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
        except Exception:
            pass
        return conn
    elif BACKEND == "duckdb":
        if duckdb is None:
            raise RuntimeError("duckdb пакет не установлен")
        return duckdb.connect(DB_FILE)
    else:
        raise ValueError("Неизвестный backend: " + BACKEND)


def init_schema():
    global _SCHEMA_INITIALIZED
    def _safe_exec(conn, sql: str, skip_on_lock: bool = False):
        import time, logging
        log = logging.getLogger(__name__)
        attempts = 0
        max_attempts = 10
        delay = 0.15
        while True:
            try:
                conn.execute(sql)
                return True
            except sqlite3.OperationalError as ex:
                msg = str(ex).lower()
                if "locked" in msg and attempts < max_attempts:
                    time.sleep(delay)
                    attempts += 1
                    delay *= 1.5  # экспоненциальный рост задержки
                    continue
                if "locked" in msg and skip_on_lock:
                    log.warning("Skip SQL due to persistent lock after %s attempts: %s", attempts, sql.split('\n')[0][:120])
                    return False
                raise
    with get_connection() as conn:
        if BACKEND == "sqlite":
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=7000")
            except Exception:
                pass
        _safe_exec(conn, SCHEMA_SQL)
        # создаём новые таблицы
        if BACKEND == "sqlite":
            _safe_exec(conn, MOEX_SHARES_HISTORY_SQL_SQLITE)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_moex_history_secid_tradedate ON moex_shares_history(SECID, TRADEDATE)")
        else:
            _safe_exec(conn, MOEX_SHARES_HISTORY_SQL_DUCKDB)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_moex_history_secid_tradedate ON moex_shares_history(SECID, TRADEDATE)")
        _safe_exec(conn, PERSPECTIVE_SHARES_SQL)
        _safe_exec(conn, CONSENSUS_FORECASTS_SQL)
        _safe_exec(conn, CONSENSUS_TARGETS_SQL)
        _safe_exec(conn, INSTRUMENT_POTENTIALS_SQL)
        _safe_exec(conn, SHARES_POTENTIALS_SQL)
        if not _SCHEMA_INITIALIZED:
            # Индексы (создаём один раз) – при конфликте блокировки пропускаем
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_consensus_uid_date ON consensus_forecasts(uid, recommendationDate DESC)", skip_on_lock=True)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_targets_uid_date ON consensus_targets(uid, recommendationDate DESC)", skip_on_lock=True)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_instrument_potentials_rel ON instrument_potentials(pricePotentialRel)", skip_on_lock=True)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_shares_potentials_rel ON shares_potentials(pricePotentialRel)", skip_on_lock=True)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_shares_potentials_uid_computedAt ON shares_potentials(uid, computedAt)", skip_on_lock=True)
            _safe_exec(conn, "CREATE INDEX IF NOT EXISTS idx_shares_potentials_uid_rel ON shares_potentials(uid, pricePotentialRel)", skip_on_lock=True)
            _SCHEMA_INITIALIZED = True
        if BACKEND == "sqlite":
            conn.commit()
    # Миграция новых столбцов (если таблица уже была создана без них)
    if BACKEND == "sqlite":
        with get_connection() as conn:
            cur = conn.execute("PRAGMA table_info(consensus_forecasts)")
            existing_cols = {r[1] for r in cur.fetchall()}
            for col in ["currentPrice", "priceChange", "priceChangeRel"]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE consensus_forecasts ADD COLUMN {col} REAL")
            conn.commit()
    elif BACKEND == "duckdb":
        with get_connection() as conn:
            cur = conn.execute("DESCRIBE consensus_forecasts")
            existing_cols = {r[0] for r in cur.fetchall()}
            for col in ["currentPrice", "priceChange", "priceChangeRel"]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE consensus_forecasts ADD COLUMN {col} DOUBLE")
    # Миграция consensus_targets: если старая PK без id – создаём новую таблицу и копируем
    with get_connection() as conn:
        try:
            # Проверим наличие столбца id
            if BACKEND == 'sqlite':
                cur = conn.execute("PRAGMA table_info(consensus_targets)")
                cols = {r[1] for r in cur.fetchall()}
                if 'id' not in cols:
                    conn.execute("ALTER TABLE consensus_targets RENAME TO consensus_targets_old")
                    conn.execute(CONSENSUS_TARGETS_SQL)
                    conn.execute("INSERT INTO consensus_targets(uid,ticker,company,recommendation,recommendationDate,currency,targetPrice,showName) SELECT uid,ticker,company,recommendation,recommendationDate,currency,targetPrice,showName FROM consensus_targets_old")
                    conn.execute("DROP TABLE consensus_targets_old")
                    conn.commit()
            else:  # duckdb
                cur = conn.execute("DESCRIBE consensus_targets")
                cols = {r[0] for r in cur.fetchall()}
                if 'id' not in cols:
                    conn.execute("ALTER TABLE consensus_targets RENAME TO consensus_targets_old")
                    conn.execute(CONSENSUS_TARGETS_SQL)
                    conn.execute("INSERT INTO consensus_targets(uid,ticker,company,recommendation,recommendationDate,currency,targetPrice,showName) SELECT uid,ticker,company,recommendation,recommendationDate,currency,targetPrice,showName FROM consensus_targets_old")
                    conn.execute("DROP TABLE consensus_targets_old")
        except Exception:
            pass


def get_last_tradedate(secid: str) -> str | None:
    """Получить последнюю сохранённую дату торговли для бумаги.
    Возвращает строку TRADEDATE или None."""
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT TRADEDATE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1"
            if BACKEND == "sqlite" else
            "SELECT TRADEDATE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1",
            (secid,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def list_perspective_secids() -> list[str]:
    """Вернуть список SECID из таблицы perspective_shares (исключая NULL)."""
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT secid FROM perspective_shares WHERE secid IS NOT NULL AND secid <> ''"
        )
        return [r[0] for r in cur.fetchall()]

