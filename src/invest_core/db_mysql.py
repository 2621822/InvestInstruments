"""db_mysql.py

Чистый MySQL слой доступа; замена повреждённого db.py.
"""
from __future__ import annotations
import os
import logging
from .config_loader import cfg_val, get_config  # Новый источник конфигурации

try:
    import pymysql  # type: ignore
except ImportError as ex:
    raise RuntimeError("Установите PyMySQL: pip install PyMySQL") from ex

log = logging.getLogger(__name__)

try:
    from . import db_settings  # type: ignore
    _CFG = getattr(db_settings, 'DB_CONFIG', {})
except Exception:
    _CFG = {}

# Загрузка настроек приоритетно из config.ini, затем из окружения, затем из db_settings/_CFG.
# (ENV остаётся fallback чтобы не ломать существующие сценарии запуска.)
DB_HOST = os.getenv('INVEST_DB_HOST') or cfg_val('database', 'host', _CFG.get('HOST', 'localhost'))
DB_PORT = int(os.getenv('INVEST_DB_PORT') or cfg_val('database', 'port', _CFG.get('PORT', 3306)))
DB_NAME = os.getenv('INVEST_DB_NAME') or cfg_val('database', 'name', _CFG.get('NAME', 'invest'))
DB_USER = os.getenv('INVEST_DB_USER') or cfg_val('database', 'user', _CFG.get('USER', 'root'))
DB_PASSWORD = os.getenv('INVEST_DB_PASSWORD') or cfg_val('database', 'password', _CFG.get('PASSWORD', ''))
DB_CHARSET = os.getenv('INVEST_DB_CHARSET') or cfg_val('database', 'charset', _CFG.get('CHARSET', 'utf8mb4'))

_SCHEMA_INITIALIZED = False

def get_connection():
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset=DB_CHARSET,
        autocommit=False,
    )
    class _Conn:
        def __init__(self, inner):
            self._inner = inner
        def execute(self, sql: str, params: tuple | list | None = None):
            cur = self._inner.cursor()
            cur.execute(sql, params or ())
            # Авто-commit для операций изменения данных вне контекстного менеджера.
            try:
                first = sql.strip().split()[0].upper()
                if first in {"INSERT","UPDATE","DELETE","REPLACE","CREATE","DROP","ALTER"}:
                    self._inner.commit()
            except Exception:
                pass
            return cur
        def executemany(self, sql: str, seq):
            cur = self._inner.cursor()
            cur.executemany(sql, seq)
            # Авто-commit батчевых операций
            try:
                first = sql.strip().split()[0].upper()
                if first in {"INSERT","UPDATE","DELETE","REPLACE"}:
                    self._inner.commit()
            except Exception:
                pass
            return cur
        def commit(self):
            self._inner.commit()
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                try:
                    self._inner.commit()
                except Exception:
                    pass
            try:
                self._inner.close()
            except Exception:
                pass
    return _Conn(conn)

def exec_sql(conn, sql: str, params: tuple | list | None = None):
    if '?' in sql:
        sql = sql.replace('?', '%s')
    return conn.execute(sql, params or ())

def _create_tables(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS moex_shares_history (
        SECID VARCHAR(32) NOT NULL,
        TRADEDATE DATE NOT NULL,
        BOARDID VARCHAR(16),
        OPEN DOUBLE,
        CLOSE DOUBLE,
        HIGH DOUBLE,
        LOW DOUBLE,
        WAPRICE DOUBLE,
        SHORTNAME VARCHAR(128),
        NUMTRADES INT,
        VOLUME BIGINT,
        VALUE DOUBLE,
        WAVAL BIGINT,
        PRIMARY KEY (SECID, TRADEDATE)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS perspective_shares (
        ticker VARCHAR(32),
        name VARCHAR(255),
        uid VARCHAR(48) PRIMARY KEY,
        secid VARCHAR(32),
        isin VARCHAR(32),
        figi VARCHAR(32),
        classCode VARCHAR(32),
        instrumentType VARCHAR(32),
        assetUid VARCHAR(48)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS consensus_forecasts (
        uid VARCHAR(48) NOT NULL,
        ticker VARCHAR(32),
        recommendation VARCHAR(64),
        recommendationDate DATE NOT NULL,
        currency VARCHAR(16),
        priceConsensus DOUBLE,
        minTarget DOUBLE,
        maxTarget DOUBLE,
        currentPrice DOUBLE,
        priceChange DOUBLE,
        priceChangeRel DOUBLE,
        PRIMARY KEY (uid, recommendationDate)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS consensus_targets (
        uid VARCHAR(48) NOT NULL,
        ticker VARCHAR(32),
        company VARCHAR(255) NOT NULL,
        recommendation VARCHAR(64),
        recommendationDate DATE NOT NULL,
        currency VARCHAR(16),
        targetPrice DOUBLE,
        showName VARCHAR(255),
        PRIMARY KEY (uid, recommendationDate, company)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS shares_potentials (
        uid VARCHAR(48) NOT NULL,
        secid VARCHAR(32) NOT NULL,
        ticker VARCHAR(32),
        computedAt VARCHAR(32) NOT NULL,
        prevClose DOUBLE,
        consensusPrice DOUBLE,
        pricePotentialRel DOUBLE,
        PRIMARY KEY (uid, computedAt)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")

def _create_indexes(conn):
    def ensure(table: str, index: str, ddl: str):
        cur = conn.execute("SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME=%s", (DB_NAME, table, index))
        if not cur.fetchone():
            conn.execute(ddl)
    ensure('moex_shares_history', 'idx_moex_history_secid_tradedate', 'CREATE INDEX idx_moex_history_secid_tradedate ON moex_shares_history(SECID, TRADEDATE)')
    ensure('consensus_forecasts', 'idx_consensus_uid_date', 'CREATE INDEX idx_consensus_uid_date ON consensus_forecasts(uid, recommendationDate)')
    # Индекс по (uid,recommendationDate) теперь частично покрыт составным PRIMARY KEY; отдельный индекс не создаём.
    ensure('shares_potentials', 'idx_shares_potentials_rel', 'CREATE INDEX idx_shares_potentials_rel ON shares_potentials(pricePotentialRel)')
    ensure('shares_potentials', 'idx_shares_potentials_uid_computedAt', 'CREATE INDEX idx_shares_potentials_uid_computedAt ON shares_potentials(uid, computedAt)')
    ensure('shares_potentials', 'idx_shares_potentials_uid_rel', 'CREATE INDEX idx_shares_potentials_uid_rel ON shares_potentials(uid, pricePotentialRel)')

def init_schema():
    global _SCHEMA_INITIALIZED
    with get_connection() as conn:
        _create_tables(conn)
        # --- Миграция: удалить столбец id если он существовал в старой схеме и ввести составной PK ---
        try:
            cur = conn.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME='consensus_targets'", (DB_NAME,))
            cols = {r[0] for r in cur.fetchall()}
            if 'id' in cols:
                # Создаём новую таблицу со схемой без id и с составным PK
                conn.execute("""
                CREATE TABLE IF NOT EXISTS consensus_targets_new (
                    uid VARCHAR(48) NOT NULL,
                    ticker VARCHAR(32),
                    company VARCHAR(255) NOT NULL,
                    recommendation VARCHAR(64),
                    recommendationDate DATE NOT NULL,
                    currency VARCHAR(16),
                    targetPrice DOUBLE,
                    showName VARCHAR(255),
                    PRIMARY KEY (uid, recommendationDate, company)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
                # Перенос данных: берём последнюю (по максимальному id) запись для каждой тройки ключа
                conn.execute("""
                INSERT INTO consensus_targets_new(uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName)
                SELECT t.uid, t.ticker, t.company, t.recommendation, t.recommendationDate, t.currency, t.targetPrice, t.showName
                FROM consensus_targets t
                INNER JOIN (
                  SELECT uid, recommendationDate, company, MAX(id) AS mid
                  FROM consensus_targets
                  GROUP BY uid, recommendationDate, company
                ) m ON t.uid=m.uid AND t.recommendationDate=m.recommendationDate AND t.company=m.company AND t.id=m.mid
                """)
                conn.execute("RENAME TABLE consensus_targets TO consensus_targets_old")
                conn.execute("RENAME TABLE consensus_targets_new TO consensus_targets")
                conn.execute("DROP TABLE consensus_targets_old")
        except Exception:
            pass
        # Удаление устаревшей таблицы instrument_potentials если осталась от старой схемы
        try:
            cur2 = conn.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME='instrument_potentials'", (DB_NAME,))
            if cur2.fetchone():
                conn.execute("DROP TABLE IF EXISTS instrument_potentials")
        except Exception:
            pass
        if not _SCHEMA_INITIALIZED:
            _create_indexes(conn)
            _SCHEMA_INITIALIZED = True
        try:
            conn.commit()
        except Exception:
            pass

def get_last_tradedate(secid: str) -> str | None:
    if not secid:
        return None
    with get_connection() as conn:
        cur = exec_sql(conn, "SELECT TRADEDATE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1", (secid,))
        row = cur.fetchone()
        return row[0] if row else None

def list_perspective_secids() -> list[str]:
    with get_connection() as conn:
        cur = exec_sql(conn, "SELECT secid FROM perspective_shares WHERE secid IS NOT NULL AND secid <> ''")
        return [r[0] for r in cur.fetchall()]

__all__ = [
    'get_connection', 'exec_sql', 'init_schema',
    'get_last_tradedate', 'list_perspective_secids',
    'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_CHARSET',
]

def dedupe_consensus_targets() -> dict:
    """Удалить дубликаты из consensus_targets, оставив по одной строке на ключ.

    Ключ: (uid, recommendationDate, company).
    Стратегия: агрегируем и создаём временную таблицу с выбором:
      ticker          -> MIN(ticker)
      recommendation  -> MIN(recommendation)
      currency        -> MIN(currency)
      targetPrice     -> MAX(targetPrice) (берём наибольший таргет)
      showName        -> MIN(showName)
    Затем переименовываем таблицу обратно.
    Возвращает статистику.
    """
    init_schema()
    with get_connection() as conn:
        # Подсчёт групп с >1 строкой
        cur = conn.execute("""
            SELECT COUNT(*) AS dup_count FROM (
              SELECT uid, recommendationDate, company, COUNT(*) c
              FROM consensus_targets
              GROUP BY uid, recommendationDate, company
              HAVING c > 1
            ) d
        """)
        row = cur.fetchone()
        duplicates_groups = row[0] if row else 0
        if duplicates_groups == 0:
            return {"status": "no-duplicates", "duplicate_groups": 0}
        # Создать временную агрегированную таблицу
        conn.execute("""
            CREATE TABLE consensus_targets_dedup (
                uid VARCHAR(48) NOT NULL,
                ticker VARCHAR(32),
                company VARCHAR(255) NOT NULL,
                recommendation VARCHAR(64),
                recommendationDate DATE NOT NULL,
                currency VARCHAR(16),
                targetPrice DOUBLE,
                showName VARCHAR(255),
                PRIMARY KEY (uid, recommendationDate, company)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.execute("""
            INSERT INTO consensus_targets_dedup(uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName)
            SELECT uid,
                   MIN(ticker) AS ticker,
                   company,
                   MIN(recommendation) AS recommendation,
                   recommendationDate,
                   MIN(currency) AS currency,
                   MAX(targetPrice) AS targetPrice,
                   MIN(showName) AS showName
            FROM consensus_targets
            GROUP BY uid, recommendationDate, company
        """)
        # Подсчёт до/после
        cur_before = conn.execute("SELECT COUNT(*) FROM consensus_targets")
        total_before = cur_before.fetchone()[0]
        cur_after = conn.execute("SELECT COUNT(*) FROM consensus_targets_dedup")
        total_after = cur_after.fetchone()[0]
        removed = total_before - total_after
        # Переименование
        conn.execute("RENAME TABLE consensus_targets TO consensus_targets_orig")
        conn.execute("RENAME TABLE consensus_targets_dedup TO consensus_targets")
        conn.execute("DROP TABLE consensus_targets_orig")
        return {
            "status": "deduped",
            "duplicate_groups": duplicates_groups,
            "removed_rows": removed,
            "final_rows": total_after,
        }
