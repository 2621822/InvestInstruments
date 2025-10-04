import sqlite3
from pathlib import Path

from GorbunovInvestInstruments.potentials import compute_all

DB_PATH = Path("GorbunovInvestInstruments.db")


def test_potentials_schema_no_computed_at():
    # Запускаем расчет (создаст таблицу при необходимости)
    compute_all(store=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(instrument_potentials)")
        cols = {r[1] for r in cur.fetchall()}
        assert 'computedDate' in cols
        assert 'computedAt' not in cols
        # Убедимся что ключ работает: вставим дублирующую строку
        cur.execute("SELECT uid, ticker, computedDate FROM instrument_potentials LIMIT 1")
        row = cur.fetchone()
        if row:
            uid, ticker, cdate = row
            # Повторная вставка того же uid/date должна upsert-нуть без ошибок
            cur.execute(
                "INSERT OR REPLACE INTO instrument_potentials (uid,ticker,computedDate,prevClose,consensusPrice,pricePotentialRel,isStale) VALUES (?,?,?,?,?,?,?)",
                (uid, ticker, cdate, None, None, None, 0)
            )
            conn.commit()
        # Проверим отсутствие повторов по (uid, computedDate)
        cur.execute("SELECT uid, computedDate, COUNT(*) FROM instrument_potentials GROUP BY uid, computedDate HAVING COUNT(*)>1")
        assert not cur.fetchall(), "Дубликаты по (uid, computedDate) обнаружены"
