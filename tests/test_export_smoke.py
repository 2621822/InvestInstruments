import os
from pathlib import Path
import sqlite3

from GorbunovInvestInstruments import potentials
from GorbunovInvestInstruments.exporting import export_potentials

DB = Path("GorbunovInvestInstruments.db")


def ensure_min_tables():
    with sqlite3.connect(DB) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS perspective_shares (uid TEXT PRIMARY KEY, ticker TEXT, secid TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS consensus_forecasts (id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT, ticker TEXT, recommendation TEXT, recommendationDate TEXT, currency TEXT, priceConsensus REAL, minTarget REAL, maxTarget REAL)")
        # Dummy instrument if empty
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM perspective_shares")
        if cur.fetchone()[0] == 0:
            conn.execute("INSERT INTO perspective_shares (uid, ticker, secid) VALUES (?,?,?)", ("dummy-uid", "DUMMY", "DUMMY"))
            # consensus row
            conn.execute("INSERT INTO consensus_forecasts (uid,ticker,recommendation,recommendationDate,currency,priceConsensus,minTarget,maxTarget) VALUES (?,?,?,?,?,?,?,?)",
                         ("dummy-uid", "DUMMY", "BUY", "2025-10-01T00:00:00Z", "RUB", 100.0, 90.0, 110.0))
        conn.commit()


def test_export_smoke(tmp_path):
    ensure_min_tables()
    # Compute potentials (creates table & row)
    potentials.compute_all(store=True)
    out_xlsx = tmp_path / "potentials_smoke.xlsx"
    out_json = tmp_path / "potentials_smoke.json"
    export_potentials(str(out_xlsx), str(out_json))
    assert out_xlsx.exists() and out_xlsx.stat().st_size > 0
    assert out_json.exists() and out_json.stat().st_size > 0
