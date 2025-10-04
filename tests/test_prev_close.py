import sqlite3
import os
from pathlib import Path

from GorbunovInvestInstruments.potentials import _get_prev_close  # type: ignore

DB_PATH = Path('GorbunovInvestInstruments.db')


def ensure_table():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS moex_history_perspective_shares (
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
        )""")
        conn.commit()


def test_get_prev_close_inserts_and_reads():
    ticker = 'TESTSEC'
    ensure_table()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"DELETE FROM moex_history_perspective_shares WHERE SECID=?", (ticker,))
        conn.execute(
            f"INSERT INTO moex_history_perspective_shares (BOARDID,TRADEDATE,SHORTNAME,SECID,NUMTRADES,VALUE,OPEN,LOW,HIGH,LEGALCLOSEPRICE,WAPRICE,CLOSE,VOLUME,MARKETPRICE2,MARKETPRICE3,ADMITTEDQUOTE,MP2VALTRD,MARKETPRICE3TRADESVALUE,ADMITTEDVALUE,WAVAL,TRADINGSESSION,CURRENCYID,TRENDCLSPR,TRADE_SESSION_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("TQBR","2025-10-01","TEST",ticker,10,1000,100,95,105,100,100,101.23,1000,0,0,None,0,0,None,0,1,"RUB",0,"2025-10-01")
        )
        conn.commit()
    price, d = _get_prev_close(ticker)
    assert price == 101.23
    assert d in ("2025-10-01",)
