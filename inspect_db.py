import sqlite3, json, sys, os
DB='GorbunovInvestInstruments.db'
if not os.path.exists(DB):
    print('DB missing')
    sys.exit(1)
with sqlite3.connect(DB) as conn:
    cur=conn.cursor()
    # perspective_shares summary
    try:
        cur.execute("PRAGMA table_info(perspective_shares)")
        cols=[r[1] for r in cur.fetchall()]
    except Exception as e:
        print('perspective_shares not found', e)
        cols=[]
    print('perspective_shares columns:', cols)
    if cols:
        cur.execute('SELECT COUNT(*) FROM perspective_shares')
        total=cur.fetchone()[0]
        print('perspective_shares rows:', total)
        # distinct values per candidate column
        candidates=[c for c in cols if c.lower() in ('secid','ticker','share')]
        for c in candidates:
            cur.execute(f'SELECT COUNT(DISTINCT {c}) FROM perspective_shares WHERE {c} IS NOT NULL AND TRIM({c})<>""')
            print(f'distinct {c}:', cur.fetchone()[0])
        cur.execute('SELECT * FROM perspective_shares LIMIT 5')
        print('sample perspective_shares rows:', cur.fetchall())
    # moex history coverage
    try:
        cur.execute('SELECT COUNT(*) FROM moex_history_perspective_shares')
        print('history rows:', cur.fetchone()[0])
        cur.execute('SELECT COUNT(DISTINCT SECID) FROM moex_history_perspective_shares')
        print('history distinct SECID:', cur.fetchone()[0])
        cur.execute('SELECT SECID, MIN(TRADE_SESSION_DATE), MAX(TRADE_SESSION_DATE), COUNT(*) cnt FROM moex_history_perspective_shares GROUP BY SECID ORDER BY cnt DESC LIMIT 10')
        print('top10 secid coverage:', cur.fetchall())
    except Exception as e:
        print('history table error', e)
print('DONE')
