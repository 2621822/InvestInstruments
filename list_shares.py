import sqlite3
from collections import defaultdict

TICKERS = [
    "ASTR","BELU","DELI","ETLN","EUTR","HNFG","IRAO","LEAS",
    "OZPH","PRMD","RAGR","RENI","RTKMP","SFIN","SMLT","SNGS",
    "SOFL","SVAV","UGLD","VSEH"
]

def main():
    conn = sqlite3.connect('GorbunovInvestInstruments.db')
    c = conn.cursor()
    c.execute('select ticker, name, uid from perspective_shares')
    rows = c.fetchall()
    by_ticker = { (r[0] or '').upper(): (r[1], r[2]) for r in rows }

    # counts of forecasts / targets
    cf_counts = defaultdict(int)
    ct_counts = defaultdict(int)
    for (tbl, dct) in (("consensus_forecasts", cf_counts), ("consensus_targets", ct_counts)):
        try:
            c.execute(f'SELECT ticker, COUNT(*) FROM {tbl} GROUP BY ticker')
            for t, cnt in c.fetchall():
                if t:
                    dct[t.upper()] = cnt
        except sqlite3.DatabaseError:
            pass

    print('TOTAL SHARES IN TABLE:', len(rows))
    print('-' * 70)
    missing = []
    for t in TICKERS:
        if t in by_ticker:
            name, uid = by_ticker[t]
            print(f"{t:<6} | {name or '?':<30} | UID={uid} | CF={cf_counts.get(t,0)} | CT={ct_counts.get(t,0)}")
        else:
            missing.append(t)
    if missing:
        print('\nMISSING TICKERS (not found in perspective_shares):', ', '.join(missing))
    else:
        print('\nAll requested tickers are present.')

if __name__ == '__main__':
    main()
