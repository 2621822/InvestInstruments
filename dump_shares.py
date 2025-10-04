import sqlite3, json
out = []
conn = sqlite3.connect('GorbunovInvestInstruments.db')
cur = conn.cursor()
cur.execute('select ticker,name,uid,figi,isin from perspective_shares order by ticker')
for t,name,uid,figi,isin in cur.fetchall():
    out.append({'ticker': t, 'name': name, 'uid': uid, 'figi': figi, 'isin': isin})
conn.close()
with open('shares_snapshot.json','w',encoding='utf-8') as f:
    json.dump(out,f,ensure_ascii=False,indent=2)
print(f'Dumped {len(out)} records to shares_snapshot.json')
