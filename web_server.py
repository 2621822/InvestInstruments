"""web_server.py

Минимальный FastAPI сервер поверх текущего MySQL слоя.
Возможности:
  GET /health              -> состояние и версия
  GET /potentials/top?limit=10 -> топ-N текущих потенциалов (из последнего computedAt)
  GET /summary/daily       -> последняя JSON строка из daily_history_job.log
  POST /run/daily-job      -> триггернуть запуск daily_history_job (async fire-and-forget)

Запуск:
  uvicorn web_server:app --host 0.0.0.0 --port 8000

"""
from __future__ import annotations
import os
import json
import threading
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# Ленивая загрузка MySQL слоя
from src.invest_core import db_mysql
from src.invest_core.potentials import GetTopSharePotentials

LOG_FILE = 'daily_history_job.log'

app = FastAPI(title="InvestInstruments API", version="1.0")

@app.get('/health')
def health():
    return {
        'status': 'ok',
        'time': datetime.utcnow().isoformat() + 'Z',
        'db': {
            'host': db_mysql.DB_HOST,
            'name': db_mysql.DB_NAME,
            'user': db_mysql.DB_USER,
        },
    }

@app.get('/potentials/top')
def top_potentials(limit: int = 10):
    if limit <= 0:
        raise HTTPException(status_code=400, detail='limit must be > 0')
    try:
        data = GetTopSharePotentials(limit=limit).get('data') or []
        return {'limit': limit, 'count': len(data), 'data': data}
    except Exception as ex:  # noqa
        raise HTTPException(status_code=500, detail=str(ex))

@app.get('/summary/daily')
def daily_summary():
    if not os.path.exists(LOG_FILE):
        raise HTTPException(status_code=404, detail='log file not found')
    last_json = None
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if 'DailyUnifiedJSON ' in line:
                # Берём последнюю
                payload = line.split('DailyUnifiedJSON ', 1)[1].strip()
                last_json = payload
    if not last_json:
        return {'status': 'no-summary'}
    try:
        return json.loads(last_json)
    except json.JSONDecodeError:
        return {'status': 'corrupt-json', 'raw': last_json}

def _run_job_thread(args: dict):
    import daily_history_job  # локальный импорт
    daily_history_job.run(
        board=args.get('board'),
        top_limit=args.get('top_limit'),
        collapse_duplicates=args.get('collapse', False),
    )

@app.post('/run/daily-job')
def run_daily_job(board: str | None = None, top_limit: int | None = None, collapse: bool = False):
    # Fire-and-forget поток, чтобы не держать запрос до окончания.
    t = threading.Thread(target=_run_job_thread, args=({'board': board, 'top_limit': top_limit, 'collapse': collapse},), daemon=True)
    t.start()
    return {'status': 'started', 'board': board, 'top_limit': top_limit, 'collapse': collapse}

@app.get('/')
def root():
    return JSONResponse({'service': 'InvestInstruments API', 'docs': '/docs'})

__all__ = ['app']
