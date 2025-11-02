"""Ежедневный запуск догрузки истории MOEX.

Использование:
  python daily_history_job.py

Добавьте в планировщик (Windows Task Scheduler) или через schtasks.
"""
from __future__ import annotations
import sys
import os
import time
import sqlite3
from datetime import datetime, UTC
sys.path.append('d:/#Work/#Invest/Project/src')
from invest_core.moex_history import FillingMoexHistory  # noqa: E402
from invest_core.potentials import FillingPotentialData  # noqa: E402
from invest_core.forecasts import FillingConsensusData  # noqa: E402
from invest_core import db as db_layer  # noqa: E402

LOCK_FILE = 'daily_history_job.lock'
LOG_FILE = 'daily_history_job.log'
BOARD = os.getenv('INVEST_MOEX_BOARD', 'TQBR')


def _acquire_lock() -> bool:
    if os.path.exists(LOCK_FILE):
        # Если файл моложе 2 часов, считаем что задача ещё идёт / блокирована
        age_sec = time.time() - os.path.getmtime(LOCK_FILE)
        if age_sec < 2 * 3600:
            return False
    with open(LOCK_FILE, 'w', encoding='utf-8') as f:
        f.write(f'Start {datetime.now(UTC).isoformat()}')
    return True


def _release_lock():
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


def _append_log(line: str):
    ts = datetime.now(UTC).isoformat()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f'[{ts}] {line}\n')


def run():
    if not _acquire_lock():
        _append_log('Skip: lock active')
        return
    start = time.time()
    try:
        db_layer.init_schema()
        # 1. Догрузка истории MOEX
        res_hist = FillingMoexHistory(board=BOARD)
        # 2. Обновление консенсусов и таргетов (для всех перспективных)
        res_cons = FillingConsensusData(limit=None, sleep_sec=0.0)
        # 3. Пересчёт потенциалов на основе свежих данных (skip_null пропускает без консенсуса или цены)
        res_pot = FillingPotentialData(skip_null=True)
        dur = round(time.time() - start, 3)
        _append_log(
            'DailyLoad board={board} hist_status={hstatus} hist_fetched={hfetched} hist_inserted={hins} hist_duplicates={hdup} cons_processed={cproc} cons_inserted={cins} cons_dups={cdup} targets_inserted={tins} targets_dups={tdup} pot_processed={pproc} pot_inserted={pins} pot_skipped={pskip} duration={dur}s'.format(
                board=BOARD,
                hstatus=res_hist.get('status'),
                hfetched=res_hist.get('fetched'),
                hins=res_hist.get('inserted'),
                hdup=res_hist.get('duplicates'),
                cproc=res_cons.get('processed'),
                cins=res_cons.get('consensus_inserted'),
                cdup=res_cons.get('consensus_duplicates'),
                tins=res_cons.get('targets_inserted'),
                tdup=res_cons.get('targets_duplicates'),
                pproc=res_pot.get('processed'),
                pins=res_pot.get('inserted'),
                pskip=res_pot.get('skipped'),
                dur=dur,
            )
        )
    except Exception as ex:  # noqa
        _append_log(f'ERROR: {ex}')
    finally:
        _release_lock()


if __name__ == '__main__':
    run()
