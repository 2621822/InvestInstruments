"""Единый ежедневный job по обновлению данных:

Шаги:
    1. Догрузка истории цен MOEX (FillingMoexHistory)
    2. Обновление консенсусов и таргетов (FillingConsensusData)
    3. Пересчёт потенциалов (FillingPotentialData)
    4. Очистка устаревших потенциалов (CleanOldSharePotentials)
    5. (Опционально) Вывод топ-N потенциалов (GetTopSharePotentials)

Использование:
    python daily_history_job.py [--board TQBR] [--skip-consensus] [--skip-history] [--skip-potentials] [--retention-days 90] [--top 10]

Можно добавить в планировщик (Windows Task Scheduler) или через schtasks.
"""
from __future__ import annotations
import sys
import os
import time
import sqlite3
from datetime import datetime, UTC
sys.path.append('d:/#Work/#Invest/Project/src')
from invest_core.moex_history import FillingMoexHistory  # noqa: E402
from invest_core.potentials import (
    FillingPotentialData,
    CleanOldSharePotentials,
    GetTopSharePotentials,
)  # noqa: E402
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


def run(
    board: str = BOARD,
    *,
    skip_history: bool = False,
    skip_consensus: bool = False,
    skip_potentials: bool = False,
    retention_days: int = 90,
    top_limit: int | None = None,
    skip_null_potentials: bool = True,
):
    """Запуск единого процесса обновления.

    Args:
        board: Код торговой доски (напр. 'TQBR').
        skip_history: Пропустить догрузку истории.
        skip_consensus: Пропустить загрузку консенсусов.
        skip_potentials: Пропустить расчёт потенциалов.
        retention_days: Возраст для очистки старых потенциалов (shares_potentials).
        top_limit: Если задано – вывести топ-N потенциалов в лог.
        skip_null_potentials: Не вставлять строки без рассчитанного относительного потенциала.
    """
    if not _acquire_lock():
        _append_log('Skip: lock active')
        return
    start = time.time()
    try:
        db_layer.init_schema()
        res_hist = {"status": "skipped"}
        res_cons = {"status": "skipped"}
        res_pot = {"status": "skipped"}
        res_ret = {"status": "skipped"}
        res_top = {"status": "skipped"}

        if not skip_history:
            res_hist = FillingMoexHistory(board=board)
        if not skip_consensus:
            # limit=None => все перспективные; sleep_sec=0 для скорости
            res_cons = FillingConsensusData(limit=None, sleep_sec=0.0)
        if not skip_potentials:
            res_pot = FillingPotentialData(skip_null=skip_null_potentials)
            if retention_days and retention_days > 0:
                res_ret = CleanOldSharePotentials(max_age_days=retention_days)
            if top_limit and top_limit > 0:
                res_top = GetTopSharePotentials(limit=top_limit)
        dur = round(time.time() - start, 3)
        _append_log(
            'DailyUnified board={board} hist_status={hstatus} hist_fetched={hfetched} hist_inserted={hins} hist_duplicates={hdup} '
            'cons_processed={cproc} cons_cins={cins} cons_cdup={cdup} targets_inserted={tins} targets_dups={tdup} '
            'pot_processed={pproc} pot_inserted={pins} pot_skipped={pskip} pot_unchanged={punch} retention_deleted={rdel} top_rows={trows} duration={dur}s'.format(
                board=board,
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
                punch=res_pot.get('unchanged'),
                rdel=res_ret.get('deleted'),
                trows=(res_top.get('rows') if isinstance(res_top.get('rows'), int) else None),
                dur=dur,
            )
        )
        if res_top.get('data'):
            for rec in res_top['data']:
                _append_log('TOP uid={uid} ticker={ticker} rel={rel:.4f} prevClose={pc} consensus={cons}'.format(
                    uid=rec['uid'], ticker=rec['ticker'], rel=rec['pricePotentialRel'], pc=rec['prevClose'], cons=rec['consensusPrice']
                ))
    except Exception as ex:  # noqa
        _append_log(f'ERROR: {ex}')
    finally:
        _release_lock()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Единый ежедневный job обновления данных')
    parser.add_argument('--board', default=BOARD, help='MOEX board (default: TQBR)')
    parser.add_argument('--skip-history', action='store_true', help='Пропустить загрузку истории MOEX')
    parser.add_argument('--skip-consensus', action='store_true', help='Пропустить загрузку консенсусов')
    parser.add_argument('--skip-potentials', action='store_true', help='Пропустить расчёт потенциалов')
    parser.add_argument('--retention-days', type=int, default=90, help='Очистить потенциалы старше этого количества дней (0=нет)')
    parser.add_argument('--top', type=int, default=None, help='Вывести топ-N потенциалов после пересчёта')
    parser.add_argument('--no-skip-null', action='store_true', help='Вставлять строки потенциалов с NULL rel')
    args = parser.parse_args()
    run(
        board=args.board,
        skip_history=args.skip_history,
        skip_consensus=args.skip_consensus,
        skip_potentials=args.skip_potentials,
        retention_days=args.retention_days,
        top_limit=args.top,
        skip_null_potentials=(not args.no_skip_null),
    )
