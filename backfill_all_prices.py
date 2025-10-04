"""Mass backfill of MOEX history for all perspective shares.

Runs one full 1100‑day (configurable) window load for every perspective share
that does NOT yet have history (or has only partial) in moex_history_perspective_shares.
Avoids PowerShell console interaction issues by being callable from an embedded
Python executor.
"""
from __future__ import annotations
import datetime as dt
import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, Any

from GorbunovInvestInstruments.moex_history_4_perspective_shares import GetMoexHistory, DB_PATH, DEFAULT_HORIZON_DAYS
from GorbunovInvestInstruments import main as app_main  # for ComputePotentials


def _collect_identifiers(conn: sqlite3.Connection) -> list[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT UPPER(
            CASE
              WHEN secid IS NOT NULL AND TRIM(secid)<>'' THEN secid
              WHEN ticker IS NOT NULL AND TRIM(ticker)<>'' THEN ticker
              ELSE NULL END)
        FROM perspective_shares
        WHERE (secid IS NOT NULL AND TRIM(secid)<>'') OR (ticker IS NOT NULL AND TRIM(ticker)<>'')
        """
    )
    return [r[0] for r in cur.fetchall() if r[0]]


def backfill_missing_prices(board: str = "TQBR", horizon_days: int = DEFAULT_HORIZON_DAYS, recompute_potentials: bool = True) -> Dict[str, Any]:
    start_all = time.time()
    with sqlite3.connect(DB_PATH) as conn:
        all_ids = _collect_identifiers(conn)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT SECID FROM moex_history_perspective_shares WHERE BOARDID=?", (board,))
        existing = {r[0] for r in cur.fetchall() if r[0]}
    missing = [s for s in all_ids if s not in existing]

    summary: Dict[str, Any] = {
        'board': board,
        'horizon_days': horizon_days,
        'total_perspective': len(all_ids),
        'already_with_history': len(existing),
        'missing_to_backfill': len(missing),
        'per_security': {},
        'started_ts': time.time(),
    }

    if not missing:
        summary['note'] = 'No missing securities to backfill.'
    else:
        for idx, secid in enumerate(missing, 1):
            metrics: Dict[str, Any] = {}
            try:
                today = dt.date.today()
                start_date = (today - dt.timedelta(days=horizon_days)).isoformat()
                inserted = GetMoexHistory(board=board, secid=secid, dr_start=start_date, dr_end=today.isoformat(), metrics=metrics)
                summary['per_security'][secid] = {
                    'inserted': inserted,
                    'http_requests': metrics.get('http_requests'),
                    'ranges': metrics.get('ranges'),
                }
            except Exception as exc:  # noqa: BLE001
                summary['per_security'][secid] = {'error': str(exc)}
            # Lightweight progress line (safe for non-interactive capture)
            print(f"[{idx}/{len(missing)}] {secid} done")

    # Coverage after attempts
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(DISTINCT SECID) FROM moex_history_perspective_shares WHERE BOARDID=?', (board,))
        summary['history_distinct_after'] = cur.fetchone()[0]
        cur.execute('SELECT SECID, COUNT(*) c FROM moex_history_perspective_shares WHERE BOARDID=? GROUP BY SECID ORDER BY c DESC LIMIT 5', (board,))
        summary['top'] = cur.fetchall()

    if recompute_potentials:
        try:
            app_main.ComputePotentials(Path(DB_PATH), store=True)
            summary['potentials_recomputed'] = True
        except Exception as exc:  # noqa: BLE001
            summary['potentials_recomputed'] = False
            summary['potentials_recompute_error'] = str(exc)

    summary['duration_sec'] = round(time.time() - start_all, 2)
    return summary


if __name__ == '__main__':  # pragma: no cover
    res = backfill_missing_prices()
    print(json.dumps(res, ensure_ascii=False, indent=2))
