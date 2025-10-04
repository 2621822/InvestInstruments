"""Automatic price history update and potentials recomputation utilities.

Environment variables controlling behaviour (all optional):

  AUTO_UPDATE_ON_START=1            -- run update once when main module is imported / started
  AUTO_UPDATE_FULL_COVERAGE=1       -- first ensure full coverage (backfill missing secids)
  AUTO_UPDATE_RECOMPUTE=1           -- recompute potentials after history update
  AUTO_UPDATE_SILENT=1              -- reduce logging noise during auto run
  AUTO_UPDATE_BOARD=TQBR            -- board id (default TQBR)
  AUTO_UPDATE_HORIZON_DAYS=1100     -- sliding window horizon (default loader default)

The auto update is idempotent per process: it will only execute once even if main code imports multiple times.

Usage (implicit): set env AUTO_UPDATE_ON_START=1 before launching your main tooling relying on `main.py`.

Provides a helper `run_one(auto_full=True, recompute=True)` you can call manually.
"""
from __future__ import annotations

import os
import logging
from pathlib import Path

from . import moex_history_4_perspective_shares as hist

_AUTO_RAN = False  # process sentinel


def run_one(*, full_coverage: bool = True, recompute: bool = True, silent: bool = True) -> dict:
    """Run one automatic update cycle.

    Steps:
      1. Optionally ensure full coverage (backfill for any missing SECIDs)
      2. Daily incremental update for all perspective shares
      3. Optional potentials recompute (batch)
    Returns merged metrics summary.
    """
    board = os.getenv("AUTO_UPDATE_BOARD", "TQBR")
    horizon_env = os.getenv("AUTO_UPDATE_HORIZON_DAYS")
    try:
        horizon_days = int(horizon_env) if horizon_env else hist.DEFAULT_HORIZON_DAYS
    except ValueError:
        horizon_days = hist.DEFAULT_HORIZON_DAYS
    summaries: dict[str, object] = {"board": board, "horizon_days": horizon_days}
    if full_coverage:
        try:
            cov = hist.ensure_full_coverage(board=board, horizon_days=horizon_days, recompute_potentials=False, silent=silent)
            summaries["full_coverage"] = cov
        except Exception as exc:  # noqa: BLE001
            logging.warning("auto_update: full coverage step failed: %s", exc)
            summaries["full_coverage_error"] = str(exc)
    try:
        daily = hist.daily_update_all(board=board, recompute_potentials=recompute, horizon_days=horizon_days, silent=silent)
        summaries["daily_update"] = daily
    except Exception as exc:  # noqa: BLE001
        logging.error("auto_update: daily update failed: %s", exc)
        summaries["daily_update_error"] = str(exc)
    return summaries


def maybe_run_on_start() -> None:
    global _AUTO_RAN  # noqa: PLW0603
    if _AUTO_RAN:
        return
    if os.getenv("AUTO_UPDATE_ON_START", "0") not in {"1", "true", "True", "YES", "yes"}:
        return
    full = os.getenv("AUTO_UPDATE_FULL_COVERAGE", "1") in {"1", "true", "True", "YES", "yes"}
    recompute = os.getenv("AUTO_UPDATE_RECOMPUTE", "1") in {"1", "true", "True", "YES", "yes"}
    silent = os.getenv("AUTO_UPDATE_SILENT", "1") in {"1", "true", "True", "YES", "yes"}
    logging.info("AUTO_UPDATE_ON_START triggered (full_coverage=%s recompute=%s silent=%s)", full, recompute, silent)
    try:
        summary = run_one(full_coverage=full, recompute=recompute, silent=silent)
        logging.info("AUTO_UPDATE summary: %s", {k: v for k, v in summary.items() if k in {"board", "horizon_days"}})
    except Exception as exc:  # noqa: BLE001
        logging.error("AUTO_UPDATE failed: %s", exc)
    _AUTO_RAN = True


if __name__ == "__main__":  # pragma: no cover
    maybe_run_on_start()
