import asyncio
import sqlite3
import logging
from pathlib import Path
import sys
import time

from moex import build_arg_parser, async_run
from GorbunovInvestInstruments.main import (
    ComputePotentials,
    export_potentials_to_excel,
    DB_PATH as MAIN_DB_PATH,
)

SUMMARY_JSON = "moex_summary.json"
POTENTIALS_XLSX = "potentials.xlsx"
DAYS_WINDOW = 30
LOG_FILE = "run_moex_batch.log"


def get_perspective_tickers() -> list[str]:
    conn = sqlite3.connect(MAIN_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker FROM perspective_shares WHERE ticker IS NOT NULL ORDER BY ticker")
        return [r[0] for r in cur.fetchall() if r[0]]
    finally:
        conn.close()


def print_top10():
    conn = sqlite3.connect(MAIN_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ticker,
                   ROUND(pricePotentialRel*100,2) AS pct,
                   prevClose,
                   consensusPrice,
                   isStale
            FROM instrument_potentials ip
            WHERE computedAt = (
                SELECT MAX(computedAt) FROM instrument_potentials ip2 WHERE ip2.ticker = ip.ticker
            )
            ORDER BY pricePotentialRel DESC
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        print("Top10 potentials after MOEX update:")
        for t, pct, prev_close, consensus, stale in rows:
            print(f"{t:<8} {pct:>7.2f}% prev={prev_close} consensus={consensus} stale={stale}")
    finally:
        conn.close()


def log_print(msg: str) -> None:
    """Print with flush and also mirror to logging."""
    print(msg, flush=True)
    logging.info(msg)


def main():
    start_ts = time.time()
    tickers = get_perspective_tickers()
    if not tickers:
        log_print("No perspective tickers found; aborting.")
        return
    parser = build_arg_parser()
    # Build CLI-style args list (without relying on shell parsing)
    args_list = ["--instruments", *tickers, "--days", str(DAYS_WINDOW), "--log-level", "INFO", "--summary-json", SUMMARY_JSON]
    args = parser.parse_args(args_list)
    log_print(f"Running MOEX loader for {len(tickers)} tickers (last {DAYS_WINDOW} days)...")
    asyncio.run(async_run(args))
    log_print("MOEX load complete. Recomputing potentials...")
    ComputePotentials()
    log_print("Exporting potentials to Excel...")
    export_potentials_to_excel(POTENTIALS_XLSX)
    print_top10()
    if Path(SUMMARY_JSON).exists():
        log_print(f"Summary JSON saved to {SUMMARY_JSON}")
    if Path(POTENTIALS_XLSX).exists():
        log_print(f"Potentials Excel saved to {POTENTIALS_XLSX}")
    log_print(f"Total elapsed: {time.time()-start_ts:.1f}s")


if __name__ == "__main__":
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.exception("run_moex_batch failed: %s", exc)
        raise
