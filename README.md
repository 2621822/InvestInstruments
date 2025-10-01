# Investment Instruments CLI

Utilities for managing a local SQLite database with perspective shares, consensus forecasts, and analyst targets fetched from the Tinkoff Invest public API.

## Prerequisites

- Python 3.11+
- Dependencies listed in `GorbunovInvestInstruments/main.py` (`requests`, `urllib3`, `openpyxl`)

Install dependencies once:

```
pip install -r requirements.txt
```

or, if you prefer not to use a requirements file:

```
pip install requests urllib3 openpyxl
```

## Usage (Tinkoff + Consensus)

All commands are executed from the project root with PowerShell:

```
python GorbunovInvestInstruments/main.py [options]
```

Available options:

| Option | Description |
| --- | --- |
| `--fill-start` | Populate `perspective_shares` with the starter watchlist (id/name pairs). |
| `--fill-attributes` | Enrich every share in `perspective_shares` with ticker, FIGI, ISIN, etc. |
| `--add-share "QUERY"` | Find a share by name/ticker and append it to the watchlist. |
| `--export [FILENAME]` | Export the current watchlist to Excel (default `perspective_shares.xlsx`). |
| `--export-consensus [FILENAME]` | Export both consensus tables to an Excel workbook (default `consensus_data.xlsx`). |
| `--update-consensus [UID]` | Fetch consensus forecast and analyst targets for all shares or for the provided UID only (with pruning of historical depth afterwards). |
| `--fill-consensus` | One-time (or ad‑hoc) bulk initial load of consensus + analyst targets for all shares (no pruning). |
| `--fill-consensus-limit N` | Limit number of instruments processed during `--fill-consensus` (useful for testing). |
| `--fill-consensus-sleep SEC` | Add delay (seconds, can be fractional) between API calls during `--fill-consensus`. |

Examples:

- Initialize the database and fill it with starter tickers:

	```
	python GorbunovInvestInstruments/main.py --fill-start --fill-attributes
	```

- Update consensus data for all tracked shares:

	```
	python GorbunovInvestInstruments/main.py --update-consensus
	```

- Perform initial consensus load without pruning (first populate history):

	```
	python GorbunovInvestInstruments/main.py --fill-consensus
	```

- Test initial load only for first 3 instruments with 0.3s pause:

	```
	python GorbunovInvestInstruments/main.py --fill-consensus --fill-consensus-limit 3 --fill-consensus-sleep 0.3
	```

- Export consensus forecasts and analyst targets:

	```
	python GorbunovInvestInstruments/main.py --export-consensus
	```

- Update consensus only for a specific UID:

	```
	python GorbunovInvestInstruments/main.py --update-consensus 7de75794-a27f-4d81-a39b-492345813822
	```

The script logs confirmations for every write operation and skips duplicates based on the latest stored values.

### Update vs Fill Consensus

- `--fill-consensus` is intended for initial accumulation of current consensus snapshots; it does NOT prune history afterwards.
- `--update-consensus` is for regular (e.g. daily) refreshes and will prune history according to limits:
	- Max consensus rows per instrument: 300
	- Max analyst target rows per (uid, company): 100
	- Optional age-based pruning of rows older than 1000 days.

You can override the API token by setting environment variable `TINKOFF_INVEST_TOKEN`. If not set, a built-in default token is used (not recommended for prolonged production usage).

### Environment Variables

| Variable | Purpose | Default |
| --- | --- | --- |
| `TINKOFF_INVEST_TOKEN` | Auth token for API requests | (required, none) |
| `API_TIMEOUT` | Per-request timeout (seconds) | 15 |
| `API_MAX_ATTEMPTS` | Max retry attempts for network/5xx errors | 3 |
| `API_BACKOFF_BASE` | Base backoff (seconds), grows exponentially | 0.5 |
| `APP_LOG_LEVEL` | Logging level (INFO/DEBUG/...) | INFO |
| `APP_LOG_FILE` | Log file name for rotating logs | app.log |
| `APP_DISABLE_SSL_VERIFY` | Set to 1 to disable TLS verification (dev only) | 0 |

---

## Async MOEX History Loader

File: `moex.py` — асинхронная утилита загрузки исторических данных с ISS MOEX.

Features:
- Incremental updates: starts from last stored TRADEDATE+1 per (BOARDID, SECID)
- Override start date with `--since` or sliding window with `--days`
- Batching by date ranges (`--step-days`)
- Bounded concurrency (`--max-concurrency`)
- Retries with exponential backoff
- Optional JSON / Excel exports
- Automatic pagination of ISS history endpoint if result spans multiple pages

### Basic Run

```
python moex.py --instruments SBER GAZP LKOH
```

### Options

| Option | Description |
| --- | --- |
| `--instruments SECID...` | Limit to specific SECIDs (otherwise all from `share` table in `moex_data.db`). |
| `--to-date YYYY-MM-DD` | Upper bound date (default = today). |
| `--since YYYY-MM-DD` | Force absolute start date (highest precedence). |
| `--days N` | Load only last N days (ignored if `--since` provided). |
| `--step-days N` | Date batch size (default from env `MOEX_DATE_STEP`, default 100). |
| `--max-concurrency N` | Parallel fetch limit (env `MOEX_MAX_CONCURRENCY`, default 8). |
| `--export [FILE]` | Export combined fetched data to Excel (default `moex_data.xlsx`). |
| `--export-json [FILE]` | Export fetched data to JSON (default `moex_data.json`). |
| `--log-level LEVEL` | Logging level. |

### Environment Variables (MOEX)

| Variable | Purpose | Default |
| --- | --- | --- |
| `MOEX_DB_PATH` | SQLite path for MOEX data | `moex_data.db` |
| `MOEX_DATE_STEP` | Default batch size (days) | 100 |
| `MOEX_MAX_CONCURRENCY` | Max simultaneous HTTP requests | 8 |
| `MOEX_HTTP_TIMEOUT` | Per-request timeout seconds | 20 |
| `MOEX_HTTP_RETRIES` | Retry attempts | 3 |
| `MOEX_HTTP_BACKOFF` | Base seconds for exp. backoff | 0.5 |

### Example: Last 30 Days Only

```
python moex.py --instruments SBER GAZP --days 30
```

### Example: Force Fresh Load From Fixed Date

```
python moex.py --instruments SBER --since 2024-01-01 --export-json sber_2024.json
```

Precedence of start date: --since > --days > incremental-from-last > default(2022-01-01)

---

Disabling SSL verification (`APP_DISABLE_SSL_VERIFY=1`) is strongly discouraged outside of local debugging.

## Legacy Hello World

The original tutorial artifact `hello.py` is still present and can be run with `python hello.py`.
