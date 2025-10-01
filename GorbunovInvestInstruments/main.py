"""Investment instruments helper utilities."""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, Any

import openpyxl
import requests
import sqlite3
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --- CONFIG ------------------------------------------------------------------
DB_PATH = Path("GorbunovInvestInstruments.db")

# ВАЖНО: Лучше не хранить токен в коде. Если переменная окружения отсутствует — выводим предупреждение.
TOKEN = os.getenv("TINKOFF_INVEST_TOKEN")
if not TOKEN:
	logging.warning(
		"Переменная окружения TINKOFF_INVEST_TOKEN не установлена. Запросы могут завершиться ошибкой авторизации."
	)
	TOKEN = ""  # пустая строка — явный маркер отсутствия токена

API_BASE_URL = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService"
FIND_ENDPOINT = "FindInstrument"
GET_ENDPOINT = "GetInstrumentBy"
GET_FORECAST_ENDPOINT = "GetForecastBy"

SESSION = requests.Session()
# Возможность отключать верификацию SSL только через переменную (по умолчанию включено)
SESSION.verify = os.getenv("APP_DISABLE_SSL_VERIFY", "0") not in {"1", "true", "True"}
if not SESSION.verify:
	urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
	logging.warning("SSL verify отключен (APP_DISABLE_SSL_VERIFY=1). Используйте только в отладочных целях.")

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "15"))
API_MAX_ATTEMPTS = int(os.getenv("API_MAX_ATTEMPTS", "3"))
API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "0.5"))  # сек
MAX_CONSENSUS_PER_UID = 300
MAX_TARGETS_PER_ANALYST = 100
MAX_HISTORY_DAYS = 1000  # возраст записей (дней), старше которого данные удаляются дополнительно

def setup_logging() -> None:
	"""Configure logging with both console and rotating file handler (idempotent)."""
	if getattr(setup_logging, "_configured", False):  # prevent duplicate handlers
		return
	log_level = os.getenv("APP_LOG_LEVEL", "INFO").upper()
	logger = logging.getLogger()
	logger.setLevel(log_level)
	for h in list(logger.handlers):  # remove default handlers if any
		logger.removeHandler(h)
	formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
	console = logging.StreamHandler()
	console.setFormatter(formatter)
	logger.addHandler(console)
	log_file = os.getenv("APP_LOG_FILE", "app.log")
	file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
	file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)
	setup_logging._configured = True  # type: ignore[attr-defined]


setup_logging()


# --- API HELPERS -------------------------------------------------------------
def PostApiHeaders(token: str) -> dict[str, str]:
	"""Return headers required for API authorization."""

	return {"Authorization": f"Bearer {token}"}


def _post(endpoint: str, payload: dict, token: str) -> dict | None:
	"""Send a POST request to the API with retry/backoff; return JSON or None.

	Retry logic configurable через переменные окружения:
	- API_MAX_ATTEMPTS (default 3)
	- API_BACKOFF_BASE (default 0.5s), экспоненциально: base * 2^(attempt-1)
	"""

	url = f"{API_BASE_URL}/{endpoint}"
	if not token:
		logging.error("Токен не задан. Запрос %s не будет выполнен.", endpoint)
		return None
	headers = PostApiHeaders(token)
	for attempt in range(1, API_MAX_ATTEMPTS + 1):
		try:
			response = SESSION.post(url, json=payload, headers=headers, timeout=API_TIMEOUT)
			status = response.status_code
			if 500 <= status < 600:
				raise requests.RequestException(f"Server error {status}")
			response.raise_for_status()
			try:
				return response.json()
			except ValueError:
				logging.error("API response from %s is not valid JSON", endpoint)
				return None
		except requests.RequestException as exc:
			if attempt == API_MAX_ATTEMPTS:
				logging.error("API request to %s failed after %s attempts: %s", endpoint, attempt, exc)
				return None
			backoff = API_BACKOFF_BASE * (2 ** (attempt - 1))
			logging.warning(
				"API request to %s failed (attempt %s/%s): %s. Retry in %.2fs",
				endpoint,
				attempt,
				API_MAX_ATTEMPTS,
				exc,
				backoff,
			)
			time.sleep(backoff)
	return None


def _parse_money(value: dict | int | float | None) -> float | None:
	"""Convert API money representation {units, nano} to a float."""

	if isinstance(value, dict):
		try:
			units = int(value.get("units", 0))
		except (TypeError, ValueError):
			units = 0
		try:
			nano = int(value.get("nano", 0))
		except (TypeError, ValueError):
			nano = 0
		return units + nano / 1_000_000_000

	if isinstance(value, (int, float)):
		return float(value)

	return None


def _float_equal(a: float | None, b: float | None, tol: float = 1e-6) -> bool:
	"""Compare two floating point numbers with tolerance, treating None as distinct."""

	if a is None and b is None:
		return True
	if a is None or b is None:
		return False
	return abs(a - b) <= tol


def _now_iso() -> str:
	"""Return current UTC timestamp (ISO, seconds precision)."""
	return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _count_rows(conn: sqlite3.Connection, table: str, where: str | None = None, params: Iterable[Any] | None = None) -> int:
	"""Return row count for a table with optional WHERE clause."""
	sql = f"SELECT COUNT(*) FROM {table}"
	if where:
		sql += f" WHERE {where}"
	cur = conn.execute(sql, tuple(params) if params else ())
	return int(cur.fetchone()[0])


def _is_older(dt_str: str | None, cutoff: datetime) -> bool:
	"""Return True если строка даты (ISO или с суффиксом Z) старше cutoff.

	При ошибке парсинга возвращает False (считаем запись валидной).
	"""
	if not dt_str:
		return False
	val = dt_str.strip()
	# Нормализуем Z -> +00:00 чтобы datetime.fromisoformat понял
	if val.endswith("Z"):
		val = val[:-1] + "+00:00"
	try:
		parsed = datetime.fromisoformat(val)
		if parsed.tzinfo is None:
			parsed = parsed.replace(tzinfo=timezone.utc)
	except Exception:
		return False
	return parsed < cutoff


# --- API OPERATIONS ----------------------------------------------------------
def GetUidInstrument(search_phrase: str, token: str) -> str | None:
	"""Return instrument UID for a given search phrase."""

	payload = {
		"query": search_phrase,
		"instrumentKind": "INSTRUMENT_TYPE_SHARE",
		"apiTradeAvailableFlag": True,
	}
	data = _post(FIND_ENDPOINT, payload, token)
	if not data:
		logging.warning("Не удалось найти бумаги по запросу '%s'", search_phrase)
		return None

	instruments = data.get("instruments", [])
	if not instruments:
		logging.warning("Инструменты по запросу '%s' не найдены", search_phrase)
		return None

	lower_query = search_phrase.lower()
	for inst in instruments:
		ticker = inst.get("ticker", "")
		name = inst.get("name", "")
		if ticker.lower() == lower_query or name.lower() == lower_query:
			return inst.get("uid")

	return instruments[0].get("uid")


def GetInstrumentByUid(uid: str, token: str) -> dict | None:
	"""Return full instrument data for a UID."""

	payload = {"idType": "INSTRUMENT_ID_TYPE_UID", "id": uid}
	data = _post(GET_ENDPOINT, payload, token)
	instrument = data.get("instrument") if data else None
	if not instrument:
		logging.warning("Не удалось получить данные по uid %s", uid)
		return None

	return {
		"ticker": instrument.get("ticker", ""),
		"name": instrument.get("name", ""),
		"uid": instrument.get("uid", ""),
		"secid": instrument.get("ticker", ""),
		"isin": instrument.get("isin", ""),
		"figi": instrument.get("figi", ""),
		"classCode": instrument.get("classCode", ""),
		"instrumentType": instrument.get("instrumentType", ""),
		"assetUid": instrument.get("assetUid", ""),
	}


def GetConsensusByUid(uid: str, token: str) -> tuple[dict | None, list[dict]]:
	"""Return consensus forecast and analyst targets for a given instrument UID."""

	payload = {"instrumentId": uid}
	data = _post(GET_FORECAST_ENDPOINT, payload, token)
	if not data:
		logging.warning("Не удалось получить консенсус прогноз по uid %s", uid)
		return None, []

	consensus = data.get("consensus")
	targets = data.get("targets", [])
	if consensus is None:
		logging.warning("Ответ API не содержит блока consensus для uid %s", uid)

	return consensus, targets


# --- DATABASE OPERATIONS -----------------------------------------------------
def initialize_database(db_path: Path) -> None:
	"""Ensure that the SQLite database file exists."""

	try:
		with sqlite3.connect(db_path) as conn:
			conn.execute("SELECT 1")
	except sqlite3.DatabaseError as exc:
		logging.error("Не удалось инициализировать базу данных: %s", exc)
		raise


def CreateTables(db_path: Path) -> None:
	"""Create required tables if they do not exist."""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute(
			"""CREATE TABLE IF NOT EXISTS perspective_shares (
				ticker TEXT,
				name TEXT,
				uid TEXT PRIMARY KEY,
				secid TEXT,
				isin TEXT,
				figi TEXT,
				classCode TEXT,
				instrumentType TEXT,
				assetUid TEXT
			)"""
		)
		cursor.execute(
			"""CREATE TABLE IF NOT EXISTS consensus_forecasts (
				uid TEXT PRIMARY KEY,
				ticker TEXT,
				recommendation TEXT,
				recommendationDate DATE,
				currency TEXT,
				priceConsensus REAL,
				minTarget REAL,
				maxTarget REAL
			)"""
		)
		cursor.execute(
			"""CREATE TABLE IF NOT EXISTS consensus_targets (
				uid TEXT PRIMARY KEY,
				ticker TEXT,
				company TEXT,
				recommendation TEXT,
				recommendationDate DATE,
				currency TEXT,
				targetPrice REAL,
				showName TEXT
			)"""
		)
		# Индексы для ускорения выборок по тикеру и дате
		cursor.execute(
			"CREATE INDEX IF NOT EXISTS idx_consensus_forecasts_ticker ON consensus_forecasts(ticker)"
		)
		cursor.execute(
			"CREATE INDEX IF NOT EXISTS idx_consensus_targets_ticker_date ON consensus_targets(ticker, recommendationDate)"
		)
		conn.commit()


def migrate_schema(db_path: Path) -> None:
	"""Migrate existing schema to support historical consensus and multiple analyst targets.

	Old schema had PRIMARY KEY(uid) in consensus_forecasts / consensus_targets which prevented history.
	New schema:
		consensus_forecasts(id INTEGER PK AUTOINCREMENT, uid TEXT, ...)
		consensus_targets(id INTEGER PK AUTOINCREMENT, uid TEXT, company TEXT, ... UNIQUE(uid, company, recommendationDate))
	"""
	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		# Detect old consensus_forecasts (uid PRIMARY KEY)
		cursor.execute("PRAGMA table_info(consensus_forecasts)")
		cols = cursor.fetchall()
		if cols and len(cols) == 8 and cols[0][1] == "uid":
			logging.info("Миграция consensus_forecasts -> историческая модель")
			cursor.execute(
				"""CREATE TABLE IF NOT EXISTS consensus_forecasts_new (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					uid TEXT,
					ticker TEXT,
					recommendation TEXT,
					recommendationDate DATE,
					currency TEXT,
					priceConsensus REAL,
					minTarget REAL,
					maxTarget REAL
				)"""
			)
			cursor.execute(
				"INSERT INTO consensus_forecasts_new (uid, ticker, recommendation, recommendationDate, currency, priceConsensus, minTarget, maxTarget) SELECT uid, ticker, recommendation, recommendationDate, currency, priceConsensus, minTarget, maxTarget FROM consensus_forecasts"
			)
			cursor.execute("DROP TABLE consensus_forecasts")
			cursor.execute("ALTER TABLE consensus_forecasts_new RENAME TO consensus_forecasts")
			cursor.execute(
				"CREATE INDEX IF NOT EXISTS idx_consensus_forecasts_uid_date ON consensus_forecasts(uid, recommendationDate DESC)"
			)

		# Detect old consensus_targets (uid PRIMARY KEY)
		cursor.execute("PRAGMA table_info(consensus_targets)")
		cols_t = cursor.fetchall()
		if cols_t and len(cols_t) == 8 and cols_t[0][1] == "uid":
			logging.info("Миграция consensus_targets -> поддержка нескольких аналитиков")
			cursor.execute(
				"""CREATE TABLE IF NOT EXISTS consensus_targets_new (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					uid TEXT,
					ticker TEXT,
					company TEXT,
					recommendation TEXT,
					recommendationDate DATE,
					currency TEXT,
					targetPrice REAL,
					showName TEXT
				)"""
			)
			cursor.execute(
				"INSERT INTO consensus_targets_new (uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName) SELECT uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName FROM consensus_targets"
			)
			cursor.execute("DROP TABLE consensus_targets")
			cursor.execute("ALTER TABLE consensus_targets_new RENAME TO consensus_targets")
			cursor.execute(
				"CREATE UNIQUE INDEX IF NOT EXISTS uq_consensus_targets_uid_company_date ON consensus_targets(uid, company, recommendationDate)"
			)
		conn.commit()


def FillingStartDdata(db_path: Path) -> None:
	"""Fill the perspective_shares table with initial set of instruments."""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		count_before = _count_rows(conn, "perspective_shares")
		if count_before != 0:
			logging.info(
				"Таблица perspective_shares уже содержит данные (строк: %s). Стартовое наполнение пропущено.",
				count_before,
			)
			return

		start_data = [
			("7de75794-a27f-4d81-a39b-492345813822", "Яндекс"),
			("87db07bc-0e02-4e29-90bb-05e8ef791d7b", "Т-Технологии"),
			("e6123145-9665-43e0-8413-cd61b8aa9b13", "Сбер Банк"),
			("c190ff1f-1447-4227-b543-316332699ca5", "Сбер Банк - привилегированные акции"),
			("10620843-28ce-44e8-80c2-f26ceb1bd3e1", "Полюс"),
			("02cfdf61-6298-4c0f-a9ca-9cabc82afaf3", "ЛУКОЙЛ"),
		]
		cursor.executemany("INSERT INTO perspective_shares (uid, name) VALUES (?, ?)", start_data)
		conn.commit()

		count_after = _count_rows(conn, "perspective_shares")
		logging.info("Таблица perspective_shares заполнена стартовыми бумагами. Текущих строк: %s", count_after)


def FillingSharesData(db_path: Path, token: str) -> None:
	"""Ensure all attributes for each perspective share are populated."""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT * FROM perspective_shares")
		rows = cursor.fetchall()
		columns = [desc[0] for desc in cursor.description]

		updated_rows = 0
		for row in rows:
			data = dict(zip(columns, row))
			missing = [col for col in columns if col != "uid" and not data.get(col)]
			if not missing:
				continue

			full_data = GetInstrumentByUid(data["uid"], token)
			if not full_data:
				continue

			cursor.execute(
				"""UPDATE perspective_shares SET
						ticker=?,
						name=?,
						secid=?,
						isin=?,
						figi=?,
						classCode=?,
						instrumentType=?,
						assetUid=?
					WHERE uid=?""",
				(
					full_data["ticker"],
					full_data["name"],
					full_data["secid"],
					full_data["isin"],
					full_data["figi"],
					full_data["classCode"],
					full_data["instrumentType"],
					full_data["assetUid"],
					data["uid"],
				),
			)
			conn.commit()
			updated_rows += 1
			logging.info("Обновлены атрибуты для %s (%s)", full_data["name"], full_data["ticker"])

		total_rows = _count_rows(conn, "perspective_shares")
		logging.info(
			"Обновление атрибутов завершено. Затронуто строк: %s. Текущее количество строк: %s",
			updated_rows,
			total_rows,
		)


def AddShareData(db_path: Path, search_phrase: str, token: str) -> None:
	"""Add a new share to the perspective_shares table using a search phrase."""

	uid = GetUidInstrument(search_phrase, token)
	if not uid:
		logging.warning("Не удалось найти бумагу по запросу '%s'", search_phrase)
		return

	full_data = GetInstrumentByUid(uid, token)
	if not full_data:
		logging.warning("Не удалось получить данные по бумаге '%s'", search_phrase)
		return

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT uid FROM perspective_shares WHERE uid = ?", (uid,))
		if cursor.fetchone():
			total_rows = _count_rows(conn, "perspective_shares")
			logging.info(
				"Данные по бумаге %s уже присутствуют в таблице. Текущее количество строк: %s",
				full_data["name"],
				total_rows,
			)
			return

		cursor.execute(
			"""INSERT INTO perspective_shares
					(ticker, name, uid, secid, isin, figi, classCode, instrumentType, assetUid)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
			(
				full_data["ticker"],
				full_data["name"],
				full_data["uid"],
				full_data["secid"],
				full_data["isin"],
				full_data["figi"],
				full_data["classCode"],
				full_data["instrumentType"],
				full_data["assetUid"],
			),
		)
		conn.commit()
		cursor.execute("SELECT COUNT(*) FROM perspective_shares")
		total_rows = cursor.fetchone()[0]
		logging.info(
			"Бумага %s успешно добавлена. Текущее количество строк: %s",
			full_data["name"],
			total_rows,
		)


def AddConsensusForecasts(db_path: Path, consensus: dict | None) -> None:
	"""Save consensus forecast if it differs from the latest stored record."""

	if not consensus:
		logging.info("Консенсус данные отсутствуют, сохранение пропущено.")
		return

	uid = consensus.get("uid")
	if not uid:
		logging.warning("Консенсус прогноз не содержит uid, сохранение невозможно.")
		return

	ticker = consensus.get("ticker", "")
	recommendation = consensus.get("recommendation")
	currency = consensus.get("currency")
	price_consensus = _parse_money(consensus.get("consensus"))
	min_target = _parse_money(consensus.get("minTarget"))
	max_target = _parse_money(consensus.get("maxTarget"))

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute(
			"""SELECT uid, ticker, recommendation, currency, priceConsensus, minTarget, maxTarget
		       FROM consensus_forecasts
		      WHERE uid = ?
		   ORDER BY recommendationDate DESC
		      LIMIT 1""",
			(uid,),
		)
		last_row = cursor.fetchone()

		if last_row and (
			last_row[0] == uid
			and last_row[1] == ticker
			and last_row[2] == recommendation
			and last_row[3] == currency
			and _float_equal(last_row[4], price_consensus)
			and _float_equal(last_row[5], min_target)
			and _float_equal(last_row[6], max_target)
		):
			total_rows = _count_rows(conn, "consensus_forecasts", "uid = ?", [uid])
			logging.info(
				"Прогноз по бумаге %s уже сохранен ранее. Всего записей по бумаге: %s",
				ticker,
				total_rows,
			)
			return

		recommendation_date = _now_iso()
		cursor.execute(
			"""INSERT INTO consensus_forecasts
				(uid, ticker, recommendation, recommendationDate, currency, priceConsensus, minTarget, maxTarget)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
			(
				uid,
				ticker,
				recommendation,
				recommendation_date,
				currency,
				price_consensus,
				min_target,
				max_target,
			),
		)
		conn.commit()

		total_rows = _count_rows(conn, "consensus_forecasts", "uid = ?", [uid])

	logging.info(
		"Консенсус прогноз для %s сохранен. Текущее количество записей по бумаге: %s",
		ticker,
		total_rows,
	)


def AddConsensusTargets(db_path: Path, targets: list[dict]) -> None:
	"""Persist analyst targets, avoiding duplicates."""

	if not targets:
		logging.info("По прогнозам аналитиков данных нет.")
		return

	inserted = 0
	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()

		for target in targets:
			uid = target.get("uid")
			ticker = target.get("ticker")
			company = target.get("company")
			recommendation = target.get("recommendation")
			recommendation_date = target.get("recommendationDate")
			currency = target.get("currency")
			target_price = _parse_money(target.get("targetPrice"))
			show_name = target.get("showName")

			if not uid or not company or not recommendation_date:
				logging.warning(
					"Пропущен прогноз аналитика из-за отсутствия ключевых полей: uid=%s, company=%s, recommendationDate=%s",
					uid,
					company,
					recommendation_date,
				)
				continue

			cursor.execute(
				"""SELECT uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName
				       FROM consensus_targets
				      WHERE uid = ? AND company = ? AND recommendationDate = ?""",
				(uid, company, recommendation_date),
			)
			existing = cursor.fetchone()

			if existing and (
				existing[0] == uid
				and existing[1] == ticker
				and existing[2] == company
				and existing[3] == recommendation
				and existing[4] == recommendation_date
				and existing[5] == currency
				and _float_equal(existing[6], target_price)
				and existing[7] == show_name
			):
				logging.info(
					"Прогноз %s по %s за %s уже сохранен ранее.",
					company,
					ticker,
					recommendation_date,
				)
				continue

			cursor.execute(
				"""INSERT INTO consensus_targets
					(uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
				(uid, ticker, company, recommendation, recommendation_date, currency, target_price, show_name),
			)
			inserted += 1

		conn.commit()
	total_rows = _count_rows(conn, "consensus_targets")

	logging.info(
		"Загрузка прогнозов аналитиков завершена. Добавлено записей: %s. Всего записей: %s",
		inserted,
		total_rows,
	)


def UpdateConsensusForecasts(db_path: Path, token: str, *, uid: str | None = None) -> None:
	"""Fetch and store consensus data for a specific instrument or all instruments."""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		if uid:
			cursor.execute("SELECT uid FROM perspective_shares WHERE uid = ?", (uid,))
		else:
			cursor.execute("SELECT uid FROM perspective_shares")
		rows = cursor.fetchall()

	if not rows:
		logging.info("Нет бумаг для обновления консенсус прогноза.")
		return

	for (instrument_uid,) in rows:
		consensus, targets = GetConsensusByUid(instrument_uid, token)
		AddConsensusForecasts(db_path, consensus)
		AddConsensusTargets(db_path, targets)

	# После загрузки — очистка лишней истории
	PruneHistory(db_path, MAX_CONSENSUS_PER_UID, MAX_TARGETS_PER_ANALYST)


def FillingConsensusData(db_path: Path, token: str, *, limit: int | None = None, sleep_sec: float = 0.0) -> None:
	"""Первичное массовое наполнение consensus данными по всем бумагам.

	Проходит по всем uid из perspective_shares (опционально ограничивая их числом limit),
	для каждой бумаги получает (consensus, targets) через GetConsensusByUid и сохраняет
	их посредством AddConsensusForecasts / AddConsensusTargets.

	Отличия от UpdateConsensusForecasts:
	- Не выполняет PruneHistory (цель — накопить стартовый слой данных)
	- Легковесное логирование процесса

	Параметры:
	- limit: если задано, обрабатывает только первые N бумаг
	- sleep_sec: задержка между запросами (для throttling при необходимости)
	"""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT uid FROM perspective_shares ORDER BY uid")
		uids = [row[0] for row in cursor.fetchall()]

	if not uids:
		logging.info("Нет инструментов для первичного консенсус наполнения.")
		return

	if limit is not None and limit >= 0:
		uids = uids[:limit]

	logging.info("Старт FillingConsensusData: инструментов к обработке %s", len(uids))

	processed = 0
	for uid in uids:
		consensus, targets = GetConsensusByUid(uid, token)
		AddConsensusForecasts(db_path, consensus)
		AddConsensusTargets(db_path, targets)
		processed += 1
		if sleep_sec:
			time.sleep(sleep_sec)
		if processed % 20 == 0:  # периодический прогресс
			logging.info("FillingConsensusData прогресс: %s/%s", processed, len(uids))

	logging.info("FillingConsensusData завершено. Обработано бумаг: %s", processed)


def PruneHistory(db_path: Path, max_consensus: int, max_targets_per_analyst: int, *, max_age_days: int | None = MAX_HISTORY_DAYS) -> None:
	"""Ограничить глубину хранения:

	- consensus_forecasts: максимум max_consensus записей на каждый uid
	- consensus_targets: максимум max_targets_per_analyst записей на каждую пару (uid, company)
	- (опционально) удалить записи старше max_age_days
	"""
	deleted_consensus = 0
	deleted_targets = 0
	deleted_consensus_age = 0
	deleted_targets_age = 0
	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		# --- consensus_forecasts ---
		cursor.execute("SELECT DISTINCT uid FROM consensus_forecasts")
		for (c_uid,) in cursor.fetchall():
			cursor.execute(
				"SELECT id FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC, id DESC LIMIT ?", (c_uid, max_consensus)
			)
			keep_ids = {row[0] for row in cursor.fetchall()}
			cursor.execute(
				"SELECT id FROM consensus_forecasts WHERE uid=?", (c_uid,)
			)
			all_ids = {row[0] for row in cursor.fetchall()}
			remove_ids = list(all_ids - keep_ids)
			if remove_ids:
				cursor.executemany("DELETE FROM consensus_forecasts WHERE id=?", [(rid,) for rid in remove_ids])
				deleted_consensus += len(remove_ids)

		# --- consensus_targets (per uid, company) ---
		cursor.execute("SELECT DISTINCT uid, company FROM consensus_targets")
		for c_uid, company in cursor.fetchall():
			cursor.execute(
				"SELECT id FROM consensus_targets WHERE uid=? AND company=? ORDER BY recommendationDate DESC, id DESC LIMIT ?",
				(c_uid, company, max_targets_per_analyst),
			)
			keep_ids = {row[0] for row in cursor.fetchall()}
			cursor.execute(
				"SELECT id FROM consensus_targets WHERE uid=? AND company=?",
				(c_uid, company),
			)
			all_ids = {row[0] for row in cursor.fetchall()}
			remove_ids = list(all_ids - keep_ids)
			if remove_ids:
				cursor.executemany("DELETE FROM consensus_targets WHERE id=?", [(rid,) for rid in remove_ids])
				deleted_targets += len(remove_ids)

		# --- age-based pruning (optional) ---
		if max_age_days is not None and max_age_days > 0:
			cutoff_dt = datetime.now(timezone.utc) - timedelta(days=max_age_days)
			cutoff_iso_prefix = cutoff_dt.isoformat()  # ISO для лексикографического сравнения при одинаковом формате
			# Так как форматы дат могут включать Z или +00:00, делаем отбор в Python
			# consensus_forecasts
			cursor.execute("SELECT id, recommendationDate FROM consensus_forecasts")
			for _id, dt_str in cursor.fetchall():
				if _is_older(dt_str, cutoff_dt):
					cursor.execute("DELETE FROM consensus_forecasts WHERE id=?", (_id,))
					deleted_consensus_age += 1
			# consensus_targets
			cursor.execute("SELECT id, recommendationDate FROM consensus_targets")
			for _id, dt_str in cursor.fetchall():
				if _is_older(dt_str, cutoff_dt):
					cursor.execute("DELETE FROM consensus_targets WHERE id=?", (_id,))
					deleted_targets_age += 1

		conn.commit()

	if any((deleted_consensus, deleted_targets, deleted_consensus_age, deleted_targets_age)):
		logging.info(
			"Очистка истории: по лимитам удалено %s consensus_forecasts, %s consensus_targets; по возрасту удалено %s + %s (дней>%s) (лимиты %s / %s)",
			deleted_consensus,
			deleted_targets,
			deleted_consensus_age,
			deleted_targets_age,
			max_age_days,
			max_consensus,
			max_targets_per_analyst,
		)
	else:
		logging.info(
			"Очистка истории: ничего не удалено (лимиты %s / %s не превышены, возрастной порог %s дней)",
			max_consensus,
			max_targets_per_analyst,
			max_age_days,
		)


def export_perspective_shares_to_excel(db_path: Path, filename: str = "perspective_shares.xlsx") -> None:
	"""Export perspective_shares table to an Excel file."""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT * FROM perspective_shares")
		rows = cursor.fetchall()
		columns = [desc[0] for desc in cursor.description]

	wb = openpyxl.Workbook()
	ws = wb.active
	ws.title = "Perspective Shares"
	ws.append(columns)
	for row in rows:
		ws.append(list(row))

	wb.save(filename)
	logging.info("Таблица perspective_shares экспортирована в %s", filename)


def export_consensus_to_excel(db_path: Path, filename: str = "consensus_data.xlsx") -> None:
	"""Export consensus_forecasts and consensus_targets tables into a single Excel file."""

	tables = [
		("consensus_forecasts", "Consensus Forecasts"),
		("consensus_targets", "Consensus Targets"),
	]

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()

		wb = openpyxl.Workbook()

		for index, (table_name, sheet_title) in enumerate(tables):
			cursor.execute(f"SELECT * FROM {table_name}")
			rows = cursor.fetchall()
			columns = [desc[0] for desc in cursor.description] if cursor.description else []

			ws = wb.active if index == 0 else wb.create_sheet()
			ws.title = sheet_title
			if columns:
				ws.append(columns)
			for row in rows:
				ws.append(list(row))

	wb.save(filename)
	logging.info(
		"Таблицы consensus_forecasts и consensus_targets экспортированы в %s",
		filename,
	)


# --- CLI ---------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Инструменты работы с инвестиционными данными.")
	parser.add_argument("--fill-start", action="store_true", help="заполнить таблицу perspective_shares стартовыми бумагами")
	parser.add_argument("--fill-attributes", action="store_true", help="обновить отсутствующие атрибуты бумаг")
	parser.add_argument("--add-share", metavar="QUERY", help="добавить новую бумагу по поисковой фразе")
	parser.add_argument("--export", nargs="?", const="perspective_shares.xlsx", metavar="FILENAME", help="экспортировать таблицу perspective_shares в Excel")
	parser.add_argument(
		"--export-consensus",
		nargs="?",
		const="consensus_data.xlsx",
		metavar="FILENAME",
		help="экспортировать таблицы consensus_forecasts и consensus_targets в Excel",
	)
	parser.add_argument("--update-consensus", nargs="?", const="ALL", metavar="UID", help="обновить консенсус прогнозы для всех бумаг или указанного UID")
	parser.add_argument(
		"--fill-consensus",
		action="store_true",
		help="первичное массовое наполнение консенсус прогнозов и целей аналитиков без очистки истории",
	)
	parser.add_argument(
		"--fill-consensus-limit",
		type=int,
		metavar="N",
		help="ограничить число инструментов при выполнении --fill-consensus (для теста)",
	)
	parser.add_argument(
		"--fill-consensus-sleep",
		type=float,
		metavar="SEC",
		help="пауза (секунды) между запросами при --fill-consensus для снижения нагрузки",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	initialize_database(DB_PATH)
	CreateTables(DB_PATH)
	migrate_schema(DB_PATH)

	if args.fill_start:
		FillingStartDdata(DB_PATH)

	if args.fill_attributes:
		FillingSharesData(DB_PATH, TOKEN)

	if args.add_share:
		AddShareData(DB_PATH, args.add_share, TOKEN)

	if args.export:
		export_perspective_shares_to_excel(DB_PATH, args.export)

	if args.export_consensus:
		export_consensus_to_excel(DB_PATH, args.export_consensus)

	if args.update_consensus:
		uid = None if args.update_consensus == "ALL" else args.update_consensus
		UpdateConsensusForecasts(DB_PATH, TOKEN, uid=uid)

	if args.fill_consensus:
		FillingConsensusData(
			DB_PATH,
			TOKEN,
			limit=getattr(args, "fill_consensus_limit", None),
			sleep_sec=(getattr(args, "fill_consensus_sleep", None) or 0.0),
		)

	if not any((
		args.fill_start,
		args.fill_attributes,
		args.add_share,
		args.export,
		args.export_consensus,
		args.update_consensus,
		args.fill_consensus,
	)):
		logging.info("База данных подготовлена. Дополнительные действия не выполнялись. Используйте --help для подсказки.")


if __name__ == "__main__":
	main()
