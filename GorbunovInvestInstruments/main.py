"""Investment instruments helper utilities."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import openpyxl
import requests
import sqlite3
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --- CONFIG ------------------------------------------------------------------
DB_PATH = Path("GorbunovInvestInstruments.db")
TOKEN = "t.pdllofbQHnH9F0SyYtg1YZwMPM_eAbB-V51HqAI_AVS61ODDiS4O-mMc3YaGk25kEFN1k6_iq2rnedhoWlCRLQ"

API_BASE_URL = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService"
FIND_ENDPOINT = "FindInstrument"
GET_ENDPOINT = "GetInstrumentBy"

SESSION = requests.Session()
SESSION.verify = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


# --- API HELPERS -------------------------------------------------------------
def PostApiHeaders(token: str) -> dict[str, str]:
	"""Return headers required for API authorization."""

	return {"Authorization": f"Bearer {token}"}


def _post(endpoint: str, payload: dict, token: str) -> dict | None:
	"""Send a POST request to the API and return JSON response or None on error."""

	url = f"{API_BASE_URL}/{endpoint}"
	try:
		response = SESSION.post(url, json=payload, headers=PostApiHeaders(token), timeout=15)
		response.raise_for_status()
	except requests.RequestException as exc:
		logging.error("API request to %s failed: %s", endpoint, exc)
		return None

	try:
		return response.json()
	except ValueError:
		logging.error("API response from %s is not valid JSON", endpoint)
		return None


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
		conn.commit()


def FillingStartDdata(db_path: Path) -> None:
	"""Fill the perspective_shares table with initial set of instruments."""

	with sqlite3.connect(db_path) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT COUNT(*) FROM perspective_shares")
		count_before = cursor.fetchone()[0]
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

		cursor.execute("SELECT COUNT(*) FROM perspective_shares")
		count_after = cursor.fetchone()[0]
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

		cursor.execute("SELECT COUNT(*) FROM perspective_shares")
		total_rows = cursor.fetchone()[0]
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
			cursor.execute("SELECT COUNT(*) FROM perspective_shares")
			total_rows = cursor.fetchone()[0]
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
	logging.info("Данные экспортированы в %s", filename)


# --- CLI ---------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Инструменты работы с инвестиционными данными.")
	parser.add_argument("--fill-start", action="store_true", help="заполнить таблицу perspective_shares стартовыми бумагами")
	parser.add_argument("--fill-attributes", action="store_true", help="обновить отсутствующие атрибуты бумаг")
	parser.add_argument("--add-share", metavar="QUERY", help="добавить новую бумагу по поисковой фразе")
	parser.add_argument("--export", nargs="?", const="perspective_shares.xlsx", metavar="FILENAME", help="экспортировать таблицу perspective_shares в Excel")
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	initialize_database(DB_PATH)
	CreateTables(DB_PATH)

	if args.fill_start:
		FillingStartDdata(DB_PATH)

	if args.fill_attributes:
		FillingSharesData(DB_PATH, TOKEN)

	if args.add_share:
		AddShareData(DB_PATH, args.add_share, TOKEN)

	if args.export:
		export_perspective_shares_to_excel(DB_PATH, args.export)

	if not any((args.fill_start, args.fill_attributes, args.add_share, args.export)):
		logging.info("База данных подготовлена. Дополнительные действия не выполнялись. Используйте --help для подсказки.")


if __name__ == "__main__":
	main()
