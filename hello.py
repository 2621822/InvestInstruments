import requests

def GetInstrumentByUID(db_path, token):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	cursor.execute("SELECT uid FROM perspective_shares")
	uids = [row[0] for row in cursor.fetchall()]
	headers = get_api_headers(token)
	for uid in uids:
		# Получаем основные атрибуты через FindInstrument
		url_find = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
		payload_find = {
			"query": uid,
			"instrumentKind": "INSTRUMENT_TYPE_SHAR"
		}
		response_find = requests.post(url_find, json=payload_find, headers=headers, verify=False)
		data_find = response_find.json()
		instrument = None
		if "instruments" in data_find and data_find["instruments"]:
			instrument = data_find["instruments"][0]
		# Получаем assetUid через GetInstrumentBy
		url_asset = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy"
		payload_asset = {
			"idType": "INSTRUMENT_ID_TYPE_UID",
			"id": uid
		}
		response_asset = requests.post(url_asset, json=payload_asset, headers=headers, verify=False)
		data_asset = response_asset.json()
		assetUid = data_asset.get("instrument", {}).get("assetUid", None)
		# Заполняем все атрибуты
		if instrument:
			cursor.execute("""
				UPDATE perspective_shares SET
					ticker=?, name=?, secid=?, isin=?, figi=?, classCode=?, instrumentType=?, assetUid=?
				WHERE uid=?
			""", (
				instrument.get("ticker"),
				instrument.get("name"),
				instrument.get("ticker"), # secid = ticker
				instrument.get("isin"),
				instrument.get("figi"),
				instrument.get("classCode"),
				instrument.get("instrumentType"),
				assetUid,
				uid
			))
			conn.commit()
	conn.close()
def get_api_headers(token):
	"""Возвращает заголовки для авторизации по API с Bearer токеном."""
	return {"Authorization": f"Bearer {token}"}
import openpyxl

def export_perspective_shares_to_excel(db_path, filename="perspective_shares.xlsx"):
	conn = sqlite3.connect(db_path)
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
	conn.close()
	print(f"Данные экспортированы в {filename}")
def fillingStartDdata(db_path):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	cursor.execute("SELECT COUNT(*) FROM perspective_shares")
	count = cursor.fetchone()[0]
	if count == 0:
		start_data = [
			("7de75794-a27f-4d81-a39b-492345813822", "Яндекс"),
			("87db07bc-0e02-4e29-90bb-05e8ef791d7b", "Т-Технологии"),
			("e6123145-9665-43e0-8413-cd61b8aa9b13", "Сбер Банк"),
			("c190ff1f-1447-4227-b543-316332699ca5", "Сбер Банк - привилегированные акции"),
			("10620843-28ce-44e8-80c2-f26ceb1bd3e1", "Полюс"),
			("02cfdf61-6298-4c0f-a9ca-9cabc82afaf3", "ЛУКОЙЛ"),
		]
		cursor.executemany(
			"INSERT INTO perspective_shares (uid, name) VALUES (?, ?)",
			start_data
		)
		conn.commit()
		print("Таблица perspective_shares заполнена стартовыми бумагами.")
	else:
		print("Таблица perspective_shares уже содержит данные. Стартовое наполнение не требуется.")
	conn.close()
import sqlite3

def create_tables(db_path):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	# 1. Таблица perspective_shares
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS perspective_shares (
			ticker TEXT,
			name TEXT,
			uid TEXT PRIMARY KEY,
			secid TEXT,
			isin TEXT,
			figi TEXT,
			classCode TEXT,
			instrumentType TEXT,
			assetUid TEXT
		)
	''')
	# 2. Таблица consensus_forecasts
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS consensus_forecasts (
			uid TEXT PRIMARY KEY,
			ticker TEXT,
			recommendation TEXT,
			recommendationDate TEXT,
			currency TEXT,
			price_consensus REAL,
			minTarget REAL,
			maxTarget REAL
		)
	''')
	# 3. Таблица consensus_targets
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS consensus_targets (
			uid TEXT PRIMARY KEY,
			ticker TEXT,
			company TEXT,
			recommendation TEXT,
			recommendationDate TEXT,
			currency TEXT,
			targetPrice REAL,
			showName TEXT
		)
	''')
	conn.commit()
	conn.close()

import sqlite3
# Автоматический запуск при старте
if __name__ == "__main__":
	db_path = "project.db"
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	cursor.execute("SELECT COUNT(*) FROM perspective_shares")
	count = cursor.fetchone()[0]
	conn.close()
	if count == 0:
		fillingStartDdata(db_path)
import requests

def GetInstrumentByUID(db_path, token):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	cursor.execute("SELECT uid FROM perspective_shares")
	uids = [row[0] for row in cursor.fetchall()]
	headers = get_api_headers(token)
	for uid in uids:
		# Получаем основные атрибуты через FindInstrument
		url_find = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
		payload_find = {
			"query": uid,
			"instrumentKind": "INSTRUMENT_TYPE_SHAR"
		}
		response_find = requests.post(url_find, json=payload_find, headers=headers, verify=False)
		data_find = response_find.json()
		instrument = None
		if "instruments" in data_find and data_find["instruments"]:
			instrument = data_find["instruments"][0]
		# Получаем assetUid через GetInstrumentBy
		url_asset = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetInstrumentBy"
		payload_asset = {
			"idType": "INSTRUMENT_ID_TYPE_UID",
			"id": uid
		}
		response_asset = requests.post(url_asset, json=payload_asset, headers=headers, verify=False)
		data_asset = response_asset.json()
		assetUid = data_asset.get("instrument", {}).get("assetUid", None)
		# Заполняем все атрибуты
		if instrument:
			cursor.execute("""
				UPDATE perspective_shares SET
					ticker=?, name=?, secid=?, isin=?, figi=?, classCode=?, instrumentType=?, assetUid=?
				WHERE uid=?
			""", (
				instrument.get("ticker"),
				instrument.get("name"),
				instrument.get("ticker"), # secid = ticker
				instrument.get("isin"),
				instrument.get("figi"),
				instrument.get("classCode"),
				instrument.get("instrumentType"),
				assetUid,
				uid
			))
			conn.commit()
	conn.close()
def get_api_headers(token):
	"""Возвращает заголовки для авторизации по API с Bearer токеном."""
	return {"Authorization": f"Bearer {token}"}
import openpyxl

def export_perspective_shares_to_excel(db_path, filename="perspective_shares.xlsx"):
	conn = sqlite3.connect(db_path)
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
	conn.close()
	print(f"Данные экспортированы в {filename}")
def fillingStartDdata(db_path):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	cursor.execute("SELECT COUNT(*) FROM perspective_shares")
	count = cursor.fetchone()[0]
	if count == 0:
		start_data = [
			("7de75794-a27f-4d81-a39b-492345813822", "Яндекс"),
			("87db07bc-0e02-4e29-90bb-05e8ef791d7b", "Т-Технологии"),
			("e6123145-9665-43e0-8413-cd61b8aa9b13", "Сбер Банк"),
			("c190ff1f-1447-4227-b543-316332699ca5", "Сбер Банк - привилегированные акции"),
			("10620843-28ce-44e8-80c2-f26ceb1bd3e1", "Полюс"),
			("02cfdf61-6298-4c0f-a9ca-9cabc82afaf3", "ЛУКОЙЛ"),
		]
		cursor.executemany(
			"INSERT INTO perspective_shares (uid, name) VALUES (?, ?)",
			start_data
		)
		conn.commit()
		print("Таблица perspective_shares заполнена стартовыми бумагами.")
	else:
		print("Таблица perspective_shares уже содержит данные. Стартовое наполнение не требуется.")
	conn.close()
import sqlite3

def create_tables(db_path):
	conn = sqlite3.connect(db_path)
	cursor = conn.cursor()
	# 1. Таблица perspective_shares
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS perspective_shares (
			ticker TEXT,
			name TEXT,
			uid TEXT PRIMARY KEY,
			secid TEXT,
			isin TEXT,
			figi TEXT,
			classCode TEXT,
			instrumentType TEXT,
			assetUid TEXT
		)
	''')
	# 2. Таблица consensus_forecasts
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS consensus_forecasts (
			uid TEXT PRIMARY KEY,
			ticker TEXT,
			recommendation TEXT,
			recommendationDate TEXT,
			currency TEXT,
			price_consensus REAL,
			minTarget REAL,
			maxTarget REAL
		)
	''')
	# 3. Таблица consensus_targets
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS consensus_targets (
			uid TEXT PRIMARY KEY,
			ticker TEXT,
			company TEXT,
			recommendation TEXT,
			recommendationDate TEXT,
			currency TEXT,
			targetPrice REAL,
			showName TEXT
		)
	''')
	conn.commit()
	conn.close()
