"""
T-Ivest: Автоматизация работы с таблицей promising_shares и интеграция с Tinkoff Invest API.
"""
"""
"""
import sqlite3
import requests
import openpyxl
import pandas as pd

# Константы
DB_PATH = "moex_data.db"
API_BASE = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService"
TOKEN = "t.pdllofbQHnH9F0SyYtg1YZwMPM_eAbB-V51HqAI_AVS61ODDiS4O-mMc3YaGk25kEFN1k6_iq2rnedhoWlCRLQ"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

def create_promising_shares_table_once():
	"""Создаёт таблицу promising_shares с нужной структурой и начальными акциями, если пуста."""
	conn = sqlite3.connect(DB_PATH)
	cursor = conn.cursor()
	cursor.execute(
		"""
		CREATE TABLE IF NOT EXISTS promising_shares (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ticker TEXT,
			name TEXT,
			instrument_uid TEXT UNIQUE,
			secid TEXT,
			ISIN TEXT,
			assetUid TEXT
		)
		"""
	)
	conn.commit()
	cursor.execute("SELECT COUNT(*) FROM promising_shares")
	count = cursor.fetchone()[0]
	if count == 0:
		initial_shares = [
			{"instrument_uid": "7de75794-a27f-4d81-a39b-492345813822", "name": "Яндекс"},
			{"instrument_uid": "87db07bc-0e02-4e29-90bb-05e8ef791d7b", "name": "Т-Технологии"},
			{"instrument_uid": "e6123145-9665-43e0-8413-cd61b8aa9b13", "name": "Сбер Банк"},
			{"instrument_uid": "c190ff1f-1447-4227-b543-316332699ca5", "name": "Сбер Банк - привилегированные акции"},
			{"instrument_uid": "10620843-28ce-44e8-80c2-f26ceb1bd3e1", "name": "Полюс"},
			{"instrument_uid": "02cfdf61-6298-4c0f-a9ca-9cabc82afaf3", "name": "ЛУКОЙЛ"},
		]
		for share in initial_shares:
			cursor.execute(
				"INSERT INTO promising_shares (name, instrument_uid) VALUES (?, ?)",
				(share["name"], share["instrument_uid"])
			)
		conn.commit()
	conn.close()

def update_promising_shares_from_csv(csv_path="share.csv"):
	"""Заглушка: не заполняет поля ticker и ISIN из share.csv. Оставлено для совместимости, не изменяет таблицу."""
	print(f"update_promising_shares_from_csv: заполнение ticker и ISIN из share.csv отключено по требованию.")

def get_instrument_by_uid(instrument_uid):
	"""Получить данные по инструменту через API по instrument_uid. Возвращает JSON-ответ или None при ошибке."""
	url = f"{API_BASE}/GetInstrumentBy"
	payload = {
		"idType": "INSTRUMENT_ID_TYPE_UID",
		"id": instrument_uid
	}
	try:
		response = requests.post(url, headers=HEADERS, json=payload, verify=False)
		response.raise_for_status()
		return response.json()
	except Exception as e:
		print(f"Ошибка при получении данных по инструменту {instrument_uid}: {e}")
		return None

def update_assetUid_for_all():
	"""Для всех записей в promising_shares получить assetUid, ISIN, ticker, secid через API и обновить таблицу."""
	conn = sqlite3.connect(DB_PATH)
	cursor = conn.cursor()
	cursor.execute("SELECT id, instrument_uid FROM promising_shares")
	rows = cursor.fetchall()
	for rec_id, instrument_uid in rows:
		data = get_instrument_by_uid(instrument_uid)
		assetUid = None
		ISIN = None
		ticker = None
		secid = None
		if data and isinstance(data, dict):
			instr = data.get("instrument", {})
			assetUid = instr.get("assetUid")
			ISIN = instr.get("isin")
			ticker = instr.get("ticker")
			secid = ticker
		cursor.execute(
			"UPDATE promising_shares SET assetUid=?, ISIN=?, ticker=?, secid=? WHERE id=?",
			(assetUid, ISIN, ticker, secid, rec_id)
		)
		print(f"Обновлено: id={rec_id}, instrument_uid={instrument_uid}, assetUid={assetUid}, ISIN={ISIN}, ticker={ticker}")
	conn.commit()
	conn.close()



def main():
	"""
	Основной сценарий: 1. Создать таблицу promising_shares (если нет) 2. Заполнить начальными акциями (если пуста) 3. Обновить данные по instrument_uid через API 4. (Заглушка) update_promising_shares_from_csv 5. Экспортировать результат в Excel
	"""
	create_promising_shares_table_once()
	print("Таблица promising_shares создана (или уже существует) и заполнена начальными акциями, если была пуста.")
	update_assetUid_for_all()
	update_promising_shares_from_csv()
