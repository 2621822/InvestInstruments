"""rest_instruments.py

REST взаимодействия с публичным API Tinkoff Invest для поиска инструментов.

Реализует функцию GetUidInstrument(ticker):
  * Делает POST на InstrumentsService/FindInstrument
  * Передаёт payload: {query: <ticker>, instrumentKind: INSTRUMENT_TYPE_SHARE, apiTradeAvailableFlag: True}
  * Парсит список найденных инструментов
  * Фильтрует по точному совпадению поля ticker (регистронезависимо), instrumentType == share и classCode == TQBR
  * Возвращает UID (str) или None.

Примечания:
  * verify=False отключает проверку TLS (в проекте есть локальный сертификат). Можно включить при необходимости.
  * Возвращает первый подходящий инструмент когда тикеров несколько.
"""
from __future__ import annotations
import requests
import logging
from typing import Optional, Any, Dict

from . import sdk_client

log = logging.getLogger(__name__)

FIND_URL = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"

HEADERS_TEMPLATE: Dict[str,str] = {
	"Content-Type": "application/json",
	"Accept": "application/json",
}

def _safe_get(obj: Any, *names: str):
	for n in names:
		if isinstance(obj, dict) and n in obj:
			return obj[n]
		if hasattr(obj, n):
			return getattr(obj, n)
	return None

def GetUidInstrument(ticker: str) -> Optional[str]:
	"""Вернуть UID акции по точному тикеру через REST FindInstrument.

	Алгоритм:
		1. Запрос FindInstrument (share, apiTradeAvailableFlag=True)
		2. Первая попытка: exact ticker + instrumentType=share + classCode=TQBR
		3. Если не найдено – fallback: exact ticker + instrumentType=share (любой classCode) -> предупреждение.
	"""
	token = sdk_client.load_token().strip()
	if not token:
		log.warning("Token not found for REST call")
		return None
	headers = HEADERS_TEMPLATE.copy()
	headers["Authorization"] = f"Bearer {token}"
	payload = {
		"query": ticker,
		"instrumentKind": "INSTRUMENT_TYPE_SHARE",
		"apiTradeAvailableFlag": True,
	}
	try:
		resp = requests.post(FIND_URL, headers=headers, json=payload, timeout=10, verify=False)
	except Exception as ex:
		log.warning("REST FindInstrument error ticker=%s err=%s", ticker, ex)
		return None
	if resp.status_code != 200:
		log.debug("FindInstrument non-200 ticker=%s status=%s body=%s", ticker, resp.status_code, resp.text[:500])
		return None
	data = resp.json()
	instruments = data.get("instruments") or data.get("items") or []
	if not instruments:
		return None
	t_upper = ticker.upper()
	primary_uid: Optional[str] = None
	fallback_uid: Optional[str] = None
	fallback_class: Optional[str] = None
	for item in instruments:
		inst = item.get("instrument") if isinstance(item, dict) else _safe_get(item, "instrument")
		inst = inst or item
		cand_ticker = _safe_get(inst, "ticker")
		cand_type = _safe_get(inst, "instrument_type", "instrumentType")
		cand_class = _safe_get(inst, "class_code", "classCode")
		uid = _safe_get(inst, "uid")
		if not (cand_ticker and cand_type and uid):
			continue
		if cand_ticker.upper() != t_upper:
			continue
		if str(cand_type).lower() != "share":
			continue
		# Если класс совпал – первичный выбор
		if cand_class == "TQBR" and primary_uid is None:
			primary_uid = uid
		# Иначе запомним как fallback (первый подходящий)
		elif fallback_uid is None:
			fallback_uid = uid
			fallback_class = cand_class
	if primary_uid:
		return primary_uid
	if fallback_uid:
		log.warning("Fallback (no TQBR) for ticker=%s using classCode=%s", ticker, fallback_class)
		return fallback_uid
	return None

__all__ = ["GetUidInstrument", "FIND_URL", "HEADERS_TEMPLATE"]

