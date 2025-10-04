"""Асинхронные обёртки для работы с API Tinkoff Invest (aiohttp).

Используются web-приложением для фоновых задач и REST эндпоинтов.
CLI продолжает использовать синхронные функции из main.py.
"""
from __future__ import annotations

import asyncio
import time
import logging
import os
from typing import Tuple, List, Dict, Any

import aiohttp
import asyncio


API_BASE_URL = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService"
FIND_ENDPOINT = "FindInstrument"
GET_ENDPOINT = "GetInstrumentBy"
GET_FORECAST_ENDPOINT = "GetForecastBy"

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "15"))
API_MAX_ATTEMPTS = int(os.getenv("API_MAX_ATTEMPTS", "3"))
API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "0.5"))


async def _post_async(session: aiohttp.ClientSession, endpoint: str, payload: Dict[str, Any], token: str) -> Dict | None:
    if not token:
        logging.error("_post_async: токен не задан (%s)", endpoint)
        return None
    url = f"{API_BASE_URL}/{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(1, API_MAX_ATTEMPTS + 1):
        try:
            async with session.post(url, json=payload, headers=headers, timeout=API_TIMEOUT) as resp:
                if 500 <= resp.status < 600:
                    raise aiohttp.ClientError(f"Server {resp.status}")
                if resp.status >= 400:
                    text = await resp.text()
                    logging.warning("API %s ошибка %s: %s", endpoint, resp.status, text[:200])
                    return None
                return await resp.json()
        except Exception as exc:
            if attempt == API_MAX_ATTEMPTS:
                logging.error("API %s не выполнен после %s попыток: %s", endpoint, attempt, exc)
                return None
            backoff = API_BACKOFF_BASE * (2 ** (attempt - 1))
            logging.warning("Сбой API %s попытка %s/%s: %s (retry %.2fs)", endpoint, attempt, API_MAX_ATTEMPTS, exc, backoff)
            await asyncio.sleep(backoff)
    return None


async def GetUidInstrumentAsync(session: aiohttp.ClientSession, search_phrase: str, token: str) -> str | None:
    payload = {
        "query": search_phrase,
        "instrumentKind": "INSTRUMENT_TYPE_SHARE",
        "apiTradeAvailableFlag": True,
    }
    data = await _post_async(session, FIND_ENDPOINT, payload, token)
    if not data:
        return None
    instruments = data.get("instruments", [])
    if not instruments:
        return None
    lower_query = search_phrase.lower()
    for inst in instruments:
        ticker = inst.get("ticker", "")
        name = inst.get("name", "")
        if ticker.lower() == lower_query or name.lower() == lower_query:
            return inst.get("uid")
    return instruments[0].get("uid")


async def GetInstrumentByUidAsync(session: aiohttp.ClientSession, uid: str, token: str) -> Dict | None:
    payload = {"idType": "INSTRUMENT_ID_TYPE_UID", "id": uid}
    data = await _post_async(session, GET_ENDPOINT, payload, token)
    instrument = data.get("instrument") if data else None
    if not instrument:
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


async def GetConsensusByUidAsync(session: aiohttp.ClientSession, uid: str, token: str) -> Tuple[Dict | None, List[Dict]]:
    payload = {"instrumentId": uid}
    data = await _post_async(session, GET_FORECAST_ENDPOINT, payload, token)
    if not data:
        return None, []
    return data.get("consensus"), data.get("targets", [])


async def FetchConsensusBatch(
    uids: list[str],
    token: str,
    *,
    concurrency: int = 5,
    timeout: float | None = None,
) -> tuple[dict[str, Tuple[Dict | None, List[Dict]]], dict]:
    """Пакетно получить консенсус/таргеты (параллельно, одна сессия) + метрики.

    Возврат: (results, metrics)
      results: uid -> (consensus, targets)
      metrics: {
         total, attempted, success, empty, errors, duration, concurrency
      }
    timeout — общий лимит времени (сек) на весь батч (оставшиеся задачи отменяются).
    """
    start_time = time.perf_counter()
    results: dict[str, Tuple[Dict | None, List[Dict]]] = {}
    total = len(uids)
    if not uids:
        return results, {
            "total": 0,
            "attempted": 0,
            "success": 0,
            "empty": 0,
            "errors": 0,
            "duration": 0.0,
            "concurrency": concurrency,
            "timeout": timeout,
        }

    sem = asyncio.Semaphore(max(1, concurrency))
    success = 0
    empty = 0
    errors = 0
    attempted = 0
    log = __import__('logging')

    async with aiohttp.ClientSession() as session:
        async def worker(uid: str):
            nonlocal success, empty, errors, attempted
            async with sem:
                attempted += 1
                try:
                    c, t = await GetConsensusByUidAsync(session, uid, token)
                    if c or t:
                        success += 1
                    else:
                        empty += 1
                    results[uid] = (c, t)
                except Exception as e:  # noqa
                    errors += 1
                    log.warning("FetchConsensusBatch: ошибка uid=%s: %s", uid, e)
                    results[uid] = (None, [])

        tasks = [asyncio.create_task(worker(u)) for u in uids]
        if timeout is not None and timeout > 0:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                for p in pending:
                    p.cancel()
                errors += len(pending)
                log.warning("FetchConsensusBatch: превышен таймаут %.2fs, отменено задач: %s", timeout, len(pending))
        else:
            await asyncio.gather(*tasks)

    duration = time.perf_counter() - start_time
    metrics = {
        "total": total,
        "attempted": attempted,
        "success": success,
        "empty": empty,
        "errors": errors,
        "duration": round(duration, 3),
        "concurrency": concurrency,
        "timeout": timeout,
    }
    return results, metrics
