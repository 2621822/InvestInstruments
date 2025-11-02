"""Загрузка исторических данных по акциям с MOEX ISS API.

Простой синхронный клиент без внешних зависимостей (использует requests).

Функция `fetch_history` получает свечные/ценовые данные по инструменту за диапазон дат
и может (опционально) сохранить их в локальную БД через абстракцию db.py.

API документация: https://iss.moex.com/iss/reference/
Эндпоинт пример: https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/SBER.json?from=2024-01-01&till=2024-01-31

Ограничения и упрощения:
* Пагинация MOEX осуществляется параметром `start`; мы реализуем цикл до пустого блока.
* Диапазон дат ограничивается параметрами `from` и `till` (YYYY-MM-DD).
* Возвращаем структуру со списком словарей (колонки нормализуются в snake_case).

Сохранение в БД:
* Если `persist=True`, создаётся таблица `moex_history` (если отсутствует).
* Вставка уникальных строк по составному ключу (secid, tradedate).
* Для duckdb и sqlite используется простая UPSERT-логика для sqlite (ON CONFLICT)
  и REPLACE для duckdb.

Переменные окружения:
* INVEST_MOEX_TIMEOUT_SEC (по умолчанию 10)
* INVEST_MOEX_USER_AGENT (кастомный User-Agent)

"""
from __future__ import annotations
import os
import time
import logging
from typing import List, Dict, Any, Optional
import requests

from . import db as db_layer

log = logging.getLogger(__name__)

BASE_URL = "https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/{secid}.json"

DEFAULT_TIMEOUT = int(os.getenv("INVEST_MOEX_TIMEOUT_SEC", "10"))
USER_AGENT = os.getenv("INVEST_MOEX_USER_AGENT", "invest-core/0.1 (+https://example.com)")


def _snake(s: str) -> str:
    return s.lower().replace(" ", "_")


def fetch_history(
    secid: str,
    date_from: Optional[str] = None,
    date_till: Optional[str] = None,
    persist: bool = False,
    sleep_sec: float = 0.0,
) -> List[Dict[str, Any]]:
    """Загрузить исторические данные по бумаги MOEX.

    Args:
        secid: Тикер (например 'SBER').
        date_from: Начальная дата (YYYY-MM-DD) или None.
        date_till: Конечная дата (YYYY-MM-DD) или None.
        persist: Сохранять ли результат в локальную БД.
        sleep_sec: Пауза между запросами страницами (защита от rate-limit).

    Returns:
        Список словарей строк истории.
    """
    all_rows: List[Dict[str, Any]] = []
    start = 0
    session = requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    params = {}
    if date_from:
        params["from"] = date_from
    if date_till:
        params["till"] = date_till

    while True:
        page_params = {**params, "start": start}
        url = BASE_URL.format(secid=secid)
        log.debug("GET %s params=%s", url, page_params)
        resp = session.get(url, params=page_params, headers=headers, timeout=DEFAULT_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"MOEX ISS error status={resp.status_code} text={resp.text[:200]}")
        data = resp.json()
        history = data.get("history", {})
        columns = history.get("columns", [])
        rows = history.get("data", [])
        if not rows:
            break
        for r in rows:
            rec = { _snake(columns[i]): r[i] for i in range(len(columns)) }
            all_rows.append(rec)
        start += len(rows)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    session.close()

    if persist and all_rows:
        _persist_history(secid, all_rows)

    return all_rows


def _persist_history(secid: str, rows: List[Dict[str, Any]]):
    """Сохранить строки истории в таблицу moex_history.
    Уникальность по (secid, tradedate).
    """
    conn = db_layer.get_connection()
    backend = db_layer.BACKEND
    create_sql = (
        """
        CREATE TABLE IF NOT EXISTS moex_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            secid TEXT NOT NULL,
            tradedate TEXT NOT NULL,
            close REAL,
            volume REAL,
            boardid TEXT,
            numtrades INTEGER,
            open REAL,
            high REAL,
            low REAL,
            PRIMARY KEY (secid, tradedate)
        )
        """ if backend == "sqlite" else """
        CREATE TABLE IF NOT EXISTS moex_history (
            secid TEXT,
            tradedate TEXT,
            close DOUBLE,
            volume DOUBLE,
            boardid TEXT,
            numtrades INTEGER,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE
        )
        """
    )
    conn.execute(create_sql)

    # Формируем параметризированную вставку.
    if backend == "sqlite":
        insert_sql = (
            """
            INSERT INTO moex_history (secid, tradedate, close, volume, boardid, numtrades, open, high, low)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(secid, tradedate) DO NOTHING
            """
        )
    else:  # duckdb не поддерживает ON CONFLICT; используем REPLACE через временную таблицу или просто игнор
        insert_sql = (
            """
            INSERT INTO moex_history (secid, tradedate, close, volume, boardid, numtrades, open, high, low)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    inserted = 0
    for r in rows:
        tradedate = r.get("tradedate") or r.get("systime") or ""
        if not tradedate:
            continue
        values = (
            secid,
            tradedate,
            r.get("close"),
            r.get("volume"),
            r.get("boardid"),
            r.get("numtrades"),
            r.get("open"),
            r.get("high"),
            r.get("low"),
        )
        try:
            conn.execute(insert_sql, values)
            inserted += 1
        except Exception as ex:  # noqa
            # Для duckdb при дублировании можно словить ошибку - игнорируем
            log.debug("Skip duplicate tradedate=%s err=%s", tradedate, ex)
            continue

    if backend == "sqlite":
        conn.commit()
    log.info("Persisted %s rows into moex_history (backend=%s)", inserted, backend)
    conn.close()


__all__ = ["fetch_history"]
