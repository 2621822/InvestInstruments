"""data_forecasts.py

Минимально необходимая логика для:
 - дозагрузки отсутствующих прогнозов (EnsureForecastsForMissingShares)
 - массового первичного наполнения (FillingConsensusData)
 - обновления всех прогнозов (UpdateConsensusForecasts)

Обрезанная упрощённая версия, извлечённая из legacy main.py без лишних CLI / побочных эффектов.
"""
from __future__ import annotations

import logging
import math
import hashlib
import os
import random
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable

import requests
from requests import exceptions as req_exc

DB_PATH = Path("GorbunovInvestInstruments.db")

API_BASE_URL = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService"
FIND_ENDPOINT = "FindInstrument"
GET_ENDPOINT = "GetInstrumentBy"
GET_FORECAST_ENDPOINT = "GetForecastBy"
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "15"))
API_MAX_ATTEMPTS = int(os.getenv("API_MAX_ATTEMPTS", "3"))
API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "0.5"))
MAX_CONSENSUS_PER_UID = int(os.getenv("CONSENSUS_MAX_PER_UID", "300"))
MAX_TARGETS_PER_ANALYST = int(os.getenv("CONSENSUS_MAX_TARGETS_PER_ANALYST", "100"))
MAX_HISTORY_DAYS = int(os.getenv("CONSENSUS_MAX_HISTORY_DAYS", "1000"))

SESSION = requests.Session()


def _post(endpoint: str, payload: dict, token: str) -> dict | None:
    if not token:
        logging.error("Токен не задан, запрос %s пропущен", endpoint)
        return None
    url = f"{API_BASE_URL}/{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    # Поддержка SSL-настроек через переменные окружения:
    #  TINKOFF_CA_BUNDLE=path/to/ca.pem  — использовать кастомный корневой сертификат(ы)
    #  TINKOFF_SSL_NO_VERIFY=1          — отключить проверку сертификата (НЕ РЕКОМЕНДУЕТСЯ; только для диагностики)
    ca_bundle = os.getenv("TINKOFF_CA_BUNDLE")
    no_verify = os.getenv("TINKOFF_SSL_NO_VERIFY", "").lower() in {"1", "true", "yes"}
    verify_param: bool | str = True
    if ca_bundle:
        if os.path.exists(ca_bundle):
            verify_param = ca_bundle
        else:
            logging.warning("TINKOFF_CA_BUNDLE указан, но файл не найден: %s — используется системный trust store.", ca_bundle)
    if no_verify:
        verify_param = False
        # Логируем один раз (через атрибут функции)
        if not getattr(_post, "_no_verify_logged", False):  # type: ignore[attr-defined]
            logging.warning("ВНИМАНИЕ: проверка SSL сертификата отключена (TINKOFF_SSL_NO_VERIFY=1). Это небезопасно.")
            setattr(_post, "_no_verify_logged", True)  # type: ignore[attr-defined]
    # (Диагностический режим удалён при откате изменений)
    for attempt in range(1, API_MAX_ATTEMPTS + 1):
        try:
            resp = SESSION.post(url, json=payload, headers=headers, timeout=API_TIMEOUT, verify=verify_param)
            if resp.status_code == 404 and endpoint == GET_FORECAST_ENDPOINT:
                return None
            if 500 <= resp.status_code < 600:
                raise requests.RequestException(f"Server {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except req_exc.SSLError:  # noqa: BLE001
            # Подавляем длинный стек и деталь сертификата — фиксируем только факт.
            if attempt == API_MAX_ATTEMPTS:
                logging.error("API %s неуспешен (SSL) после %s попыток", endpoint, attempt)
                return None
            backoff = API_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, API_BACKOFF_BASE)
            logging.warning(
                "API %s SSL сбой попытка %s/%s (sleep %.2fs) — сертификат не подтверждён",
                endpoint, attempt, API_MAX_ATTEMPTS, backoff,
            )
            time.sleep(backoff)
        except requests.RequestException as exc:  # noqa: BLE001
            if attempt == API_MAX_ATTEMPTS:
                logging.error("API %s окончательно неуспешен: %s", endpoint, exc)
                return None
            backoff = API_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, API_BACKOFF_BASE)
            logging.warning("API %s сбой попытка %s/%s: %s (sleep %.2fs)", endpoint, attempt, API_MAX_ATTEMPTS, exc, backoff)
            time.sleep(backoff)
    return None


def _parse_money(val: Any) -> float | None:
    if isinstance(val, dict):
        try:
            units = int(val.get("units", 0)); nano = int(val.get("nano", 0))
            return units + nano / 1_000_000_000
        except Exception:  # noqa: BLE001
            return None
    if isinstance(val, (int, float)):
        return float(val)
    return None


def GetConsensusByUid(uid: str, token: str) -> tuple[dict | None, list[dict]]:  # noqa: N802
    payload = {"instrumentId": uid}
    data = _post(GET_FORECAST_ENDPOINT, payload, token)
    if not data:
        return None, []
    return data.get("consensus"), data.get("targets", [])


def AddConsensusForecasts(db_path: Path, consensus: dict | None) -> None:  # noqa: N802
    if not consensus:
        return
    uid = consensus.get("uid")
    if not uid:
        return
    ticker = consensus.get("ticker", "")
    recommendation = consensus.get("recommendation")
    currency = consensus.get("currency")
    price_consensus = _parse_money(consensus.get("consensus"))
    min_target = _parse_money(consensus.get("minTarget"))
    max_target = _parse_money(consensus.get("maxTarget"))

    def _num_eq(a, b, *, tol=1e-6) -> bool:
        # None == None; if one is None and other not — different
        if a is None or b is None:
            return a is None and b is None
        try:
            return math.isclose(float(a), float(b), rel_tol=0, abs_tol=tol)
        except Exception:  # noqa: BLE001
            return a == b

    def _same_record(last_row) -> bool:
        # last_row layout: uid,ticker,recommendation,currency,priceConsensus,minTarget,maxTarget
        if not last_row:
            return False
        _uid, _tkr, _rec, _cur, _pc, _minT, _maxT = last_row
        return (
            (_tkr or "") == (ticker or "")
            and (_rec or None) == (recommendation or None)
            and (_cur or None) == (currency or None)
            and _num_eq(_pc, price_consensus)
            and _num_eq(_minT, min_target)
            and _num_eq(_maxT, max_target)
        )

    def _fingerprint() -> str:
        parts = [
            str(ticker or ""),
            str(recommendation or ""),
            str(currency or ""),
            "" if price_consensus is None else f"{float(price_consensus):.8f}",
            "" if min_target is None else f"{float(min_target):.8f}",
            "" if max_target is None else f"{float(max_target):.8f}",
        ]
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    fp = _fingerprint()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # --- Миграция: добавить колонку snapshotHash при отсутствии ---
        try:
            cur.execute("PRAGMA table_info(consensus_forecasts)")
            cols = [r[1] for r in cur.fetchall()]
            if 'snapshotHash' not in cols:
                cur.execute("ALTER TABLE consensus_forecasts ADD COLUMN snapshotHash TEXT")
                conn.commit()
                # Обновление существующих строк (легкое, без больших объёмов)
                cur.execute("SELECT id, ticker, recommendation, currency, priceConsensus, minTarget, maxTarget FROM consensus_forecasts")
                rows = cur.fetchall()
                for rid, tkr, rec, curcy, pc, mint, maxt in rows:
                    parts = [
                        str(tkr or ""),
                        str(rec or ""),
                        str(curcy or ""),
                        "" if pc is None else f"{float(pc):.8f}",
                        "" if mint is None else f"{float(mint):.8f}",
                        "" if maxt is None else f"{float(maxt):.8f}",
                    ]
                    raw = "|".join(parts)
                    h = hashlib.sha256(raw.encode('utf-8')).hexdigest()
                    cur.execute("UPDATE consensus_forecasts SET snapshotHash=? WHERE id=?", (h, rid))
                conn.commit()
                cur.execute("CREATE INDEX IF NOT EXISTS ix_cf_uid_hash ON consensus_forecasts(uid, snapshotHash)")
                conn.commit()
        except Exception:  # noqa: BLE001
            pass
        cur.execute(
            """SELECT uid,ticker,recommendation,currency,priceConsensus,minTarget,maxTarget
                 FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC LIMIT 1""",
            (uid,),
        )
        last = cur.fetchone()
        # Быстрая ветка: если есть snapshotHash и совпадает — сразу skip
        fast_skipped = False
        try:
            cur.execute("SELECT snapshotHash FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC LIMIT 1", (uid,))
            row_hash = cur.fetchone()
            if row_hash and row_hash[0] and row_hash[0] == fp:
                logging.debug("Consensus unchanged (hash match) skip uid=%s", uid)
                return
        except Exception:
            pass
        if _same_record(last):
            logging.debug("Consensus unchanged (field compare) skip uid=%s", uid)
            return
        rec_date = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        cur.execute(
            """INSERT INTO consensus_forecasts
                (uid,ticker,recommendation,recommendationDate,currency,priceConsensus,minTarget,maxTarget,snapshotHash)
                VALUES (?,?,?,?,?,?,?,?,?)""",
            (uid, ticker, recommendation, rec_date, currency, price_consensus, min_target, max_target, fp),
        )
        conn.commit()


def AddConsensusTargets(db_path: Path, targets: list[dict]) -> None:  # noqa: N802
    if not targets:
        return
    ins = 0
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for t in targets:
            uid = t.get("uid"); company = t.get("company"); rdate = t.get("recommendationDate")
            if not uid or not company or not rdate:
                continue
            cur.execute(
                """INSERT OR IGNORE INTO consensus_targets
                    (uid,ticker,company,recommendation,recommendationDate,currency,targetPrice,showName)
                    VALUES (?,?,?,?,?,?,?,?)""",
                (
                    uid,
                    t.get("ticker"),
                    company,
                    t.get("recommendation"),
                    rdate,
                    t.get("currency"),
                    _parse_money(t.get("targetPrice")),
                    t.get("showName"),
                ),
            )
            if cur.rowcount:
                ins += 1
        conn.commit()
    if ins:
        logging.info("Добавлено целей аналитиков: %s", ins)


def EnsureForecastsForMissingShares(db_path: Path, token: str, *, prune: bool = True) -> None:  # noqa: N802
    start = time.perf_counter()
    if not token:
        return
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT ps.uid, ps.ticker FROM perspective_shares ps
                WHERE NOT EXISTS (SELECT 1 FROM consensus_forecasts cf WHERE cf.uid=ps.uid)
                  AND NOT EXISTS (SELECT 1 FROM consensus_targets ct WHERE ct.uid=ps.uid)"""
        )
        rows = cur.fetchall()
    stats = {"total_missing": len(rows), "added": 0, "empty": 0, "errors": 0}
    for uid, _ticker in rows:
        try:
            c, t = GetConsensusByUid(uid, token)
            if not c and not t:
                stats["empty"] += 1
                continue
            AddConsensusForecasts(db_path, c)
            AddConsensusTargets(db_path, t)
            stats["added"] += 1
        except Exception:  # noqa: BLE001
            stats["errors"] += 1
    if stats["total_missing"]:
        logging.info(
            "Forecasts missing=%s added=%s empty=%s errors=%s",
            stats["total_missing"], stats["added"], stats["empty"], stats["errors"],
        )
    if prune and stats["added"]:
        PruneHistory(db_path, MAX_CONSENSUS_PER_UID, MAX_TARGETS_PER_ANALYST, max_age_days=MAX_HISTORY_DAYS)
    logging.debug("EnsureForecastsForMissingShares %.2fs", time.perf_counter() - start)


def UpdateConsensusForecasts(  # noqa: N802
    db_path: Path,
    token: str,
    *,
    uid: str | None = None,
    max_consensus: int | None = None,
    max_targets_per_analyst: int | None = None,
    max_history_days: int | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        if uid:
            cur.execute("SELECT uid FROM perspective_shares WHERE uid=?", (uid,))
        else:
            cur.execute("SELECT uid FROM perspective_shares")
        uids = [r[0] for r in cur.fetchall()]
    stats = {"total": len(uids), "updated": 0, "empty": 0, "errors": 0}
    for u in uids:
        try:
            c, t = GetConsensusByUid(u, token)
            if not c and not t:
                stats["empty"] += 1
                continue
            AddConsensusForecasts(db_path, c)
            AddConsensusTargets(db_path, t)
            stats["updated"] += 1
        except Exception:  # noqa: BLE001
            stats["errors"] += 1
    logging.info(
        "Forecasts all total=%s updated=%s empty=%s errors=%s",
        stats["total"], stats["updated"], stats["empty"], stats["errors"],
    )
    PruneHistory(
        db_path,
        max_consensus if max_consensus is not None else MAX_CONSENSUS_PER_UID,
        max_targets_per_analyst if max_targets_per_analyst is not None else MAX_TARGETS_PER_ANALYST,
        max_age_days=(max_history_days if max_history_days is not None else MAX_HISTORY_DAYS),
    )


def FillingConsensusData(  # noqa: N802
    db_path: Path,
    token: str,
    *,
    limit: int | None = None,
    sleep_sec: float = 0.0,
) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT uid FROM perspective_shares ORDER BY uid")
        uids = [r[0] for r in cur.fetchall()]
    if limit is not None and limit >= 0:
        uids = uids[:limit]
    for i, u in enumerate(uids, 1):
        c, t = GetConsensusByUid(u, token)
        AddConsensusForecasts(db_path, c)
        AddConsensusTargets(db_path, t)
        if sleep_sec:
            time.sleep(sleep_sec)
        if i % 20 == 0:
            logging.info("FillingConsensusData прогресс %s/%s", i, len(uids))
    logging.info("FillingConsensusData завершено (%s)", len(uids))


def PruneHistory(db_path: Path, max_consensus: int, max_targets_per_analyst: int, *, max_age_days: int | None) -> None:  # noqa: N802
    if max_age_days is not None and max_age_days <= 0:
        max_age_days = None
    deleted_c = deleted_t = deleted_c_age = deleted_t_age = 0
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT uid FROM consensus_forecasts")
        for (fu,) in cur.fetchall():
            cur.execute(
                "SELECT id FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC, id DESC LIMIT ?",
                (fu, max_consensus),
            )
            keep = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT id FROM consensus_forecasts WHERE uid=?", (fu,))
            all_ids = {r[0] for r in cur.fetchall()}
            rem = list(all_ids - keep)
            if rem:
                cur.executemany("DELETE FROM consensus_forecasts WHERE id=?", [(x,) for x in rem])
                deleted_c += len(rem)
        cur.execute("SELECT DISTINCT uid, company FROM consensus_targets")
        for fu, company in cur.fetchall():
            cur.execute(
                "SELECT id FROM consensus_targets WHERE uid=? AND company=? ORDER BY recommendationDate DESC, id DESC LIMIT ?",
                (fu, company, max_targets_per_analyst),
            )
            keep = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT id FROM consensus_targets WHERE uid=? AND company=?", (fu, company))
            all_ids = {r[0] for r in cur.fetchall()}
            rem = list(all_ids - keep)
            if rem:
                cur.executemany("DELETE FROM consensus_targets WHERE id=?", [(x,) for x in rem])
                deleted_t += len(rem)
        if max_age_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            cur.execute("SELECT id, recommendationDate FROM consensus_forecasts")
            for _id, dt_str in cur.fetchall():
                if _is_older(dt_str, cutoff):
                    cur.execute("DELETE FROM consensus_forecasts WHERE id=?", (_id,))
                    deleted_c_age += 1
            cur.execute("SELECT id, recommendationDate FROM consensus_targets")
            for _id, dt_str in cur.fetchall():
                if _is_older(dt_str, cutoff):
                    cur.execute("DELETE FROM consensus_targets WHERE id=?", (_id,))
                    deleted_t_age += 1
        conn.commit()
    logging.info(
        "PruneHistory: del_consensus=%s del_targets=%s del_cons_age=%s del_tgt_age=%s", deleted_c, deleted_t, deleted_c_age, deleted_t_age
    )


def _is_older(dt_str: str | None, cutoff) -> bool:
    if not dt_str:
        return False
    val = dt_str.rstrip('Z')
    try:
        if val.endswith('+00:00') or '+' in val:
            dtv = datetime.fromisoformat(val)
        else:
            dtv = datetime.fromisoformat(val)
        if dtv.tzinfo is None:
            dtv = dtv.replace(tzinfo=timezone.utc)
        return dtv < cutoff
    except Exception:  # noqa: BLE001
        return False


__all__ = [
    "EnsureForecastsForMissingShares",
    "UpdateConsensusForecasts",
    "FillingConsensusData",
    "AddConsensusForecasts",
    "AddConsensusTargets",
    "GetConsensusByUid",
]
