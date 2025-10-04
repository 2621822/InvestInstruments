"""potentials.py

Расчёт и пересчёт потенциалов (самодостаточная версия без зависимости от legacy main.py).

Схема хранения (мигрированная):
  instrument_potentials(
    uid TEXT,
    ticker TEXT,
    computedDate TEXT,          # YYYY-MM-DD (UTC date вычисления)
    prevClose REAL,
    consensusPrice REAL,
    pricePotentialRel REAL,     # (consensusPrice - prevClose)/prevClose
    isStale INTEGER DEFAULT 0,  # 1 если recommendationDate старше stale_days
    PRIMARY KEY(uid, computedDate)
  )

ВНИМАНИЕ: Никаких computedAt больше нет. Только одна запись в день на бумагу.
"""
from __future__ import annotations

from math import isfinite, isclose
from pathlib import Path
from typing import Any, List, Dict
from datetime import datetime, timezone
import logging
import sqlite3
import os

DB_PATH = Path("GorbunovInvestInstruments.db")


def _get_prev_close(secid: str) -> tuple[float | None, str | None]:
    """Вернуть последнюю цену закрытия для SECID из таблицы moex_history_perspective_shares."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT CLOSE, COALESCE(TRADE_SESSION_DATE, TRADEDATE) FROM moex_history_perspective_shares "
                "WHERE SECID=? AND CLOSE IS NOT NULL ORDER BY COALESCE(TRADE_SESSION_DATE, TRADEDATE) DESC LIMIT 1",
                (secid,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0]), row[1]
    except Exception:  # noqa: BLE001
        return None, None
    return None, None


def _ensure_table() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='instrument_potentials'")
        if not cur.fetchone():
            cur.execute(
                """CREATE TABLE instrument_potentials (
                    uid TEXT,
                    ticker TEXT,
                    computedDate TEXT,
                    prevClose REAL,
                    consensusPrice REAL,
                    pricePotentialRel REAL,
                    isStale INTEGER DEFAULT 0,
                    PRIMARY KEY(uid, computedDate)
                )"""
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_instrument_potentials_rel ON instrument_potentials(pricePotentialRel)")
            conn.commit()


def compute_all(store: bool = True, stale_days: int = 10) -> List[Dict[str, Any]]:
    """Пересчитать потенциалы по ВСЕМ бумагам.

    НЕ создаём новую запись за сегодня, если prevClose и consensusPrice не
    изменились (с допуском) относительно последней сохранённой записи (любая дата).
    Так база фиксирует только моменты изменения данных.

    Возвращает список словарей с полями:
      uid, ticker, prevClose, consensusPrice, pricePotentialRel, isStale,
      changed(bool), changeKind(str)
    changeKind: 'none' | 'price' | 'forecast' | 'both' | 'new'
    """
    results: List[Dict[str, Any]] = []
    if store:
        _ensure_table()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT uid, ticker, secid FROM perspective_shares ORDER BY ticker")
        shares = c.fetchall()
    today = datetime.now(timezone.utc).date().isoformat()
    now_utc = datetime.now(timezone.utc)
    for uid, ticker, secid in shares:
        consensus_price: float | None = None
        rec_date: str | None = None
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT priceConsensus, recommendationDate FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC LIMIT 1",
                (uid,),
            )
            row = cur.fetchone()
            if row:
                consensus_price, rec_date = row
        prev_close, _dt = (None, None)
        if secid:
            prev_close, _dt = _get_prev_close(secid)
        potential = None
        if consensus_price is not None and prev_close is not None and isfinite(prev_close) and prev_close > 0:
            potential = (consensus_price - prev_close) / prev_close
        is_stale = 0
        if rec_date:
            try:
                rd = rec_date.rstrip('Z')
                rd_dt = datetime.fromisoformat(rd.replace('Z', '+00:00'))
                if rd_dt.tzinfo is None:
                    rd_dt = rd_dt.replace(tzinfo=timezone.utc)
                if (now_utc - rd_dt).days > stale_days:
                    is_stale = 1
            except Exception:  # noqa: BLE001
                pass
        # Определяем было ли изменение относительно последней сохранённой записи (любая дата)
        last_prev = last_cons = None
        last_date = None
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT prevClose, consensusPrice, computedDate FROM instrument_potentials WHERE uid=? ORDER BY computedDate DESC LIMIT 1",
                (uid,),
            )
            rlast = cur.fetchone()
            if rlast:
                last_prev, last_cons, last_date = rlast

        tol = 1e-9
        changed_price = not (last_prev is not None and prev_close is not None and isclose(float(last_prev), float(prev_close), abs_tol=tol))
        if last_prev is None and prev_close is None:
            changed_price = False
        changed_forecast = not (last_cons is not None and consensus_price is not None and isclose(float(last_cons), float(consensus_price), abs_tol=tol))
        if last_cons is None and consensus_price is None:
            changed_forecast = False

        changed_kind = "none"
        if last_date is None:
            changed_kind = "new"
        elif changed_price and changed_forecast:
            changed_kind = "both"
        elif changed_price:
            changed_kind = "price"
        elif changed_forecast:
            changed_kind = "forecast"

        entry = {
            "uid": uid,
            "ticker": ticker,
            "prevClose": prev_close,
            "consensusPrice": consensus_price,
            "pricePotentialRel": potential,
            "isStale": is_stale,
            "changed": changed_kind != "none",
            "changeKind": changed_kind,
        }
        results.append(entry)
        if store:
            # Вставляем новую запись ТОЛЬКО если нет предыдущей или есть изменение в цене или прогнозе
            if last_date is None or changed_kind != "none" or os.getenv("POTENTIALS_FORCE_DAILY", "0").lower() in {"1","true","yes"}:
                try:
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute(
                            "INSERT OR REPLACE INTO instrument_potentials (uid,ticker,computedDate,prevClose,consensusPrice,pricePotentialRel,isStale) VALUES (?,?,?,?,?,?,?)",
                            (uid, ticker, today, prev_close, consensus_price, potential, is_stale),
                        )
                        conn.commit()
                except Exception as exc:  # noqa: BLE001
                    logging.debug("compute_all: не удалось сохранить %s: %s", ticker, exc)
    logging.info("compute_all: завершено для %s бумаг (%s)", len(results), today)
    return results


def recompute_one(uid: str, stale_days: int = 10) -> None:
    """Пересчитать потенциал для одной бумаги."""
    compute_all(store=True, stale_days=stale_days)  # упрощённо: пересчитываем всё (объём небольшой)


# Для совместимости с тестами, которые импортируют _get_prev_close из legacy main
__all__ = ["compute_all", "recompute_one", "_get_prev_close"]
