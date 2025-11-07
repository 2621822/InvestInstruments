"""potentials.py (MySQL only)

Модуль расчёта относительных потенциалов акций, сохраняемых в таблицу `shares_potentials`.

Краткая задача: сравнить последний консенсус ("сколько рынок ожидает") с последней ценой закрытия
и получить относительное отклонение: (consensusPrice - prevClose) / prevClose.

Источники данных:
    * Последняя цена закрытия (`moex_shares_history.CLOSE` по SECID)
    * Последний консенсус (`consensus_forecasts.priceConsensus` по UID)

Отсеивание аномалий и мусора:
    * Любая цена <= 0 отбрасывается.
    * Цена > MAX_PRICE (по умолчанию 1_000_000) отбрасывается как явно ошибочная.
    * Если consensus или close отсутствуют/невалидны – относительный потенциал (pricePotentialRel) не вычисляется.

Анти-дублирование:
    * При вставке новой строки мы проверяем последнюю сохранённую запись по UID.
    * Если относительный потенциал не изменился (с точностью до 1e-9) – вставка пропускается и помечается как "unchanged".
    * NULL потенциалы (невозможно вычислить) могут накапливаться; последующая процедура CollapseDuplicateSharePotentials их схлопывает.

Сопутствующие задачи обслуживания:
    * CleanOldSharePotentials: удаляет записи старше заданного порога.
    * GetTopSharePotentials: выбирает последнюю запись по каждому UID и сортирует по относительному потенциалу.
    * CollapseDuplicateSharePotentials: чистит исторические дубли (unchanged rel / повторяющиеся NULL подряд).

Архитектурные принципы:
    * MySQL-only: плейсхолдеры всегда указываем как '?' (преобразуются слоем db_mysql в %s).
    * Каждая функция открывает собственное соединение через контекст, минимизируя время удержания ресурсов.
    * Вспомогательные чистые функции (ComputeRelativePotential, FetchLastPotentialRecord, ShouldSkipRel) изолируют бизнес-логику.

Публичные функции см. в __all__ ниже.
"""
from __future__ import annotations                      # Совместимость аннотаций
import logging                                          # Логирование
import datetime as dt                                   # Время для метки computedAt
from typing import List, Dict, Any                      # Типы аргументов и возвращаемых значений

from . import db_mysql as db_layer                        # Чистый MySQL слой (exec_sql адаптирует плейсхолдеры)

log = logging.getLogger(__name__)                       # Локальный логгер

MAX_PRICE = 1_000_000                                   # Порог для фильтрации аномальных цен


def _valid_price(val: Any) -> float | None:
    """Привести значение к float и отфильтровать аномалии.

    Критерии отбрасывания:
      * None
      * Не парсится в число
      * <= 0
      * > MAX_PRICE
    Возвращает нормализованное число или None.
    """
    if val is None:  # Пустое значение
        return None
    if isinstance(val, (int, float)):
        num = float(val)
    else:
        try:
            num = float(str(val).replace(',', '.'))
        except Exception:
            return None
    if num <= 0:
        return None
    if num > MAX_PRICE:
        return None
    return num


def _latest_consensus_price(conn, uid: str) -> float | None:
    """Получить последнюю цену консенсуса (priceConsensus) для заданного UID.

    Возвращает нормализованную цену или None если отсутствует/аномальна.
    """
    # exec_sql гарантирует корректные плейсхолдеры под MySQL
    cur = db_layer.exec_sql(
        conn,
        "SELECT priceConsensus FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1",
        (uid,)
    )                                                   # Выполняем запрос
    row = cur.fetchone()                                # Извлекаем результат
    return _valid_price(row[0]) if row else None        # Возвращаем фильтрованное значение


def _latest_close_price(conn, secid: str) -> float | None:
    """Получить последнюю цену закрытия (CLOSE) по SECID.

    Возвращает нормализованную цену или None.
    """
    cur = db_layer.exec_sql(
        conn,
        "SELECT CLOSE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1",
        (secid,)
    )                                                   # Выполняем запрос
    row = cur.fetchone()                                # Извлекаем строку
    return _valid_price(row[0]) if row else None        # Возвращаем отфильтрованную цену




__all__ = [                                            # Публичный API модуля
    "GetLastCloseBySecId",
    "GetLastConsensusByUid",
    "CalculateSharesPotential",
    "FillingPotentialData",
    "CleanOldSharePotentials",
    "GetTopSharePotentials",
    "CollapseDuplicateSharePotentials",
]

# ===================== НОВЫЕ ФУНКЦИИ ПО СПЕЦИФИКАЦИИ =====================

def GetLastCloseBySecId(secid: str) -> Dict[str, Any] | None:
    """Получить последнюю цену закрытия и дату сделки.

    Формат возвращаемого dict:
      {"secid": SECID, "close": float|None, "tradedate": DATE}
    Если записей нет – возвращает None.
    """
    if not secid:
        return None
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = db_layer.exec_sql(
            conn,
            "SELECT CLOSE, TRADEDATE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1",
            (secid,)
        )
        row = cur.fetchone()
        if not row:
            return None
        close_raw, tradedate = row
        close = _valid_price(close_raw)
        return {"secid": secid, "close": close, "tradedate": tradedate}


def GetLastConsensusByUid(uid: str) -> Dict[str, Any] | None:
    """Получить последний консенсус по UID.

    Формат dict:
      {"uid": UID, "priceConsensus": float|None, "recommendationDate": DATE}
    Если записей нет – возвращает None.
    """
    if not uid:
        return None
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = db_layer.exec_sql(
            conn,
            "SELECT priceConsensus, recommendationDate FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1",
            (uid,)
        )
        row = cur.fetchone()
        if not row:
            return None
        price_raw, recommendation_date = row
        price = _valid_price(price_raw)
        return {"uid": uid, "priceConsensus": price, "recommendationDate": recommendation_date}


def ComputeRelativePotential(prev_close: float | None, consensus_price: float | None) -> float | None:
    """Чистая функция вычисления относительного потенциала.

    Возвращает None если любой из аргументов None или prev_close <= 0.
    """
    if prev_close is None or consensus_price is None:
        return None
    if prev_close <= 0:
        return None
    try:
        return (consensus_price - prev_close) / prev_close
    except Exception:
        return None


def FetchLastPotentialRecord(uid: str) -> Dict[str, Any] | None:
    """Извлечь последнюю сохранённую запись потенциала для UID.

    Возвращает dict или None.
    """
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = db_layer.exec_sql(
            conn,
            "SELECT computedAt, prevClose, consensusPrice, pricePotentialRel FROM shares_potentials WHERE uid = ? ORDER BY computedAt DESC LIMIT 1",
            (uid,)
        )
        r = cur.fetchone()
    if not r:
        return None
    return {
        "computedAt": r[0],
        "prevClose": r[1],
        "consensusPrice": r[2],
        "pricePotentialRel": r[3],
    }


def ShouldSkipRel(last_rel: float | None, new_rel: float | None, *, epsilon: float = 1e-9) -> bool:
    """Определить, считать ли потенциал неизменившимся (дубликат).

    Правила:
      * Если оба значения не None и |diff| < epsilon -> True (пропуск).
      * Иначе False. NULL не обновляет эталон.
    """
    if last_rel is not None and new_rel is not None:
        return abs(last_rel - new_rel) < epsilon
    return False


def CalculateSharesPotential(secid: str, uid: str, ticker: str | None = None, *, skip_null: bool = False) -> Dict[str, Any]:
    """Рассчитать и (при необходимости) сохранить потенциал в `shares_potentials`.

    Возвращает структуру с полями uid, secid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel
    и флагами skipped / unchanged (если применимо).
    """
    db_layer.init_schema()
    # Используем миллисекундную (микросекундную при коллизии) точность для снижения вероятности конфликтов PK
    now_dt = dt.datetime.now(dt.UTC)
    now_ts = now_dt.isoformat(timespec="milliseconds")
    last_close = GetLastCloseBySecId(secid)
    last_cons = GetLastConsensusByUid(uid)
    prev_close = (last_close or {}).get("close")
    consensus_price = (last_cons or {}).get("priceConsensus")
    rel = ComputeRelativePotential(prev_close, consensus_price)
    # Получаем предыдущую запись для сравнения
    last_row = FetchLastPotentialRecord(uid)
    last_rel = (last_row or {}).get("pricePotentialRel")
    unchanged = ShouldSkipRel(last_rel, rel)
    if unchanged:
        return {
            "uid": uid,
            "secid": secid,
            "ticker": ticker,
            "computedAt": now_ts,
            "prevClose": prev_close,
            "consensusPrice": consensus_price,
            "pricePotentialRel": rel,
            "skipped": True,
            "unchanged": True,
        }
    if skip_null and (rel is None):
        # Пропускаем вставку NULL потенциала (конфигурационно), возвращаем вычисленные значения
        return {
            "uid": uid,
            "secid": secid,
            "ticker": ticker,
            "computedAt": now_ts,
            "prevClose": prev_close,
            "consensusPrice": consensus_price,
            "pricePotentialRel": rel,
            "skipped": True,
        }
    with db_layer.get_connection() as conn:  # Сохраняем запись
        try:
            db_layer.exec_sql(
                conn,
                "INSERT INTO shares_potentials(uid, secid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uid, secid, ticker, now_ts, prev_close, consensus_price, rel)
            )
        except Exception:
            alt_ts = now_dt.isoformat(timespec="microseconds")
            db_layer.exec_sql(
                conn,
                "INSERT INTO shares_potentials(uid, secid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uid, secid, ticker, alt_ts, prev_close, consensus_price, rel)
            )
            now_ts = alt_ts
    return {
        "uid": uid,
        "secid": secid,
        "ticker": ticker,
        "computedAt": now_ts,
        "prevClose": prev_close,
        "consensusPrice": consensus_price,
        "pricePotentialRel": rel,
    }


def FillingPotentialData(*, skip_null: bool = False) -> Dict[str, Any]:
    """Массовый пересчёт потенциалов для всех бумаг в `perspective_shares`.

    Счётчики:
      processed  – количество обработанных бумаг
      inserted   – вставки новых строк (включая вставку с NULL rel если skip_null=False)
      skipped    – пропуски (NULL rel при skip_null=True или пометка skipped)
      unchanged  – относительный потенциал не изменился (дубль)
    """
    db_layer.init_schema()
    processed = 0
    inserted = 0
    skipped = 0
    unchanged = 0
    rows: List[Dict[str, Any]] = []
    with db_layer.get_connection() as conn:
        cur = conn.execute(
            "SELECT uid, secid, ticker FROM perspective_shares WHERE uid IS NOT NULL AND secid IS NOT NULL"
        )
        insts = cur.fetchall()
    for uid, secid, ticker in insts:
        processed += 1
        res = CalculateSharesPotential(secid=secid, uid=uid, ticker=ticker, skip_null=skip_null)
        rows.append(res)
        if res.get("unchanged"):
            unchanged += 1
            continue
        if res.get("pricePotentialRel") is None:
            if res.get("skipped") or skip_null:
                skipped += 1
            else:
                inserted += 1  # вставка с NULL rel
        else:
            if res.get("skipped"):
                skipped += 1
            else:
                inserted += 1
    return {
        "processed": processed,
        "inserted": inserted,
        "skipped": skipped,
        "unchanged": unchanged,
        "rows": rows,
    }


# ===================== РЕТЕНШН И ТОП-10 =====================

def CleanOldSharePotentials(max_age_days: int = 90) -> Dict[str, Any]:
    """Удалить записи старше заданного возраста.

    Порог = now_utc - max_age_days. Сравнение как строк ISO допустимо (лексикографический порядок сохранён).
    """
    if max_age_days <= 0:
        return {"deleted": 0, "max_age_days": max_age_days, "skipped": True}
    db_layer.init_schema()
    threshold_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=max_age_days)
    threshold_iso = threshold_dt.isoformat(timespec="seconds")  # Секундной точности достаточно
    deleted = 0
    with db_layer.get_connection() as conn:
        # Подсчёт кандидатов
        cur = db_layer.exec_sql(
            conn,
            "SELECT COUNT(*) FROM shares_potentials WHERE computedAt < ?",
            (threshold_iso,)
        )
        deleted = cur.fetchone()[0]
        if deleted:
            db_layer.exec_sql(
                conn,
                "DELETE FROM shares_potentials WHERE computedAt < ?",
                (threshold_iso,)
            )
    return {"deleted": deleted, "threshold": threshold_iso, "max_age_days": max_age_days}


def GetTopSharePotentials(limit: int = 10, *, max_age_days: int | None = None, min_prev_close: float | None = None) -> Dict[str, Any]:
    """Получить топ-N относительных потенциалов (последняя запись на UID)."""
    if limit <= 0:
        return {"status": "ok", "rows": 0, "data": []}
    db_layer.init_schema()
    params: list[Any] = []
    where_clauses = ["p.pricePotentialRel IS NOT NULL"]
    if max_age_days is not None and max_age_days > 0:
        threshold_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=max_age_days)
        threshold_iso = threshold_dt.isoformat(timespec="seconds")
        where_clauses.append("p.computedAt >= ?")
        params.append(threshold_iso)
    if min_prev_close is not None and min_prev_close > 0:
        where_clauses.append("(p.prevClose IS NULL OR p.prevClose >= ?)")
        params.append(min_prev_close)
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    sql = (
        "SELECT p.uid, p.secid, p.ticker, p.computedAt, p.prevClose, p.consensusPrice, p.pricePotentialRel "
        "FROM shares_potentials p "
        "JOIN (SELECT uid, MAX(computedAt) AS mx FROM shares_potentials GROUP BY uid) last "
        "ON p.uid = last.uid AND p.computedAt = last.mx "
        f"{where_sql} "
        "ORDER BY p.pricePotentialRel DESC LIMIT ?"
    )
    params.append(limit)
    rows: list[tuple] = []
    with db_layer.get_connection() as conn:
        cur = db_layer.exec_sql(conn, sql, tuple(params))
        rows = cur.fetchall()
    data = [
        {
            "uid": r[0],
            "secid": r[1],
            "ticker": r[2],
            "computedAt": r[3],
            "prevClose": r[4],
            "consensusPrice": r[5],
            "pricePotentialRel": r[6],
            "pricePotentialRel_pct": (r[6] * 100.0 if r[6] is not None else None),
        }
        for r in rows
    ]
    return {"status": "ok", "rows": len(data), "data": data, "limit": limit}


def CollapseDuplicateSharePotentials(*, rel_epsilon: float = 1e-9) -> Dict[str, Any]:
    """Удалить исторические дубли потенциальных записей.

    Стратегия:
      * Сканируем все записи для каждого UID (ASC по времени)
      * Удаляем записи, где rel не изменился больше порога rel_epsilon от предыдущего ненулевого.
      * Оставляем только первую NULL; последующие подряд NULL удаляем.
      * Удаление по (uid, computedAt).
    Возвращает статистику.
    """
    db_layer.init_schema()
    deleted = 0
    scanned_rows = 0
    scanned_uids = 0
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid FROM shares_potentials GROUP BY uid HAVING COUNT(*) > 1")
        uid_rows = [r[0] for r in cur.fetchall()]
        for uid in uid_rows:
            scanned_uids += 1
            c2 = db_layer.exec_sql(
                conn,
                "SELECT computedAt, pricePotentialRel FROM shares_potentials WHERE uid = ? ORDER BY computedAt ASC",
                (uid,)
            )
            rows = c2.fetchall()
            scanned_rows += len(rows)
            prev_rel: float | None = None
            first_null_kept = False
            to_delete_keys: list[str] = []
            for computedAt, rel in rows:
                if rel is None:
                    if first_null_kept:
                        to_delete_keys.append(computedAt)
                    else:
                        first_null_kept = True
                    continue
                if prev_rel is None:
                    prev_rel = rel
                    continue
                if abs(rel - prev_rel) < rel_epsilon:
                    to_delete_keys.append(computedAt)
                else:
                    prev_rel = rel
            for ck in to_delete_keys:
                db_layer.exec_sql(
                    conn,
                    "DELETE FROM shares_potentials WHERE uid = ? AND computedAt = ?",
                    (uid, ck)
                )
            deleted += len(to_delete_keys)
    return {
        "deleted": deleted,
        "scanned_uids": scanned_uids,
        "scanned_rows": scanned_rows,
        "rel_epsilon": rel_epsilon,
    }
