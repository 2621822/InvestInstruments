"""potentials.py

Модуль расчёта потенциалов инструментов на основе:
  * последней цены закрытия из `moex_shares_history`
  * последнего консенсуса (`consensus_forecasts.priceConsensus`)

Выходные данные записываются в таблицу `instrument_potentials`:
  (uid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel)

Защита от аномалий:
  * consensusPrice > 1_000_000 игнорируется
  * consensusPrice <= 0 игнорируется
  * prevClose <= 0 игнорируется

Функции:
  compute_all_potentials() – полный пересчёт по всем бумагам
  compute_potentials_for_uids(updated_uids) – частичный пересчёт (напр. после обновлённых прогнозов)

Все строки прокомментированы.
"""
from __future__ import annotations                      # Совместимость аннотаций
import logging                                          # Логирование
import datetime as dt                                   # Время для метки computedAt
from typing import List, Dict, Any                      # Типы аргументов и возвращаемых значений

from . import db as db_layer                            # Слой работы с БД

log = logging.getLogger(__name__)                       # Локальный логгер

MAX_PRICE = 1_000_000                                   # Порог для фильтрации аномальных цен


def _valid_price(val: Any) -> float | None:
    """Проверить и привести цену к float, вернуть None если аномалия."""
    if val is None:                                     # Пустое значение
        return None
    if isinstance(val, (int, float)):                   # Уже число
        num = float(val)
    else:                                               # Пробуем парсить строку или другой тип
        try:
            num = float(str(val).replace(',', '.'))     # Заменяем запятую на точку
        except Exception:                               # Парсинг не удался
            return None
    if num <= 0:                                        # Нереалистичная или неиспользуемая цена
        return None
    if num > MAX_PRICE:                                 # Слишком большая -> игнорируем
        return None
    return num                                          # Возвращаем допустимое значение


def _latest_consensus_price(conn, uid: str) -> float | None:
    """Получить последнюю цену консенсуса из таблицы consensus_forecasts."""
    cur = conn.execute(
        "SELECT priceConsensus FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1",
        (uid,)
    )                                                   # Выполняем запрос
    row = cur.fetchone()                                # Извлекаем результат
    return _valid_price(row[0]) if row else None        # Возвращаем фильтрованное значение


def _latest_close_price(conn, secid: str) -> float | None:
    """Получить последнюю цену закрытия из moex_shares_history."""
    cur = conn.execute(
        "SELECT CLOSE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1",
        (secid,)
    )                                                   # Выполняем запрос
    row = cur.fetchone()                                # Извлекаем строку
    return _valid_price(row[0]) if row else None        # Возвращаем отфильтрованную цену


def _insert_potential(conn, uid: str, ticker: str, prev_close: float | None, consensus_price: float | None, computed_at: str):
    """Вставить строку в instrument_potentials."""
    sql = (
        "INSERT INTO instrument_potentials(uid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?)"
    )                                                   # SQL вставки
    rel = None                                          # Относительный потенциал (пока None)
    if prev_close and consensus_price:                  # Если обе цены валидны
        rel = (consensus_price - prev_close) / prev_close  # Вычисляем (консенсус - закрытие)/закрытие
    conn.execute(sql, (uid, ticker, computed_at, prev_close, consensus_price, rel))  # Выполняем вставку


def compute_all_potentials() -> Dict[str, Any]:
    """Полный пересчёт потенциалов по всем uid в perspective_shares."""
    db_layer.init_schema()                              # Убедиться в наличии схемы
    now_ts = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")  # ISO UTC
    processed = 0                                       # Сколько инструментов обработано
    inserted = 0                                        # Сколько строк вставлено
    skipped = 0                                         # Сколько пропущено (нет данных)
    with db_layer.get_connection() as conn:             # Работаем в контексте соединения
        cur = conn.execute(
            "SELECT uid, ticker, secid FROM perspective_shares WHERE uid IS NOT NULL"
        )                                               # Запрос списка бумаг
        rows = cur.fetchall()                           # Извлекаем все строки
        for uid, ticker, secid in rows:                 # Итерируем по каждой бумаге
            processed += 1                              # Инкремент счётчика
            consensus_price = _latest_consensus_price(conn, uid)  # Последний консенсус
            prev_close = _latest_close_price(conn, secid)         # Последняя цена закрытия
            if consensus_price is None or prev_close is None:     # Если чего-то нет
                skipped += 1                            # Увеличиваем пропуск
                _insert_potential(conn, uid, ticker, prev_close, consensus_price, now_ts)  # Всё равно фиксируем факт
                continue                                # Переходим к следующему инструменту
            _insert_potential(conn, uid, ticker, prev_close, consensus_price, now_ts)  # Вставка валидных данных
            inserted += 1                               # Считаем успешную вставку
        if db_layer.BACKEND == "sqlite":               # Коммит для sqlite
            conn.commit()
    return {                                            # Возвращаем статистику
        "processed": processed,
        "inserted": inserted,
        "skipped": skipped,
        "computedAt": now_ts,
    }


def compute_potentials_for_uids(updated_uids: List[str]) -> Dict[str, Any]:
    """Частичный пересчёт потенциалов только для указанных uid.

    Используется после вставки новых консенсусов, чтобы не пересчитывать всё.
    """
    if not updated_uids:                               # Если список пуст
        return {"processed": 0, "inserted": 0, "skipped": 0, "computedAt": None, "empty": True}
    db_layer.init_schema()                             # Инициализация схемы
    now_ts = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")  # ISO UTC
    processed = 0                                      # Счётчик обработанных
    inserted = 0                                       # Счётчик вставок
    skipped = 0                                        # Счётчик пропусков
    with db_layer.get_connection() as conn:            # Контекст соединения
        for uid in updated_uids:                       # Итерируем по списку uid
            processed += 1                             # Инкремент
            cur_i = conn.execute(
                "SELECT uid, ticker, secid FROM perspective_shares WHERE uid = ?", (uid,)
            )                                          # Получаем строку по uid
            inst_row = cur_i.fetchone()                # Извлекаем результат
            if not inst_row:                           # Если нет такой бумаги
                skipped += 1                           # Пропускаем
                continue                               # Переход к следующему
            uid2, ticker, secid = inst_row             # Распаковка полей
            consensus_price = _latest_consensus_price(conn, uid2)  # Последний консенсус
            prev_close = _latest_close_price(conn, secid)          # Последняя цена закрытия
            if consensus_price is None or prev_close is None:      # Отсутствуют данные
                skipped += 1                           # Пропуск
                _insert_potential(conn, uid2, ticker, prev_close, consensus_price, now_ts)  # Фиксация строки
                continue                               # Следующий uid
            _insert_potential(conn, uid2, ticker, prev_close, consensus_price, now_ts)      # Вставка
            inserted += 1                              # Считаем вставку
        if db_layer.BACKEND == "sqlite":              # Коммит если sqlite
            conn.commit()
    return {                                           # Возвращаем статистику
        "processed": processed,
        "inserted": inserted,
        "skipped": skipped,
        "computedAt": now_ts,
        "uids": updated_uids,
    }


__all__ = [                                            # Экспортируем публичные символы
    "compute_all_potentials",
    "compute_potentials_for_uids",
    # Новые функции расчёта потенциала акций (shares_potentials)
    "GetLastCloseBySecId",
    "GetLastConsensusByUid",
    "CalculateSharesPotential",
    "FillingPotentialData",
    "CleanOldSharePotentials",
    "GetTopSharePotentials",
    "CleanDuplicateSharePotentials",
]

# ===================== НОВЫЕ ФУНКЦИИ ПО СПЕЦИФИКАЦИИ =====================

def GetLastCloseBySecId(secid: str) -> Dict[str, Any] | None:
    """Получить последнюю цену закрытия и дату для SECID из moex_shares_history.

    Возвращает dict: {secid, close, tradedate} или None если нет данных.
    """
    if not secid:
        return None
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute(
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
    """Получить последний консенсус и дату рекомендации для UID из consensus_forecasts.

    Возвращает dict: {uid, priceConsensus, recommendationDate} или None если нет.
    """
    if not uid:
        return None
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute(
            "SELECT priceConsensus, recommendationDate FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1",
            (uid,)
        )
        row = cur.fetchone()
        if not row:
            return None
        price_raw, recommendation_date = row
        price = _valid_price(price_raw)
        return {"uid": uid, "priceConsensus": price, "recommendationDate": recommendation_date}


def CalculateSharesPotential(secid: str, uid: str, ticker: str | None = None, *, skip_null: bool = False) -> Dict[str, Any]:
    """Вычислить потенциал акции и записать в shares_potentials.

    Формула: pricePotentialRel = (consensusPrice - prevClose) / prevClose
    Условия расчёта: consensusPrice не None, prevClose не None и > 0.
    Если условия не выполнены – pricePotentialRel остаётся NULL.
    """
    db_layer.init_schema()
    # Используем миллисекундную точность для снижения коллизий PK
    now_dt = dt.datetime.now(dt.UTC)
    now_ts = now_dt.isoformat(timespec="milliseconds")
    last_close = GetLastCloseBySecId(secid)
    last_cons = GetLastConsensusByUid(uid)
    prev_close = (last_close or {}).get("close")
    consensus_price = (last_cons or {}).get("priceConsensus")
    rel = None
    if prev_close and consensus_price:
        try:
            rel = (consensus_price - prev_close) / prev_close
        except Exception:
            rel = None
    # Проверка последней сохранённой записи для uid, чтобы не копить дубли при неизменном rel
    last_row: Dict[str, Any] | None = None
    last_rel: float | None = None
    with db_layer.get_connection() as _c:
        cur = _c.execute(
            "SELECT computedAt, prevClose, consensusPrice, pricePotentialRel FROM shares_potentials WHERE uid = ? ORDER BY computedAt DESC LIMIT 1",
            (uid,)
        )
        r = cur.fetchone()
        if r:
            last_row = {
                "computedAt": r[0],
                "prevClose": r[1],
                "consensusPrice": r[2],
                "pricePotentialRel": r[3],
            }
            last_rel = r[3]
    unchanged = False
    # Если последний относительный потенциал совпадает с новым (учитываем небольшую плавающую погрешность)
    if last_rel is not None and rel is not None:
        # Допускаем расхождение до 1e-9 (float арифметика)
        if abs(last_rel - rel) < 1e-9:
            unchanged = True
    # Если оба None (rel не рассчитывается) считаем что дубли не критичны, но можем пропустить при skip_null
    if last_rel is None and rel is None:
        # Не считаем как unchanged для метрики inserted/ skipped, но дадим возможность skip_null пропустить
        pass
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
        # Пропуск вставки, возвращаем только вычисленные значения
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
    with db_layer.get_connection() as conn:
        try:
            conn.execute(
                "INSERT INTO shares_potentials(uid, secid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uid, secid, ticker, now_ts, prev_close, consensus_price, rel)
            )
        except Exception:
            alt_ts = now_dt.isoformat(timespec="microseconds")
            conn.execute(
                "INSERT INTO shares_potentials(uid, secid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uid, secid, ticker, alt_ts, prev_close, consensus_price, rel)
            )
            now_ts = alt_ts
        if db_layer.BACKEND == "sqlite":
            conn.commit()
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
    """Ежедневный обход перспективных акций и расчёт их потенциала в shares_potentials."""
    db_layer.init_schema()
    processed = 0
    inserted = 0      # сколько вставок (новых или изменившихся rel)
    skipped = 0       # rel = NULL (и пропущено по skip_null)
    unchanged = 0     # сколько пропущено из-за отсутствия изменения rel
    rows: list[Dict[str, Any]] = []
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid, secid, ticker FROM perspective_shares WHERE uid IS NOT NULL AND secid IS NOT NULL")
        insts = cur.fetchall()
    for uid, secid, ticker in insts:
        processed += 1
        res = CalculateSharesPotential(secid=secid, uid=uid, ticker=ticker, skip_null=skip_null)
        rows.append(res)
        if res.get("unchanged"):
            unchanged += 1
        else:
            if res.get("pricePotentialRel") is None:
                # Если rel = None и была реально вставка (skip_null=False), считаем в skipped
                skipped += 1 if res.get("skipped") or skip_null else 0
                if not res.get("skipped") and not skip_null:
                    # Вставлена запись с NULL rel
                    inserted += 1
            else:
                if res.get("skipped"):
                    # Это случай skip_null=True для валидного rel? Теоретически не должно происходить
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
    """Удалить устаревшие записи из shares_potentials старше max_age_days.

    Критерий: computedAt < (now_utc - max_age_days).
    Формат computedAt: ISO (секунды/миллисекунды/микросекунды) – сравнение лексикографически корректно для ISO.
    """
    if max_age_days <= 0:
        return {"deleted": 0, "max_age_days": max_age_days, "skipped": True}
    db_layer.init_schema()
    threshold_dt = dt.datetime.now(dt.UTC) - dt.timedelta(days=max_age_days)
    threshold_iso = threshold_dt.isoformat(timespec="seconds")  # Секундной точности достаточно
    deleted = 0
    with db_layer.get_connection() as conn:
        # Подсчёт кандидатов
        cur = conn.execute(
            "SELECT COUNT(*) FROM shares_potentials WHERE computedAt < ?", (threshold_iso,)
        )
        deleted = cur.fetchone()[0]
        if deleted:
            conn.execute(
                "DELETE FROM shares_potentials WHERE computedAt < ?", (threshold_iso,)
            )
        if db_layer.BACKEND == "sqlite":
            conn.commit()
    return {"deleted": deleted, "threshold": threshold_iso, "max_age_days": max_age_days}


def GetTopSharePotentials(limit: int = 10, *, max_age_days: int | None = None,
                          min_prev_close: float | None = None) -> Dict[str, Any]:
    """Получить топ-N бумаг по относительному потенциалу из shares_potentials.

    Логика:
      1. Берём последнюю запись по каждому uid (MAX computedAt).
      2. Фильтруем pricePotentialRel IS NOT NULL.
      3. Доп. фильтры: возраст записи (max_age_days), минимальный prevClose.
      4. Сортируем по pricePotentialRel DESC.
    """
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
        cur = conn.execute(sql, params)
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


def CleanDuplicateSharePotentials(epsilon: float = 1e-9, *, keep_last: bool = True) -> Dict[str, Any]:
    """Удалить логические дубли из shares_potentials.

    Дубликат определяется как запись для uid с тем же pricePotentialRel (|diff| < epsilon),
    где существует более поздняя запись с тем же значением.

    Алгоритм:
      1. Находим все uid, для которых есть >=2 записей с ненулевым (в смысле not NULL) rel.
      2. Для каждого uid выбираем группы по rel (округление / сравнение с допуском).
      3. В каждой группе оставляем только самую позднюю запись (MAX computedAt), остальные удаляем.
      4. Отдельно по записям с NULL rel: можно оставить только самую последнюю (если их много), чтобы сократить шум.

    Параметры:
      epsilon: допуск сравнения значений rel.
      keep_last: если True – оставляем последнюю запись в каждой группе; если False – первую.

    Возвращает словарь со статистикой: {deleted, groups, uids, null_collapsed}.
    """
    db_layer.init_schema()
    deleted_total = 0
    groups_total = 0
    affected_uids: list[str] = []
    null_collapsed = 0
    with db_layer.get_connection() as conn:
        # Получаем все записи (uid, computedAt, rel, rowid) – rowid для sqlite облегчает удаление
        cur = conn.execute("SELECT uid, computedAt, pricePotentialRel FROM shares_potentials")
        rows = cur.fetchall()
        by_uid: dict[str, list[tuple[str, str, float | None]]] = {}
        for uid, computedAt, rel in rows:
            by_uid.setdefault(uid, []).append((uid, computedAt, rel))
        for uid, items in by_uid.items():
            # Сортируем по времени чтобы было проще выбирать последнюю
            items.sort(key=lambda x: x[1])
            # Группы по rel (с epsilon). Используем список групп: каждая группа = [indices]
            groups: list[list[int]] = []
            rel_values: list[float | None] = []
            for idx, (_, _, rel) in enumerate(items):
                placed = False
                if rel is None:
                    continue  # NULL группы обрабатываем отдельно
                for g_i, g in enumerate(groups):
                    ref_rel = rel_values[g_i]
                    if ref_rel is not None and abs(ref_rel - rel) < epsilon:
                        g.append(idx)
                        placed = True
                        break
                if not placed:
                    groups.append([idx])
                    rel_values.append(rel)
            # Удаляем лишние в группах с >1 элементом
            for g_i, g in enumerate(groups):
                if len(g) <= 1:
                    continue
                groups_total += 1
                affected_uids.append(uid)
                # Определяем индекс записи которую оставляем
                keep_idx = max(g) if keep_last else min(g)
                for idx in g:
                    if idx == keep_idx:
                        continue
                    # Удаляем запись по (uid, computedAt)
                    _, computedAt, rel_val = items[idx]
                    conn.execute("DELETE FROM shares_potentials WHERE uid = ? AND computedAt = ?", (uid, computedAt))
                    deleted_total += 1
            # COLLAPSE NULL rel (оставляем только последнюю если их >1)
            null_indices = [i for i, (_, _, rel) in enumerate(items) if rel is None]
            if len(null_indices) > 1:
                # Оставить последнюю
                keep_null = max(null_indices)
                for idx in null_indices:
                    if idx == keep_null:
                        continue
                    _, computedAt, _rel_val = items[idx]
                    conn.execute("DELETE FROM shares_potentials WHERE uid = ? AND computedAt = ?", (uid, computedAt))
                    deleted_total += 1
                    null_collapsed += 1
        if db_layer.BACKEND == "sqlite":
            conn.commit()
    return {
        "deleted": deleted_total,
        "groups": groups_total,
        "uids": list(set(affected_uids)),
        "null_collapsed": null_collapsed,
        "epsilon": epsilon,
        "keep_last": keep_last,
    }
