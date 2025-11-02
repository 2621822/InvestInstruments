"""tinkoff_api.py (DEPRECATED)

Весь функционал перенесён в:
  * sdk_client.py – работа с официальным SDK
  * instruments.py – поиск и сохранение инструментов
  * forecasts.py – консенсус и таргеты
  * history.py – исторические цены MOEX
  * potentials.py – расчёт потенциалов

Этот модуль сохранён как заглушка для совместимости. Все прежние функции удалены.
Любая попытка доступа к атрибуту вызывает исключение с подсказкой.
"""
from __future__ import annotations
from datetime import datetime, UTC  # Для TZ-aware времени

DEPRECATED = True  # Явный флаг для внешних проверок

def __getattr__(name: str):
    raise AttributeError(
        f"tinkoff_api.{name} устарел. Используйте соответствующий модуль: sdk_client, instruments, forecasts, history, potentials."
    )

__all__: list[str] = ["DEPRECATED"]
_LEGACY = """
    payload_variants = []
    # базовый вариант
    payload_variants.append({"instrumentId": uid})
    # возможные альтернативы
    payload_variants.append({"instrumentUid": uid})
    payload_variants.append({"uid": uid})
    payload_variants.append({"id": uid, "idType": "INSTRUMENT_ID_TYPE_UID"})
    payload_variants.append({"idType": "INSTRUMENT_ID_TYPE_UID", "id": uid, "instrumentId": uid})

    # POST + GET попытки для каждого варианта тела
    for pv in payload_variants:
        for ep in _forecast_urls():
            try:
                resp = _session().post(ep, headers=PostApiHeaders(), data=json.dumps(pv), timeout=15, verify=(CA_PATH if CA_PATH else VERIFY_SSL_DEFAULT))
                if resp.status_code == 200:
                    data = resp.json() if resp.content else {}
                    return {
                        "consensus": data.get("consensus") or {},
                        "targets": data.get("targets") or [],
                        "endpoint": ep,
                        "method": "POST",
                        "payload_used": pv
                    }
                # если json – показать ключевые поля ошибки
                err_info = ''
                if LOG_BODY and resp.headers.get('Content-Type','').startswith('application/json'):
                    try:
                        j = resp.json()
                        err_info = json.dumps({k: j.get(k) for k in ['status','error','message','path','requestId'] if k in j}, ensure_ascii=False)
                    except Exception:
                        err_info = resp.text[:300]
                log.warning("Forecast POST status=%s ep=%s uid=%s payload=%s err=%s", resp.status_code, ep, uid, pv, err_info)
            except Exception as ex:  # noqa
                log.warning("Forecast POST exception ep=%s uid=%s payload=%s err=%s", ep, uid, pv, ex)
        # GET fallback для этого payload
        for ep in _forecast_urls():
            status, data, body = _get_json(ep, pv)
            if status == 200 and data:
                return {
                    "consensus": data.get("consensus") or {},
                    "targets": data.get("targets") or [],
                    "endpoint": ep,
                    "method": "GET",
                    "payload_used": pv
                }
            if LOG_BODY:
                log.warning("Forecast GET status=%s ep=%s uid=%s payload=%s body=%s", status, ep, uid, pv, body[:300])
    return {"consensus": None, "targets": []}


def AddConsensusForecasts(consensus: Dict[str, Any]) -> Dict[str, Any]:
    """Вставка консенсус-прогноза в consensus_forecasts с историчностью.

    Если последняя запись идентична – не добавляем. Иначе создаём новую строку
    (uid, recommendationDate) как составной ключ.

    Источник даты: если в ответе есть поле recommendationDate / date – используем его,
    иначе текущая UTC дата.
    """
    if not consensus:
        return {"status": "empty"}
    uid = consensus.get("uid")
    ticker = consensus.get("ticker")
    recommendation = consensus.get("recommendation")
    currency = consensus.get("currency")
    def _num(val):
        """Attempt to extract a numeric (int/float) from arbitrary value.
        Supports dict (value/price/consensus), list/tuple (first numeric), str (parse), else returns val if numeric or None."""
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, dict):
            # common keys
            for k in ["value", "price", "consensus", "amount", "target", "data"]:
                if k in val and isinstance(val[k], (int, float)):
                    return val[k]
            # fallback: first numeric among values
            for v in val.values():
                if isinstance(v, (int, float)):
                    return v
            return None
        if isinstance(val, (list, tuple)):
            for v in val:
                nv = _num(v)
                if isinstance(nv, (int, float)):
                    return nv
            return None
        if isinstance(val, str):
            try:
                return float(val.replace(',', '.'))
            except Exception:
                return None
        return None

    raw_price_consensus = consensus.get("consensus") or consensus.get("priceConsensus")
    price_consensus = _num(raw_price_consensus)
    min_target = _num(consensus.get("minTarget"))
    max_target = _num(consensus.get("maxTarget"))
    rec_date = (consensus.get("recommendationDate") or consensus.get("date") or datetime.now(UTC).date().isoformat())
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid, ticker, recommendation, currency, priceConsensus, minTarget, maxTarget, recommendationDate FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1", (uid,))
        row = cur.fetchone()
        if row and all([
            row[0] == uid,
            row[1] == ticker,
            row[2] == recommendation,
            row[3] == currency,
            (row[4] == price_consensus),
            (row[5] == min_target),
            (row[6] == max_target)
        ]):
            log.info("Прогноз по бумаге %s уже сохранен ранее.", ticker)
            return {"status": "dup", "uid": uid}
        sql = ("INSERT INTO consensus_forecasts(uid, ticker, recommendation, recommendationDate, currency, priceConsensus, minTarget, maxTarget) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        conn.execute(sql, (uid, ticker, recommendation, rec_date, currency, price_consensus, min_target, max_target))
        if db_layer.BACKEND == "sqlite":
            conn.commit()
    return {"status": "inserted", "uid": uid, "recommendationDate": rec_date}


def AddConsensusTargets(targets: List[Dict[str, Any]]) -> Dict[str, Any]:
    inserted = 0
    skipped = 0
    if not targets:
        return {"inserted": inserted, "skipped": skipped}
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        for t in targets:
            tuid = t.get("uid")
            ticker = t.get("ticker")
            company = t.get("company")
            recommendation = t.get("recommendation")
            recommendationDate = t.get("recommendationDate") or t.get("date") or datetime.now(UTC).date().isoformat()
            currency = t.get("currency")
            def _num(val):
                if isinstance(val, (int, float)):
                    return val
                if isinstance(val, dict):
                    for k in ["value", "price", "target", "amount"]:
                        if k in val and isinstance(val[k], (int, float)):
                            return val[k]
                    for v in val.values():
                        if isinstance(v, (int, float)):
                            return v
                    return None
                if isinstance(val, (list, tuple)):
                    for v in val:
                        nv = _num(v)
                        if isinstance(nv, (int, float)):
                            return nv
                    return None
                if isinstance(val, str):
                    try:
                        return float(val.replace(',', '.'))
                    except Exception:
                        return None
                return None
            targetPrice = _num(t.get("targetPrice"))
            showName = t.get("showName")
            cur = conn.execute("SELECT uid, company, recommendationDate, ticker, recommendation, currency, targetPrice, showName FROM consensus_targets WHERE uid = ? AND company = ? AND recommendationDate = ?", (tuid, company, recommendationDate))
            row = cur.fetchone()
            if row and all([
                row[0] == tuid,
                row[1] == company,
                row[2] == recommendationDate,
                row[3] == ticker,
                row[4] == recommendation,
                row[5] == currency,
                row[6] == targetPrice,
                row[7] == showName,
            ]):
                log.info("Прогноз %s по %s за %s уже сохранен ранее.", company, ticker, recommendationDate)
                skipped += 1
                continue
            sql = ("INSERT INTO consensus_targets(uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            try:
                conn.execute(sql, (tuid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName))
                inserted += 1
            except sqlite3.IntegrityError:
                # duplicate (UNIQUE constraint) – treat as skipped
                log.debug("Duplicate target skipped uid=%s company=%s date=%s", tuid, company, recommendationDate)
                skipped += 1
        if db_layer.BACKEND == "sqlite":
            conn.commit()
    return {"inserted": inserted, "skipped": skipped}


def GetConsensusAndStore(uid: str) -> Dict[str, Any]:
    """Комбинированная операция: получить консенсус и сохранить (forecast + targets)."""
    data = GetConsensusByUid(uid)
    consensus = data.get("consensus") or {}
    targets = data.get("targets") or []
    r1 = AddConsensusForecasts(consensus)
    r2 = AddConsensusTargets(targets)
    return {"forecast": r1, "targets": r2}


def FillingConsensusData() -> Dict[str, Any]:
    db_layer.init_schema()
    processed = 0
    results = {}
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid, ticker, secid FROM perspective_shares")
        for uid, ticker, secid in cur.fetchall():
            processed += 1
            results[uid] = GetConsensusAndStore(uid)
    return {"processed": processed, "results": results}


def ComputeInstrumentPotentials() -> Dict[str, Any]:
    """Вычислить потенциалы и сохранить в instrument_potentials.
    Алгоритм шагов 1-6. Шаг 7 (устаревший потенциал) и дивиденды/сплиты помечены TODO.
    """
    db_layer.init_schema()
    now_ts = datetime.now(UTC).isoformat(timespec="seconds")
    saved = 0
    with db_layer.get_connection() as conn:
        # 1. Список uid,ticker,secid
        cur = conn.execute("SELECT uid, ticker, secid FROM perspective_shares WHERE uid IS NOT NULL")
        instruments = cur.fetchall()
        for uid, ticker, secid in instruments:
            # 2-3. Последний consensus
            cur_c = conn.execute("SELECT priceConsensus FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1", (uid,))
            row_c = cur_c.fetchone()
            price_consensus = row_c[0] if row_c else None
            # 4. Последняя цена закрытия по secid
            cur_p = conn.execute("SELECT CLOSE, TRADEDATE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1", (secid,))
            row_p = cur_p.fetchone()
            prev_close = row_p[0] if row_p else None
            last_trade_date = row_p[1] if row_p else None
            pricePotentialRel = None
            if price_consensus is not None and prev_close and prev_close > 0:
                pricePotentialRel = (price_consensus - prev_close) / prev_close
            # 6. Сохранить
            sql = ("INSERT INTO instrument_potentials(uid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?)")
            conn.execute(sql, (uid, ticker, now_ts, prev_close, price_consensus, pricePotentialRel))
            saved += 1
        if db_layer.BACKEND == "sqlite":
            conn.commit()
    return {"saved": saved, "computedAt": now_ts}


def ComputeInstrumentPotentialsForUpdated(updated_uids: List[str]) -> Dict[str, Any]:
    """Пересчитать потенциалы только для тех бумаг, где появился новый консенсус.

    updated_uids: список uid для которых AddConsensusForecasts вернул status 'inserted'.
    Возвращает статистику сохранённых строк.
    """
    if not updated_uids:
        return {"saved": 0, "computedAt": None, "skipped": True}
    db_layer.init_schema()
    now_ts = datetime.now(UTC).isoformat(timespec="seconds")
    saved = 0
    with db_layer.get_connection() as conn:
        for uid in updated_uids:
            cur_i = conn.execute("SELECT uid, ticker, secid FROM perspective_shares WHERE uid = ?", (uid,))
            inst_row = cur_i.fetchone()
            if not inst_row:
                continue
            uid2, ticker, secid = inst_row
            # последний консенсус
            cur_c = conn.execute("SELECT priceConsensus FROM consensus_forecasts WHERE uid = ? ORDER BY recommendationDate DESC LIMIT 1", (uid2,))
            row_c = cur_c.fetchone()
            price_consensus = row_c[0] if row_c else None
            # последняя цена закрытия
            cur_p = conn.execute("SELECT CLOSE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1", (secid,))
            row_p = cur_p.fetchone()
            prev_close = row_p[0] if row_p else None
            pricePotentialRel = None
            if price_consensus is not None and prev_close and prev_close > 0:
                pricePotentialRel = (price_consensus - prev_close) / prev_close
            sql = ("INSERT INTO instrument_potentials(uid, ticker, computedAt, prevClose, consensusPrice, pricePotentialRel) VALUES (?, ?, ?, ?, ?, ?)")
            conn.execute(sql, (uid2, ticker, now_ts, prev_close, price_consensus, pricePotentialRel))
            saved += 1
        if db_layer.BACKEND == "sqlite":
            conn.commit()
    return {"saved": saved, "computedAt": now_ts, "uids": updated_uids}


def ExportTopPotentials(limit: int = 10, file_path: Optional[str] = None) -> Dict[str, Any]:
    """Экспорт топ-N бумаг по текущему относительному потенциалу.

    Выбираем последние вычисления по каждому uid (максимальный computedAt) и сортируем
    по pricePotentialRel DESC. Экспортируем в CSV если указан file_path.
    Округление в выводе до 4 знаков, CSV до 4. (Excel можно форматировать отдельно.)
    """
    db_layer.init_schema()
    rows: List[tuple] = []
    with db_layer.get_connection() as conn:
        # Получаем последнюю запись по каждому uid через подзапрос
        sql = (
            "SELECT p.uid, p.ticker, p.computedAt, p.prevClose, p.consensusPrice, p.pricePotentialRel "
            "FROM instrument_potentials p "
            "JOIN (SELECT uid, MAX(computedAt) AS mx FROM instrument_potentials GROUP BY uid) last "
            "ON p.uid = last.uid AND p.computedAt = last.mx "
            "WHERE p.pricePotentialRel IS NOT NULL "
            "ORDER BY p.pricePotentialRel DESC LIMIT ?"
        )
        cur = conn.execute(sql, (limit,))
        rows = cur.fetchall()
    result = [
        {
            "uid": r[0],
            "ticker": r[1],
            "computedAt": r[2],
            "prevClose": r[3],
            "consensusPrice": r[4],
            "pricePotentialRel": r[5],
            "pricePotentialRel_pct": (r[5] * 100.0 if r[5] is not None else None)
        }
        for r in rows
    ]
    if file_path:
        import csv
        try:
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["uid", "ticker", "computedAt", "prevClose", "consensusPrice", "pricePotentialRel", "pricePotentialRel_pct"])
                for rec in result:
                    w.writerow([
                        rec["uid"], rec["ticker"], rec["computedAt"], rec["prevClose"], rec["consensusPrice"],
                        f"{rec['pricePotentialRel']:.6f}" if rec["pricePotentialRel"] is not None else "",
                        f"{rec['pricePotentialRel_pct']:.2f}" if rec["pricePotentialRel_pct"] is not None else ""
                    ])
        except Exception:
            log.exception("Не удалось экспортировать CSV %s", file_path)
            return {"status": "error", "file": file_path, "rows": len(result)}
        return {"status": "ok", "file": file_path, "rows": len(result)}
    return {"status": "ok", "rows": len(result), "data": result}

# TODO: Шаг 7 (пометка устаревших потенциалов) – определить критерий и хранить флаг
# TODO: Коррекция по дивидендам / сплитам – потребуются корпоративные действия
# TODO: Поддержка пересчёта топ-10 перспективных бумаг (отдельная таблица или поле)
"""
