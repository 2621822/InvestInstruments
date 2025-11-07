"""Модуль получения и сохранения прогнозов (консенсус + таргеты аналитиков) строго по заданному алгоритму.

Реализованные функции:
  GetConsensusByUid(uid)            – отправляет запрос к API InstrumentsService/GetForecastBy и возвращает ответ.
  AddConsensusForecasts(...)        – сохраняет консенсус, предварительно сравнив с последней записью по uid.
  AddConsensusTargets(...)          – сохраняет один таргет аналитика с проверкой дублей.
  FillingConsensusData(...)         – ежедневно обходит таблицу perspective_shares и сохраняет свежие данные.

Правила сохранения:
  Consensus: сравнение всех семи полей (uid,ticker,recommendation,currency,consensus,minTarget,maxTarget) с последней записью по uid.
  Targets: поиск записи по (uid,recommendationDate,company) и затем сравнение полного набора полей.
"""
from __future__ import annotations  # Отложенные аннотации (Python <3.11)
from typing import Any, Dict, List, Optional
import json                                   # Парсинг JSON ответов API
import http.client                            # Низкоуровневый HTTPS клиент
import ssl                                    # SSL контекст (возможен кастомный сертификат)
import logging                                # Логирование предупреждений и статуса
from datetime import datetime, UTC            # Работа с датами (TZ aware)
import os                                     # Переменные окружения / пути
from dataclasses import dataclass             # Структурирование записей

from . import db_mysql as db_layer            # Чистый MySQL слой доступа к БД
from .normalization import to_number, normalize_date  # Универсальные утилиты приведения

log = logging.getLogger(__name__)

_RUN_CACHE: Dict[str, Any] = {}              # In-memory кэш ответов API на время запуска

# ------------------------- Dataclasses -------------------------
@dataclass(slots=True)
class ConsensusRecord:
    """Нормализованная запись консенсуса для вставки.

    Все числовые поля переводятся в float или None.
    recommendationDate обрезается до YYYY-MM-DD.
    """
    uid: str
    ticker: Optional[str]
    recommendation: Optional[str]
    recommendationDate: str
    currency: Optional[str]
    consensus: float | None
    minTarget: float | None
    maxTarget: float | None

    @classmethod
    def from_raw(cls, uid: str, ticker: str | None, recommendation: str | None,
                 recommendationDate: str, currency: str | None,
                 consensus: Any, minTarget: Any, maxTarget: Any) -> 'ConsensusRecord':
        return cls(
            uid=uid,
            ticker=ticker,
            recommendation=recommendation,
            recommendationDate=normalize_date(recommendationDate),
            currency=currency,
            consensus=to_number(consensus),
            minTarget=to_number(minTarget),
            maxTarget=to_number(maxTarget),
        )

@dataclass(slots=True)
class TargetRecord:
    """Нормализованная запись таргета аналитика."""
    uid: str
    ticker: Optional[str]
    company: Optional[str]
    recommendation: Optional[str]
    recommendationDate: str
    currency: Optional[str]
    targetPrice: float | None
    showName: Optional[str]

    @classmethod
    def from_raw(cls, uid: str, ticker: str | None, company: str | None,
                 recommendation: str | None, recommendationDate: str,
                 currency: str | None, targetPrice: Any, showName: str | None) -> 'TargetRecord':
        return cls(
            uid=uid,
            ticker=ticker,
            company=company,
            recommendation=recommendation,
            recommendationDate=normalize_date(recommendationDate),
            currency=currency,
            targetPrice=to_number(targetPrice),
            showName=showName,
        )

# ----------------------------------------------------------------------------
# НИЗКОУРОВНЕВЫЙ ВЫЗОВ API (если SDK недоступен используем прямой POST)
# ----------------------------------------------------------------------------

API_HOST = "invest-public-api.tbank.ru"
API_PATH = "/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/GetForecastBy"


def _load_token() -> str | None:
    """Загрузить токен авторизации.

    Приоритет:
      1. Переменная окружения INVEST_TINKOFF_TOKEN или INVEST_TOKEN.
      2. Файл tinkoff_token.txt в корне проекта.
    Возвращает строку токена или None.
    """
    token = os.getenv("INVEST_TINKOFF_TOKEN") or os.getenv("INVEST_TOKEN")
    if token:
        return token.strip()
    # Попытка прочитать файл рядом с рабочей директорией
    for name in ("tinkoff_token.txt", "token.txt"):
        path = os.path.join(os.getcwd(), name)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        return content
            except Exception:
                pass
    return None


def _build_ssl_context() -> ssl.SSLContext:
    """Создать SSLContext с учётом пользовательского сертификата и флага отключения проверки.

    Поведение:
      * Если INVEST_TINKOFF_VERIFY_SSL в окружении равен '0' / 'false' / 'no' – возвращаем контекст без проверки.
      * Иначе пробуем загрузить файл invest/_.tbank.ru.crt (если существует).
      * При ошибке загрузки сертификата логируем предупреждение и возвращаем контекст без доп. сертификата.
    """
    verify_flag = os.getenv('INVEST_TINKOFF_VERIFY_SSL', '1').lower()
    disable_verify = verify_flag in ('0', 'false', 'no', 'off')
    if disable_verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    # Попытка загрузить пользовательский сертификат
    crt_path = os.path.join(os.getcwd(), 'invest', '_.tbank.ru.crt')
    try:
        if os.path.exists(crt_path):
            ctx = ssl.create_default_context()
            ctx.load_verify_locations(crt_path)
            return ctx
    except Exception as ex:  # noqa
        log.warning("Не удалось загрузить сертификат %s ex=%s; продолжаю без него", crt_path, ex)
    # fallback стандартный контекст
    return ssl.create_default_context()


def _post_get_forecast_by(uid: str) -> Dict[str, Any] | None:
    """Сделать POST запрос к официальному API InstrumentsService/GetForecastBy.

    Тело запроса: {"instrumentId": "<uid>"}
    Возвращает разобранный JSON (dict) или None при ошибке.
    """
    payload = json.dumps({"instrumentId": uid})
    headers = {"Content-Type": "application/json"}
    token = _load_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        context = _build_ssl_context()
        conn = http.client.HTTPSConnection(API_HOST, timeout=20, context=context)
        conn.request("POST", API_PATH, body=payload, headers=headers)
        resp = conn.getresponse()
        if resp.status == 401 or resp.status == 403:
            # Явная ошибка авторизации
            return {"status": "auth_error", "code": resp.status, "uid": uid, "message": "Unauthorized or forbidden"}
        if resp.status != 200:
            log.warning("API GetForecastBy status=%s uid=%s", resp.status, uid)
            return {"status": "http_error", "code": resp.status, "uid": uid}
        data = resp.read()
        return json.loads(data.decode("utf-8"))
    except Exception as ex:  # noqa
        log.warning("API GetForecastBy error uid=%s ex=%s", uid, ex)
        return None


def GetConsensusByUid(uid: str, *, refresh: bool = False) -> Dict[str, Any] | None:
    """Получить прогноз по одной акции с кэшированием на время запуска.

    Параметры:
      uid      – идентификатор инструмента
      refresh  – принудительно игнорировать кэш и сделать новый запрос

    Кэш (_RUN_CACHE): хранит исходный ответ словарём. Используется только в памяти.
    """
    if not refresh and uid in _RUN_CACHE:
        return _RUN_CACHE[uid]
    data = _post_get_forecast_by(uid)
    if data is not None:
        _RUN_CACHE[uid] = data
    return data


def ResetForecastCache() -> None:
    """Очистить кэш прогнозов (используется при необходимости освобождения памяти)."""
    _RUN_CACHE.clear()


# ----------------------------------------------------------------------------
# Сохранение консенсуса
# ----------------------------------------------------------------------------

def AddConsensusForecasts(uid: str, ticker: str | None, recommendation: str | None, recommendationDate: str,
                          currency: str | None, consensus: Any, minTarget: Any,
                          maxTarget: Any) -> Dict[str, Any]:
    """Сохранить консенсус прогноз.

    Алгоритм:
      1. Найти последнюю запись по uid (ORDER BY recommendationDate DESC LIMIT 1).
      2. Сравнить все поля (uid,ticker,recommendation,currency,priceConsensus,minTarget,maxTarget).
      3. Если полностью совпадает – пропустить.
      4. Иначе вставить новую запись с текущей recommendationDate.
    """
    db_layer.init_schema()  # Инициализация схемы
    rec_obj = ConsensusRecord.from_raw(uid, ticker, recommendation, recommendationDate,
                                       currency, consensus, minTarget, maxTarget)
    with db_layer.get_connection() as conn:
        # Получаем последнюю запись по uid для сравнения (ORDER BY recommendationDate DESC)
        cur = db_layer.exec_sql(
            conn,
            "SELECT uid, ticker, recommendation, currency, priceConsensus, minTarget, maxTarget FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC LIMIT 1",
            (uid,)
        )
        row = cur.fetchone()
        if row and all([
            row[0] == rec_obj.uid,
            row[1] == rec_obj.ticker,
            row[2] == rec_obj.recommendation,
            row[3] == rec_obj.currency,
            row[4] == rec_obj.consensus,
            row[5] == rec_obj.minTarget,
            row[6] == rec_obj.maxTarget,
        ]):
            print(f"Прогноз по бумаге {ticker} уже сохранен ранее.")
            return {"status": "dup", "uid": uid}
        try:
            db_layer.exec_sql(  # Пытаемся вставить новую строку
                conn,
                "INSERT INTO consensus_forecasts(uid, ticker, recommendation, recommendationDate, currency, priceConsensus, minTarget, maxTarget) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (rec_obj.uid, rec_obj.ticker, rec_obj.recommendation, rec_obj.recommendationDate, rec_obj.currency, rec_obj.consensus, rec_obj.minTarget, rec_obj.maxTarget)
            )
        except Exception as ex:  # noqa
            ex_up = str(ex).upper()
            if 'UNIQUE' in ex_up or 'DUPLICATE ENTRY' in ex_up:
                # Проверим существующую запись на эту дату
                cur2 = db_layer.exec_sql(conn, "SELECT ticker, recommendation, currency, priceConsensus, minTarget, maxTarget FROM consensus_forecasts WHERE uid=? AND recommendationDate=?", (uid, recommendationDate))
                row2 = cur2.fetchone()
                if row2 and all([
                    row2[0] == rec_obj.ticker,
                    row2[1] == rec_obj.recommendation,
                    row2[2] == rec_obj.currency,
                    row2[3] == rec_obj.consensus,
                    row2[4] == rec_obj.minTarget,
                    row2[5] == rec_obj.maxTarget,
                ]):
                    print(f"Прогноз по бумаге {ticker} уже сохранен ранее (same date).")
                    return {"status": "dup", "uid": uid}
                else:
                    # Обновим запись этой даты новыми значениями
                    db_layer.exec_sql(conn, "UPDATE consensus_forecasts SET ticker=?, recommendation=?, currency=?, priceConsensus=?, minTarget=?, maxTarget=? WHERE uid=? AND recommendationDate=?",  # Обновляем существующую запись этой даты
                                      (rec_obj.ticker, rec_obj.recommendation, rec_obj.currency, rec_obj.consensus, rec_obj.minTarget, rec_obj.maxTarget, rec_obj.uid, rec_obj.recommendationDate))
            else:
                raise
    print(f"Консенсус по {rec_obj.ticker} сохранен (uid={rec_obj.uid}).")
    return {"status": "inserted", "uid": rec_obj.uid, "recommendationDate": rec_obj.recommendationDate}


# ----------------------------------------------------------------------------
# Сохранение таргета аналитика
# ----------------------------------------------------------------------------

def AddConsensusTargets(uid: str, ticker: str | None, company: str | None, recommendation: str | None,
                        recommendationDate: str, currency: str | None, targetPrice: Any,
                        showName: str | None) -> Dict[str, Any]:
    """Сохранить/обновить таргет аналитика без дублей.

    Логика:
      1. Нормализуем дату (обрезаем до YYYY-MM-DD).
      2. Приводим targetPrice к float.
      3. Пытаемся найти запись по составному ключу (uid, recommendationDate, company).
         a) Если запись найдена и все поля совпадают -> статус dup.
         b) Если запись найдена и часть полей отличается -> выполняем UPDATE и статус updated.
         c) Если запись не найдена -> INSERT и статус inserted.
    """
    db_layer.init_schema()  # Инициализация схемы
    rec_obj = TargetRecord.from_raw(uid, ticker, company, recommendation,
                                    recommendationDate, currency, targetPrice, showName)
    with db_layer.get_connection() as conn:
        cur = db_layer.exec_sql(
            conn,
            "SELECT uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName FROM consensus_targets WHERE uid=? AND recommendationDate=? AND company=?",
            (rec_obj.uid, rec_obj.recommendationDate, rec_obj.company)
        )
        row = cur.fetchone()
        if row:
            same = (
                row[1] == rec_obj.ticker and
                row[3] == rec_obj.recommendation and
                row[5] == rec_obj.currency and
                (row[6] == rec_obj.targetPrice or (row[6] is None and rec_obj.targetPrice is None)) and
                row[7] == rec_obj.showName
            )
            if same:
                print(f"Прогноз {rec_obj.company} по {rec_obj.ticker} за {rec_obj.recommendationDate} уже сохранен ранее.")
                return {"status": "dup", "uid": rec_obj.uid, "recommendationDate": rec_obj.recommendationDate, "company": rec_obj.company}
            # Обновляем запись (данные изменились)
            db_layer.exec_sql(  # Выполняем UPDATE изменившихся полей
                conn,
                "UPDATE consensus_targets SET ticker=?, recommendation=?, currency=?, targetPrice=?, showName=? WHERE uid=? AND recommendationDate=? AND company=?",
                (rec_obj.ticker, rec_obj.recommendation, rec_obj.currency, rec_obj.targetPrice, rec_obj.showName, rec_obj.uid, rec_obj.recommendationDate, rec_obj.company)
            )
            print(f"Прогноз {rec_obj.company} по {rec_obj.ticker} за {rec_obj.recommendationDate} обновлен.")
            return {"status": "updated", "uid": rec_obj.uid, "recommendationDate": rec_obj.recommendationDate, "company": rec_obj.company}
        # Вставка новой записи
        db_layer.exec_sql(  # INSERT новой строки
            conn,
            "INSERT INTO consensus_targets(uid, ticker, company, recommendation, recommendationDate, currency, targetPrice, showName) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rec_obj.uid, rec_obj.ticker, rec_obj.company, rec_obj.recommendation, rec_obj.recommendationDate, rec_obj.currency, rec_obj.targetPrice, rec_obj.showName)
        )
    print(f"Прогноз {rec_obj.recommendation} от {rec_obj.company} по {rec_obj.ticker} за {rec_obj.recommendationDate} сохранен.")
    return {"status": "inserted", "uid": rec_obj.uid, "recommendationDate": rec_obj.recommendationDate, "company": rec_obj.company}


# ----------------------------------------------------------------------------
# Основная ежедневная функция FillingConsensusData
# ----------------------------------------------------------------------------

def FillingConsensusData(limit: int | None = None, sleep_sec: float = 0.2) -> Dict[str, Any]:
    """Ежедневный проход по всем перспективным акциям.

    Для каждой акции:
      1. Получить полные прогнозы (consensus + targets).
      2. Сохранить консенсус (текущая дата recommendationDate).
      3. Сохранить каждый таргет аналитика с оригинальной recommendationDate из ответа.
    """
    db_layer.init_schema()  # Гарантируем наличие таблиц и индексов
    with db_layer.get_connection() as conn:  # Получаем список всех перспективных uid
        cur = db_layer.exec_sql(conn, "SELECT uid FROM perspective_shares ORDER BY uid")
        uids = [r[0] for r in cur.fetchall() if r[0]]
    if limit is not None:
        uids = uids[:limit]
    processed = 0
    not_found = 0
    consensus_inserted = 0
    consensus_dups = 0
    targets_inserted = 0
    targets_dups = 0
    import time
    today_iso = datetime.now(UTC).date().isoformat()
    auth_failed = False
    for uid in uids:  # Основной цикл по инструментам
        if auth_failed:
            break
        data = GetConsensusByUid(uid)  # Запрос к API (кэшируется)
        if not data or (isinstance(data, dict) and data.get('status') == 'http_error'):
            not_found += 1
            processed += 1
            time.sleep(sleep_sec)
            continue
        if isinstance(data, dict) and data.get('status') == 'auth_error':
            auth_failed = True
            print(f"Авторизация не удалась (code={data.get('code')}). Прекращаю обход.")
            break
        # ---------------- Консенсус ----------------
        consensus_block = data.get('consensus') or {}
        c_uid = consensus_block.get('uid') or uid
        ticker = consensus_block.get('ticker')
        recommendation = consensus_block.get('recommendation')
        currency = consensus_block.get('currency')
        consensus_val = (
            consensus_block.get('consensus')
            or consensus_block.get('price_consensus')
            or consensus_block.get('priceConsensus')
        )
        min_target = consensus_block.get('minTarget') or consensus_block.get('min_target')
        max_target = consensus_block.get('maxTarget') or consensus_block.get('max_target')
        if c_uid:
            r_cons = AddConsensusForecasts(
                c_uid, ticker, recommendation, today_iso, currency, consensus_val, min_target, max_target
            )
            if r_cons['status'] == 'inserted':
                consensus_inserted += 1
            elif r_cons['status'] == 'dup':
                consensus_dups += 1
        # ---------------- Таргеты ----------------
        targets_list = data.get('targets') or []
        for t in targets_list:  # Проходим по таргетам аналитиков
            t_uid = t.get('uid') or c_uid
            t_ticker = t.get('ticker') or ticker
            t_company = t.get('company')
            t_rec = t.get('recommendation')
            t_date = t.get('recommendationDate') or t.get('date')
            t_currency = t.get('currency')
            t_price = t.get('targetPrice') or t.get('target_price')
            t_show = t.get('showName') or t.get('show_name')
            if t_uid and t_company and t_date:  # Требуем минимальный набор для сохранения
                r_t = AddConsensusTargets(t_uid, t_ticker, t_company, t_rec, t_date, t_currency, t_price, t_show)
                if r_t['status'] == 'inserted':
                    targets_inserted += 1
                elif r_t['status'] == 'dup':
                    targets_dups += 1
        processed += 1
        time.sleep(sleep_sec)
    return {
        'processed': processed,
        'not_found': not_found,
        'consensus_inserted': consensus_inserted,
        'consensus_duplicates': consensus_dups,
        'targets_inserted': targets_inserted,
        'targets_duplicates': targets_dups,
        'auth_failed': auth_failed,
    'timestamp': datetime.now(UTC).isoformat(),
    }


__all__ = [
    'GetConsensusByUid',
    'AddConsensusForecasts',
    'AddConsensusTargets',
    'FillingConsensusData',
]

