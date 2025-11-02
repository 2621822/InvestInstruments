"""sdk_client.py

Единая точка доступа к официальному tinkoff-investments SDK.

Цели:
  * Централизованная загрузка токена (из env или файла).
  * Повторное использование синхронного и асинхронного клиента.
  * Упрощение кода модулей instruments / forecasts / potentials.
  * Унифицированный перехват ошибок и логирование.

Построчные комментарии добавлены по требованию.
"""
from __future__ import annotations  # поддержка аннотаций типов без циклического импорта в ранних версиях
import os                           # модуль для работы с переменными окружения и путями
import logging                      # стандартный модуль логирования
from contextlib import asynccontextmanager, contextmanager  # менеджеры контекста для клиентов
from typing import Iterator, AsyncIterator, Optional        # типы для аннотаций

try:
    # Импортируем классы SDK. Если пакет не установлен, перехватим ImportError.
    from tinkoff.invest import Client, AsyncClient
except ImportError:  # Если tinkoff-investments отсутствует, даём понятное сообщение при использовании.
    Client = None    # type: ignore
    AsyncClient = None  # type: ignore

log = logging.getLogger(__name__)  # Логгер модуля


def _read_token_file(path: str) -> str:
    """Прочитать токен из файла, вернуть пустую строку при ошибке."""
    try:                                  # Пытаемся открыть файл токена
        with open(path, "r", encoding="utf-8") as f:  # Открываем с UTF-8
            return f.read().strip()        # Удаляем пробелы по краям
    except Exception:                      # Любая ошибка чтения -> пусто
        return ""                          # Возвращаем пустую строку


def load_token() -> str:
    """Загрузить токен SDK из приоритетов: INVEST_TOKEN, INVEST_TINKOFF_TOKEN, tinkoff_token.txt.

    Возвращает строку; если пустая – пользователь должен установить переменную.
    """
    # Сначала пробуем официальный переменную примеров SDK.
    token = os.getenv("INVEST_TOKEN")
    if token:               # Если найдена – возвращаем после trim
        return token.strip()
    # Далее legacy имя из текущего проекта.
    token = os.getenv("INVEST_TINKOFF_TOKEN")
    if token:
        return token.strip()
    # Путь к файлу с токеном в корне проекта.
    file_token = _read_token_file("tinkoff_token.txt")
    if file_token:
        return file_token.strip()
    return ""  # Ничего не нашли – пустая строка


def assert_token_exists(token: str) -> None:
    """Бросить исключение если токен отсутствует."""
    if not token:                       # Проверяем пустоту
        raise RuntimeError("SDK token not found (set INVEST_TOKEN or provide tinkoff_token.txt)")


@contextmanager
def client() -> Iterator[Client]:
    """Синхронный менеджер контекста для Client.

    Применение:
        with client() as c:
            c.instruments.find_instrument(...)
    """
    token = load_token()       # Загружаем токен
    assert_token_exists(token) # Проверяем что он существует
    if Client is None:         # Если пакет не установлен
        raise RuntimeError("tinkoff-investments package not installed")
    try:                       # Открываем контекст Client
        with Client(token) as c:
            yield c            # Передаём наружу клиента
    except Exception as ex:    # Логируем исключение
        log.exception("Client context error: %s", ex)
        raise                  # Прокидываем дальше


@asynccontextmanager
async def aclient() -> AsyncIterator[AsyncClient]:
    """Асинхронный менеджер контекста для AsyncClient."""
    token = load_token()        # Загружаем токен
    assert_token_exists(token)  # Проверяем наличие
    if AsyncClient is None:     # Проверка установки пакета
        raise RuntimeError("tinkoff-investments package not installed")
    try:                        # Открываем асинхронный контекст
        async with AsyncClient(token) as ac:
            yield ac            # Возвращаем наружу
    except Exception as ex:     # Логируем ошибки
        log.exception("AsyncClient context error: %s", ex)
        raise                   # Прокидываем


def find_instrument(query: str):
    """Синхронный удобный вызов поиска инструмента через SDK (InstrumentsService.find_instrument)."""
    with client() as c:                                 # Открываем контекст клиента
        resp = c.instruments.find_instrument(query=query)  # Вызываем метод SDK
        return resp                                      # Возвращаем оригинальный объект ответа


def instrument_by(uid: str):
    """Получить данные инструмента по UID (share_by / get_instrument_by).

    Порядок попыток:
      1. instrument_by через get_instrument_by(id_type=UID)
      2. fallback share_by (если это акция и первый метод не дал результата)
    """
    from tinkoff.invest.schemas import InstrumentIdType  # Импорт enum внутри функции
    with client() as c:                                  # Контекст клиента
        try:                                             # Первая попытка: универсальный метод
            resp = c.instruments.get_instrument_by(id=uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID)
            if getattr(resp, "instrument", None):       # Проверяем наличие объекта
                return resp.instrument                   # Возвращаем сам инструмент
        except Exception as ex:                          # Логируем, но продолжаем
            log.debug("get_instrument_by failed uid=%s err=%s", uid, ex)
        try:                                             # Вторая попытка (для акций)
            share = c.instruments.share_by(id=uid)       # Метод share_by
            if getattr(share, "instrument", None):      # Проверка результата
                return share.instrument                  # Возвращаем объект акции
        except Exception as ex:                          # Логируем если не получилось
            log.debug("share_by failed uid=%s err=%s", uid, ex)
    return None                                          # Ни один способ не дал результата


def consensus_page(limit: int = 50, page_number: int = 0):
    """Получить страницу консенсус-прогнозов (пагинация)."""
    from tinkoff.invest.schemas import GetConsensusForecastsRequest, Page  # Локальный импорт схем
    with client() as c:                                                  # Контекст
        req = GetConsensusForecastsRequest(paging=Page(page_number=page_number, limit=limit))  # Формируем запрос
        return c.instruments.get_consensus_forecasts(request=req)        # Возвращаем ответ SDK


def forecast_by(uid: str):
    """Получить детальный прогноз по одному инструменту (get_forecast_by)."""
    # ВАЖНО: Метод get_forecast_by принимает не именованный параметр instrument_id,
    # а объект запроса GetForecastByRequest (request=...). Ошибка 'unexpected keyword'
    # возникала из-за неправильного способа вызова.
    # Актуальное имя схемы в SDK: GetForecastRequest (GetForecastResponse возвращается).
    from tinkoff.invest.schemas import GetForecastRequest  # локальный импорт схемы
    with client() as c:                                      # Контекст
        try:                                                 # Пытаемся вызвать метод SDK
            req = GetForecastRequest(instrument_id=uid)       # Формируем объект запроса
            resp = c.instruments.get_forecast_by(request=req)  # Корректный вызов
            return resp                                      # Возвращаем оригинальный ответ SDK
        except Exception as ex:                              # Логируем исключение
            log.debug("get_forecast_by failed uid=%s err=%s", uid, ex)
            return None                                      # Возвращаем None при ошибке


__all__ = [  # Экспортируем публичные функции модуля
    "load_token",
    "client",
    "aclient",
    "find_instrument",
    "instrument_by",
    "consensus_page",
    "forecast_by",
]
