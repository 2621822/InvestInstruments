"""history.py

Обёрточный модуль над функциональностью загрузки истории цен с MOEX.

Цели:
  * Предоставить единый высокоуровневый интерфейс для массовой загрузки истории
    по всем бумагам в `perspective_shares` или по конкретному SECID.
  * Линейно прокомментировать каждую строку для прозрачности.
  * Делегировать фактическую пагинированную загрузку функции `GetMoexHistory`
    из модуля `moex_history.py` (которая пишет в таблицу `moex_shares_history`).
  * Добавить тонкий уровень параметризации: дата начала/конца, доска, лимит
    страниц (через прерывание по пустым данным), хотя базовая функция сама
    завершает цикл.

Примечания:
  * Историческая таблица: `moex_shares_history` (ключ SECID + TRADEDATE).
  * Дополнительная таблица `moex_history` в `prices.py` остаётся пока как
    альтернатива; основной целевой источник для потенциалов – `moex_shares_history`.
  * Функции возвращают словари статистик для отчётности.
"""
from __future__ import annotations                 # Совместимость аннотаций
import logging                                     # Логирование
from typing import Optional, Dict, Any             # Типы для аннотаций

from . import moex_history                         # Низкоуровневый модуль загрузки (GetMoexHistory)
from . import db as db_layer                       # Слой БД (инициализация схемы)

log = logging.getLogger(__name__)                  # Локальный логгер модуля


def load_history_bulk(
    board: str = "TQBR",                             # Код торговой доски (по умолчанию TQBR)
    date_from: Optional[str] = None,                 # Начальная дата YYYY-MM-DD или None (рассчитать автоматически)
    date_to: Optional[str] = None,                   # Конечная дата YYYY-MM-DD или None (сегодня)
) -> Dict[str, Any]:
    """Массовая загрузка истории по всем бумагам из `perspective_shares`.

    Делегирует вызов в `moex_history.GetMoexHistory`, которая сама извлечёт
    список SECID и выполнит пагинированную загрузку.

    Args:
        board: Торговая доска MOEX (например 'TQBR').
        date_from: Начальная дата или None (внутри будет вычислена).
        date_to: Конечная дата или None (сегодня).

    Returns:
        Словарь статистики (total_inserted, per_security, pruned).
    """
    # Вызов низкоуровневой функции с передачей параметров
    stats = moex_history.GetMoexHistory(board=board, secid=None, dr_start=date_from, dr_end=date_to)
    # Логируем сводку
    log.info(
        "Bulk history load board=%s inserted_total=%s securities=%s pruned=%s",
        board, stats.get("total_inserted"), len(stats.get("per_security", {})), stats.get("pruned")
    )
    return stats                                      # Возвращаем статистику наверх


def load_history_for_secid(
    secid: str,                                      # Один тикер/SECID MOEX
    board: str = "TQBR",                             # Доска по умолчанию
    date_from: Optional[str] = None,                 # Начальная дата
    date_to: Optional[str] = None,                   # Конечная дата
) -> Dict[str, Any]:
    """Загрузить историю только для одного SECID.

    Использует ту же низкоуровневую функцию, передавая параметр secid.
    Возвращает статистику формата аналогичного bulk вызову (но per_security с одним ключом).
    """
    stats = moex_history.GetMoexHistory(board=board, secid=secid, dr_start=date_from, dr_end=date_to)
    log.info(
        "Single history load secid=%s board=%s inserted=%s pruned=%s",
        secid, board, stats.get("total_inserted"), stats.get("pruned")
    )
    return stats                                      # Словарь с результатами


def summarize_latest_trade_dates() -> Dict[str, Any]:
    """Собрать сводку по последним датам торгов для всех SECID.

    Выбирает список SECID из `perspective_shares`, затем читает последнюю дату
    из таблицы `moex_shares_history`. Формирует отчёт пропусков.
    """
    db_layer.init_schema()                           # Убедиться что таблицы есть
    latest: Dict[str, Optional[str]] = {}            # Словарь SECID -> последняя дата или None
    missing = 0                                      # Количество SECID без истории
    with db_layer.get_connection() as conn:          # Открываем соединение с БД
        cur = conn.execute("SELECT secid FROM perspective_shares WHERE secid IS NOT NULL AND secid <> ''")
        for (secid,) in cur.fetchall():              # Итерируем по всем строкам
            cur2 = conn.execute(
                "SELECT TRADEDATE FROM moex_shares_history WHERE SECID = ? ORDER BY TRADEDATE DESC LIMIT 1",
                (secid,)
            )                                       # Запрос последней даты
            row = cur2.fetchone()                   # Получаем строку
            last_date = row[0] if row else None     # Извлекаем дату или None
            if last_date is None:                   # Если нет данных
                missing += 1                        # Увеличиваем счётчик пропусков
            latest[secid] = last_date               # Сохраняем результат
    return {                                        # Возврат агрегированной сводки
        "latest_trade_dates": latest,
        "total": len(latest),
        "missing": missing,
    }


__all__ = [                                         # Публичный экспортируемый интерфейс
    "load_history_bulk",
    "load_history_for_secid",
    "summarize_latest_trade_dates",
]
