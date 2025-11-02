"""consensus_fill.py

Утилита: загрузить консенсус для всех акций, по которым уже есть исторические котировки
в таблице `moex_shares_history`, и дополнить их атрибуты в `perspective_shares`.

Шаги:
  1. Собрать множество SECID из moex_shares_history.
  2. Для каждого SECID убедиться что есть запись в perspective_shares (поиск по ticker).
  3. Выполнить массовое обогащение атрибутов (enrich_all_perspective).
  4. По каждому UID из perspective_shares, имеющему историю, получить детальный forecast_by (консенсус + targets).
  5. Вернуть агрегированную статистику.

Каждая строка прокомментирована.
"""
from __future__ import annotations                      # Совместимость аннотаций
from typing import Dict, Any, Set                        # Типы
import datetime as dt                                    # Метка времени
from datetime import UTC                                 # TZ-aware

from . import db as db_layer                             # Слой БД
from . import instruments                                # Модуль работы с инструментами
from . import forecasts                                  # Модуль прогнозов


def _distinct_secids_with_history() -> Set[str]:
    """Получить множество SECID из таблицы moex_shares_history."""
    db_layer.init_schema()                               # Инициализация схемы
    secids: Set[str] = set()                             # Пустое множество
    with db_layer.get_connection() as conn:              # Соединение
        cur = conn.execute("SELECT DISTINCT SECID FROM moex_shares_history WHERE SECID IS NOT NULL AND SECID <> ''")
        for (secid,) in cur.fetchall():                  # Итерация по результатам
            secids.add(secid)                            # Добавляем в множество
    return secids                                        # Возвращаем итоговый набор


def fill_consensus_for_history_shares() -> Dict[str, Any]:
    """Основной сценарий утилиты (см. описание в шапке)."""
    started = dt.datetime.now(UTC).isoformat(timespec="seconds")  # Время начала
    secids = _distinct_secids_with_history()              # Собираем SECID из истории
    ensured = 0                                           # Счётчик добавленных бумаг
    existed = 0                                           # Счётчик уже существующих
    not_found = 0                                         # Счётчик не найденных через поиск
    for secid in secids:                                  # Проход по каждому SECID
        res = instruments.ensure_perspective_share(secid) # Убеждаемся в наличии записи
        st = res.get('status')                           # Статус операции
        if st == 'inserted':                             # Новая запись
            ensured += 1
        elif st == 'exists':                             # Уже была
            existed += 1
        elif st == 'not-found':                          # Не удалось найти через SDK search
            not_found += 1
    enrich_stats = instruments.enrich_all_perspective()  # Массовое обогащение атрибутов
    # Сбор UID только для тех SECID, которые реально присутствуют и имели историю
    db_layer.init_schema()                               # Повторная инициализация (на случай новых вставок)
    uid_list = []                                        # Список UID для прогнозов
    with db_layer.get_connection() as conn:              # Соединение
        cur = conn.execute("SELECT uid, secid FROM perspective_shares WHERE secid IS NOT NULL AND secid <> ''")
        for uid, secid in cur.fetchall():                # Итерация по записям
            if secid in secids:                          # Только если SECID есть в истории
                uid_list.append(uid)                     # Добавляем UID в список
    # Загрузка детальных прогнозов по каждому UID
    consensus_inserted = 0                               # Счётчик новых консенсусов
    consensus_dup = 0                                    # Счётчик дублей
    targets_inserted = 0                                 # Счётчик новых таргет-записей
    targets_skipped = 0                                  # Счётчик пропущенных/дубликатов
    for uid in uid_list:                                 # Итерация по UID
        r = forecasts.load_forecast_by_uid(uid)          # Вызов SDK forecast_by
        cstat = r.get('consensus_save', {})              # Сохранение консенсуса
        tstat = r.get('targets_save', {})                # Сохранение таргетов
        if cstat.get('status') == 'inserted':            # Новая запись
            consensus_inserted += 1
        elif cstat.get('status') == 'dup':               # Дубликат
            consensus_dup += 1
        targets_inserted += tstat.get('inserted', 0)     # Новые таргеты
        targets_skipped += tstat.get('skipped', 0)       # Пропущенные таргеты
    finished = dt.datetime.now(UTC).isoformat(timespec="seconds")  # Время окончания
    return {                                             # Итоговый отчёт
        'started': started,
        'finished': finished,
        'secids_total': len(secids),
        'ensured_new': ensured,
        'ensured_existing': existed,
        'not_found_secid_search': not_found,
        'enrich_stats': enrich_stats,
        'uid_forecast_attempts': len(uid_list),
        'consensus_inserted': consensus_inserted,
        'consensus_duplicates': consensus_dup,
        'targets_inserted': targets_inserted,
        'targets_skipped': targets_skipped,
    }


__all__ = ['fill_consensus_for_history_shares']          # Экспортируем публичную функцию

if __name__ == '__main__':                               # CLI запуск
    import json                                           # Форматирование вывода
    rep = fill_consensus_for_history_shares()             # Выполняем утилиту
    print(json.dumps(rep, ensure_ascii=False, indent=2))  # Печатаем отчёт
