"""maintenance.py

Утилиты обслуживания данных:
  purge_anomalous_consensus(threshold=1_000_000) – удалить записи consensus_forecasts с priceConsensus > threshold.
  refetch_forecasts_for_all() – запросить детальный прогноз (forecast_by) для каждого uid из perspective_shares.
  recompute_potentials_after_refresh() – пересчитать потенциалы после обновления консенсусов.
  run_maintenance(threshold=1_000_000) – объединённый сценарий: очистка -> refetch -> пересчёт -> отчёт.

Все строки прокомментированы для прозрачности.
"""
from __future__ import annotations                    # Совместимость аннотаций
import datetime as dt                                 # Временные метки
from datetime import UTC                              # TZ-aware
from typing import Dict, Any                          # Аннотации типов

from . import db as db_layer                          # Слой БД
from . import forecasts                               # Модуль загрузки детальных прогнозов
from . import potentials                              # Модуль расчёта потенциалов


def purge_anomalous_consensus(threshold: float = 1_000_000) -> Dict[str, Any]:
    """Удалить строки consensus_forecasts где priceConsensus > threshold.

    Возвращает статистику {'deleted': N, 'threshold': threshold}.
    """
    db_layer.init_schema()                            # Гарантируем наличие схемы
    backend = db_layer.BACKEND                        # Тип БД
    with db_layer.get_connection() as conn:           # Открываем соединение
        if backend == 'sqlite':                       # SQLite вариант с параметром
            cur = conn.execute(
                "SELECT COUNT(*) FROM consensus_forecasts WHERE priceConsensus > ?", (threshold,)
            )                                         # Подсчёт кандидатов
            to_delete = cur.fetchone()[0]             # Извлекаем число
            conn.execute(
                "DELETE FROM consensus_forecasts WHERE priceConsensus > ?", (threshold,)
            )                                         # Удаляем
            conn.commit()                             # Фиксируем
        else:                                         # DuckDB (без параметризации для простоты)
            cur = conn.execute(
                f"SELECT COUNT(*) FROM consensus_forecasts WHERE priceConsensus > {threshold}"
            )
            to_delete = cur.fetchone()[0]
            conn.execute(
                f"DELETE FROM consensus_forecasts WHERE priceConsensus > {threshold}"
            )
        return {'deleted': to_delete, 'threshold': threshold}


def refetch_forecasts_for_all() -> Dict[str, Any]:
    """Запросить детальный forecast_by для всех uid в perspective_shares.

    Возвращает статистику по вставкам: {'uids': count, 'consensus_inserted': X, 'consensus_dup': Y, 'targets_inserted': Z}.
    """
    db_layer.init_schema()                            # Инициализация схемы
    consensus_inserted = 0                            # Счётчик новых консенсусов
    consensus_dup = 0                                 # Счётчик дубликатов
    targets_inserted = 0                              # Счётчик новых таргетов
    with db_layer.get_connection() as conn:           # Соединение
        cur = conn.execute("SELECT uid FROM perspective_shares WHERE uid IS NOT NULL")
        uids = [r[0] for r in cur.fetchall()]         # Список UID
    for uid in uids:                                  # Проход по каждому UID
        r = forecasts.load_forecast_by_uid(uid)       # Загрузка детального прогноза
        c_stat = r.get('consensus_save', {})          # Статус сохранения консенсуса
        t_stat = r.get('targets_save', {})            # Статус сохранения таргетов
        if c_stat.get('status') == 'inserted':        # Новая запись
            consensus_inserted += 1
        elif c_stat.get('status') == 'dup':           # Дубликат
            consensus_dup += 1
        targets_inserted += t_stat.get('inserted', 0) # Таргеты
    return {
        'uids': len(uids),
        'consensus_inserted': consensus_inserted,
        'consensus_duplicates': consensus_dup,
        'targets_inserted': targets_inserted,
    }


def recompute_potentials_after_refresh() -> Dict[str, Any]:
    """Пересчитать потенциалы после обновления консенсусов."""
    stats = potentials.compute_all_potentials()        # Вызов пересчёта
    return stats                                       # Возврат статистики


def run_maintenance(threshold: float = 1_000_000) -> Dict[str, Any]:
    """Полный сценарий обслуживания.

    1. Очистка аномалий.
    2. Повторный детальный запрос прогнозов.
    3. Пересчёт потенциалов.
    4. Возврат агрегированного отчёта.
    """
    t_clean = purge_anomalous_consensus(threshold=threshold)  # Шаг 1
    t_refresh = refetch_forecasts_for_all()                   # Шаг 2
    t_potentials = recompute_potentials_after_refresh()       # Шаг 3
    return {                                                  # Итоговый отчёт
    'timestamp': dt.datetime.now(UTC).isoformat(timespec='seconds'),
        'cleanup': t_clean,
        'refresh': t_refresh,
        'potentials': t_potentials,
    }


__all__ = [                                                  # Экспортируем публичный интерфейс
    'purge_anomalous_consensus',
    'refetch_forecasts_for_all',
    'recompute_potentials_after_refresh',
    'run_maintenance',
]

if __name__ == '__main__':                                   # Запуск из CLI
    import json                                               # Для красивого вывода
    print(json.dumps(run_maintenance(), ensure_ascii=False, indent=2))
