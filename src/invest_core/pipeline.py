"""pipeline.py

Высокоуровневый оркестратор массовой загрузки данных:
  1. Загрузка / обновление консенсусов (по perspective_shares)
  2. Детальные прогнозы (опционально)
  3. Загрузка истории цен MOEX
  4. Пересчёт потенциалов
  5. Формирование отчёта (количества, аномалии, пропуски)

Каждая строка прокомментирована для прозрачности.

Параметры функции run_pipeline:
  dry_run: bool – если True, только имитация шагов без сетевых вызовов.
  consensus_pages: int – максимум страниц консенсусов.
  consensus_page_limit: int – количество элементов на страницу консенсуса.
  fetch_forecast_details: bool – получать детальные таргеты по каждому uid.
  load_history: bool – выполнять ли загрузку истории MOEX.
  board: str – код торговой доски MOEX.
  recalc_potentials: bool – выполнять ли пересчёт потенциалов.
  anomaly_price_threshold: float – порог для выявления аномальных consensusPrice.

Отчёт включает ключи:
  perspective_count, consensus_stats, forecast_detail_stats, history_stats,
  potentials_stats, anomalies, missing_history, timing.
"""
from __future__ import annotations               # Совместимость аннотаций
import time                                      # Засечка времени
import datetime as dt                            # Формат времени отчёта
from datetime import UTC                         # Используем timezone-aware UTC
from typing import Dict, Any                     # Аннотации типов

from . import db as db_layer                     # Слой БД
from . import forecasts                          # Модуль загрузки консенсусов
from . import history                            # Модуль загрузки истории
from . import potentials                         # Модуль расчёта потенциалов


def _now_ts() -> str:                            # Текущая UTC метка времени (ISO, TZ-aware)
    return dt.datetime.now(UTC).isoformat(timespec="seconds")


def run_pipeline(
    dry_run: bool = False,                       # Имитация без сетевых вызовов
    consensus_pages: int = 10,                   # Максимум страниц консенсуса
    consensus_page_limit: int = 50,              # Элементов на страницу
    fetch_forecast_details: bool = False,        # Детальные таргеты по uid
    load_history: bool = True,                   # Выполнять историю MOEX
    board: str = "TQBR",                        # Торговая доска
    recalc_potentials: bool = True,              # Выполнять пересчёт потенциалов
    anomaly_price_threshold: float = 1_000_000,  # Порог аномальной цены консенсуса
) -> Dict[str, Any]:
    start_ts = _now_ts()                         # Начало пайплайна
    t0 = time.time()                             # Числовая отметка
    db_layer.init_schema()                       # Убедиться что таблицы есть
    # Считываем количество перспективных бумаг
    with db_layer.get_connection() as conn:      # Соединение с БД
        cur = conn.execute("SELECT COUNT(*) FROM perspective_shares")
        perspective_count = cur.fetchone()[0]
        cur2 = conn.execute("SELECT uid FROM perspective_shares")
        perspective_uids = [r[0] for r in cur2.fetchall()]
    if perspective_count == 0:                   # Нет данных для работы
        return {
            "status": "empty-perspective",
            "perspective_count": 0,
            "started": start_ts,
            "finished": _now_ts(),
            "duration_sec": round(time.time() - t0, 3),
        }
    # Инициализация отчётных структур
    consensus_stats: Dict[str, Any] = {}
    forecast_detail_stats: Dict[str, Any] = {}
    history_stats: Dict[str, Any] = {}
    potentials_stats: Dict[str, Any] = {}
    anomalies = []                                # Список аномалий цен
    # Шаг 1: Загрузка страниц консенсусов (фильтрация по perspective)
    if not dry_run:
        consensus_stats = forecasts.load_consensus_for_perspective(
            limit_per_page=consensus_page_limit, max_pages=consensus_pages
        )                                         # Вызов функции модуля forecasts
    else:                                        # В режиме dry_run имитируем
        consensus_stats = {
            "inserted": 0, "duplicates": 0, "errors": 0, "matched": perspective_count,
            "pages_processed": 0, "limit_per_page": consensus_page_limit
        }
    # Шаг 2: Детальные прогнозы (опционально)
    if fetch_forecast_details and not dry_run:
        inserted_consensus_detail = 0            # Новые агрегированные прогнозы
        inserted_targets_detail = 0              # Новые таргет-записи
        dup_consensus_detail = 0                 # Дубликаты агрегированных прогнозов
        for uid in perspective_uids:             # Итерируем по каждому uid
            r = forecasts.load_forecast_by_uid(uid)
            cstat = r.get("consensus_save", {})
            tstat = r.get("targets_save", {})
            if cstat.get("status") == "inserted":
                inserted_consensus_detail += 1
            elif cstat.get("status") == "dup":
                dup_consensus_detail += 1
            inserted_targets_detail += tstat.get("inserted", 0)
        forecast_detail_stats = {
            "consensus_inserted": inserted_consensus_detail,
            "consensus_duplicates": dup_consensus_detail,
            "targets_inserted": inserted_targets_detail,
        }
    elif fetch_forecast_details and dry_run:
        forecast_detail_stats = {
            "consensus_inserted": 0,
            "consensus_duplicates": 0,
            "targets_inserted": 0,
            "dry_run": True,
        }
    # Шаг 3: Загрузка истории цен (MOEX)
    if load_history and not dry_run:
        history_stats = history.load_history_bulk(board=board)  # Вызываем обёртку
    elif load_history and dry_run:
        history_stats = {"total_inserted": 0, "per_security": {}, "pruned": {}}
    # Определяем пропуски истории
    if not dry_run:
        summary_hist = history.summarize_latest_trade_dates()
        missing_history = summary_hist.get("missing", 0)
    else:
        missing_history = 0
    # Шаг 4: Пересчёт потенциалов
    if recalc_potentials and not dry_run:
        potentials_stats = potentials.compute_all_potentials()
    elif recalc_potentials and dry_run:
        potentials_stats = {"processed": perspective_count, "inserted": 0, "skipped": perspective_count, "computedAt": _now_ts(), "dry_run": True}
    # Шаг 5: Поиск аномалий (цены консенсуса > порога)
    with db_layer.get_connection() as conn:      # Новое соединение
        cur = conn.execute(
            "SELECT uid, ticker, priceConsensus FROM consensus_forecasts ORDER BY recommendationDate DESC"
        )                                        # Селект всех значений
        for uid, ticker, price in cur.fetchall():
            if price is not None and price > anomaly_price_threshold:
                anomalies.append({"uid": uid, "ticker": ticker, "priceConsensus": price})
    # Итоговый отчёт
    report = {
        "status": "ok",                        # Статус выполнения
        "started": start_ts,                   # Время старта
        "finished": _now_ts(),                 # Время окончания
        "duration_sec": round(time.time() - t0, 3),  # Длительность
        "perspective_count": perspective_count,      # Кол-во бумаг
        "consensus_stats": consensus_stats,          # Статистика консенсусов
        "forecast_detail_stats": forecast_detail_stats,  # Детальные прогнозы
        "history_stats": history_stats,              # История цен
        "potentials_stats": potentials_stats,        # Потенциалы
        "anomalies": anomalies,                      # Аномальные цены
        "missing_history": missing_history,          # Сколько бумаг без истории
        "dry_run": dry_run,                          # Был ли режим имитации
    }
    return report                                  # Возврат отчёта наружу


__all__ = ["run_pipeline"]                        # Экспорт публичной функции

if __name__ == "__main__":                        # Простой тест запуска из CLI
    import json                                    # Форматирование вывода
    rep = run_pipeline(dry_run=True)               # Запуск в имитационном режиме
    print(json.dumps(rep, ensure_ascii=False, indent=2))  # Печать отчёта
