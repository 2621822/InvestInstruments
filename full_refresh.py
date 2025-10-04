"""Полный цикл обновления данных.

Шаги:
 1. Загрузка / актуализация котировок всех перспективных бумаг (полное покрытие + инкремент на сегодня)
 2. Загрузка / актуализация консенсус‑прогнозов (если есть токен)
 3. Загрузка / актуализация прогнозов аналитиков (targets) (если есть токен)
 4. Пересчёт потенциалов по всем бумагам
 5. Экспорт потенциалов в Excel (умная таблица) и JSON (опционально)

Переменные окружения (опционально):
  TINKOFF_INVEST_TOKEN — токен Tinkoff Invest API
    PRICE_LIMIT_INSTRUMENTS=N  — ограничить число обрабатываемых инструментов при загрузке цен
    PRICE_GLOBAL_TIMEOUT_SEC=NN — прервать загрузку цен после N секунд (частичный результат сохраняется)
  FULL_REFRESH_EXPORT_EXCEL=1  — включить экспорт Excel (по умолчанию включено)
  FULL_REFRESH_EXPORT_JSON=1   — экспорт JSON (по умолчанию выключено)
  FULL_REFRESH_EXCEL_NAME=potentials_export.xlsx
  FULL_REFRESH_JSON_NAME=potentials_export.json

Запуск:
  python full_refresh.py

Для планировщика Windows: создайте задачу, запускающую бат‑файл с активацией venv и вызовом этого скрипта.
"""
from __future__ import annotations

import os
import logging
from pathlib import Path

from GorbunovInvestInstruments import data_prices as hist
from GorbunovInvestInstruments import data_forecasts as forecasts
from GorbunovInvestInstruments import potentials
try:
    from GorbunovInvestInstruments.exporting import export_potentials  # новый модуль централизованного экспорта
except Exception:  # noqa: BLE001
    export_potentials = None  # type: ignore

DB_PATH = Path("GorbunovInvestInstruments.db")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def refresh_all() -> dict:
    """Выполнить полный цикл обновления и вернуть агрегированные метрики."""
    summary: dict[str, object] = {}

    logging.info("[1/5] Проверка полного покрытия и догрузка недостающих котировок (ensure_full_coverage)...")
    cov = hist.ensure_full_coverage()
    summary["full_coverage"] = {
        "всего_перспективных": cov.get("total_perspective"),
        "отсутствовало_до": cov.get("missing_before"),
        "загружено_новых": cov.get("processed_missing"),
        "покрытие_после": cov.get("coverage_after"),
    }

    logging.info("[2/5] Ежедневная инкрементальная догрузка котировок (daily_update_all)...")
    daily = hist.daily_update_all(recompute_potentials=False)
    summary["daily_update"] = {
        "добавлено_строк": daily.get("total_inserted"),
        "удалено_старых": daily.get("total_deleted_old"),
        "http": daily.get("http_requests"),
        "повторы": daily.get("retries"),
    }

    # Вспомогательная функция маскировки токена (для логов): показываем первые 4 и последние 2 символа.
    def _mask_token(t: str) -> str:
        if not t:
            return ""
        if len(t) <= 8:
            return t[0] + "***" + t[-1]
        return t[:4] + "..." + t[-2:]

    # 1) Токен из переменной окружения имеет приоритет.
    # 2) Если не задан — пытаемся прочитать первый непустой (и не комментарий) рядок из tinkoff_token.txt
    token_source = None
    token = os.getenv("TINKOFF_INVEST_TOKEN", "").strip()
    if token:
        token_source = "env"
    if not token:
        token_path = Path("tinkoff_token.txt")
        if token_path.exists():
            try:
                with token_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        token = line
                        token_source = "tinkoff_token.txt"
                        break
                if not token:
                    logging.warning("Файл tinkoff_token.txt найден, но валидных (непустых) строк не обнаружено — шаг прогнозов будет пропущен.")
            except Exception as exc:  # noqa: BLE001
                logging.warning("Не удалось прочитать tinkoff_token.txt: %s", exc)
    if token:
        logging.info("[3/5] Обнаружен токен для прогнозов (source=%s, masked=%s)", token_source, _mask_token(token))
        logging.info("[3/5] Загрузка / актуализация консенсус‑прогнозов и целей аналитиков...")
        try:
            forecasts.EnsureForecastsForMissingShares(DB_PATH, token, prune=True)
            summary["forecasts"] = {"статус": "обновление выполнено (только отсутствующие)"}
        except Exception as exc:  # noqa: BLE001
            logging.warning("Ошибка загрузки прогнозов: %s", exc)
            summary["forecasts_error"] = str(exc)
    else:
        logging.info("[3/5] Токен TINKOFF_INVEST_TOKEN не задан — пропуск шага прогнозов.")

    logging.info("[4/5] Пересчёт потенциалов...")
    try:
        potentials.compute_all(store=True)
        summary["potentials"] = {"статус": "пересчитано"}
    except Exception as exc:  # noqa: BLE001
        logging.error("Ошибка пересчёта потенциалов: %s", exc)
        summary["potentials_error"] = str(exc)

    do_excel = os.getenv("FULL_REFRESH_EXPORT_EXCEL", "1") in {"1", "true", "True", "YES", "yes"}
    do_json = os.getenv("FULL_REFRESH_EXPORT_JSON", "0") in {"1", "true", "True", "YES", "yes"}
    excel_name = os.getenv("FULL_REFRESH_EXCEL_NAME", "potentials_export.xlsx")
    json_name = os.getenv("FULL_REFRESH_JSON_NAME", "potentials_export.json") if do_json else None
    if do_excel and export_potentials:
        logging.info("[5/5] Экспорт потенциалов (%s%s)...", excel_name, f", {json_name}" if json_name else "")
        try:
            exp = export_potentials(excel_name, json_name)
            summary["export"] = exp
        except Exception as exc:  # noqa: BLE001
            logging.warning("Ошибка экспорта потенциалов: %s", exc)
            summary["export_error"] = str(exc)
    else:
        logging.info("[5/5] Экспорт потенциалов отключён или модуль не доступен.")

    logging.info("Полный цикл завершён.")
    return summary


if __name__ == "__main__":  # pragma: no cover
    _setup_logging()
    result = refresh_all()
    # Краткое резюме в консоль
    print("Итог:")
    for k, v in result.items():
        print(f" - {k}: {v}")
