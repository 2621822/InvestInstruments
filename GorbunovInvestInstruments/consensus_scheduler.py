"""Простой планировщик периодического обновления/дозагрузки консенсус-прогнозов.

Использование (PowerShell):

  python GorbunovInvestInstruments/consensus_scheduler.py --interval 1800 --mode update

Режимы:
  update  - вызывает UpdateConsensusForecasts (полное обновление + чистка)
  ensure  - вызывает EnsureForecastsForMissingShares (только для отсутствующих прогнозов)
  both    - сначала ensure, затем update

Остановка: Ctrl + C

Переменные окружения могут управлять лимитами, токеном и т.п. (та же семантика, что в main.py)
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path

from . import main as main_module  # переиспользуем функции из main.py
from .main import (
    DB_PATH,
    TOKEN,
    UpdateConsensusForecasts,
    EnsureForecastsForMissingShares,
    MAX_CONSENSUS_PER_UID,
    MAX_TARGETS_PER_ANALYST,
    MAX_HISTORY_DAYS,
    setup_logging,
)

stop_flag = False


def _handle_signal(signum, frame):  # type: ignore[override]
    global stop_flag
    logging.info("Получен сигнал %s — завершаем цикл после текущей итерации", signum)
    stop_flag = True


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Планировщик периодического обновления консенсус-прогнозов")
    p.add_argument("--interval", type=int, default=1800, help="Интервал между итерациями (сек). По умолчанию 1800 (30 мин)")
    p.add_argument("--mode", choices=["update", "ensure", "both"], default="update", help="Режим работы планировщика")
    p.add_argument("--once", action="store_true", help="Выполнить только одну итерацию и выйти")
    p.add_argument("--max-consensus", type=int, help="override CONSENSUS_MAX_PER_UID для update режима")
    p.add_argument("--max-targets-per-analyst", type=int, help="override CONSENSUS_MAX_TARGETS_PER_ANALYST для update режима")
    p.add_argument("--max-history-days", type=int, help="override CONSENSUS_MAX_HISTORY_DAYS для update режима")
    return p.parse_args()


def run_iteration(mode: str, *, max_consensus: int | None, max_targets: int | None, max_history_days: int | None) -> None:
    if mode in {"ensure", "both"}:
        EnsureForecastsForMissingShares(DB_PATH, TOKEN, prune=True)
    if mode in {"update", "both"}:
        UpdateConsensusForecasts(
            DB_PATH,
            TOKEN,
            uid=None,
            max_consensus=max_consensus,
            max_targets_per_analyst=max_targets,
            max_history_days=max_history_days,
        )


def main() -> None:
    setup_logging()
    args = parse_args()

    # Перехватываем сигналы для корректной остановки
    for sig in (signal.SIGINT, signal.SIGTERM):  # type: ignore[attr-defined]
        try:
            signal.signal(sig, _handle_signal)
        except Exception:  # платформа может не поддерживать
            pass

    logging.info("Старт планировщика: mode=%s interval=%s", args.mode, args.interval)

    iteration = 0
    while True:
        iteration += 1
        started = time.time()
        try:
            run_iteration(
                args.mode,
                max_consensus=getattr(args, "max_consensus", None),
                max_targets=getattr(args, "max_targets_per_analyst", None),
                max_history_days=getattr(args, "max_history_days", None),
            )
        except Exception as exc:  # noqa: BLE001
            logging.exception("Ошибка в итерации планировщика: %s", exc)
        duration = time.time() - started
        logging.info("Итерация #%s завершена за %.2fс", iteration, duration)

        if args.once or stop_flag:
            logging.info("Остановка планировщика (once=%s stop_flag=%s)", args.once, stop_flag)
            break

        sleep_left = max(0, args.interval - duration)
        if sleep_left:
            logging.debug("Пауза %.2fс до следующей итерации", sleep_left)
            time.sleep(sleep_left)


if __name__ == "__main__":  # pragma: no cover
    main()
