"""scheduler.py

Вспомогательные функции для планового запуска обновлений.
 - Генерация .bat файла для Windows Task Scheduler
 - Простой внутренний цикл (опционально)
"""
from __future__ import annotations

import time
from pathlib import Path
import logging

BAT_TEMPLATE = r"""@echo off
REM Автоматический запуск полного обновления
cd /d {project_dir}
python full_refresh.py >> refresh.log 2>&1
"""


def write_task_bat(path: str = "run_full_refresh.bat", project_dir: str | None = None) -> str:
    project_dir = project_dir or str(Path(__file__).resolve().parent.parent)
    content = BAT_TEMPLATE.format(project_dir=project_dir)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    logging.info("Создан .bat для планировщика: %s", path)
    return path


def loop(interval_sec: int = 3600) -> None:
    """Простой цикл периодического запуска full_refresh без внешнего планировщика."""
    import subprocess, sys
    logging.info("Старт внутреннего цикла. Интервал %s сек", interval_sec)
    while True:
        start = time.time()
        logging.info("Цикл refresh запускается...")
        proc = subprocess.run([sys.executable, "full_refresh.py"], capture_output=True, text=True)
        if proc.returncode != 0:
            logging.error("full_refresh завершился с кодом %s: %s", proc.returncode, proc.stderr[:400])
        else:
            logging.info("full_refresh OK (%s bytes stdout)", len(proc.stdout))
        elapsed = time.time() - start
        sleep_for = max(0, interval_sec - elapsed)
        logging.info("Ожидание %.1f сек до следующего запуска", sleep_for)
        time.sleep(sleep_for)
