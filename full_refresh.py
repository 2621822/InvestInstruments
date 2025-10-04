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
import sqlite3
import time
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


def _prune_orphans(board: str) -> dict[str, int]:
    """Удалить 'осиротевшие' записи цен (те SECID, которых больше нет в perspective_shares).

    Возвращает метрики: сколько найдено и сколько строк удалено.
    Выполняется только если включено переменной окружения FULL_REFRESH_PRUNE_ORPHANS.
    """
    stats = {"orphans_before": 0, "rows_deleted": 0, "orphans_list": []}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("SELECT UPPER(secid) FROM perspective_shares WHERE secid IS NOT NULL AND TRIM(secid)<>''")
            perspective = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT DISTINCT SECID FROM moex_history_perspective_shares WHERE BOARDID=?", (board,))
            in_history = { (r[0] or "").upper() for r in cur.fetchall() if r[0] }
            orphans = [o for o in in_history if o not in perspective]
            stats["orphans_before"] = len(orphans)
            stats["orphans_list"] = sorted(orphans)
            deleted_total = 0
            for sec in orphans:
                cur.execute("DELETE FROM moex_history_perspective_shares WHERE BOARDID=? AND SECID=?", (board, sec))
                deleted_total += cur.rowcount or 0
            conn.commit()
            stats["rows_deleted"] = deleted_total
            for sec in orphans:
                logging.info("Удалён лишний SECID (нет в perspective_shares): %s", sec)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Не удалось выполнить очистку осиротевших бумаг: %s", exc)
    return stats


def _human_readable_report(summary: dict) -> str:
    """Сформировать человекочитаемый многострочный отчёт (вариант 1)."""
    lines: list[str] = []
    fc = summary.get("full_coverage", {}) or {}
    du = summary.get("daily_update", {}) or {}
    forecasts_part = summary.get("forecasts") or summary.get("forecasts_error")  # noqa: F841 (оставлено для возможного будущего использования)
    pot_part = summary.get("potentials") or summary.get("potentials_error")  # noqa: F841
    exp_part = summary.get("export") or summary.get("export_error")
    orphans = summary.get("orphans") or {}

    total_persp = fc.get("всего_перспективных") or 0
    coverage_after = fc.get("покрытие_после") or 0
    extra = None
    try:
        extra = int(coverage_after) - int(total_persp)
    except Exception:
        extra = None
    percent_cov = None
    try:
        percent_cov = f"{(int(total_persp) / int(total_persp) * 100):.0f}%" if total_persp else "0%"
    except Exception:
        percent_cov = "—"

    lines.append("Итоги выполнения:\n")
    lines.append("Перспективные бумаги:")
    lines.append(f"  В списке сейчас: {total_persp}")
    lines.append(f"  Покрытие до: {total_persp - (fc.get('отсутствовало_до') or 0)} (отсутствовало: {fc.get('отсутствовало_до')})")
    lines.append(f"  Новых добавлено: {fc.get('загружено_новых')}")
    if extra and extra > 0:
        lines.append(f"  Покрытие после: {coverage_after} (лишних: {extra})")
    else:
        lines.append(f"  Покрытие после: {coverage_after}")
    lines.append(f"  Процент покрытия: {percent_cov}")
    if orphans and orphans.get("orphans_before"):
        lines.append(f"  Осиротевших до очистки: {orphans.get('orphans_before')} (удалено строк: {orphans.get('rows_deleted')})")
        if orphans.get("orphans_list"):
            shown = ",".join(orphans.get("orphans_list")[:10])
            lines.append(f"  Удалены SECID: {shown}{' ...' if len(orphans.get('orphans_list'))>10 else ''}")

    lines.append("")
    lines.append("Ежедневное обновление цен:")
    lines.append(f"  Новых строк: {du.get('добавлено_строк')}")
    lines.append(f"  Удалено старых: {du.get('удалено_старых')}")
    lines.append(f"  HTTP-запросов: {du.get('http')}")
    lines.append(f"  Повторов: {du.get('повторы')}")

    lines.append("")
    lines.append("Прогнозы:")
    if summary.get("forecasts_error"):
        lines.append(f"  Ошибка: {summary['forecasts_error']}")
    else:
        lines.append("  Режим: только отсутствующие")
        lines.append("  Статус: OK")
        fobj = summary.get("forecasts", {}) or {}
        if isinstance(fobj, dict):
            lines.append(
                "  UID с новыми данными: "
                f"{fobj.get('added', 0)} (forecasts_new={fobj.get('forecast_new', 0)}, targets_new={fobj.get('target_new', 0)})"
            )
            details = fobj.get("details") or []
            if details:
                lines.append("  Детализация новых (ticker rec consensus targets+):")
                # ограничим вывод первыми 25 строками
                shown = 0
                for d in details:
                    if not d.get("forecast_added") and not d.get("targets_added"):
                        continue
                    if shown >= 25:
                        lines.append("    ... (ещё скрыто)")
                        break
                    rec = d.get("recommendation") or "?"
                    cp = d.get("consensus_price")
                    cp_str = f"{cp:.2f}" if isinstance(cp, (int, float)) else "—"
                    lines.append(
                        f"    {d.get('ticker')} {rec} {cp_str} {d.get('targets_added')}"
                    )
                    shown += 1

    lines.append("")
    lines.append("Потенциалы:")
    if summary.get("potentials_error"):
        lines.append(f"  Ошибка: {summary['potentials_error']}")
    else:
        pobj = summary.get("potentials", {}) or {}
        lines.append("  Статус: пересчитано")
        if isinstance(pobj, dict):
            if pobj.get("count") is not None:
                lines.append(f"  Всего записей: {pobj.get('count')}")
            chf = pobj.get("changed_by_forecast") or []
            if chf:
                lines.append(f"  Затронуты новыми прогнозами/таргетами: {len(chf)} uid")
        top_list = summary.get("potentials_top") or []
        if top_list:
            lines.append("  ТОП по потенциалу (ticker %):")
            for p in top_list[:15]:
                lines.append(f"    {p['ticker']} {p['potential_pct']}")

    lines.append("")
    lines.append("Экспорт:")
    if isinstance(exp_part, dict) and exp_part.get("excel"):
        if exp_part.get("status") == "fallback":
            lines.append(f"  Основной файл был заблокирован, сохранено во временный: {exp_part.get('excel_fallback')}")
        else:
            lines.append(f"  Статус: OK (файл: {exp_part.get('excel')})")
        if exp_part.get("json"):
            lines.append(f"  JSON: {exp_part.get('json')}")
    else:
        # строковое сообщение об ошибке
        if isinstance(exp_part, str):
            lines.append(f"  Ошибка: {exp_part}")
        else:
            lines.append("  Экспорт отключён или не выполнялся")

    # Сводка по этапам
    stages = [
        ("full_coverage", summary.get("full_coverage"), summary.get("full_coverage") is not None),
        ("daily_update", summary.get("daily_update"), summary.get("daily_update") is not None),
        ("forecasts", summary.get("forecasts"), summary.get("forecasts_error") is None),
        ("potentials", summary.get("potentials"), summary.get("potentials_error") is None),
        ("export", summary.get("export"), summary.get("export_error") is None),
    ]
    total = len(stages)
    ok = sum(1 for _n, _obj, good in stages if good)
    lines.append("")
    lines.append(f"Сводка: успешных этапов {ok} из {total}")

    return "\n".join(lines)


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
    # Автоматическое удаление осиротевших (если обнаружены лишние SECID) сразу после покрытия
    try:
        total_persp = summary["full_coverage"]["всего_перспективных"] or 0
        coverage_after = summary["full_coverage"]["покрытие_после"] or 0
        board = cov.get("board", "TQBR") if isinstance(cov, dict) else "TQBR"
        if coverage_after and total_persp and coverage_after > total_persp:
            logging.info("Обнаружены лишние SECID в истории (coverage_after=%s > перспективных=%s) — выполняю очистку сразу.", coverage_after, total_persp)
            orphan_stats = _prune_orphans(board)
            summary["orphans"] = orphan_stats
            # Пересчитать coverage_after (новое значение после удаления)
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(DISTINCT SECID) FROM moex_history_perspective_shares WHERE BOARDID=?", (board,))
                new_cov = cur.fetchone()[0] or 0
            summary["full_coverage"]["покрытие_после"] = new_cov
    except Exception as exc:  # noqa: BLE001
        logging.warning("Не удалось автоматически пересчитать покрытие после очистки: %s", exc)

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
    forecast_stats = None
    if token:
        logging.info("[3/5] Обнаружен токен для прогнозов (source=%s, masked=%s)", token_source, _mask_token(token))
        logging.info("[3/5] Загрузка / актуализация консенсус‑прогнозов и целей аналитиков...")
        try:
            forecast_stats = forecasts.EnsureForecastsForMissingShares(DB_PATH, token, prune=True)
            summary["forecasts"] = {"статус": "обновление выполнено (только отсутствующие)", **forecast_stats}
        except Exception as exc:  # noqa: BLE001
            logging.warning("Ошибка загрузки прогнозов: %s", exc)
            summary["forecasts_error"] = str(exc)
    else:
        logging.info("[3/5] Токен TINKOFF_INVEST_TOKEN не задан — пропуск шага прогнозов.")

    # (Сохранено: ручная очистка по переменной окружения — вдруг пользователь хочет принудительно)
    board = cov.get("board", "TQBR") if isinstance(cov, dict) else "TQBR"
    if os.getenv("FULL_REFRESH_PRUNE_ORPHANS", "0").lower() in {"1", "true", "yes"} and "orphans" not in summary:
        logging.info("Очистка осиротевших бумаг (FULL_REFRESH_PRUNE_ORPHANS=1)...")
        summary["orphans"] = _prune_orphans(board)

    logging.info("[4/5] Пересчёт потенциалов...")
    try:
        pot_entries = potentials.compute_all(store=True)
        changed_uids = set()
        if forecast_stats:
            changed_uids.update(forecast_stats.get("forecast_uids", []))
            changed_uids.update(forecast_stats.get("target_uids", []))
        top_sorted = [p for p in pot_entries if p.get("pricePotentialRel") is not None]
        top_sorted.sort(key=lambda x: x.get("pricePotentialRel") or 0, reverse=True)
        summary["potentials"] = {"статус": "пересчитано", "count": len(pot_entries), "changed_by_forecast": sorted(changed_uids)}
        summary["potentials_top"] = [
            {"ticker": p["ticker"], "potential_pct": f"{(p['pricePotentialRel']*100):.0f}%"} for p in top_sorted
        ]
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
        except PermissionError as exc:
            logging.warning("PermissionError при сохранении '%s': %s", excel_name, exc)
            # fallback имя
            fallback_name = f"{Path(excel_name).stem}_{time.strftime('%Y%m%d_%H%M%S')}_lock.xlsx"
            try:
                exp_fb = export_potentials(fallback_name, json_name)
                exp_fb["status"] = "fallback"
                exp_fb["excel_fallback"] = fallback_name
                exp_fb["original_excel"] = excel_name
                summary["export"] = exp_fb
                logging.info("Экспорт сохранён во временный файл: %s", fallback_name)
            except Exception as exc2:  # noqa: BLE001
                summary["export_error"] = f"PermissionError: {exc}; fallback_fail: {exc2}"
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
    print(_human_readable_report(result))
