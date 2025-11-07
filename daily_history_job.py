"""Единый ежедневный job по обновлению данных.

Высокоуровневые шаги пайплайна:
    1. История цен MOEX (FillingMoexHistory): дозагружаем последние дни по SECID.
    2. Консенсус / таргеты (FillingConsensusData): обращение к API и сохранение без дублей.
    3. Потенциалы (FillingPotentialData): вычисление относительного потенциала shares_potentials.
    4. Очистка старых потенциалов (CleanOldSharePotentials): ретеншн по возрасту.
    5. Топ потенциалов (GetTopSharePotentials): агрегированный список для логов.
    6. Сжатие дублей потенциалов (CollapseDuplicateSharePotentials) опционально.

CLI использование:
    python daily_history_job.py [--board TQBR] [--skip-consensus] [--skip-history] [--skip-potentials]
                                                            [--retention-days 90] [--top 10] [--collapse-duplicates]

Может быть запланирован через Windows Task Scheduler (schtasks) или cron (WSL).
"""
from __future__ import annotations
import sys
import os
import time
import json
from datetime import datetime, UTC
sys.path.append('d:/#Work/#Invest/Project/src')
from invest_core.moex_history import FillingMoexHistory  # Загрузка/обновление истории MOEX
from invest_core.potentials import (
    FillingPotentialData,             # Расчёт потенциалов для всех перспективных акций
    CleanOldSharePotentials,          # Удаление старых записей по возрасту
    GetTopSharePotentials,            # Получение топ-N актуальных потенциалов
    CollapseDuplicateSharePotentials, # Удаление исторических дублей (почти неизменных rel)
)  # noqa: E402
from invest_core.forecasts import FillingConsensusData  # noqa: E402
from invest_core import db_mysql as db_layer  # noqa: E402  # Переход на новый MySQL слой

LOCK_FILE = 'daily_history_job.lock'
LOG_FILE = 'daily_history_job.log'
try:
    from invest_core.config_loader import cfg_val  # Загружаем BOARD из config.ini если есть
    BOARD = cfg_val('job', 'board', os.getenv('INVEST_MOEX_BOARD', 'TQBR'))
    _CFG_RETENTION = cfg_val('job', 'retention_days', 90)
    _CFG_TOP_LIMIT = cfg_val('job', 'TopLimit', None)
    _CFG_COLLAPSE = cfg_val('job', 'collapse_duplicates', 0)
except Exception:  # noqa
    BOARD = os.getenv('INVEST_MOEX_BOARD', 'TQBR')
    _CFG_RETENTION = 90
    _CFG_TOP_LIMIT = None
    _CFG_COLLAPSE = 0


def _acquire_lock() -> bool:
    """Создать lock-файл чтобы предотвратить параллельный запуск.

    Правило: если lock существует и моложе 2 часов – считаем что предыдущий job ещё выполняется.
    Возвращает True если lock установлен, False если запуск нужно пропустить.
    """
    if os.path.exists(LOCK_FILE):
        age_sec = time.time() - os.path.getmtime(LOCK_FILE)  # возраст lock в секундах
        if age_sec < 2 * 3600:  # порог 2 часа
            return False
    with open(LOCK_FILE, 'w', encoding='utf-8') as f:
        f.write(f'Start {datetime.now(UTC).isoformat()}')  # фиксируем момент запуска
    return True


def _release_lock():
    """Удалить lock-файл; игнорировать ошибки (файл мог быть удалён руками)."""
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


def _append_log(line: str):
    """Добавить строку в лог с UTC timestamp.

    Формат: [ISO8601] текст
    """
    ts = datetime.now(UTC).isoformat()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f'[{ts}] {line}\n')


def run(
    board: str = BOARD,
    *,
    skip_history: bool = False,
    skip_consensus: bool = False,
    skip_potentials: bool = False,
    retention_days: int = 90,
    top_limit: int | None = None,
    skip_null_potentials: bool = True,
    collapse_duplicates: bool = False,
    consensus_limit: int | None = None,
):
    """Запуск единого процесса обновления.

    Args:
        board: Код торговой доски (напр. 'TQBR').
        skip_history: Пропустить догрузку истории.
        skip_consensus: Пропустить загрузку консенсусов.
        skip_potentials: Пропустить расчёт потенциалов.
        retention_days: Возраст для очистки старых потенциалов (shares_potentials).
        top_limit: Если задано – вывести топ-N потенциалов в лог.
        skip_null_potentials: Не вставлять строки без рассчитанного относительного потенциала.
    """
    # 1. Защита от параллельных запусков
    if not _acquire_lock():
        _append_log('Skip: lock active')
        return  # ранний выход
    start = time.time()
    try:
        db_layer.init_schema()
        res_hist = {"status": "skipped"}
        res_cons = {"status": "skipped"}
        res_pot = {"status": "skipped"}
        res_ret = {"status": "skipped"}
        res_top = {"status": "skipped"}
        res_collapse = {"status": "skipped"}

        # 2. История MOEX (опционально)
        if not skip_history:
            res_hist = FillingMoexHistory(board=board)
        # 3. Консенсус и таргеты (опционально)
        if not skip_consensus:
            # limit=None => все перспективные; sleep_sec=0 для ускорения
            res_cons = FillingConsensusData(limit=consensus_limit, sleep_sec=0.0)
        # 4. Потенциалы и сопутствующие операции
        if not skip_potentials:
            res_pot = FillingPotentialData(skip_null=skip_null_potentials)
            # 4a. Очистка старых (ретеншн) – если задан положительный срок
            if retention_days and retention_days > 0:
                res_ret = CleanOldSharePotentials(max_age_days=retention_days)
            # 4b. Сжатие дублей потенциалов (почти неизменное rel)
            if collapse_duplicates:
                res_collapse = CollapseDuplicateSharePotentials()
            # 4c. Топ-N потенциалов
            if top_limit and top_limit > 0:
                res_top = GetTopSharePotentials(limit=top_limit)
        dur = round(time.time() - start, 3)
        # Текстовая строка (обратная совместимость)
        _append_log(
            'DailyUnified board={board} hist_status={hstatus} hist_fetched={hfetched} hist_inserted={hins} hist_duplicates={hdup} '
            'cons_processed={cproc} cons_cins={cins} cons_cdup={cdup} targets_inserted={tins} targets_dups={tdup} '
            'pot_processed={pproc} pot_inserted={pins} pot_skipped={pskip} pot_unchanged={punch} retention_deleted={rdel} collapse_deleted={cdel} top_rows={trows} duration={dur}s'.format(
                board=board,
                hstatus=res_hist.get('status'),
                hfetched=res_hist.get('fetched'),
                hins=res_hist.get('inserted'),
                hdup=res_hist.get('duplicates'),
                cproc=res_cons.get('processed'),
                cins=res_cons.get('consensus_inserted'),
                cdup=res_cons.get('consensus_duplicates'),
                tins=res_cons.get('targets_inserted'),
                tdup=res_cons.get('targets_duplicates'),
                pproc=res_pot.get('processed'),
                pins=res_pot.get('inserted'),
                pskip=res_pot.get('skipped'),
                punch=res_pot.get('unchanged'),
                rdel=res_ret.get('deleted'),
                cdel=res_collapse.get('deleted'),
                trows=(res_top.get('rows') if isinstance(res_top.get('rows'), int) else None),
                dur=dur,
            )
        )  # Текстовая строка для старых парсеров логов
        # Сбор текущих counts по ключевым таблицам
        table_counts = {}
        try:
            # Используем единый MySQL слой (db_mysql) вместо устаревшего db
            from invest_core import db_mysql as _db_layer
            with _db_layer.get_connection() as _c:
                for _t in ['perspective_shares','moex_shares_history','consensus_forecasts','consensus_targets','shares_potentials']:
                    try:
                        cur_cnt = _db_layer.exec_sql(_c, f'SELECT COUNT(*) FROM {_t}')
                        table_counts[_t] = cur_cnt.fetchone()[0]
                    except Exception as ex_cnt:  # noqa
                        table_counts[_t] = f'error:{ex_cnt}'
        except Exception as ex_outer:  # noqa
            table_counts['error'] = str(ex_outer)
        # Формирование JSON summary
        summary = {  # Структурированная JSON сводка
            'type': 'DailyUnified',
            'board': board,
            'duration_sec': dur,
            'history': {
                'status': res_hist.get('status'),
                'fetched': res_hist.get('fetched'),
                'inserted': res_hist.get('inserted'),
                'duplicates': res_hist.get('duplicates'),
            },
            'consensus': {
                'processed': res_cons.get('processed'),
                'inserted': res_cons.get('consensus_inserted'),
                'duplicates': res_cons.get('consensus_duplicates'),
                'targets_inserted': res_cons.get('targets_inserted'),
                'targets_duplicates': res_cons.get('targets_duplicates'),
            },
            'potentials': {
                'processed': res_pot.get('processed'),
                'inserted': res_pot.get('inserted'),
                'skipped': res_pot.get('skipped'),
                'unchanged': res_pot.get('unchanged'),
            },
            'retention': res_ret,
            'collapse': res_collapse,
            'top': res_top.get('data'),
            'table_counts': table_counts,
        }
        _append_log('DailyUnifiedJSON ' + json.dumps(summary, ensure_ascii=False))
        if res_top.get('data'):
            for rec in res_top['data']:  # Пошаговая печать топа для удобного grep
                _append_log('TOP uid={uid} ticker={ticker} rel={rel:.4f} prevClose={pc} consensus={cons}'.format(
                    uid=rec['uid'], ticker=rec['ticker'], rel=rec['pricePotentialRel'], pc=rec['prevClose'], cons=rec['consensusPrice']
                ))
    except Exception as ex:  # noqa
        _append_log(f'ERROR: {ex}')
    finally:
        _release_lock()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Единый ежедневный job обновления данных')
    parser.add_argument('--board', default=BOARD, help=f'MOEX board (default from config/job: {BOARD})')
    parser.add_argument('--skip-history', action='store_true', help='Пропустить загрузку истории MOEX')
    parser.add_argument('--skip-consensus', action='store_true', help='Пропустить загрузку консенсусов')
    parser.add_argument('--skip-potentials', action='store_true', help='Пропустить расчёт потенциалов')
    parser.add_argument('--retention-days', type=int, default=_CFG_RETENTION or 1100, help='Очистить потенциалы старше этого количества дней (0=нет, config/job override)')
    parser.add_argument('--top', type=int, default=_CFG_TOP_LIMIT, help='Вывести топ-N потенциалов (config/job TopLimit override)')
    parser.add_argument('--no-skip-null', action='store_true', help='Вставлять строки потенциалов с NULL rel')
    default_collapse = bool(int(_CFG_COLLAPSE)) if isinstance(_CFG_COLLAPSE, int) else False
    parser.add_argument('--collapse-duplicates', action='store_true', default=default_collapse, help='Удалить исторические дубли неизменного потенциала (config/job collapse_duplicates override)')
    parser.add_argument('--consensus-limit', type=int, default=None, help='Ограничить число UID для обхода консенсусов (для ускоренных тестовых запусков)')
    args = parser.parse_args()
    run(
        board=args.board,
        skip_history=args.skip_history,
        skip_consensus=args.skip_consensus,
        skip_potentials=args.skip_potentials,
        retention_days=args.retention_days,
        top_limit=args.top,
        skip_null_potentials=(not args.no_skip_null),
        collapse_duplicates=args.collapse_duplicates,
        consensus_limit=args.consensus_limit,
    )
