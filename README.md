<div align="center">

# InvestInstruments

Модульный инструментальный набор для:

* Загрузки исторических цен с MOEX ISS
* Получения консенсус‑прогнозов и индивидуальных таргетов (InstrumentsService/GetForecastBy)
* Расчёта ценовых потенциалов на основе консенсуса
* Сервисных операций очистки и повторной загрузки

</div>

## Архитектура модулей

```
src/invest_core/
  sdk_client.py      # Контекстные менеджеры sync/async клиента SDK, базовые вызовы
  instruments.py     # Поиск и сохранение инструментов в perspective_shares
  forecasts.py       # Получение прогнозов (GetForecastBy) и сохранение по спецификации
  history.py         # Обёртка над moex_history для массовой загрузки истории
  moex_history.py    # Низкоуровневый клиент MOEX ISS (пагинация, вставка)
  potentials.py      # Расчёт ценовых потенциалов (консенсус vs последняя цена)
  pipeline.py        # Оркестратор полного цикла (консенсус → история → потенциалы → отчёт)
  maintenance.py     # Очистка аномалий, повторный per‑uid прогноз, пересчёт потенциалов
  db.py              # Схема и соединения (SQLite / DuckDB)
  tinkoff_api.py     # DEPRECATED заглушка (исторический REST код удалён)
  tinkoff_sdk.py     # DEPRECATED заглушка (заменён sdk_client/forecasts)
  hello.py           # Простой тест запуска
```

### Основные таблицы

| Таблица | Назначение | Ключ |
|---------|------------|------|
| perspective_shares | Список интересующих инструментов | uid PRIMARY KEY |
| moex_shares_history | Исторические котировки MOEX | (SECID, TRADEDATE) |
| consensus_forecasts | История агрегированных консенсусов | (uid, recommendationDate) |
| consensus_targets | Индивидуальные таргеты аналитиков | id AUTOINCREMENT (ранее (uid, recommendationDate, company)) |
| instrument_potentials | История расчётов потенциала | (uid, computedAt) |

## Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| INVEST_TINKOFF_TOKEN / INVEST_TOKEN | (нет) | Токен доступа к Tinkoff Invest SDK |
| INVEST_DB_BACKEND | sqlite | Хранилище: sqlite или duckdb |
| INVEST_DB_FILE | invest_data.db | Имя файла БД |
| INVEST_MOEX_TIMEOUT_SEC | 10 | Таймаут HTTP MOEX запросов (сек) |
| INVEST_MOEX_SLEEP_SEC | 0 | Пауза между страницами MOEX (анти rate‑limit) |
| INVEST_HISTORY_RETENTION_DAYS | 1100 | Срок хранения исторических данных |
| INVEST_TINKOFF_VERIFY_SSL | 1 | Проверка SSL (legacy, сейчас REST отключён) |

## Быстрая установка

```powershell
python -m venv invest
./invest/Scripts/Activate.ps1
pip install -r requirements.txt
```

## Первичная инициализация схемы

```powershell
python db_init.py
```

## Поиск и вставка инструмента (пример)

```python
from invest_core.instruments import ensure_perspective_share
ensure_perspective_share("SBER")  # добавит, если отсутствует
```

## Загрузка истории цен

```python
from invest_core.history import load_history_bulk
load_history_bulk(board="TQBR")
```

## Прогнозы: минимальный API

```python
from invest_core.forecasts import FillingConsensusData
stats = FillingConsensusData(sleep_sec=0.2)
print(stats)
```

Функция `FillingConsensusData` выполняет:

1. Чтение всех `uid` из таблицы `perspective_shares`.
2. Для каждого `uid` POST запрос к `InstrumentsService/GetForecastBy`.
3. Сохранение блока `consensus` через `AddConsensusForecasts` (с текущей датой).
4. Сохранение каждого элемента массива `targets` через `AddConsensusTargets` с оригинальной датой `recommendationDate`.
5. Пропуск дублей с сообщениями в консоль.

Ручные вызовы (пример для одного инструмента):

```python
from invest_core.forecasts import GetConsensusByUid, AddConsensusForecasts, AddConsensusTargets
data = GetConsensusByUid('<uid>')
if data:
  c = data.get('consensus') or {}
  AddConsensusForecasts(c.get('uid'), c.get('ticker'), c.get('recommendation'), '2025-10-30', c.get('currency'), c.get('consensus'), c.get('minTarget'), c.get('maxTarget'))
  for t in data.get('targets', []):
    AddConsensusTargets(t.get('uid') or c.get('uid'), t.get('ticker') or c.get('ticker'), t.get('company'), t.get('recommendation'), t.get('recommendationDate'), t.get('currency'), t.get('targetPrice'), t.get('showName'))
```

Правила дублей:
* Consensus: сравнение всех полей с последней записью по `uid` (дата не учитывается для сравнения содержимого, всегда вставляем при изменении значений).
* Targets: поиск существующей строки по `(uid, recommendationDate, company)` и затем сравнение всех полей; при полном совпадении пропуск.

### Кэширование на время запуска

Запросы `GetForecastBy` кэшируются в памяти процесса:

* При первом обращении к `GetConsensusByUid(uid)` результат ответа сохраняется в `_RUN_CACHE`.
* Повторные вызовы для того же `uid` в рамках одного запуска возвращают уже полученный ответ без повторного HTTP запроса.
* Принудительный обход кэша: `GetConsensusByUid(uid, refresh=True)`.
* Сброс всего кэша: `ResetForecastCache()`.

Цель: избежать повторных сетевых запросов при повторных попытках сохранения данных (например, при конфликте уникальности и повторном проходе).

### Авторизация

Для вызова `GetForecastBy` требуется передача Bearer токена.
Источник токена выбирается в порядке приоритета:

1. Переменная окружения `INVEST_TINKOFF_TOKEN` (или альтернативно `INVEST_TOKEN`).
2. Файл `tinkoff_token.txt` (или `token.txt`) в рабочем каталоге.

Если токен найден – добавляется заголовок `Authorization: Bearer <token>`.

Коды ошибок:
* 401 / 403 → модуль возвращает `{"status": "auth_error", "code": <code>, "uid": <uid>, "message": "Unauthorized or forbidden"}` и `FillingConsensusData` прекращает дальнейший обход (поле `auth_failed: true` в статистике).
* Любой другой HTTP код ≠ 200 → возвращается `{"status": "http_error", "code": <code>, "uid": <uid>}` и конкретный `uid` учитывается как `not_found`.

Пример установки токена:

```powershell
$env:INVEST_TINKOFF_TOKEN = 'your-token-here'
```

Или поместите токен в файл `tinkoff_token.txt` (без перевода строки в конце).

### SSL проверка и пользовательский сертификат

При обращении к API создаётся `SSLContext`:

1. Переменная окружения `INVEST_TINKOFF_VERIFY_SSL` управляет проверкой.
  * Значения `0`, `false`, `no`, `off` (регистр не важен) отключают проверку (hostname + сертификат).
  * Любое другое значение (по умолчанию `1`) включает стандартную проверку.
2. Если проверка включена и существует файл `invest/_.tbank.ru.crt`, он добавляется через `load_verify_locations`.
3. Ошибка загрузки сертификата логируется и используется системный trust store.

Примеры:

Отключить проверку (только для отладки):
```powershell
$env:INVEST_TINKOFF_VERIFY_SSL = '0'
```

Использовать кастомный сертификат (поместите файл):
```
invest/_.tbank.ru.crt
```

Проверить текущий режим:
```python
import os
print(os.getenv('INVEST_TINKOFF_VERIFY_SSL', '1'))
```

## Пересчёт потенциалов

```python
from invest_core.potentials import compute_all_potentials
compute_all_potentials()
```

## Полный оркестратор

```python
from invest_core.pipeline import run_pipeline
report = run_pipeline(dry_run=False, fetch_forecast_details=True)
print(report)
```

## Обслуживание данных

```python
from invest_core.maintenance import run_maintenance
maintenance_report = run_maintenance(threshold=1_000_000)
print(maintenance_report)
```

## Аномалии и ограничения

* Консенсус цены > 1e6 считается аномалией и при очистке удаляется.
* Пагинация консенсусов (get_consensus_forecasts) может не возвращать нужные UID — fallback через per‑uid forecast_by.
* Потенциал не вычисляется если нет валидной пары (prevClose > 0 и consensusPrice в разумных пределах).
* Часть методов SDK (например forecast_by) может быть недоступна на тарифе, что приводит к статусам NOT_FOUND в логах.

## Упрощённая логика парсинга

Ответ API должен содержать два блока: `consensus` (один объект) и `targets` (массив объектов). Модуль не пытается разбирать альтернативные вложенные структуры: ожидаются поля напрямую в этих блоках.

## Диагностика

При сохранении выводятся сообщения о пропуске дублей и вставках. При отсутствии ответа по `uid` статистика `FillingConsensusData` увеличивает счётчик `not_found`.

## SQLite in-memory caveat (тестовая стратегия)

Использование `':memory:'` в SQLite создаёт отдельную чистую БД на КАЖДОЕ новое соединение `sqlite3.connect(':memory:')`. В нашем коде слой `db.get_connection()` открывает соединение на каждый вызов, что приводит к исчезновению ранее вставленных данных между операциями в тестах при переключении на in-memory.

Из-за этого тесты, проверяющие логику дубликатов (например `AddMoexHistory` или сохранение consensus/targets), давали нулевые вставки или постоянные дубликаты.

Решение: переход на временные файловые БД внутри тестов:

1. Генерируем путь `tests/<test>_temp.db`.
2. Устанавливаем переменную окружения `INVEST_DB_FILE` перед `reload(db_layer)`.
3. Инициализируем схему и явно очищаем нужные таблицы.
4. После теста удаляем файл.

Преимущества:
* Поведение идентично реальной эксплуатации (несколько соединений видят одни и те же данные).
* Исключены скрытые расхождения в логике дубликатов.
* Упрощается диагностика: файл можно открыть внешним инструментом при необходимости.

Если нужен чистый изолированный контекст без файла, можно использовать URI `file:memdb1?mode=memory&cache=shared` (shared-cache), но это усложняет настройку. Текущий репозиторий стандартизирован на временных файлах для тестов.

## Единый ежедневный job (история + консенсус + потенциалы)

Скрипт `daily_history_job.py` теперь объединяет несколько шагов обновления данных:

1. Догрузка недостающей истории цен MOEX для всех SECID в `perspective_shares` (`FillingMoexHistory`).
2. Загрузка консенсусов и таргетов для перспективных инструментов (`FillingConsensusData`).
3. Пересчёт потенциалов акций на основе последних цен и консенсусов (`FillingPotentialData`).
4. Очистка устаревших записей потенциалов старше N дней (`CleanOldSharePotentials`).
5. (Опционально) Вывод топ-N записей по относительному потенциалу (`GetTopSharePotentials`).

Лог пишется в `daily_history_job.log`. Для взаимного исключения применяется файловая блокировка `daily_history_job.lock`.

Пример запуска по умолчанию (теперь увеличенный горизонт хранения 1100 дней):
```powershell
python daily_history_job.py --retention-days 1100 --top 10
```

Доступные аргументы CLI:
* `--board TQBR` – торговая доска MOEX (по умолчанию значение из переменной окружения или `TQBR`).
* `--skip-history` – пропустить шаг догрузки истории.
* `--skip-consensus` – пропустить шаг загрузки консенсусов/таргетов.
* `--skip-potentials` – пропустить расчёт потенциалов и связанные шаги.
* `--retention-days 1100` – удалить потенциалы старше указанного количества дней (0 отключает очистку; значение по умолчанию = 1100).
* `--top 10` – вывести топ-N потенциалов в лог после пересчёта.
* `--no-skip-null` – вставлять строки потенциалов даже если относительный потенциал не рассчитан (по умолчанию такие строки пропускаются).

Строка сводки в логе содержит агрегированные метрики: статус шагов, fetched/inserted/duplicates для истории и консенсусов, processed/inserted/skipped для потенциалов, количество удалённых старых записей и число строк в топ‑N.

### Windows Task Scheduler (schtasks)

1. Убедитесь, что виртуальная среда и зависимости установлены.
2. Получите полный путь до Python интерпретатора (например: `D:\#Work\#Invest\Project\invest\Scripts\python.exe`).
3. Создайте задание:

```powershell
schtasks /Create /SC DAILY /ST 07:30 /TN InvestDailyHistory /TR "\"D:\#Work\#Invest\Project\invest\Scripts\python.exe\" \"D:\#Work\#Invest\Project\daily_history_job.py\"" /RL LIMITED /F
```

Пояснение:
* /SC DAILY — ежедневный запуск.
* /ST 07:30 — локальное время запуска.
* /TN — имя задачи.
* Экранирование кавычек нужно из-за пробелов и символов `#` в путях.

Проверка задачи:
```powershell
schtasks /Query /TN InvestDailyHistory
```

Принудительный запуск:
```powershell
schtasks /Run /TN InvestDailyHistory
```

Удаление:
```powershell
schtasks /Delete /TN InvestDailyHistory /F
```

### Параметры окружения

Если требуется другой торговый режим:
```powershell
$env:INVEST_MOEX_BOARD = 'TQBR'
```
Можно задать в расширенных настройках задачи (вкладка «Conditions» / «Actions» через `.cmd` wrapper) или создать `.ps1` файл:

`run_daily_history.ps1`:
```powershell
$env:INVEST_MOEX_BOARD = 'TQBR'
cd 'D:\#Work\#Invest\Project'
& 'D:\#Work\#Invest\Project\invest\Scripts\python.exe' .\daily_history_job.py --retention-days 1100 --top 10
```

Тогда в schtasks:
```powershell
schtasks /Create /SC DAILY /ST 07:30 /TN InvestDailyHistory /TR "powershell.exe -ExecutionPolicy Bypass -File \"D:\#Work\#Invest\Project\run_daily_history.ps1\"" /F
```

### Linux (cron) пример (опционально)

В файл crontab:
```
30 7 * * * /usr/bin/python3 /path/to/Project/daily_history_job.py >> /path/to/Project/daily_history_job.log 2>&1
```

### Мониторинг

Проверяйте `daily_history_job.log`. Каждая строка содержит: статус, fetched, inserted, duplicates, invalid, errors, duration.

### Повторный запуск в один день

Если нужно форсировать второй запуск — удалите файл `daily_history_job.lock`.


## Индексы производительности

Добавлены индексы:

* `idx_consensus_uid_date` на `(uid, recommendationDate DESC)` – быстрый доступ к последнему консенсусу.
* `idx_targets_uid_date` на `(uid, recommendationDate DESC)` – быстрый доступ к последним таргетам (поддерживает выборку множества записей после расширения схемы).
* `idx_instrument_potentials_rel` для топ‑N потенциалов.

## Статус legacy

REST модуль `tinkoff_api.py` и ранняя SDK-обёртка `tinkoff_sdk.py` заменены новыми компонентами и оставлены как заглушки.

## Лицензия

MIT (при необходимости заменить).

