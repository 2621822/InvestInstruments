# InvestInstruments (минимальный профиль)

Упрощённый репозиторий, оставлены только компоненты, необходимые для ежедневного запуска:

```
daily_history_job.py          # Единый job: история → консенсус → потенциалы → retention/top
src/invest_core/db_mysql.py   # MySQL слой доступа и инициализация схемы
src/invest_core/normalization.py  # Нормализация чисел и дат
src/invest_core/forecasts.py  # Загрузка/сохранение консенсусов и таргетов
src/invest_core/moex_history.py   # Догрузка исторических цен MOEX
src/invest_core/potentials.py # Расчёт и обслуживание потенциалов
src/invest_core/__init__.py   # Минимальный ре‑экспорт
requirements.txt              # Сокращённый список зависимостей (requests, PyMySQL, cryptography)
README.md                     # Этот файл
```

Удалены все прочие скрипты (миграции, демо, диагностика, legacy SDK, тесты) для снижения объёма и точечной эксплуатации.

### Таблицы (активные)

| Таблица | Назначение | Ключ |
|---------|------------|------|
| perspective_shares    | Список инструментов (заполняется внешне) | uid PRIMARY KEY |
| moex_shares_history   | Исторические котировки MOEX              | (SECID, TRADEDATE) |
| consensus_forecasts   | История консенсусов                      | (uid, recommendationDate) |
| consensus_targets     | Таргеты аналитиков                       | (uid, recommendationDate, company) |
| shares_potentials     | История расчёта потенциалов              | (uid, computedAt) |

## Конфигурация

Теперь основные настройки берутся из файла `config.ini` (создан в корне репозитория). Формат ini с секциями:

```ini
[database]
host=localhost
port=3306
name=invest
user=gorbunov
password=Joj724229
charset=utf8mb4

[tinkoff]
api_token=***
verify_ssl=1
force_ip=
no_proxies=0

[job]
board=TQBR
retention_days=1100
TopLimit=10
collapse_duplicates=1
skip_history=0
skip_consensus=0
skip_potentials=0
```

Приоритет источников значений:
1. Переменные окружения (если заданы явно).
2. `config.ini`.
3. Жёстко прошитые значения по умолчанию в коде.

### Переменные окружения (fallback / переопределение)

| Переменная | Описание |
|------------|----------|
| INVEST_DB_HOST / PORT / NAME / USER / PASSWORD / CHARSET | Переопределяют параметры `[database]` |
| INVEST_TINKOFF_TOKEN / INVEST_TOKEN | Перебивают `[tinkoff] api_token` |
| INVEST_TINKOFF_VERIFY_SSL | Перебивает `verify_ssl` |
| INVEST_MOEX_BOARD | Перебивает `[job] board` |
| INVEST_HISTORY_RETENTION_DAYS | Перебивает `retention_days` |
| INVEST_MOEX_TIMEOUT_SEC / INVEST_MOEX_SLEEP_SEC | Тайминги обращения к MOEX API |
| INVEST_CONFIG_FILE | Альтернативный путь к ini файлу |

Если требуется скрыть пароль/токен из репозитория – замените значения в `config.ini` на плейсхолдеры и используйте секреты окружения.

## Быстрая установка

```powershell
python -m venv invest
./invest/Scripts/Activate.ps1
pip install -r requirements.txt
```

### MySQL backend (единственный поддерживаемый)

1. Установите сервер MySQL (Community Server Installer или Docker).
2. Создайте БД и пользователя:
  ```sql
  CREATE DATABASE invest CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
  CREATE USER 'gorbunov'@'localhost' IDENTIFIED BY '*** ваш пароль ***';
  GRANT ALL PRIVILEGES ON invest.* TO 'gorbunov'@'localhost';
  FLUSH PRIVILEGES;
  ```
3. Либо настройте `config.ini`, либо экспортируйте переменные окружения перед запуском:
  ```powershell
  $env:INVEST_DB_BACKEND='mysql'
  $env:INVEST_DB_HOST='localhost'
  $env:INVEST_DB_PORT='3306'
  $env:INVEST_DB_NAME='invest'
  $env:INVEST_DB_USER='gorbunov'
  $env:INVEST_DB_PASSWORD='your_password'
  $env:INVEST_DB_CHARSET='utf8mb4'
  ```
4. Схема создаётся автоматически при первом вызове `db_mysql.init_schema()` (например при запуске `daily_history_job.py`).

Особенности реализации:
* Для MySQL нет `CREATE INDEX IF NOT EXISTS` – индексы создаются после проверки через `INFORMATION_SCHEMA.STATISTICS`.
* Типы адаптированы: `REAL` → `DOUBLE`, `INTEGER` → `INT/BIGINT`, текстовые поля → `VARCHAR`.
* Автоинкремент для `consensus_targets.id` реализован через `INT AUTO_INCREMENT PRIMARY KEY`.
* Аутентификация по умолчанию (caching_sha2_password) требует пакета `cryptography` – он добавлен в `requirements.txt`.

Переключение обратно на SQLite:
```powershell
$env:INVEST_DB_BACKEND='sqlite'
```

Диагностика подключения (см. скрипт `mysql_check.py`):
```powershell
python mysql_check.py
```
Выводит версию сервера и список таблиц.

## Инициализация

Отдельный скрипт не требуется. Достаточно корректного `config.ini` (или переменных окружения) и запуска:
```powershell
python daily_history_job.py --top 5
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

## Прогнозы (минимальный API)

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

## Потенциалы

API через `FillingPotentialData` внутри job; отдельный вызов:
```python
from invest_core.potentials import FillingPotentialData
stats = FillingPotentialData(skip_null=True)
print(stats)
```

## Ежедневный оркестратор

## Обслуживание данных

Служебные функции (ретеншн, схлопывание дублей) вызываются внутри `daily_history_job.py`.

## Аномалии и ограничения

* Консенсус цены > 1e6 считается аномалией и при очистке удаляется.
* Пагинация консенсусов (get_consensus_forecasts) может не возвращать нужные UID — fallback через per‑uid forecast_by.
* Потенциал не вычисляется если нет валидной пары (prevClose > 0 и consensusPrice в разумных пределах).
* Часть методов SDK (например forecast_by) может быть недоступна на тарифе, что приводит к статусам NOT_FOUND в логах.

## Упрощённая логика парсинга

Ответ API должен содержать два блока: `consensus` (один объект) и `targets` (массив объектов). Модуль не пытается разбирать альтернативные вложенные структуры: ожидаются поля напрямую в этих блоках.

## Диагностика

При сохранении выводятся сообщения о пропуске дублей и вставках. При отсутствии ответа по `uid` статистика `FillingConsensusData` увеличивает счётчик `not_found`.

### MySQL диагностика

Скрипт `mysql_check.py` (если backend='mysql') показывает:
* Версию сервера.
* Список таблиц из `information_schema.tables`.
* Простейший тест выборки/вставки (не изменяет основные таблицы если уже существуют).

Пример запуска:
```powershell
$env:INVEST_DB_BACKEND='mysql'
python mysql_check.py
```

### Тесты уникальности PK
## Примечания по MySQL

Проект поддерживает полный переход с SQLite на MySQL:

Шаги выполненной миграции:
1. Добавлен backend `mysql` в `db.py` (PyMySQL, адаптированные типы, индексы через INFORMATION_SCHEMA).
2. Унификация плейсхолдеров через `exec_sql` (автозамена `?` → `%s` для MySQL).
3. Скрипт `migrate_sqlite_to_mysql.py` перенёс данные из файла `invest_data.db`:
   * `perspective_shares`: сохранены все уникальные UID (skipped — существующие).
   * `moex_shares_history`: ~32k строк перенесено.
   * `consensus_forecasts` и `shares_potentials`: данные перенесены без потерь.
   * `consensus_targets`: старые строки пропущены при конфликте автоинкремента (возможен дополнительный импорт без поля id, если потребуется).
4. Добавлен `mysql_check.py` — быстрый аудит версии и структуры.
5. Тесты (`pytest`) проходят под backend=mysql (см. раздел "Тесты" выше).

Оркестратор `daily_history_job.py`:
* Работает с MySQL после миграции (шаги: история, консенсус, потенциалы).
* Лог `daily_history_job.log` содержит строки вида `DailyUnified` с агрегированными метриками:
  - `hist_fetched` / `hist_inserted` / `hist_duplicates`
  - `cons_cins` (вставлено консенсусов) / `cons_cdup` (дубликаты)
  - `targets_inserted` / `targets_dups`
  - `pot_inserted` / `pot_skipped` / `pot_unchanged`
  - `retention_deleted` (очистка старых потенциалов) / `collapse_deleted` (удалено дублей)
  - `top_rows` (количество строк в ТОП‑N по относительному потенциалу)
* Пример успешного полного цикла:
  ```
  [2025-11-03T02:29:47.441539+00:00] DailyUnified board=TQBR hist_status=ok hist_fetched=27445 hist_inserted=27445 hist_duplicates=0 cons_processed=50 cons_cins=42 cons_cdup=7 targets_inserted=325 targets_dups=74 pot_processed=50 pot_inserted=42 pot_skipped=1 pot_unchanged=7 retention_deleted=0 collapse_deleted=None top_rows=None duration=91.359s
  ```

Известные нюансы:
* MySQL не поддерживает `CREATE INDEX IF NOT EXISTS` — индексы создаются только после проверки наличия.
* Автоинкремент PK в `consensus_targets` делает прямую переносимую вставку старых id не всегда целесообразной. При необходимости восстановить точные идентификаторы можно создать временную таблицу и выполнить INSERT с явным перечислением столбцов.
* При чтении дат из MySQL может возвращаться тип `date`; добавлена нормализация в `moex_history.py`.
* Ошибка `not all arguments converted during string formatting` указывает на использование плейсхолдеров `?` без прохождения через `exec_sql`; обновляйте такие места при расширении функционала.

Рекомендации после переезда:
1. Настроить регулярный `mysqldump` или использование MySQL Backup для критичных данных.
2. Добавить мониторинг задержек запросов (Performance Schema) при росте объёма.
3. Рассмотреть переход вычислительных аналитических выборок на отдельный движок (например ClickHouse) при росте нагрузки.

Команда быстрой проверки:
```powershell
$env:INVEST_DB_BACKEND='mysql'
python mysql_check.py
python -m pytest -q
python daily_history_job.py --retention-days 1100 --top 5
```

Если требуется откат на SQLite:
```powershell
$env:INVEST_DB_BACKEND='sqlite'
python db_init.py
```

Тесты и вспомогательные проверочные скрипты удалены – используется только runtime проверка через успешный запуск job.

## Удалённые компоненты

Скрипты миграций, диагностики, демо вызовы SDK, тесты и вспомогательные утилиты удалены для минимизации кода. Историческая справка доступна в истории Git.

## legacy_instruments (упрощённый вспомогательный модуль)

Файл `src/invest_core/legacy_instruments.py` содержит минимально сохранённые функции из удалённых модулей:

| Функция | Назначение | Источники удалённых файлов |
|---------|------------|----------------------------|
| `PostApiHeaders()` | Сформировать заголовки запроса с Bearer токеном для REST | tinkoff_search.py / rest_instruments.py |
| `GetUidInstrument(query)` | Найти первый UID акции через REST `FindInstrument` | tinkoff_search.py / rest_instruments.py |
| `FillingSharesData(uid)` | Обновить строку `perspective_shares` по UID (REST эвристика) | instruments.py |

Особенности:
* SDK клиент удалён — поиск выполняется только POST запросом к `FindInstrument`.
* Точная выборка по UID не гарантирована: используется попытка поиска по строке UID и фильтр точного совпадения.
* Токен берётся из `INVEST_TINKOFF_TOKEN` / `INVEST_TOKEN` либо файлов `tinkoff_token.txt` / `token.txt`; если не найден — fallback на жёстко прописанный (замените его при необходимости).
* SSL проверка настраивается переменной `INVEST_TINKOFF_VERIFY_SSL` (`0` = отключить).
* Модуль не импортируется в `daily_history_job.py` и может быть удалён без влияния на ежедневный job.

Пример:
```python
from invest_core.legacy_instruments import GetUidInstrument, FillingSharesData
uid = GetUidInstrument("LKOH")
print("UID", uid)
if uid:
  print(FillingSharesData(uid))
```

Для полного восстановления функционала работы с инструментами (атрибуты, массовые обновления, точный get по UID) требуется вернуть удалённые SDK модули.

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

## Избранные акции (Favorites)

Модуль `favorites.py` позволяет получить список избранных инструментов из вашего профиля Tinkoff Invest через официальный SDK.

Функции:
* `list_favorites()` – вернуть сырой список избранных инструментов.
* `list_favorite_shares()` – только акции в компактном формате (ticker, uid, figi, name...).
* `ensure_perspective_from_favorites()` – добавить отсутствующие избранные акции в таблицу `perspective_shares`.

CLI скрипт `list_favorites.py`:
```powershell
python list_favorites.py            # вывести список избранных акций
python list_favorites.py --add      # также добавить новые в perspective_shares
```

Токен берётся из `INVEST_TOKEN` / `INVEST_TINKOFF_TOKEN` или файла `tinkoff_token.txt`.
Если токен отсутствует – выводится предупреждение и список будет пуст.

Пример кода:
```python
from invest_core.favorites import list_favorite_shares
shares = list_favorite_shares()
for s in shares:
  print(s['ticker'], s['uid'])
```

## Статус legacy

REST модуль `tinkoff_api.py` и ранняя SDK-обёртка `tinkoff_sdk.py` заменены новыми компонентами и оставлены как заглушки.

## Лицензия

MIT (при необходимости заменить).

