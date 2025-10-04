@echo off
REM ------------------------------------------------------------
REM Полный автоматизированный запуск full_refresh.py
REM Делает:
REM  1. Переходит в директорию проекта
REM  2. Создаёт venv (.venv), если нет
REM  3. Устанавливает / обновляет зависимости (idempotent: pip install -r requirements.txt)
REM  4. Подхватывает токен из переменной окружения или tinkoff_token.txt
REM  5. (Опционально) Подхватывает кастомный корневой сертификат corp_ca.pem -> TINKOFF_CA_BUNDLE
REM  6. Запускает full_refresh.py и пишет лог с датой: refresh_YYYYMMDD.log
REM  7. Возвращает exit code python
REM ------------------------------------------------------------
SETLOCAL ENABLEDELAYEDEXPANSION

SET "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%" || (echo Не удалось перейти в каталог проекта & exit /b 1)

REM 1. Создание виртуального окружения при отсутствии
IF NOT EXIST .venv\Scripts\python.exe (
  echo [INFO] Создаю виртуальное окружение .venv
  py -3 -m venv .venv 2>nul || python -m venv .venv || (echo [ERROR] Не удалось создать venv & exit /b 1)
)

REM 2. Активация
CALL .venv\Scripts\activate.bat || (echo [ERROR] Не удалось активировать venv & exit /b 1)

REM 3. Установка зависимостей (быстрая / повторяемая)
echo [INFO] Установка / проверка зависимостей...
python -m pip install --upgrade pip --quiet >nul 2>&1
pip install -r requirements.txt --quiet
IF ERRORLEVEL 1 (
  echo [ERROR] Ошибка установки зависимостей
  exit /b 1
)

REM 4. Токен: приоритет у уже заданного в окружении. Если не задан — читаем первый непустой/некомментарий.
IF NOT DEFINED TINKOFF_INVEST_TOKEN (
  IF EXIST tinkoff_token.txt (
    for /f "usebackq delims=" %%L in ("tinkoff_token.txt") do (
      set "_line=%%L"
      if NOT "!_line!"=="" if /I NOT "!_line:~0,1!"=="#" if NOT DEFINED TINKOFF_INVEST_TOKEN set "TINKOFF_INVEST_TOKEN=!_line!"
    )
  )
)
IF NOT DEFINED TINKOFF_INVEST_TOKEN (
  echo [WARN] TINKOFF_INVEST_TOKEN не найден (переменная и файл) — шаг прогнозов будет пропущен.
) ELSE (
  set "_masked=!TINKOFF_INVEST_TOKEN:~0,4!...!TINKOFF_INVEST_TOKEN:~-2!"
  echo [INFO] Токен найден (masked=!_masked!)
)

REM 5. Кастомный CA (при необходимости). Приоритет: tinkoff_ca_bundle.pem -> corp_ca.pem
IF NOT DEFINED TINKOFF_CA_BUNDLE (
  IF EXIST tinkoff_ca_bundle.pem (
    SET "TINKOFF_CA_BUNDLE=%PROJECT_DIR%tinkoff_ca_bundle.pem"
    echo [INFO] Использую кастомный CA bundle (tinkoff_ca_bundle.pem): %TINKOFF_CA_BUNDLE%
  ) ELSE IF EXIST corp_ca.pem (
    SET "TINKOFF_CA_BUNDLE=%PROJECT_DIR%corp_ca.pem"
    echo [INFO] Использую кастомный CA bundle (corp_ca.pem): %TINKOFF_CA_BUNDLE%
  )
)

REM 5.1 Опциональное отключение SSL проверки через флаг-файл (НЕ РЕКОМЕНДУЕТСЯ в продакшене)
IF EXIST disable_ssl_verify.flag (
  SET "TINKOFF_SSL_NO_VERIFY=1"
  echo [WARN] SSL проверка ОТКЛЮЧЕНА (файл disable_ssl_verify.flag). Используйте только для диагностики!
)

REM 6. Формируем имя лога с датой (DATE может зависеть от региональных настроек).
for /f "tokens=1-3 delims=. -/" %%a in ("%DATE%") do (
  set "_d1=%%a" & set "_d2=%%b" & set "_d3=%%c"
)
REM Пытаемся определить где год (берём компонент с длиной 4)
set "_y=%_d1%"
if not "!_d1:~4!"=="" set "_y=%_d1%"
if "!_d1:~4!"=="" if "!_d2:~4!"=="" (set "_y=%_d3%") else (set "_y=%_d2%")
REM Остальные две — месяц и день (эвристика); если регион RU (ДД.ММ.ГГГГ) -> d1=dd d2=mm d3=yyyy
if "%_y%"=="%_d3%" (
  set "_day=%_d1%"
  set "_mon=%_d2%"
) else (
  set "_day=%_d2%"
  set "_mon=%_d1%"
)
set "LOGFILE=refresh_%_y%%_mon%%_day%.log"

echo [INFO] Запуск full_refresh.py (лог: %LOGFILE%)
python full_refresh.py >> "%LOGFILE%" 2>&1
set "_rc=%ERRORLEVEL%"
if NOT "%_rc%"=="0" (
  echo [ERROR] full_refresh завершился с кодом %_rc%
) else (
  echo [INFO] full_refresh завершён успешно
)

ENDLOCAL & exit /b %_rc%
