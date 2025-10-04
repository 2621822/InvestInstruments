<#!
.SYNOPSIS
  Универсальный PowerShell запуск полного обновления (full_refresh.py)
.DESCRIPTION
  Делает:
    * Создание и активация виртуального окружения .venv (если нет)
    * Установка / обновление зависимостей
    * Подхват токена из переменной или tinkoff_token.txt
    * (Опц.) Установка кастомного CA через corp_ca.pem или $Env:TINKOFF_CA_BUNDLE
    * Запуск full_refresh.py
    * Ротация лога в виде refresh_YYYYMMDD.log
.PARAMETER ForceInstall
  Принудительно переустановить зависимости (pip install --force-reinstall)
.PARAMETER NoDeps
  Не выполнять установку зависимостей
.PARAMETER ShowLogTail
  После завершения показать последние 40 строк лога
.PARAMETER NoToken
  Не пытаться читать tinkoff_token.txt (использовать только окружение)
.PARAMETER AllowInsecure
  Эквивалент TINKOFF_SSL_NO_VERIFY=1 (только диагностика)
.EXAMPLE
  ./full_refresh.ps1 -ShowLogTail
#>
[CmdletBinding()]
param(
  [switch]$ForceInstall,
  [switch]$NoDeps,
  [switch]$ShowLogTail,
  [switch]$NoToken,
  [switch]$AllowInsecure
)

$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectDir

Write-Host "[INFO] Project directory: $projectDir"

# 1. VENV
$venvPy = Join-Path $projectDir '.venv/Scripts/python.exe'
if (-not (Test-Path $venvPy)) {
  Write-Host "[INFO] Creating virtual environment (.venv)"
  py -3 -m venv .venv 2>$null || python -m venv .venv
}
if (-not (Test-Path $venvPy)) { throw "Не удалось создать виртуальное окружение" }

# 2. Activate (scoped for this process)
$activate = Join-Path $projectDir '.venv/Scripts/Activate.ps1'
. $activate

# 3. Dependencies
if (-not $NoDeps) {
  Write-Host "[INFO] Installing dependencies"
  python -m pip install --upgrade pip | Out-Null
  if ($ForceInstall) {
    pip install --force-reinstall -r requirements.txt
  } else {
    pip install -r requirements.txt
  }
}

# 4. Token
if (-not $Env:TINKOFF_INVEST_TOKEN -and -not $NoToken) {
  $tokenFile = Join-Path $projectDir 'tinkoff_token.txt'
  if (Test-Path $tokenFile) {
    $line = Get-Content $tokenFile | Where-Object { $_ -and -not ($_.Trim().StartsWith('#')) } | Select-Object -First 1
    if ($line) { $Env:TINKOFF_INVEST_TOKEN = $line.Trim() }
  }
}
if ($Env:TINKOFF_INVEST_TOKEN) {
  $masked = if ($Env:TINKOFF_INVEST_TOKEN.Length -gt 8) { $Env:TINKOFF_INVEST_TOKEN.Substring(0,4) + '...' + $Env:TINKOFF_INVEST_TOKEN.Substring($Env:TINKOFF_INVEST_TOKEN.Length-2) } else { $Env:TINKOFF_INVEST_TOKEN.Substring(0,1) + '***' + $Env:TINKOFF_INVEST_TOKEN.Substring($Env:TINKOFF_INVEST_TOKEN.Length-1) }
  Write-Host "[INFO] Token detected (masked=$masked)"
} else {
  Write-Host "[WARN] Token not found; forecast step will be skipped"
}

# 5. CA bundle
if (-not $Env:TINKOFF_CA_BUNDLE) {
  $cand1 = Join-Path $projectDir 'tinkoff_ca_bundle.pem'
  $cand2 = Join-Path $projectDir 'corp_ca.pem'
  if (Test-Path $cand1) {
    $Env:TINKOFF_CA_BUNDLE = $cand1; Write-Host "[INFO] Using custom CA bundle (tinkoff_ca_bundle.pem)"
  } elseif (Test-Path $cand2) {
    $Env:TINKOFF_CA_BUNDLE = $cand2; Write-Host "[INFO] Using custom CA bundle (corp_ca.pem)"
  }
}
if ($AllowInsecure) {
  $Env:TINKOFF_SSL_NO_VERIFY = '1'
  Write-Host "[WARN] SSL verification disabled for this run (diagnostics)"
}
elseif (Test-Path (Join-Path $projectDir 'disable_ssl_verify.flag')) {
  $Env:TINKOFF_SSL_NO_VERIFY = '1'
  Write-Host "[WARN] SSL verification disabled via disable_ssl_verify.flag (diagnostics)"
}

# 6. Log name
$today = Get-Date -Format 'yyyyMMdd'
$logFile = "refresh_$today.log"
Write-Host "[INFO] Running full_refresh.py (log=$logFile)"

try {
  python full_refresh.py *>> $logFile
  $rc = $LASTEXITCODE
  if ($rc -eq 0) { Write-Host "[INFO] Completed successfully" } else { Write-Host "[ERROR] Exit code $rc" }
} catch {
  Write-Host "[ERROR] Exception: $_"; $rc = 1
}

if ($ShowLogTail -and (Test-Path $logFile)) {
  Write-Host "----- LOG TAIL -----"
  Get-Content $logFile -Tail 40
}

exit $rc
