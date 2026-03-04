@echo off
setlocal enabledelayedexpansion
title Polymarket Bot Watchdog

cd /d "%~dp0"

set "PY_EXE=%CD%\.venv\Scripts\python.exe"
set "SCRIPT=%CD%\Quantify.py"
set "RESTART_DELAY=8"

if not exist "%PY_EXE%" (
  echo [ERROR] Python not found: %PY_EXE%
  pause
  exit /b 1
)

if not exist "%SCRIPT%" (
  echo [ERROR] Script not found: %SCRIPT%
  pause
  exit /b 1
)

echo ==========================================
echo   Polymarket Bot Watchdog
echo ==========================================
echo [INFO] PY_EXE=%PY_EXE%
echo [INFO] SCRIPT=%SCRIPT%
echo [INFO] LOOP_INTERVAL_SECONDS=%POLY_LOOP_INTERVAL_SECONDS%
echo [INFO] HEARTBEAT_SECONDS=%POLY_HEARTBEAT_SECONDS%
echo [INFO] DRY_RUN=%POLY_DRY_RUN%
echo.

:LOOP
echo [INFO] %date% %time% starting bot process...
"%PY_EXE%" "%SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"
echo [WARN] Bot exited. code=!EXIT_CODE!
echo [INFO] Restart in %RESTART_DELAY%s...
timeout /t %RESTART_DELAY% /nobreak >nul
goto :LOOP

