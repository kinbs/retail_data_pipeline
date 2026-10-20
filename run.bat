@echo off
REM One-click launcher for the local pipeline (Windows).
setlocal

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] .venv not found. Run setup first:
    echo     python -m venv .venv
    echo     .venv\Scripts\python -m pip install -r requirements-local.txt
    exit /b 1
)

set "PYTHONPATH=%~dp0"
".venv\Scripts\python.exe" main.py %*
