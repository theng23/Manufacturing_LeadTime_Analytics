@echo off
setlocal enabledelayedexpansion

echo [START] Leadtime Master Pipeline

REM 1) Setup venv + install requirements
python master_run\setup_venv.py
if errorlevel 1 goto :fail

REM 2) Activate venv
call .venv\Scripts\activate
if errorlevel 1 goto :fail

REM 3) Run crawler (stores raw JSON snapshots)
python ingestion\crawler\erp_crawler.py
if errorlevel 1 goto :fail

REM 4) Run transformation supervisor (JSON -> Parquet -> optional upload)
python transformation\supervisor.py
if errorlevel 1 goto :fail

echo [DONE] Pipeline completed successfully.
exit /b 0

:fail
echo [FAILED] Pipeline stopped due to an error.
exit /b 1
