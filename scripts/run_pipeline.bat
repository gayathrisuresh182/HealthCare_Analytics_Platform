@echo off
REM Windows Batch Script for Data Pipeline
REM Use with Windows Task Scheduler

echo ========================================
echo Healthcare Analytics Pipeline
echo ========================================
echo.

REM Change to project directory
cd /d "%~dp0\.."

REM Activate conda environment (if using conda)
REM call conda activate base

REM Run the enhanced pipeline script
python scripts/run_pipeline_enhanced.py

REM Check exit code
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ========================================
    echo Pipeline FAILED with exit code %ERRORLEVEL%
    echo ========================================
    exit /b %ERRORLEVEL%
) else (
    echo.
    echo ========================================
    echo Pipeline completed successfully
    echo ========================================
    exit /b 0
)

