@echo off
title AmazonIA 4.0 - MVP
chcp 65001 >nul
cls

echo ===================================================
echo   🌿 Iniciando AmazonIA 4.0 (Modo Local Unificado)
echo ===================================================
echo.

if exist .venv\Scripts\python.exe (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
) else (
    set "PYTHON_CMD=python"
)

%PYTHON_CMD% start.py %*
pause
