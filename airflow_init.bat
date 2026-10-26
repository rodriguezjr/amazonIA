@echo off
REM ============================================
REM   AmazonIA 4.0 - Inicializar Apache Airflow
REM   Ejecutar UNA SOLA VEZ antes de levantar el stack completo.
REM ============================================

echo.
echo ============================================
echo   AmazonIA 4.0 - Inicializar Airflow
echo ============================================

REM Detectar comando docker compose o docker-compose
set DOCKER_CMD=docker compose
docker compose version >nul 2>&1
if %errorlevel% neq 0 (
    set DOCKER_CMD=docker-compose
)

echo Usando comando: %DOCKER_CMD%

echo.
echo [1/4] Creando directorios necesarios...
if not exist "data\lakehouse\silver" mkdir "data\lakehouse\silver"
if not exist "data\lakehouse\gold" mkdir "data\lakehouse\gold"
if not exist "logs" mkdir "logs"
if not exist "plugins" mkdir "plugins"

echo.
echo [2/4] Levantando base de datos PostgreSQL y Redis...
%DOCKER_CMD% up -d postgres redis

echo.
echo [3/4] Esperando que PostgreSQL este listo (20 segundos)...
timeout /t 20 /nobreak

echo.
echo [4/4] Inicializando base de datos Airflow + creando usuario admin...
%DOCKER_CMD% up airflow-init

echo.
echo ============================================
echo   Airflow inicializado correctamente!
echo.
echo   Usuario: admin
echo   Contrasena: admin123
echo   URL: http://localhost:8080
echo.
echo   Para levantar todo el stack:
echo     %DOCKER_CMD% up -d
echo.
echo   Para solo Airflow:
echo     %DOCKER_CMD% up -d airflow-webserver airflow-scheduler airflow-worker
echo ============================================
pause
