@echo off
rem Deploy the notifier service as a Docker image.
rem
rem Usage: scripts\deploy.bat [env]
rem   env: target environment (default: local)
rem        local - build the image and run the full stack on Docker Desktop
rem        test/prod - reserved for future environments
setlocal enabledelayedexpansion

set "IMAGE_NAME=notifier"
rem Single source for the local API address; override by setting API_BASE.
if not defined API_BASE set "API_BASE=http://localhost:8081"
set "LOCAL_HEALTH_URL=%API_BASE%/healthz"
set "HEALTH_RETRIES=30"

set "TARGET_ENV=%~1"
if "%TARGET_ENV%"=="" set "TARGET_ENV=local"

where docker >nul 2>&1 || (echo deploy: docker not found in PATH & exit /b 1)
docker info >nul 2>&1 || (echo deploy: docker daemon is not running ^(start Docker Desktop^) & exit /b 1)

if "%TARGET_ENV%"=="local" goto :deploy_local
if "%TARGET_ENV%"=="test" (echo deploy: environment 'test' is not implemented yet & exit /b 1)
if "%TARGET_ENV%"=="prod" (echo deploy: environment 'prod' is not implemented yet & exit /b 1)
echo deploy: unknown environment '%TARGET_ENV%' ^(supported: local^)
exit /b 1

:deploy_local
for /f %%s in ('git rev-parse --short HEAD 2^>nul') do set "GIT_SHA=%%s"
if "%GIT_SHA%"=="" set "GIT_SHA=dev"

echo ==^> Building %IMAGE_NAME%:local (%GIT_SHA%)
docker build -t "%IMAGE_NAME%:local" -t "%IMAGE_NAME%:%GIT_SHA%" . || exit /b 1

echo ==^> Starting stack
docker compose up -d || exit /b 1

echo ==^> Waiting for API health
set /a attempt=1
:health_loop
curl -fsS -o nul "%LOCAL_HEALTH_URL%" 2>nul
if not errorlevel 1 goto :healthy
if %attempt% geq %HEALTH_RETRIES% (
    echo deploy: API did not become healthy within %HEALTH_RETRIES%s - check: docker compose logs api
    exit /b 1
)
set /a attempt+=1
timeout /t 1 /nobreak >nul
goto :health_loop

:healthy
echo ==^> Deployed: API healthy at %LOCAL_HEALTH_URL% (image %IMAGE_NAME%:%GIT_SHA%)
docker compose ps
exit /b 0
