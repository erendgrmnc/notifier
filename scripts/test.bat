@echo off
rem Run the full test matrix and print a summarized report with insights.
rem
rem Usage: scripts\test.bat [suite]
rem   suite: all (default) | unit | integration
setlocal enabledelayedexpansion

set "SUITE=%~1"
if "%SUITE%"=="" set "SUITE=all"
set "API_BASE=http://localhost:8081"
set "WORK_DIR=%TEMP%\notifier-tests"
if not exist "%WORK_DIR%" mkdir "%WORK_DIR%"

set "VET_RESULT=-"
set "UNIT_RESULT=-"
set "UNIT_DETAILS="
set "RACE_RESULT=-"
set "RACE_DETAILS="
set "COVER_TOTAL=n/a"
set "INTEG_RESULT=-"
set "INTEG_DETAILS="
set "FAILED=0"

if "%SUITE%"=="integration" goto :integration
if not "%SUITE%"=="all" if not "%SUITE%"=="unit" (
    echo test: unknown suite '%SUITE%' ^(supported: all, unit, integration^)
    exit /b 1
)

rem --- vet ---------------------------------------------------------------
echo [1/4] vet: go vet ./...
go vet ./... >"%WORK_DIR%\vet.out" 2>&1
if errorlevel 1 (set "VET_RESULT=FAIL" & set "FAILED=1") else set "VET_RESULT=PASS"
echo       done: %VET_RESULT%

rem --- unit + coverage ----------------------------------------------------
echo [2/4] unit: go test ./...
go test -coverprofile="%WORK_DIR%\cover.out" ./... >"%WORK_DIR%\unit.out" 2>&1
if errorlevel 1 (
    set "UNIT_RESULT=FAIL"
    set "FAILED=1"
    set "UNIT_DETAILS=see failures below"
) else (
    set "UNIT_RESULT=PASS"
    for /f %%c in ('findstr /r /c:"^ok" "%WORK_DIR%\unit.out" ^| find /c /v ""') do set "UNIT_DETAILS=%%c packages green"
)
echo       done: %UNIT_RESULT%
for /f "tokens=3" %%p in ('go tool cover -func^="%WORK_DIR%\cover.out" 2^>nul ^| findstr /b "total:"') do set "COVER_TOTAL=%%p"

rem --- race (needs cgo) ----------------------------------------------------
echo [3/4] race: go test -race ./...
set "CGO_ENABLED=1"
go test -race ./... >"%WORK_DIR%\race.out" 2>&1
if errorlevel 1 (
    findstr /c:"requires cgo" /c:"C compiler" "%WORK_DIR%\race.out" >nul 2>&1
    if not errorlevel 1 (
        set "RACE_RESULT=SKIP"
        set "RACE_DETAILS=cgo/gcc unavailable; CI enforces -race"
    ) else (
        set "RACE_RESULT=FAIL"
        set "RACE_DETAILS=data race or failure - see %WORK_DIR%\race.out"
        set "FAILED=1"
    )
) else (
    set "RACE_RESULT=PASS"
    set "RACE_DETAILS=no data races detected"
)
set "CGO_ENABLED="
echo       done: %RACE_RESULT%

if "%SUITE%"=="unit" goto :report

:integration
echo [4/4] integration: live e2e checks against %API_BASE%
curl -s -o nul -w "%%{http_code}" "%API_BASE%/healthz" >"%WORK_DIR%\code.txt" 2>nul
set /p HEALTH_CODE=<"%WORK_DIR%\code.txt"
if not "%HEALTH_CODE%"=="200" (
    set "INTEG_RESULT=SKIP"
    set "INTEG_DETAILS=stack not running; start with scripts\deploy.bat local"
    goto :report
)

set /a CHECKS=0, PASSED=0

rem 1. create returns 201
set /a CHECKS+=1
curl -s -o "%WORK_DIR%\create.json" -w "%%{http_code}" -X POST "%API_BASE%/api/v1/notifications" -H "Content-Type: application/json" -d "{\"recipient\":\"+905551234567\",\"channel\":\"sms\",\"content\":\"test-suite e2e\"}" >"%WORK_DIR%\code.txt"
set /p CREATE_CODE=<"%WORK_DIR%\code.txt"
if "%CREATE_CODE%"=="201" set /a PASSED+=1

rem extract id
set "NOTIF_ID="
for /f "delims=" %%i in ('powershell -NoProfile -Command "(Get-Content '%WORK_DIR%\create.json' | ConvertFrom-Json).id"') do set "NOTIF_ID=%%i"

rem 2. reaches sent within 10s
set /a CHECKS+=1
set "DELIVERED=0"
for /l %%n in (1,1,10) do (
    if "!DELIVERED!"=="0" (
        curl -s "%API_BASE%/api/v1/notifications/!NOTIF_ID!" | findstr /c:"\"status\":\"sent\"" >nul && set "DELIVERED=1"
        if "!DELIVERED!"=="0" timeout /t 1 /nobreak >nul
    )
)
if "!DELIVERED!"=="1" set /a PASSED+=1

rem 3. validation 400
set /a CHECKS+=1
curl -s -o "%WORK_DIR%\bad.json" -w "%%{http_code}" -X POST "%API_BASE%/api/v1/notifications" -H "Content-Type: application/json" -d "{\"recipient\":\"nope\",\"channel\":\"sms\",\"content\":\"x\"}" >"%WORK_DIR%\code.txt"
set /p BAD_CODE=<"%WORK_DIR%\code.txt"
if "%BAD_CODE%"=="400" set /a PASSED+=1

rem 4. unknown id 404
set /a CHECKS+=1
curl -s -o nul -w "%%{http_code}" "%API_BASE%/api/v1/notifications/00000000-0000-0000-0000-000000000000" >"%WORK_DIR%\code.txt"
set /p MISSING_CODE=<"%WORK_DIR%\code.txt"
if "%MISSING_CODE%"=="404" set /a PASSED+=1

if %PASSED%==%CHECKS% (
    set "INTEG_RESULT=PASS"
    set "INTEG_DETAILS=%PASSED%/%CHECKS% e2e regression checks"
) else (
    set "INTEG_RESULT=FAIL"
    set "INTEG_DETAILS=%PASSED%/%CHECKS% checks passed"
    set "FAILED=1"
)

:report
echo.
echo ============================ TEST RESULTS ============================
echo SUITE          RESULT  DETAILS
if not "%VET_RESULT%"=="-"   echo vet            %VET_RESULT%    static analysis
if not "%UNIT_RESULT%"=="-"  echo unit           %UNIT_RESULT%    %UNIT_DETAILS%
if not "%COVER_TOTAL%"=="n/a" echo coverage       -       %COVER_TOTAL% of statements
if not "%RACE_RESULT%"=="-"  echo race           %RACE_RESULT%    %RACE_DETAILS%
if not "%INTEG_RESULT%"=="-" echo integration    %INTEG_RESULT%    %INTEG_DETAILS%
echo ======================================================================
echo.
echo Insights:
if not "%COVER_TOTAL%"=="n/a" echo   - total statement coverage: %COVER_TOTAL%
if "%RACE_RESULT%"=="SKIP" echo   - race detector skipped locally; rely on CI's go test -race
if "%INTEG_RESULT%"=="SKIP" echo   - integration suite needs the stack: scripts\deploy.bat local
if "%UNIT_RESULT%"=="FAIL" type "%WORK_DIR%\unit.out" | findstr /r /c:"^---" /c:"^FAIL"
if "%FAILED%"=="1" (echo   - one or more suites failed - fix before committing) else (echo   - all executed suites green)

exit /b %FAILED%
