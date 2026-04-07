@echo off
chcp 65001 >nul
:: ============================================================
::  sync_rappi.bat — Sincroniza Rappi MKT POP 2026
::  1. Lee Google Sheets → genera JS del mapa
::  2. Copia datos al folder public/
::  3. Git commit + push → Cloudflare redeploya automáticamente
::  Programar en Task Scheduler para ejecución diaria
:: ============================================================
cd /d "%~dp0"

set LOG_FILE=%~dp0Reportes\sync_log.txt
echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo   Sync iniciado: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

echo.
echo ============================================================
echo   Rappi MKT POP ^| Sincronización Automática
echo   %date% %time%
echo ============================================================
echo.

:: ── PASO 1: Verificar Python ────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no encontrado >> "%LOG_FILE%"
    echo ERROR: Python no encontrado. Instálalo desde python.org
    exit /b 1
)

:: ── PASO 2: Sincronizar datos del mapa ──────────────────────
echo [1/3] Sincronizando Google Sheets → mapa...
python rappi_sync.py >> "%LOG_FILE%" 2>&1

if errorlevel 1 (
    echo ERROR: rappi_sync.py falló. Revisa %LOG_FILE% >> "%LOG_FILE%"
    echo ERROR: La sincronización falló. Revisa el log:
    echo   %LOG_FILE%
    exit /b 1
)
echo   OK - Mapa actualizado

:: ── PASO 3: Copiar dashboard_data.json a public/ ────────────
echo [2/3] Actualizando datos del dashboard...
copy /Y "Reportes\dashboard_data.json" "public\dashboard_data.json" >nul 2>&1
if errorlevel 1 (
    echo ADVERTENCIA: No se pudo copiar dashboard_data.json >> "%LOG_FILE%"
) else (
    echo   OK - Dashboard data actualizado
)

:: ── PASO 4: Git commit y push → Cloudflare redeploya ────────
echo [3/3] Subiendo cambios a GitHub...
git add public\data\CDMX_PROCESOOINSTALADAS_1.js public\dashboard_data.json >> "%LOG_FILE%" 2>&1
git commit -m "Auto-sync: %date% %time%" >> "%LOG_FILE%" 2>&1
git push origin master >> "%LOG_FILE%" 2>&1

if errorlevel 1 (
    echo ADVERTENCIA: Git push falló. Revisa conexión a internet. >> "%LOG_FILE%"
    echo ADVERTENCIA: Git push falló. El mapa no se actualizó en línea.
) else (
    echo   OK - Cloudflare redesplegando (2-3 min)
    echo   OK - Git push exitoso >> "%LOG_FILE%"
)

echo.
echo ============================================================
echo   Sincronización completa: %date% %time%
echo   Log: Reportes\sync_log.txt
echo ============================================================
echo. >> "%LOG_FILE%"
echo Sync completado: %date% %time% >> "%LOG_FILE%"
