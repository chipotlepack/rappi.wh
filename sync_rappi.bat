@echo off
chcp 65001 >nul
:: ============================================================
::  sync_rappi.bat — Sincroniza Rappi MKT POP 2026
::  1. Lee Google Sheets → genera JS del mapa
::  2. Copia datos al folder public/
::  3. Git commit + push → Cloudflare redeploya automáticamente
::  Programar en Task Scheduler: Al inicio + 30 min de delay
:: ============================================================
cd /d "%~dp0"

set LOG_FILE=%~dp0Reportes\sync_log.txt
set ERROR_MSG=

echo. >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"
echo   Sync iniciado: %date% %time% >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

:: ── PASO 1: Verificar Python ────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    set ERROR_MSG=Python no encontrado. Instálalo desde python.org
    echo ERROR: %ERROR_MSG% >> "%LOG_FILE%"
    goto :ERROR
)

:: ── PASO 2: Sincronizar datos del mapa ──────────────────────
echo [1/3] Sincronizando Google Sheets... >> "%LOG_FILE%"
python rappi_sync.py >> "%LOG_FILE%" 2>&1

if errorlevel 1 (
    set ERROR_MSG=rappi_sync.py fallo al leer Google Sheets
    echo ERROR: %ERROR_MSG% >> "%LOG_FILE%"
    goto :ERROR
)
echo   OK - Mapa actualizado >> "%LOG_FILE%"

:: ── PASO 3: Copiar dashboard_data.json a public/ ────────────
copy /Y "Reportes\dashboard_data.json" "public\dashboard_data.json" >nul 2>&1

:: ── PASO 4: Git commit y push → Cloudflare redeploya ────────
echo [2/3] Subiendo a GitHub... >> "%LOG_FILE%"
git add public\data\CDMX_PROCESOOINSTALADAS_1.js public\dashboard_data.json >> "%LOG_FILE%" 2>&1
git commit -m "Auto-sync: %date% %time%" >> "%LOG_FILE%" 2>&1
git push origin master >> "%LOG_FILE%" 2>&1

if errorlevel 1 (
    set ERROR_MSG=Git push fallo. Revisa tu conexion a internet
    echo ERROR: %ERROR_MSG% >> "%LOG_FILE%"
    goto :ERROR
)

echo   OK - GitHub actualizado >> "%LOG_FILE%"
echo Sync completado OK: %date% %time% >> "%LOG_FILE%"

:: Notificación de éxito (silenciosa, solo en log)
powershell -Command "Add-Type -AssemblyName System.Windows.Forms; $n = New-Object System.Windows.Forms.NotifyIcon; $n.Icon = [System.Drawing.SystemIcons]::Information; $n.Visible = $true; $n.ShowBalloonTip(5000, 'Rappi MKT POP', 'Mapa actualizado correctamente ✓', 'Info'); Start-Sleep 6; $n.Dispose()" >nul 2>&1
goto :EOF

:: ── ERROR: Notificación visible ─────────────────────────────
:ERROR
echo. >> "%LOG_FILE%"
echo !! SYNC FALLIDO: %ERROR_MSG% >> "%LOG_FILE%"
echo Log: %LOG_FILE% >> "%LOG_FILE%"

powershell -Command ^
  "Add-Type -AssemblyName System.Windows.Forms; ^
   $n = New-Object System.Windows.Forms.NotifyIcon; ^
   $n.Icon = [System.Drawing.SystemIcons]::Error; ^
   $n.Visible = $true; ^
   $n.ShowBalloonTip(15000, 'Rappi MKT POP - ERROR', '%ERROR_MSG%. Revisa: Reportes\sync_log.txt', 'Error'); ^
   Start-Sleep 16; ^
   $n.Dispose()"

exit /b 1
