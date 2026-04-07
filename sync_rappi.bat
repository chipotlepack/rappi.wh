@echo off
:: ============================================================
::  sync_rappi.bat — Sincroniza Rappi MKT POP 2026
::  - Actualiza mapas (JS GeoJSON)
::  - Genera reportes PDF
::  Puedes programar este archivo en el Programador de Tareas
:: ============================================================
cd /d "%~dp0"

echo.
echo ============================================================
echo   Rappi MKT POP — Sincronización Completa
echo ============================================================
echo.

:: Verifica que Python esté instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no encontrado. Instálalo desde python.org
    pause
    exit /b 1
)

:: Instala dependencias si no están
echo [1/3] Verificando dependencias...
python -m pip install -q -r requirements.txt

:: Sincroniza mapas (genera JS)
echo.
echo [2/3] Sincronizando datos del mapa...
python rappi_sync.py

if errorlevel 1 (
    echo.
    echo ERROR: La sincronización falló. Revisa el log arriba.
    pause
    exit /b 1
)

:: Genera reporte diario
echo.
echo [3/3] Generando reporte del día...
python rappi_reports.py

echo.
echo ============================================================
echo   ✓ Sincronización Completa
echo ============================================================
echo.
echo   Archivos generados:
echo   - Mapa JS: 4.Qgis\Carpeta Mapa WEB\...\data\CDMX_PROCESOOINSTALADAS_1.js
echo   - Reportes: Reportes\rappi_reporte_diario_YYYYMMDD_HHMM.pdf
echo   - Dashboard: Reportes\dashboard.html
echo   - CSV: Reportes\rappi_reporte_YYYYMMDD_HHMM.csv
echo.
echo   Próximo: Sube los archivos JS a tu servidor (Cloudflare)
echo.
pause
