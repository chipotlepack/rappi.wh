# Rappi MKT POP 2026 - Mapa y Reportes Interactivos

Sistema automatizado de sincronización de datos desde Google Sheets a un mapa interactivo y reportes PDF con imágenes testigo.

## 📋 Características

- 📍 **Mapa Interactivo**: Visualiza 20,227 ubicaciones de instalación en tiempo real
- 📊 **Dashboard**: Estadísticas de estatus e instalaciones
- 📄 **Reportes PDF**: Reportes diarios, semanales y mensuales con imágenes
- 🔄 **Sincronización Automática**: Actualización automática desde Google Sheets
- 🌐 **Deployment Continuo**: Actualización automática en el sitio web

## 🗂️ Estructura de Carpetas

```
rappi.wh/
├── public/                   # Archivos estáticos para el sitio web
│   ├── index.html           # Mapa interactivo (QGIS2Web)
│   ├── data/
│   │   └── CDMX_PROCESOOINSTALADAS_1.js  # GeoJSON de puntos
│   ├── js/                  # Librerías JavaScript
│   ├── css/                 # Estilos
│   └── images/              # Imágenes estáticas
├── rappi_sync.py            # Script de sincronización (Lee Sheet → genera JS)
├── rappi_reports.py         # Script de reportes PDF
├── dashboard.html           # Dashboard con Chart.js
├── requirements.txt         # Dependencias Python
└── sync_rappi.bat           # Script batch para Windows Task Scheduler
```

## 🚀 Configuración Inicial

### 1. Requisitos

- Python 3.8+
- Google Service Account (con acceso a Google Sheets)
- Git

### 2. Instalación de Dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar Credenciales de Google

1. Descarga el archivo `credentials.json` desde la Service Account de Google
2. Colócalo en la raíz del proyecto

## 🔄 Ejecutar la Sincronización

### Generar Mapa (JS) y Dashboard

```bash
python rappi_sync.py
```

Genera:
- `public/data/CDMX_PROCESOOINSTALADAS_1.js` (20,227 puntos, ~24 MB)
- `Reportes/dashboard_data.json` (estadísticas y comentarios)
- `Reportes/rappi_reporte_*.csv` (datos consolidados)

### Generar Reportes PDF

```bash
python rappi_reports.py              # Reporte del día anterior
python rappi_reports.py --weekly     # Reporte semanal (últimos 7 días)
python rappi_reports.py --monthly    # Reporte mensual (últimos 30 días)
```

Genera:
- `Reportes/rappi_reporte_diario_*.pdf`
- `Reportes/rappi_reporte_semanal_*.pdf`
- `Reportes/rappi_reporte_mensual_*.pdf`

### Automático (Windows Task Scheduler)

```batch
sync_rappi.bat
```

Ejecuta ambos scripts automáticamente.

## 📊 Categorías de Estatus

El sistema categoriza los estatus en:

1. **Instalada** - Restaurante instalado ✓
2. **Rechazada** - No existe / Visitada no instalada / Ya no existe
3. **Requiere contacto** - Necesita contacto
4. **En proceso de visita** - En proceso / En ruta

*Nota: Se excluye "Pendiente" de todos los reportes*

## 📍 Columnas Utilizadas del Google Sheet

| Columna | Nombre | Propósito |
|---------|--------|----------|
| W | Estatus Final | Categorización de estatus |
| AB | Fecha de Implementación | Filtrado de reportes por fecha |
| AC | FOTO TESTIGO | Foto testigo 1 (para PDFs) |
| AD | Foto Testigo 2 | Foto testigo 2 (alternativa) |
| L | KAM | Email del KAM responsable |
| AF | Latitud | Coordenada Y |
| AG | Longitud | Coordenada X |

## 🌐 Deployment (Cloudflare Pages)

El repositorio se sincroniza automáticamente con Cloudflare Pages:

1. Los cambios en `main` → GitHub → Cloudflare (Auto-deploy)
2. El sitio se actualiza en: https://warehouse-mx.com/rappi-wh-m2026

## 🤖 Automatización (Windows Task Scheduler)

Para ejecutar la sincronización automáticamente cada día:

1. Abre **Task Scheduler** en Windows
2. Crea una nueva tarea que ejecute `sync_rappi.bat`
3. Configura el horario deseado (ej: 6:00 AM diariamente)
4. La tarea ejecutará:
   - `python rappi_sync.py` (genera el mapa)
   - `python rappi_reports.py` (genera reporte diario)
   - Git push automático (si se configura)

## 📈 Volumen de Datos

- **Total de registros**: 20,248
- **Puntos mapeados**: 20,227 (21 omitidos sin coordenadas)
- **Tamaño del archivo JS**: ~24 MB
- **Formatos de salida**: GeoJSON, PDF, CSV, JSON Dashboard

## 🐛 Troubleshooting

### Error: "No se encontró credentials.json"
→ Descarga las credenciales de Google Service Account y coloca el archivo `credentials.json` en la raíz

### Error: "No se encontraron datos"
→ Verifica que el Google Sheet tenga datos en las filas correctas (header en fila 2, datos desde fila 3)

### Error Unicode en Windows
→ Los scripts ya incluyen soporte para UTF-8 en Windows

## 📝 Notas Importantes

- El Google Sheet tiene una fila de coordinación en la fila 1 (se ignora automáticamente)
- Los headers oficiales están en la fila 2
- Los datos comienzan en la fila 3
- El sistema detecta automáticamente las columnas (case-insensitive)

## 👥 Contacto

Para cambios o reportes de problemas, contacta al equipo de desarrollo.
