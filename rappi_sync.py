"""
rappi_sync.py
=============
Lee los Google Sheets de Rappi → genera los archivos JS (GeoJSON) para el mapa web
y un CSV de reporte general.

Uso:
    python rappi_sync.py              # Sincroniza los datos
    python rappi_sync.py --list-tabs  # Lista las pestañas del Sheet y termina
    python rappi_sync.py --csv-only   # Solo genera el CSV de reporte

Requisitos:
    pip install -r requirements.txt
    Necesitas el archivo credentials.json con la Service Account de Google.
"""

import argparse
import json
import sys
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path

# Fix Unicode encoding on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

try:
    import gspread
    from google.oauth2.service_account import Credentials
    import pandas as pd
except ImportError:
    print("ERROR: Faltan dependencias. Ejecuta:  pip install -r requirements.txt")
    sys.exit(1)

# ─── CREDENCIALES ─────────────────────────────────────────────────────────────
CREDENTIALS_FILE = Path(__file__).parent / "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ─── DIRECTORIOS DE SALIDA ────────────────────────────────────────────────────
_public_data = Path(__file__).parent / "public" / "data"
if _public_data.exists():
    OUTPUT_DATA_DIR = _public_data
else:
    OUTPUT_DATA_DIR = (
        Path(__file__).parent
        / "4.Qgis" / "Carpeta Mapa WEB"
        / "qgis2web_2026_03_31-14_33_07_525507" / "data"
    )

OUTPUT_REPORT_DIR = Path(__file__).parent / "Reportes"

# ─── CONFIGURACIÓN POR REGIÓN ─────────────────────────────────────────────────
# Cada región puede tener su propio Sheet, pestaña y columnas.
# filter_col / filter_val: si se define, solo se procesan filas donde
#   filter_col == filter_val (útil cuando varias ciudades están en una misma pestaña).

REGIONS = {
    "CDMX": {
        # Google Sheet
        "sheet_id":   "1Bt2M3GpP5iy1T2_Rc8eEKspjgLUC3_EDYFwksFhpqQc",
        "tab":        "CDMX",
        # Salida JS
        "js_var":     "json_CDMX_PROCESOOINSTALADAS_1",
        "js_file":    "CDMX_PROCESOOINSTALADAS_1.js",
        "layer_name": "CDMX_PROCESOOINSTALADAS_1",
        # Columnas
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",   # Col W — valores: Instalada, Pendiente…
        "col_store_id": "Store ID",
        "col_nombre":  "Nombre de Local",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fecha de visita/Implementación",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "KAM",
        "col_comentarios": "Comentarios (Agencia)",
        # Filtro opcional (None = sin filtro)
        "filter_col": None,
        "filter_val": None,
        # Fila header (1-based). 1 = coordinación (ignorar), 2 = header real
        "header_row": 2,
    },
    "GDL": {
        # Google Sheet diferente
        "sheet_id":   "1-zLKpVhtXggaYAYo6uPkwwJO_nQD7eyN0iXVGSriq-g",
        "tab":        "West",
        # Salida JS
        "js_var":     "json_WEST_POP_GDL_WH_3",
        "js_file":    "WEST_POP_GDL_WH_3.js",
        "layer_name": "WEST_POP_GDL_WH_3",
        # Columnas (verificadas: idx 23="Estatus Final", 32="Latitud", 33="Longitud")
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",   # idx 23 — valores: Pendiente, En Proceso de Visita…
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fecha de visita/Implementación",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        # Filtro: solo Guadalajara
        "filter_col": "Ciudad",
        "filter_val": "Guadalajara",
        # Fila header
        "header_row": 2,
    },
    "PUE": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_PUE_POP_WH",
        "js_file":    "PUE_POP_WH.js",
        "layer_name": "PUE_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Puebla",
        "filter_normalize": True,
        "header_row": 2,
    },
    "MER": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_MER_POP_WH",
        "js_file":    "MER_POP_WH.js",
        "layer_name": "MER_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Merida",
        "filter_normalize": True,
        "header_row": 2,
    },
    "QRO": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_QRO_POP_WH",
        "js_file":    "QRO_POP_WH.js",
        "layer_name": "QRO_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Queretaro",
        "filter_normalize": True,
        "header_row": 2,
    },
    "VER": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_VER_POP_WH",
        "js_file":    "VER_POP_WH.js",
        "layer_name": "VER_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Veracruz",
        "filter_normalize": True,
        "header_row": 2,
    },
    "CUE": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_CUE_POP_WH",
        "js_file":    "CUE_POP_WH.js",
        "layer_name": "CUE_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Cuernavaca",
        "filter_normalize": True,
        "header_row": 2,
    },
    "CAN": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_CAN_POP_WH",
        "js_file":    "CAN_POP_WH.js",
        "layer_name": "CAN_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Cancun",
        "filter_normalize": True,
        "header_row": 2,
    },
    "PAC": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_PAC_POP_WH",
        "js_file":    "PAC_POP_WH.js",
        "layer_name": "PAC_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Pachuca",
        "filter_normalize": True,
        "header_row": 2,
    },
    "XAL": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_XAL_POP_WH",
        "js_file":    "XAL_POP_WH.js",
        "layer_name": "XAL_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Xalapa",
        "filter_normalize": True,
        "header_row": 2,
    },
    "PDC": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur",
        "js_var":     "json_PDC_POP_WH",
        "js_file":    "PDC_POP_WH.js",
        "layer_name": "PDC_POP_WH",
        "col_lat":    "Latitud",
        "col_lon":    "Longitud",
        "col_estatus": "Estatus Final",
        "col_estatus_idx": None,
        "col_store_id": "Store ID",
        "col_nombre":  "Store Name",
        "col_dir":     "Direccion (Local/Negocio)",
        "col_fecha":   "Fechas de visita",
        "col_testigo": ["Testigo (Agencia Link)"],
        "col_kam":     "Líder",
        "col_comentarios": "Comentarios (Agencia)",
        "filter_col": "Ciudad",
        "filter_val": "Playa Del Carmen",
        "filter_normalize": True,
        "header_row": 2,
    },
}

# ─── MAPEO DE ESTATUS → VALORES DEL MAPA ──────────────────────────────────────
# Los valores del Sheet (en minúsculas) → valor que espera el JS del mapa
STATUS_MAP = {
    "instalada":              "INSTALADA",
    "requiere contacto":      "REQUIERE CONTACTO",
    "visitada no instalado":  "VISITADA",
    "visitada no instalada":  "VISITADA",
    "visitada":               "VISITADA",
    "en proceso de visita":   "EN PROCESO",
    "en proceso":             "EN PROCESO",
    "rechazada":              "NO ACEPTO",
    "no acepto":              "NO ACEPTO",
    "no existe":              "YA NO EXISTE",
    "ya no existe":           "YA NO EXISTE",
    "pendiente":              "PENDIENTE",
    "churn/más de 4 semanas sin venta": "YA NO EXISTE",
    "churn":                  "YA NO EXISTE",
}


# ─── FUNCIONES ────────────────────────────────────────────────────────────────

def normalize_str(s):
    """Normaliza string: elimina acentos, convierte a minúsculas y elimina espacios extremos."""
    return unicodedata.normalize('NFD', str(s)).encode('ascii', 'ignore').decode('ascii').lower().strip()


def get_client():
    """Crea cliente gspread autenticado."""
    if not CREDENTIALS_FILE.exists():
        print(f"ERROR: No se encontró {CREDENTIALS_FILE}")
        sys.exit(1)
    creds = Credentials.from_service_account_file(str(CREDENTIALS_FILE), scopes=SCOPES)
    return gspread.authorize(creds)


def sheet_to_df(ws, header_row: int = 2, raw_values=None) -> pd.DataFrame:
    """Convierte una hoja de Sheets a DataFrame usando header_row como encabezado."""
    all_values = raw_values if raw_values is not None else ws.get_all_values()
    if not all_values or len(all_values) < header_row + 1:
        return pd.DataFrame()

    headers = all_values[header_row - 1]   # 1-based → 0-based
    rows = []
    for row_values in all_values[header_row:]:
        row_dict = {}
        for i, header in enumerate(headers):
            h = str(header).strip()
            val = row_values[i] if i < len(row_values) else ""
            # Para columnas sin header usamos su letra/índice como clave temporal
            key = h if h else f"__col_{i}__"
            row_dict[key] = val
        if any(str(v).strip() for v in row_dict.values()):
            rows.append(row_dict)

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.columns = [c.strip() for c in df.columns]
    return df


def resolve_estatus(row, cfg: dict) -> str:
    """Obtiene el valor de estatus de una fila según la configuración de la región."""
    # Columna por nombre
    if cfg.get("col_estatus"):
        raw = str(row.get(cfg["col_estatus"], "") or "").strip().lower()
        mapped = STATUS_MAP.get(raw, raw.upper() if raw else "")
        return mapped if mapped else "PENDIENTE"

    # Columna por índice (col sin header)
    idx = cfg.get("col_estatus_idx")
    if idx is not None:
        col_key = f"__col_{idx}__"
        raw = str(row.get(col_key, "") or "").strip().lower()
        mapped = STATUS_MAP.get(raw, raw.upper() if raw else "")
        return mapped if mapped else "PENDIENTE"

    return "PENDIENTE"


def df_to_geojson(df: pd.DataFrame, cfg: dict) -> dict:
    """Convierte DataFrame a GeoJSON FeatureCollection."""
    features = []
    skipped = 0

    col_lat = cfg["col_lat"]
    col_lon = cfg["col_lon"]

    # Búsqueda case-insensitive de coordenadas
    lat_col = next((c for c in df.columns if c.lower() == col_lat.lower()), None)
    lon_col = next((c for c in df.columns if c.lower() == col_lon.lower()), None)

    if not lat_col or not lon_col:
        print(f"    ✗ ERROR: No se encontraron columnas '{col_lat}' / '{col_lon}'")
        print(f"      Disponibles: {list(df.columns[:15])}...")
        return _empty_geojson(cfg["layer_name"])

    for _, row in df.iterrows():
        try:
            # Limpieza robusta: quitar comas al final, tomar solo el primer valor
            # si por error la celda tiene "lat, lon" juntos
            def _parse_coord(raw):
                s = str(raw or "").strip().rstrip(",").strip()
                # Si tiene coma interna (ej. "19.43, -99.13"), tomar solo primer parte
                if "," in s:
                    s = s.split(",")[0].strip()
                return float(s)

            lat = _parse_coord(row.get(lat_col, ""))
            lon = _parse_coord(row.get(lon_col, ""))
        except (ValueError, TypeError):
            skipped += 1
            continue

        # Filtrar 0,0 (placeholder vacío) y coordenadas fuera del rango de México
        if abs(lat) < 0.01 and abs(lon) < 0.01:
            skipped += 1
            continue
        if not (14.0 <= lat <= 33.0) or not (-118.5 <= lon <= -86.0):
            skipped += 1
            continue

        # Propiedades limpias
        props = {}
        for col, val in row.items():
            if col.startswith("__col_"):
                continue   # omite columnas sin nombre
            if val == "" or val is None:
                props[col] = None
            else:
                try:
                    props[col] = float(val) if "." in str(val) else int(val)
                except (ValueError, TypeError):
                    props[col] = val

        # Estatus normalizado (campo que usa el mapa para colores)
        props["ESTATUS FINAL"] = resolve_estatus(row, cfg)

        # Campo de foto testigo (toma el primero con valor)
        foto = ""
        for col_t in cfg.get("col_testigo", []):
            v = str(row.get(col_t, "") or "").strip()
            if v and v not in ("#N/A", "0"):
                foto = v
                break
        props["FOTO_TESTIGO"] = foto or None

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {
                "type": "Point",
                "coordinates": [round(lon, 7), round(lat, 7)]
            }
        })

    if skipped:
        print(f"    ⚠  {skipped} filas omitidas (sin coordenadas válidas)")

    return {
        "type": "FeatureCollection",
        "name": cfg["layer_name"],
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": features
    }


def _empty_geojson(layer_name):
    return {"type": "FeatureCollection", "name": layer_name,
            "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
            "features": []}


def write_js_file(geojson: dict, js_var: str, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = f"var {js_var} = {json.dumps(geojson, ensure_ascii=False, separators=(',', ':'))};"
    output_path.write_text(content, encoding="utf-8")
    size_kb = output_path.stat().st_size / 1024
    print(f"    ✓  Escrito: {output_path.name}  ({len(geojson['features'])} puntos, {size_kb:.1f} KB)")


def generate_csv_report(all_data: dict):
    """Genera CSVs de reporte.

    Por región: rappi_reporte_CDMX.csv, rappi_reporte_GDL.csv  (columnas nativas del sheet)
    Consolidado normalizado: rappi_reporte.csv  (solo columnas clave, mismo esquema para QGIS)
    """
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    if not all_data:
        print("  Sin datos para CSV.")
        return

    # ── 1. CSV por región (columnas nativas, útil para análisis en Excel) ──────
    for region, df in all_data.items():
        clean = df[[c for c in df.columns if not c.startswith("__col_")]].copy()
        clean["REGION"] = region
        path = OUTPUT_REPORT_DIR / f"rappi_reporte_{region}.csv"
        clean.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"    ✓  CSV {region}: {path.name}  ({len(clean)} filas)")

    # ── 2. CSV consolidado normalizado (esquema fijo para QGIS) ───────────────
    # Solo incluye columnas clave que existen en ambos sheets
    COLS_QGIS = [
        "REGION",
        "Store ID",
        "Store Name",
        "ESTATUS FINAL",
        "Latitud",
        "Longitud",
        "Ciudad",
    ]
    # Columnas opcionales: se incluyen si existen en al menos una región
    COLS_OPTIONAL = [
        "Dirección", "Direccion (Local/Negocio)",   # CDMX vs GDL
        "Fecha de visita/Implementación",
        "Comentarios (Agencia)",
        "FOTO_TESTIGO",
        "KAM", "Líder",
    ]

    norm_frames = []
    for region, df in all_data.items():
        dir_col = next((c for c in ["Dirección", "Direccion (Local/Negocio)"] if c in df.columns), None)
        kam_col = next((c for c in ["KAM", "Líder"] if c in df.columns), None)

        d = pd.DataFrame({
            "REGION":               region,
            "Store ID":             df.get("Store ID",    ""),
            "Store Name":           df.get("Store Name",  ""),
            "ESTATUS FINAL":        df["ESTATUS FINAL"] if "ESTATUS FINAL" in df.columns else "",
            "Latitud":              pd.to_numeric(df.get("Latitud",  ""), errors="coerce"),
            "Longitud":             pd.to_numeric(df.get("Longitud", ""), errors="coerce"),
            "Ciudad":               df.get("Ciudad", ""),
            "Dirección":            df[dir_col] if dir_col else "",
            "Fecha Implementación": df.get("Fecha de visita/Implementación", ""),
            "Comentarios":          df.get("Comentarios (Agencia)", ""),
            "KAM":                  df[kam_col] if kam_col else "",
        }, index=df.index)
        norm_frames.append(d)

    if norm_frames:
        full_df = pd.concat(norm_frames, ignore_index=True)

        # CSV fijo (siempre el mismo nombre — QGIS apunta aquí)
        csv_fixed = OUTPUT_REPORT_DIR / "rappi_reporte.csv"
        full_df.to_csv(csv_fixed, index=False, encoding="utf-8-sig")
        print(f"    ✓  CSV consolidado QGIS: {csv_fixed.name}  ({len(full_df)} filas)")

        # CSV histórico
        full_df.to_csv(OUTPUT_REPORT_DIR / f"rappi_reporte_{ts}.csv", index=False, encoding="utf-8-sig")

        # Resumen por estatus
        summary = (
            full_df.groupby(["REGION", "ESTATUS FINAL"])
            .size().reset_index(name="TOTAL")
        )
        summary.to_csv(OUTPUT_REPORT_DIR / "rappi_resumen.csv", index=False, encoding="utf-8-sig")
        print(f"    ✓  Resumen: rappi_resumen.csv")

        print("\n  ── Resumen de estatus ──────────────────────")
        try:
            pivot = summary.pivot(index="REGION", columns="ESTATUS FINAL", values="TOTAL").fillna(0).astype(int)
            pivot["TOTAL"] = pivot.sum(axis=1)
            print(pivot.to_string())
        except Exception:
            print(summary.to_string(index=False))
        print("  ────────────────────────────────────────────\n")


def generate_dashboard_json(all_data: dict):
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    regions_summary = {}
    comentarios = []

    for region, df in all_data.items():
        if df.empty:
            continue

        total = len(df)
        counts = {}
        if "ESTATUS FINAL" in df.columns:
            vc = df["ESTATUS FINAL"].value_counts().to_dict()
            for k, v in vc.items():
                counts[str(k)] = int(v)

        instaladas = counts.get("INSTALADA", 0)
        regions_summary[region] = {
            "total": total,
            "instaladas": instaladas,
            "pct_instalada": round((instaladas / total * 100) if total else 0, 1),
            "por_estatus": counts
        }

        # Comentarios
        for col in df.columns:
            if "comentario" in col.lower():
                sub = df[df[col].astype(str).str.strip().ne("") & df[col].notna()]
                for _, r in sub.head(20).iterrows():
                    comentarios.append({
                        "region": region,
                        "store_id": str(r.get("Store ID", "—")),
                        "negocio": str(r.get("Store Name", r.get("Brand Name", "—"))),
                        "comentario": str(r.get(col, "—"))
                    })
                break

    dashboard = {
        "actualizado": datetime.now().isoformat(),
        "regiones": regions_summary,
        "comentarios": comentarios[:50]
    }

    out = OUTPUT_REPORT_DIR / "dashboard_data.json"
    out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"    ✓  Dashboard data: {out.name}")
    return dashboard


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sincroniza Google Sheets de Rappi con el mapa web.")
    parser.add_argument("--list-tabs", action="store_true")
    parser.add_argument("--csv-only",  action="store_true")
    parser.add_argument("--region",    help="Procesa solo esta región (ej: CDMX, GDL)")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Rappi MKT POP — Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    client = get_client()

    # Cache de conexiones por sheet_id
    spreadsheets = {}

    def get_spreadsheet(sheet_id):
        if sheet_id not in spreadsheets:
            spreadsheets[sheet_id] = client.open_by_key(sheet_id)
        return spreadsheets[sheet_id]

    if args.list_tabs:
        for region, cfg in REGIONS.items():
            ss = get_spreadsheet(cfg["sheet_id"])
            print(f"\n[{region}] Sheet: '{ss.title}'")
            for i, ws in enumerate(ss.worksheets()):
                print(f"  [{i}] '{ws.title}'  ({ws.row_count} filas)")
        return

    all_data = {}
    regions_to_run = {k: v for k, v in REGIONS.items()
                      if not args.region or k == args.region.upper()}

    for region, cfg in regions_to_run.items():
        ss = get_spreadsheet(cfg["sheet_id"])
        print(f"Conectando: '{ss.title}'")

        tab = cfg["tab"]
        all_ws = {ws.title: ws for ws in ss.worksheets()}
        if tab not in all_ws:
            print(f"  ✗  Pestaña '{tab}' no encontrada. Disponibles: {list(all_ws.keys())}")
            continue

        print(f"[{region}]  Leyendo pestaña '{tab}'...")
        raw = all_ws[tab].get_all_values()
        df = sheet_to_df(None, header_row=cfg["header_row"], raw_values=raw)

        if df.empty:
            print(f"  ⚠  Sin datos.")
            continue

        # Aplicar filtro de ciudad si está configurado
        if cfg.get("filter_col") and cfg.get("filter_val"):
            fc = cfg["filter_col"]
            fv = cfg["filter_val"]
            if fc in df.columns:
                before = len(df)
                if cfg.get("filter_normalize"):
                    df = df[df[fc].apply(normalize_str) == normalize_str(fv)].reset_index(drop=True)
                else:
                    df = df[df[fc].str.strip() == fv].reset_index(drop=True)
                print(f"    → Filtro '{fc}' = '{fv}': {len(df)} / {before} filas")
            else:
                print(f"    ⚠  Columna de filtro '{fc}' no encontrada")

        print(f"    → {len(df)} filas cargadas")

        # Añade ESTATUS FINAL al df para que esté disponible en CSV y dashboard
        df["ESTATUS FINAL"] = df.apply(lambda row: resolve_estatus(row, cfg), axis=1)
        all_data[region] = df

        if not args.csv_only:
            geojson = df_to_geojson(df, cfg)
            out_path = OUTPUT_DATA_DIR / cfg["js_file"]
            write_js_file(geojson, cfg["js_var"], out_path)
        print()

    print("Generando reportes...\n")
    generate_csv_report(all_data)
    generate_dashboard_json(all_data)

    print(f"\n{'='*55}")
    print("  ✓  Sincronización completa")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado.")
    except Exception as e:
        print(f"\nERROR inesperado: {e}")
        traceback.print_exc()
        sys.exit(1)
