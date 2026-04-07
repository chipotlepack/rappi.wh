"""
rappi_sync.py
=============
Lee el Google Sheet de Rappi → genera los 3 archivos JS (GeoJSON) para el mapa web
y un CSV de reporte general.

Uso:
    python rappi_sync.py              # Sincroniza los datos
    python rappi_sync.py --list-tabs  # Lista las pestañas del Sheet
    python rappi_sync.py --csv-only   # Solo genera el CSV de reporte

Requisitos:
    pip install -r requirements.txt
    Necesitas el archivo credentials.json con la Service Account de Google.
    Ver README_SETUP.md para obtenerlo.
"""

import argparse
import json
import os
import sys
import traceback
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

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

SHEET_ID = "1Bt2M3GpP5iy1T2_Rc8eEKspjgLUC3_EDYFwksFhpqQc"

# Ruta al archivo de credenciales de Google Service Account
CREDENTIALS_FILE = Path(__file__).parent / "credentials.json"

# Directorio donde se guardan los JS del mapa web (carpeta data/ del proyecto)
# Usa public/data si existe (para GitHub deployment), sino usa la carpeta QGIS
_public_data = Path(__file__).parent / "public" / "data"
if _public_data.exists():
    OUTPUT_DATA_DIR = _public_data
else:
    OUTPUT_DATA_DIR = Path(__file__).parent / "4.Qgis" / "Carpeta Mapa WEB" / \
        "qgis2web_2026_03_31-14_33_07_525507" / "data"

# Directorio donde se guarda el CSV de reporte
OUTPUT_REPORT_DIR = Path(__file__).parent / "Reportes"

# ─── MAPEO DE PESTAÑAS A REGIONES ─────────────────────────────────────────────
# Clave = nombre EXACTO de la pestaña en Google Sheets
# Si no sabes los nombres, corre: python rappi_sync.py --list-tabs
REGION_MAP = {
    # "Nombre pestaña Sheet": configuración de salida
    "CDMX": {
        "js_var":   "json_CDMX_PROCESOOINSTALADAS_1",
        "js_file":  "CDMX_PROCESOOINSTALADAS_1.js",
        "layer_name": "CDMX_PROCESOOINSTALADAS_1",
        "label":    "CDMX"
    },
    # "Seg CDMX 🚀📍": {
    #     "js_var":   "json_NORTH_POP_WH_MTY_2",
    #     "js_file":  "NORTH_POP_WH_MTY_2.js",
    #     "layer_name": "NORTH_POP_WH_MTY_2",
    #     "label":    "Monterrey"
    # },
    # "Seguimiento": {
    #     "js_var":   "json_WEST_POP_GDL_WH_3",
    #     "js_file":  "WEST_POP_GDL_WH_3.js",
    #     "layer_name": "WEST_POP_GDL_WH_3",
    #     "label":    "Guadalajara"
    # },
}

# ─── MAPEO DE COLUMNAS DEL GOOGLE SHEET ──────────────────────────────────────
# Geometría
COL_LAT = "Latitud"
COL_LON = "Longitud"

# Campos para el popup del mapa
COL_STORE_ID = "Store ID"          # ID del restaurante
COL_ESTATUS_FINAL = "Estatus Final"  # Columna W
COL_DIRECCION = "Dirección"        # Dirección
COL_COMENTARIOS = "Comentarios"    # Comentarios (si existe)
COL_TESTIGO_1 = "FOTO TESTIGO"     # Columna AC - imagen testigo
COL_TESTIGO_2 = "Foto Testigo 2"   # Columna AD - alternativa
COL_KAM_EMAIL = "KAM"              # Columna L - correo del KAM

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ─── FUNCIONES ────────────────────────────────────────────────────────────────

def connect_sheet():
    """Conecta con Google Sheets via Service Account."""
    if not CREDENTIALS_FILE.exists():
        print(f"ERROR: No se encontró {CREDENTIALS_FILE}")
        print("Descarga las credenciales de tu Service Account y colócalas ahí.")
        sys.exit(1)
    creds = Credentials.from_service_account_file(str(CREDENTIALS_FILE), scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID)


def list_tabs(spreadsheet):
    """Imprime todas las pestañas disponibles."""
    print("\nPestañas disponibles en el Sheet:")
    for i, ws in enumerate(spreadsheet.worksheets()):
        rows = ws.row_count
        print(f"  [{i}] '{ws.title}'  ({rows} filas)")
    print("\nActualiza REGION_MAP en rappi_sync.py con los nombres exactos.")


def sheet_to_df(worksheet) -> pd.DataFrame:
    """Convierte una hoja de Sheets a DataFrame.

    Nota: La fila 1 se ignora (coordinación).
    La fila 2 es el header oficial.
    Los datos comienzan en la fila 3.
    """
    all_values = worksheet.get_all_values()
    if not all_values or len(all_values) < 3:
        print(f"    ⚠  Hoja vacía o insuficientes filas")
        return pd.DataFrame()

    # Usa la fila 2 como headers (índice 1)
    headers = all_values[1]
    rows = []

    # Lee desde la fila 3 en adelante (índice 2+)
    for row_values in all_values[2:]:
        row_dict = {}
        for i, header in enumerate(headers):
            h_clean = str(header).strip()
            # Salta headers vacíos
            if not h_clean:
                continue
            # Obtén el valor seguro
            val = row_values[i] if i < len(row_values) else ""
            row_dict[h_clean] = val

        # Solo agrega si tiene al menos algunos datos
        if any(str(v).strip() for v in row_dict.values()):
            rows.append(row_dict)

    if not rows:
        print(f"    ⚠  No se encontraron datos")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Limpia espacios en nombres de columnas
    df.columns = [c.strip() for c in df.columns]
    return df


def df_to_geojson(df: pd.DataFrame, layer_name: str) -> dict:
    """Convierte un DataFrame a GeoJSON FeatureCollection."""
    features = []
    skipped = 0

    # Busca las columnas de coordenadas (case-insensitive)
    col_lat = None
    col_lon = None
    for col in df.columns:
        if col.lower() == COL_LAT.lower():
            col_lat = col
        if col.lower() == COL_LON.lower():
            col_lon = col

    if not col_lat or not col_lon:
        print(f"    ✗ ERROR: No se encontraron columnas de coordenadas")
        print(f"      Buscaba: '{COL_LAT}' y '{COL_LON}'")
        print(f"      Disponibles: {list(df.columns)[:10]}...")
        return {"type": "FeatureCollection", "name": layer_name, "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}}, "features": []}

    for _, row in df.iterrows():
        try:
            lat = float(str(row.get(col_lat, "")).replace(",", ".").strip())
            lon = float(str(row.get(col_lon, "")).replace(",", ".").strip())
        except (ValueError, TypeError):
            skipped += 1
            continue

        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            skipped += 1
            continue

        # Construye propiedades limpiando valores nulos
        props = {}
        for col, val in row.items():
            if val == "" or val is None:
                props[col] = None
            else:
                # Intenta conservar numéricos donde corresponda
                try:
                    props[col] = float(val) if "." in str(val) else int(val)
                except (ValueError, TypeError):
                    props[col] = val

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {
                "type": "Point",
                "coordinates": [round(lon, 7), round(lat, 7)]
            }
        })

    if skipped > 0:
        print(f"    ⚠  {skipped} filas omitidas (sin coordenadas válidas)")

    return {
        "type": "FeatureCollection",
        "name": layer_name,
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
        },
        "features": features
    }


def write_js_file(geojson: dict, js_var: str, output_path: Path):
    """Escribe el archivo .js con la variable GeoJSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    geojson_str = json.dumps(geojson, ensure_ascii=False, separators=(",", ":"))
    content = f"var {js_var} = {geojson_str};"
    output_path.write_text(content, encoding="utf-8")
    size_kb = output_path.stat().st_size / 1024
    print(f"    ✓  Escrito: {output_path.name}  ({len(geojson['features'])} puntos, {size_kb:.1f} KB)")


def generate_csv_report(all_data: dict):
    """Genera CSV de reporte consolidado y por región."""
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    frames = []
    for region, df in all_data.items():
        df = df.copy()
        df["__REGION__"] = region
        frames.append(df)

    if not frames:
        print("  Sin datos para CSV.")
        return

    full_df = pd.concat(frames, ignore_index=True)

    # CSV completo
    csv_path = OUTPUT_REPORT_DIR / f"rappi_reporte_{ts}.csv"
    full_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"    ✓  CSV completo: {csv_path.name}")

    # Resumen por estatus y región
    if "ESTATUS FINAL" in full_df.columns:
        summary = (
            full_df.groupby(["__REGION__", "ESTATUS FINAL"])
            .size()
            .reset_index(name="TOTAL")
        )
        summary_path = OUTPUT_REPORT_DIR / f"rappi_resumen_{ts}.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"    ✓  Resumen por estatus: {summary_path.name}")

        # Imprime tabla rápida en consola
        print("\n  ── Resumen de estatus ──────────────────────")
        pivot = summary.pivot(index="__REGION__", columns="ESTATUS FINAL", values="TOTAL").fillna(0).astype(int)
        pivot["TOTAL"] = pivot.sum(axis=1)
        print(pivot.to_string())
        print("  ────────────────────────────────────────────\n")


def generate_dashboard_json(all_data: dict):
    """Genera un dashboard_data.json que puede consumir el dashboard HTML."""
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    statuses = ["INSTALADA", "EN PROCESO", "VISITADA", "NO ACEPTO", "REQUIERE CONTACTO", "YA NO EXISTE"]
    regions_summary = {}

    for region, df in all_data.items():
        if df.empty:
            continue
        counts = {}
        total = len(df)
        if "Estatus Final" in df.columns:
            vc = df["Estatus Final"].str.strip().str.upper().value_counts().to_dict()
            for s in statuses:
                counts[s] = int(vc.get(s, 0))
        instaladas = counts.get("INSTALADA", 0)
        regions_summary[region] = {
            "total": total,
            "instaladas": instaladas,
            "pct_instalada": round((instaladas / total * 100) if total else 0, 1),
            "por_estatus": counts
        }

    # Rutas activas (EN PROCESO)
    rutas_activas = []
    for region, df in all_data.items():
        if df.empty or "Estatus Final" not in df.columns:
            continue
        activas = df[df["Estatus Final"].str.strip().str.upper() == "EN PROCESO"]
        if "SEMANA OPERATIVA" in df.columns:
            semanas = activas["SEMANA OPERATIVA"].dropna().unique().tolist()
            for sem in semanas:
                if str(sem).strip():
                    rutas_activas.append({"region": region, "semana": str(sem).strip()})

    # Comentarios recientes (columna AE o "Comentarios")
    comentarios = []
    for region, df in all_data.items():
        if df.empty:
            continue
        # Busca columna de comentarios (intenta varias variantes)
        col_comentarios = None
        for col in df.columns:
            if "comentario" in col.lower() or col.lower() in ["ae", "comentarios (agencia)"]:
                col_comentarios = col
                break

        if col_comentarios:
            comentarios_region = df[
                (df[col_comentarios].notna()) &
                (df[col_comentarios].astype(str).str.strip() != "")
            ][["Store ID", "Brand Name", col_comentarios]].head(20).to_dict('records')

            for com in comentarios_region:
                comentarios.append({
                    "region": region,
                    "store_id": str(com.get("Store ID", "—")),
                    "negocio": str(com.get("Brand Name", "—")),
                    "comentario": str(com.get(col_comentarios, "—"))
                })

    dashboard = {
        "actualizado": datetime.now().isoformat(),
        "regiones": regions_summary,
        "rutas_activas": rutas_activas,
        "comentarios": comentarios[:50]  # Top 50 comentarios
    }

    out_path = OUTPUT_REPORT_DIR / "dashboard_data.json"
    out_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"    ✓  Dashboard data: {out_path.name}")
    return dashboard


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sincroniza Google Sheet de Rappi con el mapa web.")
    parser.add_argument("--list-tabs", action="store_true", help="Lista pestañas del Sheet y termina")
    parser.add_argument("--csv-only", action="store_true", help="Solo genera CSV, sin actualizar JS")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Rappi MKT POP — Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    print("Conectando con Google Sheets...")
    spreadsheet = connect_sheet()
    print(f"  ✓  Conectado: '{spreadsheet.title}'\n")

    if args.list_tabs:
        list_tabs(spreadsheet)
        return

    # Carga todas las pestañas configuradas
    all_worksheets = {ws.title: ws for ws in spreadsheet.worksheets()}
    all_data = {}

    for tab_name, config in REGION_MAP.items():
        print(f"[{config['label']}]  Leyendo pestaña '{tab_name}'...")
        if tab_name not in all_worksheets:
            print(f"  ✗  Pestaña '{tab_name}' no encontrada. Verifica REGION_MAP.")
            available = list(all_worksheets.keys())
            print(f"     Disponibles: {available}")
            continue

        ws = all_worksheets[tab_name]
        df = sheet_to_df(ws)
        if df.empty:
            print(f"  ⚠  Pestaña vacía o sin datos.")
            continue

        print(f"    → {len(df)} filas cargadas")
        all_data[config["label"]] = df

        if not args.csv_only:
            geojson = df_to_geojson(df, config["layer_name"])
            out_path = OUTPUT_DATA_DIR / config["js_file"]
            write_js_file(geojson, config["js_var"], out_path)
        print()

    # Reportes
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
        print("\nCancelado por el usuario.")
    except Exception as e:
        print(f"\nERROR inesperado: {e}")
        traceback.print_exc()
        sys.exit(1)
