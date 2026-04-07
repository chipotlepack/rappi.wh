"""
rappi_reports.py
================
Genera reportes PDF diarios, semanales y mensuales con imágenes de testigo.

Categorías (sin Pendiente):
  1. Instalada
  2. Rechazada (No existe + Visitada no instalada)
  3. Requiere contacto
  4. En proceso de visita

Uso:
    python rappi_reports.py              # Reporte del día
    python rappi_reports.py --weekly     # Reporte semanal
    python rappi_reports.py --monthly    # Reporte mensual
"""

import argparse
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Fix Unicode encoding on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

try:
    import pandas as pd
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    import gspread
    from google.oauth2.service_account import Credentials
    import requests
    from io import BytesIO
    from PIL import Image as PILImage
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    print("ERROR: Faltan dependencias. Ejecuta: pip install -r requirements.txt")
    sys.exit(1)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

SHEET_ID = "1Bt2M3GpP5iy1T2_Rc8eEKspjgLUC3_EDYFwksFhpqQc"
CREDENTIALS_FILE = Path(__file__).parent / "credentials.json"
OUTPUT_REPORT_DIR = Path(__file__).parent / "Reportes"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Mapeo de estatus a categorías (SIN PENDIENTE)
STATUS_CATEGORIES = {
    "INSTALADA": "Instalada",
    "NO EXISTE": "Rechazada",
    "VISITADA NO INSTALADA": "Rechazada",
    "YA NO EXISTE": "Rechazada",
    "REQUIERE CONTACTO": "Requiere contacto",
    "EN PROCESO": "En proceso de visita",
    "EN RUTA": "En proceso de visita",
}

# ─── FUNCIONES ────────────────────────────────────────────────────────────────

def connect_sheet():
    """Conecta con Google Sheets."""
    if not CREDENTIALS_FILE.exists():
        print(f"ERROR: No se encontró {CREDENTIALS_FILE}")
        sys.exit(1)
    creds = Credentials.from_service_account_file(str(CREDENTIALS_FILE), scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID)


def sheet_to_df(worksheet) -> pd.DataFrame:
    """Lee la hoja y retorna DataFrame."""
    all_values = worksheet.get_all_values()
    if not all_values or len(all_values) < 3:
        return pd.DataFrame()

    headers = all_values[1]
    rows = []

    for row_values in all_values[2:]:
        row_dict = {}
        for i, header in enumerate(headers):
            h_clean = str(header).strip()
            if not h_clean:
                continue
            val = row_values[i] if i < len(row_values) else ""
            row_dict[h_clean] = val

        if any(str(v).strip() for v in row_dict.values()):
            rows.append(row_dict)

    return pd.DataFrame(rows)


def categorize_status(status_str) -> str:
    """Mapea un estatus a categoría (sin Pendiente)."""
    status_clean = str(status_str).strip().upper()
    for key, category in STATUS_CATEGORIES.items():
        if key in status_clean:
            return category
    return None  # Excluye Pendiente


def filter_by_date_range(df: pd.DataFrame, days_back: int, exact_day: bool = False) -> pd.DataFrame:
    """Filtra datos por rango de fechas usando FECHA DE IMPLEMENTACIÓN (columna AB).

    Args:
        df: DataFrame con datos
        days_back: número de días hacia atrás
        exact_day: si True, filtra solo ese día exacto (para reporte diario)
    """
    if df.empty:
        return df

    # Busca la columna de fecha de implementación (intenta varias variantes)
    col_fecha = None
    for col in df.columns:
        col_lower = col.lower()
        if "implementaci" in col_lower or col_lower == "ab":
            col_fecha = col
            break

    if not col_fecha:
        print(f"    ⚠  Advertencia: No se encontró columna de fecha de implementación")
        return df

    try:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")

        if exact_day:
            # Para reporte diario: solo ayer (1 día antes)
            target_date = (datetime.now() - timedelta(days=1)).date()
            filtered = df[df[col_fecha].dt.date == target_date].copy()
            print(f"    → Filtrando por {target_date}: {len(filtered)} registros")
            return filtered
        else:
            # Para reportes semanales/mensuales: rango de fechas
            cutoff = datetime.now() - timedelta(days=days_back)
            filtered = df[df[col_fecha] >= cutoff].copy()
            print(f"    → Filtrando últimos {days_back} días: {len(filtered)} registros")
            return filtered
    except Exception as e:
        print(f"    ⚠  Error al filtrar por fecha: {e}")
        return df


def download_image(url: str, max_width: float = 1.5) -> Image:
    """Descarga una imagen desde URL y la convierte a objeto reportlab Image."""
    try:
        if not url or str(url).strip() == "":
            return None

        url_clean = str(url).strip()
        if not url_clean.startswith("http"):
            return None

        # Descarga la imagen
        response = requests.get(url_clean, timeout=5, verify=False)
        if response.status_code != 200:
            return None

        img = PILImage.open(BytesIO(response.content))
        img.thumbnail((int(max_width * 72), int(max_width * 72 * 1.5)))

        # Guarda temporalmente
        temp_path = OUTPUT_REPORT_DIR / f"temp_img_{hash(url_clean) % 10000}.png"
        img.save(str(temp_path), "PNG")

        return Image(str(temp_path), width=max_width * inch, height=max_width * inch * 1.5)
    except Exception as e:
        return None


def generate_pdf_report(df: pd.DataFrame, filename: str, period_name: str):
    """Genera PDF con categorías (sin Pendiente) e imágenes."""
    if df.empty:
        print(f"    ⚠  No hay datos para el reporte")
        return

    # Categoriza y excluye Pendiente
    df["_Categoria"] = df["Estatus Final"].apply(categorize_status)
    df = df[df["_Categoria"].notna()]  # Excluye None (Pendiente)

    if df.empty:
        print(f"    ⚠  No hay datos después de filtrar")
        return

    # Resumen por categoría
    summary = df.groupby("_Categoria").size().reset_index(name="Total")
    summary = summary.sort_values("Total", ascending=False)

    # Crea PDF
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    pdf_path = OUTPUT_REPORT_DIR / filename
    doc = SimpleDocTemplate(str(pdf_path), pagesize=letter)
    story = []
    styles = getSampleStyleSheet()

    # Título
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#FF441F'),
        spaceAfter=6,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold',
    )
    story.append(Paragraph("Rappi MKT POP 2026", title_style))
    story.append(Paragraph(f"Reporte {period_name}", styles['Heading2']))
    story.append(Paragraph(f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles['Normal']))
    story.append(Spacer(1, 0.3 * inch))

    # Tabla resumen
    summary_data = [["Categoría", "Total", "% del Total"]]
    total = summary["Total"].sum()
    for _, row in summary.iterrows():
        pct = (row["Total"] / total * 100) if total > 0 else 0
        summary_data.append([
            row["_Categoria"],
            str(row["Total"]),
            f"{pct:.1f}%"
        ])

    summary_table = Table(summary_data, colWidths=[3 * inch, 1.5 * inch, 1.5 * inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#FF441F')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 0.4 * inch))

    # Detalle por categoría (con imágenes)
    col_foto_1 = None
    col_foto_2 = None
    for col in df.columns:
        col_lower = col.lower()
        if "foto" in col_lower and "testigo" in col_lower:
            col_foto_1 = col
        elif "foto" in col_lower:
            col_foto_2 = col

    col_foto = col_foto_1 or col_foto_2

    for category in summary["_Categoria"].values:
        cat_data = df[df["_Categoria"] == category]
        story.append(Paragraph(f"<b>{category} ({len(cat_data)} puntos)</b>", styles['Heading3']))

        # Tabla con imagen (primeras 8 por categoría)
        detail_data = [["ID", "Negocio", "Dirección", "Estatus", "Foto"]]
        for _, row in cat_data.head(8).iterrows():
            foto_cell = "—"
            if col_foto and pd.notna(row.get(col_foto)):
                img = download_image(str(row.get(col_foto)))
                if img:
                    foto_cell = img

            detail_data.append([
                str(row.get("Store ID", "—"))[:10],
                str(row.get("Brand Name", "—"))[:15],
                str(row.get("Dirección", "—"))[:20],
                str(row.get("Estatus Final", "—"))[:12],
                foto_cell
            ])

        detail_table = Table(detail_data, colWidths=[0.8 * inch, 1.2 * inch, 1.5 * inch, 1 * inch, 1.5 * inch])
        detail_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(detail_table)
        story.append(Spacer(1, 0.3 * inch))

        if len(cat_data) > 8:
            story.append(Paragraph(f"<i>... y {len(cat_data) - 8} más</i>", styles['Normal']))
            story.append(Spacer(1, 0.2 * inch))

        # Page break entre categorías si hay muchos datos
        if len(cat_data) > 5:
            story.append(PageBreak())

    # Construye PDF
    doc.build(story)
    size_mb = pdf_path.stat().st_size / (1024 * 1024)
    print(f"    ✓ PDF generado: {pdf_path.name}  ({size_mb:.2f} MB)")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Genera reportes PDF de Rappi")
    parser.add_argument("--weekly", action="store_true", help="Reporte semanal")
    parser.add_argument("--monthly", action="store_true", help="Reporte mensual")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Rappi MKT POP — Reportes  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    print("Conectando con Google Sheets...")
    spreadsheet = connect_sheet()
    print(f"  ✓ Conectado\n")

    print("[CDMX] Leyendo pestaña 'CDMX'...")
    ws = spreadsheet.worksheet("CDMX")
    df = sheet_to_df(ws)
    print(f"    → {len(df)} filas cargadas\n")

    if df.empty:
        print("  ✗ Sin datos")
        return

    if args.weekly:
        period_name = "Semanal (últimos 7 días)"
        days_back = 7
        exact_day = False
        filename = f"rappi_reporte_semanal_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    elif args.monthly:
        period_name = "Mensual (últimos 30 días)"
        days_back = 30
        exact_day = False
        filename = f"rappi_reporte_mensual_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
        period_name = f"Diario ({yesterday})"
        days_back = 1
        exact_day = True
        filename = f"rappi_reporte_diario_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"

    df_filtered = filter_by_date_range(df, days_back, exact_day=exact_day)
    print(f"Generando reporte {period_name.lower()}...")
    generate_pdf_report(df_filtered, filename, period_name)

    print(f"\n{'='*55}")
    print("  ✓ Reporte completado")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado.")
    except Exception as e:
        print(f"\nERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
