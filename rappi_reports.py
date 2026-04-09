"""
rappi_reports.py — Reportes PDF con identidad visual Rappi / WH.

Uso:
    python rappi_reports.py --daily
    python rappi_reports.py --weekly
    python rappi_reports.py --monthly
    python rappi_reports.py --weekly --region CDMX --no-email
"""

import argparse, json, os, smtplib, sys, io, calendar, traceback, unicodedata
from datetime import datetime, date, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

try:
    import gspread
    from google.oauth2.service_account import Credentials
    import pandas as pd
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import inch, mm
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                     Paragraph, Spacer, HRFlowable, Image,
                                     KeepTogether, Flowable)
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.graphics.shapes import Drawing, Rect, String, Circle, Line
    from reportlab.graphics import renderPDF
except ImportError as e:
    print(f"ERROR: {e}\nEjecuta: pip install -r requirements.txt"); sys.exit(1)

# ─── RUTAS ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
CREDENTIALS  = BASE_DIR / "credentials.json"
EMAIL_CONFIG = BASE_DIR / "email_config.json"
OUTPUT_DIR   = BASE_DIR / "Reportes"
LOGO_PATH    = BASE_DIR / "logo_wh.png"
FONTS_DIR    = BASE_DIR / "fonts"

# ─── FUENTES POPPINS ──────────────────────────────────────────────────────────
_FONT_REG    = "Poppins"
_FONT_BOLD   = "Poppins-Bold"
_FONT_SEMI   = "Poppins-SemiBold"
_FONT_LIGHT  = "Poppins-Light"

def _register_fonts():
    global _FONT_REG, _FONT_BOLD, _FONT_SEMI, _FONT_LIGHT
    mapping = {
        _FONT_REG:   "Poppins-Regular.ttf",
        _FONT_BOLD:  "Poppins-Bold.ttf",
        _FONT_SEMI:  "Poppins-SemiBold.ttf",
        _FONT_LIGHT: "Poppins-Light.ttf",
    }
    ok = True
    for name, fname in mapping.items():
        path = FONTS_DIR / fname
        if path.exists():
            try:
                pdfmetrics.registerFont(TTFont(name, str(path)))
            except Exception:
                ok = False
        else:
            ok = False
    if not ok:
        # Fallback a Helvetica
        _FONT_REG = _FONT_SEMI = _FONT_LIGHT = "Helvetica"
        _FONT_BOLD = "Helvetica-Bold"

_register_fonts()

# ─── PALETA RAPPI / WH ────────────────────────────────────────────────────────
R_ORANGE      = colors.HexColor("#FF441F")   # Naranja Rappi principal
R_ORANGE_SOFT = colors.HexColor("#FFF0EC")   # Fondo suave naranja
R_DARK        = colors.HexColor("#1C1C1C")   # Casi negro para headers
R_ANTRACITA   = colors.HexColor("#333333")   # Texto principal
R_GRAY_MED    = colors.HexColor("#6B7280")   # Texto secundario
R_GRAY_LIGHT  = colors.HexColor("#F3F4F6")   # Fondo alterno de tabla
R_GRAY_BORDER = colors.HexColor("#E5E7EB")   # Borde sutil
R_WHITE       = colors.white

# Colores de estatus (modernos, saturados)
S_COLORS = {
    "Instalada":             colors.HexColor("#22C55E"),   # verde vibrante
    "En Proceso":            colors.HexColor("#F97316"),   # naranja
    "Visitada No Instalada": colors.HexColor("#FB923C"),   # naranja claro
    "Rechazada":             colors.HexColor("#EF4444"),   # rojo
    "No Existe":             colors.HexColor("#9CA3AF"),   # gris
    "Pendiente":             colors.HexColor("#A855F7"),   # morado
    "Requiere Contacto":     colors.HexColor("#3B82F6"),   # azul
}
S_COLORS_BG = {
    "Instalada":             colors.HexColor("#DCFCE7"),
    "En Proceso":            colors.HexColor("#FFEDD5"),
    "Visitada No Instalada": colors.HexColor("#FFF7ED"),
    "Rechazada":             colors.HexColor("#FEE2E2"),
    "No Existe":             colors.HexColor("#F3F4F6"),
    "Pendiente":             colors.HexColor("#F3E8FF"),
    "Requiere Contacto":     colors.HexColor("#DBEAFE"),
}
STATUS_LABELS = list(S_COLORS.keys())

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

REGION_CFG = {
    "CDMX": {
        "sheet_id":   "1Bt2M3GpP5iy1T2_Rc8eEKspjgLUC3_EDYFwksFhpqQc",
        "tab":        "CDMX", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fecha de visita/Implementación",
        "filter_col": None, "filter_val": None,
    },
    "GDL": {
        "sheet_id":   "1-zLKpVhtXggaYAYo6uPkwwJO_nQD7eyN0iXVGSriq-g",
        "tab":        "West",  "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        # GDL aún no tiene "Fecha de visita/Implementación" poblada;
        # usa lista de fallback — se toma la primera con datos
        "col_fecha":   ["Fecha de visita/Implementación", "Fecha tentativa de Ruta"],
        "filter_col": "Ciudad", "filter_val": "Guadalajara",
    },
    "PUE": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Puebla",
        "filter_normalize": True,
    },
    "MER": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Merida",
        "filter_normalize": True,
    },
    "QRO": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Queretaro",
        "filter_normalize": True,
    },
    "VER": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Veracruz",
        "filter_normalize": True,
    },
    "CUE": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Cuernavaca",
        "filter_normalize": True,
    },
    "CAN": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Cancun",
        "filter_normalize": True,
    },
    "PAC": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Pachuca",
        "filter_normalize": True,
    },
    "XAL": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Xalapa",
        "filter_normalize": True,
    },
    "PDC": {
        "sheet_id":   "1LGnJTUexOlH-4woCAUmYurf6dTNTSFxjzYsDxpIE9H4",
        "tab":        "Sur", "header_row": 2,
        "col_opzone":  "OP Zone", "col_estatus": "Estatus Final",
        "col_fecha":   "Fechas de visita",
        "filter_col": "Ciudad", "filter_val": "Playa Del Carmen",
        "filter_normalize": True,
    },
}

STATUS_MAP = {
    "instalada":             "Instalada",
    "en proceso de visita":  "En Proceso",
    "en proceso":            "En Proceso",
    "visitada no instalada": "Visitada No Instalada",
    "visitada no instalado": "Visitada No Instalada",
    "visitada":              "Visitada No Instalada",
    "rechazada":             "Rechazada",
    "no acepto":             "Rechazada",
    "no existe":             "No Existe",
    "ya no existe":          "No Existe",
    "pendiente":             "Pendiente",
    "requiere contacto":     "Requiere Contacto",
}

MESES_ES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
            7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

# ─── CARGA DE DATOS ───────────────────────────────────────────────────────────

def normalize_str(s):
    """Normaliza string: elimina acentos, convierte a minúsculas y elimina espacios extremos."""
    return unicodedata.normalize('NFD', str(s)).encode('ascii', 'ignore').decode('ascii').lower().strip()


def get_client():
    creds = Credentials.from_service_account_file(str(CREDENTIALS), scopes=SCOPES)
    return gspread.authorize(creds)

def load_region(client, region):
    cfg = REGION_CFG[region]
    ws  = client.open_by_key(cfg["sheet_id"]).worksheet(cfg["tab"])
    raw = ws.get_all_values()
    if len(raw) < cfg["header_row"]+1: return pd.DataFrame()
    headers = raw[cfg["header_row"]-1]
    rows = []
    for rv in raw[cfg["header_row"]:]:
        rd = {(h.strip() if h.strip() else f"__col_{i}__"): (rv[i] if i<len(rv) else "")
              for i,h in enumerate(headers)}
        if any(str(v).strip() for v in rd.values()): rows.append(rd)
    df = pd.DataFrame(rows); df.columns = [c.strip() for c in df.columns]
    fc,fv = cfg.get("filter_col"), cfg.get("filter_val")
    if fc and fv and fc in df.columns:
        if cfg.get("filter_normalize"):
            df = df[df[fc].apply(normalize_str)==normalize_str(fv)].reset_index(drop=True)
        else:
            df = df[df[fc].str.strip()==fv].reset_index(drop=True)
    col_e = cfg["col_estatus"]
    df["_ESTATUS"] = (df[col_e].str.strip().str.lower().map(STATUS_MAP).fillna("Pendiente")
                     if col_e in df.columns else "Pendiente")
    # col_fecha puede ser string o lista de fallback
    col_f_cfg = cfg["col_fecha"]
    col_f_list = [col_f_cfg] if isinstance(col_f_cfg, str) else col_f_cfg
    col_f = next((c for c in col_f_list if c in df.columns and df[c].str.strip().ne("").any()), None)
    df["_FECHA"] = (pd.to_datetime(df[col_f], dayfirst=True, errors="coerce")
                   if col_f else pd.NaT)
    if col_f:
        print(f"    → Columna fecha: '{col_f}'")
    return df

def filter_period(df, start, end):
    if df["_FECHA"].isna().all(): return pd.DataFrame(columns=df.columns)
    return df[(df["_FECHA"].dt.date>=start)&(df["_FECHA"].dt.date<=end)].copy()

# ─── MÉTRICAS ─────────────────────────────────────────────────────────────────

def calc_metrics(df):
    total = len(df)
    counts = {s: int((df["_ESTATUS"]==s).sum()) for s in STATUS_LABELS}
    inst   = counts["Instalada"]
    return {"total": total, "instaladas": inst,
            "pct_inst":  round(inst/total*100,1) if total else 0,
            "pct_visit": round((total-counts["Pendiente"])/total*100,1) if total else 0,
            "por_estatus": counts}

def calc_opzone(df, col_oz):
    if col_oz not in df.columns or df.empty: return pd.DataFrame()
    groups = []
    for opz,grp in df.groupby(col_oz):
        row = {"OP Zone":str(opz), "Total":len(grp)}
        for s in STATUS_LABELS: row[s] = int((grp["_ESTATUS"]==s).sum())
        groups.append(row)
    df_oz = pd.DataFrame(groups).sort_values("Total",ascending=False)
    tots  = {"OP Zone":"Suma total","Total":df_oz["Total"].sum()}
    for s in STATUS_LABELS: tots[s] = df_oz[s].sum()
    return pd.concat([df_oz,pd.DataFrame([tots])],ignore_index=True)

def _fmt(n):
    try: return f"{int(n):,}"
    except: return str(n)

def _pct(n,t): return f"{n/t*100:.1f}%" if t else "0%"

def delta_str(c,p):
    d=int(c)-int(p)
    return f"+{d:,}" if d>0 else (f"{d:,}" if d<0 else "—")

# ─── FLOWABLES PERSONALIZADOS ─────────────────────────────────────────────────

class ProgressBar(Flowable):
    """Barra de progreso estilo Rappi."""
    def __init__(self, pct, label, color, width=200, height=22):
        super().__init__()
        self.pct    = min(max(pct,0),100)
        self.label  = label
        self.color  = color
        self.width  = width
        self.height = height

    def wrap(self, aw, ah): return self.width, self.height+4

    def draw(self):
        c = self.canv
        w, h = self.width, self.height
        bar_w = w*0.55; bar_x = w*0.35; bar_y = 4

        # Label
        c.setFont(_FONT_SEMI, 7)
        c.setFillColor(R_ANTRACITA)
        c.drawString(0, bar_y+4, self.label)

        # Track (fondo gris)
        c.setFillColor(R_GRAY_LIGHT)
        c.roundRect(bar_x, bar_y, bar_w, h-6, 4, fill=1, stroke=0)

        # Fill (color)
        fill_w = max(bar_w*(self.pct/100), 8)
        c.setFillColor(self.color)
        c.roundRect(bar_x, bar_y, fill_w, h-6, 4, fill=1, stroke=0)

        # Porcentaje
        c.setFont(_FONT_BOLD, 7.5)
        c.setFillColor(R_WHITE)
        if fill_w > 30:
            c.drawString(bar_x+6, bar_y+4, f"{self.pct:.1f}%")
        else:
            c.setFillColor(R_ANTRACITA)
            c.drawString(bar_x+fill_w+4, bar_y+4, f"{self.pct:.1f}%")


class KpiCard(Flowable):
    """Tarjeta KPI estilo dashboard moderno."""
    def __init__(self, label, value, sub="", color=None, width=120, height=60):
        super().__init__()
        self.label  = label
        self.value  = str(value)
        self.sub    = sub
        self.color  = color or R_ORANGE
        self.width  = width
        self.height = height

    def wrap(self, aw, ah): return self.width, self.height

    def draw(self):
        c = self.canv
        w, h = self.width, self.height

        # Fondo blanco con borde redondeado
        c.setFillColor(R_WHITE)
        c.setStrokeColor(R_GRAY_BORDER)
        c.setLineWidth(0.5)
        c.roundRect(1,1,w-2,h-2, 6, fill=1, stroke=1)

        # Barra superior de color
        c.setFillColor(self.color)
        c.roundRect(1, h-8, w-2, 7, 6, fill=1, stroke=0)
        c.rect(1, h-12, w-2, 6, fill=1, stroke=0)

        # Valor grande
        c.setFont(_FONT_BOLD, 20)
        c.setFillColor(R_DARK)
        c.drawCentredString(w/2, h*0.35, self.value)

        # Label
        c.setFont(_FONT_SEMI, 6.5)
        c.setFillColor(R_GRAY_MED)
        c.drawCentredString(w/2, h*0.18, self.label)

        # Sub (porcentaje)
        if self.sub:
            c.setFont(_FONT_BOLD, 8)
            c.setFillColor(self.color)
            c.drawCentredString(w/2, h*0.05, self.sub)


class RappyBadge(Flowable):
    """Sello 'Rappi-Ready' al pie del reporte."""
    def __init__(self, width=200, height=60):
        super().__init__()
        self.width = width; self.height = height

    def wrap(self, aw, ah): return self.width, self.height

    def draw(self):
        c = self.canv
        w, h = self.width, self.height
        cx, cy = w/2, h/2

        # Círculo exterior naranja
        c.setFillColor(R_ORANGE_SOFT)
        c.setStrokeColor(R_ORANGE)
        c.setLineWidth(2)
        c.circle(cx, cy, min(w,h)/2-2, fill=1, stroke=1)

        # Texto
        c.setFont(_FONT_BOLD, 8)
        c.setFillColor(R_ORANGE)
        c.drawCentredString(cx, cy+6,  "✓ INSTALACIÓN")
        c.drawCentredString(cx, cy-2,  "CERTIFICADA")
        c.setFont(_FONT_REG, 6)
        c.setFillColor(R_GRAY_MED)
        c.drawCentredString(cx, cy-12, "Rappi-Ready · WH")


# ─── ESTILOS ──────────────────────────────────────────────────────────────────

def _s(name, **kw):
    d = dict(fontSize=7.5, textColor=R_ANTRACITA, fontName=_FONT_REG, alignment=TA_CENTER)
    d.update(kw); return ParagraphStyle(name, **d)

def _make_styles():
    return {
        "hdr":   _s("hdr",   textColor=R_WHITE, fontName=_FONT_BOLD, fontSize=7),
        "cell":  _s("cell",  fontSize=7),
        "left":  _s("left",  alignment=TA_LEFT, fontSize=7),
        "bold":  _s("bold",  fontName=_FONT_BOLD, fontSize=7),
        "tot":   _s("tot",   fontName=_FONT_BOLD, fontSize=7),
        "tot_l": _s("tot_l", fontName=_FONT_BOLD, alignment=TA_LEFT, fontSize=7),
        "pos":   _s("pos",   textColor=colors.HexColor("#16A34A"), fontName=_FONT_BOLD, fontSize=7),
        "neg":   _s("neg",   textColor=colors.HexColor("#DC2626"), fontName=_FONT_BOLD, fontSize=7),
        "neu":   _s("neu",   fontSize=7),
        "sec":   _s("sec",   fontSize=9, textColor=R_WHITE, fontName=_FONT_BOLD),
        "title": _s("title", fontSize=14, textColor=R_WHITE, fontName=_FONT_BOLD, alignment=TA_LEFT),
        "sub":   _s("sub",   fontSize=9,  textColor=R_ORANGE, fontName=_FONT_SEMI, alignment=TA_LEFT),
        "tag":   _s("tag",   fontSize=6,  textColor=R_WHITE, fontName=_FONT_BOLD),
    }

ST = _make_styles()

BASE_TS = [
    ("BACKGROUND",    (0,0),(-1,0), R_DARK),
    ("ROWBACKGROUNDS",(0,1),(-1,-1),[R_WHITE, R_GRAY_LIGHT]),
    ("BOX",  (0,0),(-1,-1), 0.5, R_GRAY_BORDER),
    ("GRID", (0,0),(-1,-1), 0.3, R_GRAY_BORDER),
    ("TOPPADDING",    (0,0),(-1,-1), 4),
    ("BOTTOMPADDING", (0,0),(-1,-1), 4),
    ("LEFTPADDING",   (0,0),(-1,-1), 5),
    ("ALIGN", (1,0),(-1,-1), "CENTER"),
    ("VALIGN",(0,0),(-1,-1), "MIDDLE"),
]

def _dsty(d):
    return ST["pos"] if d.startswith("+") else (ST["neg"] if d.startswith("-") else ST["neu"])


def sec_header(text, story, color=None):
    bg = color or R_ORANGE
    t = Table([[Paragraph(text, ST["sec"])]], colWidths=["100%"])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), bg),
        ("TOPPADDING",    (0,0),(-1,-1), 6),
        ("BOTTOMPADDING", (0,0),(-1,-1), 6),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("ROUNDEDCORNERS",(0,0),(-1,-1), [4,4,0,0]),
    ]))
    story.append(t)


# ─── ENCABEZADO ───────────────────────────────────────────────────────────────

def build_header(region, report_type, subtitle, page_w):
    tipo = {"daily":"REPORTE DIARIO","weekly":"REPORTE SEMANAL","monthly":"REPORTE MENSUAL"}

    logo_cell = (Image(str(LOGO_PATH), width=1.05*inch, height=0.6*inch)
                 if LOGO_PATH.exists()
                 else Paragraph("WH", _s("whl",fontSize=22,textColor=R_DARK,fontName=_FONT_BOLD)))

    title_block = [
        [Paragraph(f"{tipo[report_type]} — {region} POP 2026", ST["title"])],
        [Paragraph(subtitle, ST["sub"])],
    ]
    title_tbl = Table(title_block, colWidths=[page_w-1.45*inch])
    title_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1), R_DARK),
        ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
        ("LEFTPADDING",(0,0),(-1,-1),10),
    ]))

    outer = Table([[logo_cell, title_tbl]], colWidths=[1.35*inch, page_w-1.35*inch])
    outer.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1), R_DARK),
        ("BACKGROUND",(0,0),(0,0),   R_WHITE),
        ("VALIGN",    (0,0),(-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0),(-1,-1), 10),
        ("BOTTOMPADDING", (0,0),(-1,-1), 10),
        ("LEFTPADDING",   (0,0),(-1,-1),  8),
    ]))
    return outer


# ─── KPI CARDS ────────────────────────────────────────────────────────────────

def build_kpi_row(m_curr, m_prev, page_w, story):
    """Fila de tarjetas KPI con números grandes."""
    n_cards = 7
    cw = page_w / n_cards - 4

    cards = []
    for s in STATUS_LABELS:
        val  = m_curr["por_estatus"][s]
        prev = m_prev["por_estatus"][s] if m_prev else None
        sub  = f"vs {_fmt(prev)}" if prev is not None else ""
        cards.append(KpiCard(
            label=s.replace(" No Instalada","\nNo Inst.").replace("Requiere Contacto","Req. Ctcto."),
            value=_fmt(val), sub=sub,
            color=S_COLORS[s], width=cw, height=62))

    row_data = [[c] for c in cards]
    # Transposición: los cards van en columnas
    t = Table([cards], colWidths=[cw+2]*n_cards)
    t.setStyle(TableStyle([
        ("ALIGN",   (0,0),(-1,-1),"CENTER"),
        ("VALIGN",  (0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",    (0,0),(-1,-1), 2),
        ("BOTTOMPADDING", (0,0),(-1,-1), 2),
    ]))
    story.append(t)

    # Barra de progreso instalación
    pbar = ProgressBar(m_curr["pct_inst"],
                       f"Avance instalación ({_fmt(m_curr['instaladas'])}/{_fmt(m_curr['total'])})",
                       S_COLORS["Instalada"], width=page_w, height=20)
    story.append(pbar)
    story.append(Spacer(1,8))


# ─── TABLA OP ZONE ────────────────────────────────────────────────────────────

def _opzone_half(oz, page_w, status_subset, story):
    s6h = _s("s6h", fontSize=6, textColor=R_WHITE, fontName=_FONT_BOLD)
    s6  = _s("s6",  fontSize=6, fontName=_FONT_REG)
    s6l = _s("s6l", fontSize=6, fontName=_FONT_REG, alignment=TA_LEFT)
    s6t = _s("s6t", fontSize=6, fontName=_FONT_BOLD)
    s6tl= _s("s6tl",fontSize=6, fontName=_FONT_BOLD, alignment=TA_LEFT)

    n_s   = len(status_subset)
    cw_oz = page_w*0.27
    cw_tt = page_w*0.07
    cw_s  = (page_w - cw_oz - cw_tt) / (n_s*2)
    cw    = [cw_oz, cw_tt] + [cw_s]*(n_s*2)

    row0 = [Paragraph("OP Zone",s6h), Paragraph("Total",s6h)]
    row1 = [Paragraph("",s6h), Paragraph("",s6h)]
    for s in status_subset:
        short = (s.replace("Visitada No Instalada","Visit.No Inst.")
                  .replace("Requiere Contacto","Req.Ctcto."))
        row0 += [Paragraph(short,s6h), Paragraph("",s6h)]
        row1 += [Paragraph("Cant.",s6h), Paragraph("%",s6h)]

    tdata = [row0, row1]
    for _, row in oz.iterrows():
        is_t = str(row["OP Zone"])=="Suma total"
        sty  = s6t  if is_t else s6
        styl = s6tl if is_t else s6l
        tt   = row["Total"]
        line = [Paragraph(str(row["OP Zone"]),styl), Paragraph(_fmt(tt),sty)]
        for s in status_subset:
            n = row[s]
            line += [Paragraph(_fmt(n),sty), Paragraph(_pct(n,tt),sty)]
        tdata.append(line)

    t   = Table(tdata, colWidths=cw)
    tot = len(tdata)-1
    ts  = [("SPAN",(0,0),(0,1)),("SPAN",(1,0),(1,1))]
    col = 2
    for s in status_subset:
        c = S_COLORS[s]
        ts += [("SPAN",(col,0),(col+1,0)),
               ("BACKGROUND",(col,0),(col+1,0),c),
               ("BACKGROUND",(col,1),(col+1,1),c)]
        col += 2
    ts += [
        ("BACKGROUND",(0,0),(1,1), R_DARK),
        ("BACKGROUND",(0,1),(1,1), R_DARK),
        ("ROWBACKGROUNDS",(0,2),(-1,tot-1),[R_WHITE,R_GRAY_LIGHT]),
        ("BACKGROUND",(0,tot),(-1,tot), R_ORANGE_SOFT),
        ("FONTNAME",  (0,tot),(-1,tot), _FONT_BOLD),
        ("BOX",  (0,0),(-1,-1), 0.5, R_GRAY_BORDER),
        ("GRID", (0,0),(-1,-1), 0.3, R_GRAY_BORDER),
        ("TOPPADDING",    (0,0),(-1,-1), 2),
        ("BOTTOMPADDING", (0,0),(-1,-1), 2),
        ("LEFTPADDING",   (0,0),(-1,-1), 4),
        ("ALIGN",(1,0),(-1,-1),"CENTER"),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]
    t.setStyle(TableStyle(ts))
    story.append(t)


def build_opzone_table(df, cfg, title_num, title_text, page_w, story):
    oz = calc_opzone(df, cfg["col_opzone"])
    if oz.empty:
        story.append(Paragraph("Sin datos de OP Zone.", _s("x",alignment=TA_LEFT)))
        story.append(Spacer(1,6)); return
    sec_header(f"{title_num}. {title_text}", story)
    story.append(Spacer(1,2))
    _opzone_half(oz, page_w, STATUS_LABELS[:4], story)
    story.append(Spacer(1,3))
    _opzone_half(oz, page_w, STATUS_LABELS[4:], story)
    story.append(Spacer(1,10))


# ─── COMPARATIVA ─────────────────────────────────────────────────────────────

def build_comparativa(m_curr, m_prev, df_curr, df_prev, cfg,
                      period_lbl, prev_lbl, page_w, story):
    sec_header(f"4. COMPARATIVA: {period_lbl}  vs  {prev_lbl}", story)
    story.append(Spacer(1,2))

    hdrs = ["Estatus", period_lbl, prev_lbl, "Δ", "% Δ"]
    cw   = [page_w*0.35, page_w*0.16, page_w*0.16, page_w*0.16, page_w*0.17]
    rows = [(s, m_curr["por_estatus"][s], m_prev["por_estatus"][s]) for s in STATUS_LABELS]
    rows += [("TOTAL PDV", m_curr["total"], m_prev["total"])]

    tdata = [[Paragraph(h, ST["hdr"]) for h in hdrs]]
    for lbl,c,p in rows:
        d   = delta_str(c,p)
        pcd = f"{(c-p)/p*100:+.1f}%" if p else "—"
        is_t= lbl=="TOTAL PDV"
        tdata.append([
            Paragraph(lbl, ST["tot_l"] if is_t else ST["left"]),
            Paragraph(_fmt(c), ST["tot"] if is_t else ST["cell"]),
            Paragraph(_fmt(p), ST["tot"] if is_t else ST["cell"]),
            Paragraph(d,  _dsty(d)),
            Paragraph(pcd,_dsty(d)),
        ])

    t = Table(tdata, colWidths=cw)
    ts = list(BASE_TS)
    for i,(s,_) in enumerate(zip(STATUS_LABELS,STATUS_LABELS),start=1):
        ts.append(("BACKGROUND",(0,i),(0,i), S_COLORS_BG[s]))
    n = len(tdata)-1
    ts += [("BACKGROUND",(0,n),(-1,n), R_ORANGE_SOFT),
           ("FONTNAME",  (0,n),(-1,n), _FONT_BOLD)]
    t.setStyle(TableStyle(ts))
    story.append(t)
    story.append(Spacer(1,10))

    # OP Zone comparativa
    if not df_curr.empty and not df_prev.empty:
        oz_c = calc_opzone(df_curr, cfg["col_opzone"])
        oz_p = calc_opzone(df_prev, cfg["col_opzone"])
        if not oz_c.empty and not oz_p.empty:
            sec_header("  OP Zone — Instaladas por período", story, color=colors.HexColor("#374151"))
            story.append(Spacer(1,2))
            pm = {r["OP Zone"]: r["Instalada"] for _,r in oz_p.iterrows()}
            cw2  = [page_w*0.40, page_w*0.15, page_w*0.15, page_w*0.15, page_w*0.15]
            hdrs2= ["OP Zone",f"Inst. {period_lbl}",f"Inst. {prev_lbl}","Δ","% Δ"]
            td2  = [[Paragraph(h,ST["hdr"]) for h in hdrs2]]
            for _,row in oz_c.iterrows():
                opz = str(row["OP Zone"]); is_t = opz=="Suma total"
                c_i = int(row["Instalada"]); p_i = int(pm.get(opz,0))
                d   = delta_str(c_i,p_i)
                pcd = f"{(c_i-p_i)/p_i*100:+.1f}%" if p_i else "—"
                sty = ST["tot"]; styl = ST["tot_l"] if is_t else ST["left"]
                if not is_t: sty = ST["cell"]
                td2.append([Paragraph(opz,styl), Paragraph(_fmt(c_i),sty),
                            Paragraph(_fmt(p_i),sty), Paragraph(d,_dsty(d)),
                            Paragraph(pcd,_dsty(d))])
            t2 = Table(td2, colWidths=cw2)
            ts2 = list(BASE_TS)
            n2  = len(td2)-1
            ts2 += [("BACKGROUND",(0,n2),(-1,n2), R_ORANGE_SOFT),
                    ("FONTNAME",  (0,n2),(-1,n2), _FONT_BOLD)]
            t2.setStyle(TableStyle(ts2))
            story.append(t2)
    story.append(Spacer(1,8))


# ─── GENERACIÓN PDF ───────────────────────────────────────────────────────────

def generate_pdf(region, report_type, df_all, df_curr, df_prev,
                 period_lbl, prev_lbl, subtitle):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    path = OUTPUT_DIR / f"Reporte_{report_type}_{region}_{ts}.pdf"

    page_size = landscape(letter)
    doc = SimpleDocTemplate(str(path), pagesize=page_size,
                            leftMargin=0.45*inch, rightMargin=0.45*inch,
                            topMargin=0.4*inch,   bottomMargin=0.4*inch)
    page_w = page_size[0] - 0.9*inch
    story  = []
    cfg    = REGION_CFG[region]

    # HEADER
    story.append(build_header(region, report_type, subtitle, page_w))
    story.append(Spacer(1,10))

    def _empty_metrics():
        return {"total": 0, "instaladas": 0, "pct_inst": 0.0, "pct_visit": 0.0,
                "por_estatus": {s: 0 for s in STATUS_LABELS}}

    m_all  = calc_metrics(df_all)
    m_curr = calc_metrics(df_curr) if not df_curr.empty else _empty_metrics()
    m_prev = calc_metrics(df_prev) if not df_prev.empty else None

    # KPI CARDS
    sec_header("1. RESUMEN GENERAL — KPIs", story)
    story.append(Spacer(1,4))
    build_kpi_row(m_curr, m_prev, page_w, story)

    # OPZONE ACUMULADO
    build_opzone_table(df_all, cfg, "2", "DISTRIBUCIÓN POR OP ZONE — ACUMULADO", page_w, story)

    # OPZONE PERÍODO
    n_p = len(df_curr)
    build_opzone_table(df_curr, cfg, "3",
                       f"{period_lbl} — POR OP ZONE ({n_p:,} registros)", page_w, story)

    # COMPARATIVA
    if not df_prev.empty and m_prev:
        build_comparativa(m_curr, m_prev, df_curr, df_prev, cfg,
                          period_lbl, prev_lbl, page_w, story)

    # FOOTER CON BADGE
    badge_row = Table([
        [RappyBadge(width=110, height=54),
         Paragraph(
            f"<font color='#6B7280'>Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}<br/>"
            f"Rappi MKT POP 2026 · WH — Warehouse<br/>"
            f"Total acumulado: {_fmt(m_all['total'])} PDV · {m_all['pct_inst']}% instalado</font>",
            _s("ft", fontSize=7, alignment=TA_LEFT, textColor=R_GRAY_MED, fontName=_FONT_LIGHT))],
    ], colWidths=[1.4*inch, page_w-1.4*inch])
    badge_row.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),4),
    ]))
    story.append(HRFlowable(width="100%", thickness=0.5, color=R_ORANGE))
    story.append(Spacer(1,4))
    story.append(badge_row)

    doc.build(story)
    print(f"    ✓  PDF: {path.name}")
    return path


# ─── CORREO ───────────────────────────────────────────────────────────────────

def send_email(paths, region, subject, body):
    if not EMAIL_CONFIG.exists(): print("    ⚠  Sin email_config.json"); return
    cfg = json.loads(EMAIL_CONFIG.read_text(encoding="utf-8"))
    pwd = cfg.get("smtp_password","")
    if not pwd or pwd=="AQUI_TU_APP_PASSWORD": print("    ⚠  Configura smtp_password"); return
    recips = cfg.get("recipients",{}).get(region,[])
    if not recips: print(f"    ⚠  Sin destinatarios para {region}"); return
    msg = MIMEMultipart()
    msg["From"] = f"{cfg['from_name']} <{cfg['from_email']}>"
    msg["To"]   = ", ".join(recips); msg["Subject"] = subject
    msg.attach(MIMEText(body,"html","utf-8"))
    for p in paths:
        with open(p,"rb") as f: part = MIMEApplication(f.read(),Name=p.name)
        part["Content-Disposition"] = f'attachment; filename="{p.name}"'
        msg.attach(part)
    try:
        with smtplib.SMTP(cfg["smtp_host"],cfg["smtp_port"]) as s:
            s.ehlo(); s.starttls(); s.login(cfg["smtp_user"],pwd)
            s.sendmail(cfg["from_email"],recips,msg.as_string())
        print(f"    ✓  Correo → {', '.join(recips)}")
    except Exception as e: print(f"    ✗  Error: {e}")


def email_body(region, report_type, m, period_lbl):
    tipo = {"daily":"Diario","weekly":"Semanal","monthly":"Mensual"}.get(report_type,"")
    rows = "".join(
        f'<tr><td style="padding:4px 10px;color:{S_COLORS[s].hexval()}">{s}</td>'
        f'<td style="text-align:center;padding:4px 8px">{m["por_estatus"][s]:,}</td>'
        f'<td style="text-align:center;padding:4px 8px">{m["por_estatus"][s]/m["total"]*100:.1f}%</td></tr>'
        for s in STATUS_LABELS if m["total"]
    )
    return f"""<html><body style="font-family:'Poppins',Arial,sans-serif;background:#F3F4F6;padding:20px">
<div style="background:#1C1C1C;padding:20px;border-radius:8px;border-left:6px solid #FF441F">
  <b style="color:white;font-size:18px">WH · Reporte {tipo} {region} POP 2026</b><br>
  <span style="color:#FF441F;font-size:13px">{period_lbl}</span>
</div>
<div style="background:white;padding:20px;border-radius:8px;margin-top:12px">
<table style="border-collapse:collapse;width:100%;max-width:440px;font-size:13px">
<tr style="background:#1C1C1C;color:white">
  <th style="padding:8px 10px;text-align:left;border-radius:4px 0 0 0">Estatus</th>
  <th style="padding:8px">Cantidad</th><th style="padding:8px">%</th>
</tr>{rows}
<tr style="background:#FFF0EC;font-weight:bold;border-top:2px solid #FF441F">
  <td style="padding:6px 10px">TOTAL PDV</td>
  <td style="text-align:center">{m["total"]:,}</td><td></td>
</tr></table>
<p style="margin-top:16px;font-size:11px;color:#9CA3AF">
PDF adjunto · Generado automáticamente · Rappi MKT POP 2026 · WH</p>
</div></body></html>"""


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def run_report(report_type, regions, send):
    today = date.today()
    if report_type == "daily":
        c_end = c_start = today-timedelta(days=1)
        p_end = p_start = c_end-timedelta(days=1)
        period_lbl = c_end.strftime("%d/%m/%Y"); prev_lbl = p_end.strftime("%d/%m/%Y")
        subtitle = f"Reporte del {c_end.strftime('%d de %B de %Y')}"
    elif report_type == "weekly":
        dow=today.weekday(); lm=today-timedelta(days=dow+7)
        c_start,c_end = lm, lm+timedelta(days=6)
        p_start,p_end = lm-timedelta(days=7), lm-timedelta(days=1)
        wk=c_start.isocalendar()[1]
        period_lbl = f"Sem {wk}: {c_start.strftime('%d/%m')} al {c_end.strftime('%d/%m/%Y')}"
        prev_lbl   = f"Sem {wk-1}: {p_start.strftime('%d/%m')} al {p_end.strftime('%d/%m/%Y')}"
        subtitle   = f"Semana del {c_start.strftime('%d de %B')} al {c_end.strftime('%d de %B de %Y')}"
    else:
        pm,py = (12,today.year-1) if today.month==1 else (today.month-1,today.year)
        c_start=date(today.year,today.month,1); c_end=today-timedelta(days=1)
        p_start=date(py,pm,1); p_end=date(py,pm,calendar.monthrange(py,pm)[1])
        period_lbl=f"{MESES_ES[today.month]} {today.year}"; prev_lbl=f"{MESES_ES[pm]} {py}"
        subtitle=f"Mes de {MESES_ES[today.month]} {today.year}"

    print(f"  Período: {c_start} → {c_end}  |  Previo: {p_start} → {p_end}\n")
    client = get_client(); pdf_paths = []

    for region in regions:
        print(f"[{region}]  Cargando datos...")
        df_all = load_region(client,region)
        if df_all.empty: print("  ⚠  Sin datos"); continue
        df_curr = filter_period(df_all,c_start,c_end)
        df_prev = filter_period(df_all,p_start,p_end)
        print(f"    → Total:{len(df_all):,}  Período:{len(df_curr):,}  Previo:{len(df_prev):,}")
        path = generate_pdf(region,report_type,df_all,df_curr,df_prev,period_lbl,prev_lbl,subtitle)
        pdf_paths.append((region,path,calc_metrics(df_all)))

    if send:
        ec = json.loads(EMAIL_CONFIG.read_text()) if EMAIL_CONFIG.exists() else {}
        for region,path,m in pdf_paths:
            subj = ec.get("subject_templates",{}).get(report_type,"{region} POP — {fecha}").format(
                region=region, fecha=period_lbl, semana=period_lbl, mes=period_lbl, año=today.year)
            send_email([path],region,subj,email_body(region,report_type,m,period_lbl))

    return [p for _,p,_ in pdf_paths]


def main():
    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--daily",  action="store_true")
    grp.add_argument("--weekly", action="store_true")
    grp.add_argument("--monthly",action="store_true")
    parser.add_argument("--region",   default=None)
    parser.add_argument("--no-email", action="store_true")
    args = parser.parse_args()
    rtype  = "daily" if args.daily else "weekly" if args.weekly else "monthly"
    regions= [args.region.upper()] if args.region else list(REGION_CFG.keys())
    send   = not args.no_email
    print(f"\n{'='*55}\n  Rappi Reports — {rtype.upper()}  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*55}")
    print(f"  Regiones: {', '.join(regions)}  |  Email: {'Sí' if send else 'No'}\n")
    paths = run_report(rtype,regions,send)
    print(f"\n{'='*55}\n  ✓  {len(paths)} PDF(s) generados\n{'='*55}\n")
    for p in paths:
        try: os.startfile(str(p))
        except: pass

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt: print("\nCancelado.")
    except Exception as e: print(f"\nERROR: {e}"); traceback.print_exc(); sys.exit(1)
