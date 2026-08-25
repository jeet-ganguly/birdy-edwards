import sqlite3
import os
import io
import warnings
import base64
from datetime import datetime

warnings.filterwarnings("ignore")

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image, KeepTogether
)

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

try:
    import ftfy
    HAS_FTFY = True
except ImportError:
    HAS_FTFY = False

try:
    from unidecode import unidecode
    HAS_UNIDECODE = True
except ImportError:
    HAS_UNIDECODE = False

import unicodedata


_HERE          = os.path.dirname(os.path.abspath(__file__))   # .../app/
_ROOT          = os.path.dirname(_HERE)                        # .../birdy-edwards/

DB_FILE        = os.path.join(_HERE, "socmint.db")
MANUAL_DB_FILE = os.path.join(_HERE, "socmint_manual.db")
REPORTS_DIR    = os.path.join(_HERE, "reports")
LOGO_PATH      = os.path.join(_HERE, "icons", "wraith.png")


from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

FONTS_DIR      = os.path.join(_HERE, 'fonts')
# FreeSerif path — checks app/fonts/ first (works in Docker), then system paths
def _find_freeserif():
    candidates = [
        os.path.join(FONTS_DIR, 'FreeSerif.ttf'),              # app/fonts/ — preferred
        '/usr/share/fonts/truetype/freefont/FreeSerif.ttf',    # Ubuntu host
        '/usr/share/fonts/freefont/FreeSerif.ttf',              # Debian variant
        '/usr/share/fonts/truetype/FreeSerif.ttf',              # minimal installs
        '/usr/share/fonts/FreeSerif.ttf',                       # generic
    ]
    for p in candidates:
        if os.path.exists(p):
            print(f"  FreeSerif found: {p}")
            return p
    print(f"  ⚠️  FreeSerif not found — non-latin text will fall back to boxes")
    return candidates[0]

FREESERIF_PATH = _find_freeserif()

import tempfile, hashlib
_TEXT_IMAGE_CACHE = {}

def detect_script(text):
    if not text:
        return 'latin'
    counts = {'arabic': 0, 'devanagari': 0, 'bengali': 0, 'latin': 0}
    for ch in str(text):
        cp = ord(ch)
        if (0x0600 <= cp <= 0x06FF or 0x0750 <= cp <= 0x077F
                or 0xFB50 <= cp <= 0xFDFF or 0xFE70 <= cp <= 0xFEFF):
            counts['arabic'] += 1
        elif 0x0900 <= cp <= 0x097F:
            counts['devanagari'] += 1
        elif 0x0980 <= cp <= 0x09FF:
            counts['bengali'] += 1
        else:
            counts['latin'] += 1
    return max(counts, key=counts.get)

def is_nonlatin(text):
    """
    Return True if text contains ANY non-latin characters.
    Uses presence check not dominance — so mixed English+Bengali
    goes through PIL rendering, not ReportLab.
    """
    if not text:
        return False
    for ch in str(text):
        cp = ord(ch)
        if (0x0600 <= cp <= 0x06FF or   # Arabic/Urdu
            0x0750 <= cp <= 0x077F or
            0xFB50 <= cp <= 0xFDFF or
            0xFE70 <= cp <= 0xFEFF or
            0x0900 <= cp <= 0x097F or   # Devanagari/Hindi
            0x0980 <= cp <= 0x09FF or   # Bengali
            0x0400 <= cp <= 0x04FF):    # Cyrillic/Russian
            return True
    return False

def _reshape_arabic(text):
    try:
        import arabic_reshaper
        from bidi.algorithm import get_display
        return get_display(arabic_reshaper.reshape(text))
    except ImportError:
        return text

def text_as_image(text, font_size=11, bold=False, color_rgb=(33,37,41),
                  max_width_pt=240):
    """
    Render text as PNG using FreeSerif.
    Handles pure non-latin AND mixed scripts (e.g. English + Bengali).
    max_width_pt: column width in PDF points — text wraps at this boundary.
    Rendered at 2x for sharpness, scaled 0.5x when embedded in PDF.
    """
    if not text or not str(text).strip():
        return None
    cache_key = hashlib.md5(f"{text}{font_size}{bold}{max_width_pt}".encode()).hexdigest()
    if cache_key in _TEXT_IMAGE_CACHE:
        return _TEXT_IMAGE_CACHE[cache_key]

    # For Arabic segments — reshape + bidi the entire string first
    # FreeSerif handles mixed rendering correctly when full string is passed
    overall_script = detect_script(text)
    if overall_script == 'arabic':
        display_text = _reshape_arabic(text)
    else:
        display_text = text

    try:
        from PIL import ImageFont, ImageDraw, Image as PILImage
        import textwrap

        fs   = font_size * 2    # render 2x for sharpness
        font = ImageFont.truetype(FREESERIF_PATH, fs)
        max_px = int(max_width_pt * 2)

        # measure full text as one line
        tmp_img  = PILImage.new('RGB', (1, 1))
        tmp_draw = ImageDraw.Draw(tmp_img)
        single_bbox = tmp_draw.textbbox((0, 0), display_text, font=font)
        single_w    = single_bbox[2] - single_bbox[0]
        line_h      = single_bbox[3] - single_bbox[1] + 8

        if single_w <= max_px:
            lines = [display_text]
        else:
            # estimate chars per line from average char width
            avg_char_w     = single_w / max(len(display_text), 1)
            chars_per_line = max(int(max_px / avg_char_w), 8)
            wrapped = textwrap.wrap(display_text, width=chars_per_line,
                                    break_long_words=True, break_on_hyphens=False)
            lines = wrapped if wrapped else [display_text]

        # measure actual max line width for image sizing
        max_line_w = min(max_px, max(
            tmp_draw.textbbox((0, 0), ln, font=font)[2] + 8
            for ln in lines
        ))
        img_w = max(max_line_w, 20)
        img_h = max(line_h * len(lines) + 4, line_h)

        img  = PILImage.new('RGB', (img_w, img_h), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        y = 2
        for line in lines:
            # FreeSerif renders full mixed script correctly in one call
            draw.text((4, y), line, font=font, fill=color_rgb)
            y += line_h

        tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        img.save(tmp.name, 'PNG')
        tmp.close()
        _TEXT_IMAGE_CACHE[cache_key] = tmp.name
        return tmp.name
    except Exception as e:
        print(f"  text_as_image failed: {e}")
        return None

def _color_to_rgb(clr):
    try:
        return (int(clr.red*255), int(clr.green*255), int(clr.blue*255))
    except Exception:
        return (33, 37, 41)

def unicode_para(text, font_size=11, bold=False, color=None, alignment=TA_LEFT,
                 leading=16, space_after=4, max_len=None, max_width_pt=240):
    """
    Return a Paragraph or Image flowable for any script.
    max_width_pt: column width in PDF points — non-latin images are constrained to this.
    Default 240 = 8.5cm (comment column width).
    """
    if not text or not str(text).strip():
        return Paragraph('', ParagraphStyle('_empty', fontName='Helvetica', fontSize=font_size))
    text = str(text)
    if max_len and len(text) > max_len:
        text = text[:max_len] + '...'
    clr = color or C_TEXT
    if is_nonlatin(text):
        rgb      = _color_to_rgb(clr)
        # pass max_width_pt so text wraps at column boundary
        img_path = text_as_image(text, font_size=font_size, bold=bold,
                                  color_rgb=rgb, max_width_pt=max_width_pt)
        if img_path and os.path.exists(img_path):
            try:
                from PIL import Image as PILImage
                pil = PILImage.open(img_path)
                w_px, h_px = pil.size
                # rendered at 2x → scale back 0.5x to get PDF points
                w_pt = w_px * 0.5
                h_pt = h_px * 0.5
                # hard cap to column width
                if w_pt > max_width_pt:
                    h_pt = h_pt * max_width_pt / w_pt
                    w_pt = max_width_pt
                img_elem = Image(img_path, width=w_pt, height=h_pt)
                img_elem.hAlign = ('LEFT' if alignment == TA_LEFT else
                                   'RIGHT' if alignment == TA_RIGHT else 'CENTER')
                return img_elem
            except Exception as e:
                print(f"  unicode_para image embed failed: {e}")
        return Paragraph(text, ParagraphStyle('_fb', fontName='Helvetica',
            fontSize=font_size, textColor=clr, leading=leading))
    else:
        _register_noto_if_needed()
        return Paragraph(text, ParagraphStyle(
            f'_up{font_size}{int(bold)}',
            fontName   = _NOTO_BOLD if bold else _NOTO_REG,
            fontSize   = font_size,
            textColor  = clr,
            alignment  = alignment,
            leading    = leading,
            spaceAfter = space_after,
            wordWrap   = 'CJK',
        ))

_NOTO_REG  = 'Helvetica'
_NOTO_BOLD = 'Helvetica-Bold'
_NOTO_DONE = False

def _register_noto_if_needed():
    global _NOTO_REG, _NOTO_BOLD, _NOTO_DONE
    if _NOTO_DONE:
        return
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    reg_path  = os.path.join(FONTS_DIR, 'NotoSans-Regular.ttf')
    bold_path = os.path.join(FONTS_DIR, 'NotoSans-Bold.ttf')
    if os.path.exists(reg_path):
        try:
            pdfmetrics.registerFont(TTFont('NotoSans', reg_path))
            _NOTO_REG = 'NotoSans'
            print("  NotoSans registered")
        except Exception:
            pass
    if os.path.exists(bold_path):
        try:
            pdfmetrics.registerFont(TTFont('NotoSans-Bold', bold_path))
            _NOTO_BOLD = 'NotoSans-Bold'
        except Exception:
            pass
    _NOTO_DONE = True

def setup_unicode_fonts():
    _register_noto_if_needed()
    print(f"  Unicode system ready. FreeSerif: {os.path.exists(FREESERIF_PATH)}")

def font_for_text(text, bold=False):
    _register_noto_if_needed()
    return _NOTO_BOLD if bold else _NOTO_REG

W, H = A4

C_WHITE     = colors.white
C_DARK      = colors.HexColor('#1a1a2e')
C_ACCENT    = colors.HexColor('#002682')
C_ACCENT2   = colors.HexColor('#2A0380')
C_LIGHT_BG  = colors.HexColor('#f8f9fa')
C_LIGHT_BG2 = colors.HexColor('#eef0f2')
C_BORDER    = colors.HexColor('#dee2e6')
C_TEXT      = colors.HexColor('#212529')
C_SUBTEXT   = colors.HexColor('#6c757d')
C_GREEN     = colors.HexColor('#27ae60')
C_BLUE      = colors.HexColor('#2980b9')
C_YELLOW    = colors.HexColor('#f39c12')
C_RED       = colors.HexColor('#c0392b')
C_SILVER    = colors.HexColor('#7f8c8d')
C_GOLD      = colors.HexColor('#f39c12')

CHART_PALETTE = ['#c0392b','#2980b9','#27ae60','#f39c12',
                  '#8e44ad','#16a085','#d35400','#2c3e50']

SCORE_COLORS = {
    'Strong Supporter': '#27ae60',
    'Supporter':        '#2980b9',
    'Neutral':          '#7f8c8d',
    'Low Interaction':  '#f39c12',
    'Critical Voice':   '#c0392b',
}


# ══════════════════════════════════════════════════════════════════════════════
#  TEXT NORMALIZATION
# ══════════════════════════════════════════════════════════════════════════════

def normalize_text(text, transliterate=False):
    """
    Fix garbled unicode, mojibake, unicode escape sequences like u00xx.
    If transliterate=True → convert non-latin scripts to ASCII
    (use for PDF rendering of names/places).
    Never translates language — Urdu stays Urdu, Arabic stays Arabic.
    """
    if not text:
        return ''

    text = str(text)

    # fix mojibake and \u00xx type garbage
    if HAS_FTFY:
        text = ftfy.fix_text(text)

    # normalize unicode form
    text = unicodedata.normalize('NFKC', text)

    # strip null bytes and control chars (except newline)
    text = ''.join(
        ch for ch in text
        if unicodedata.category(ch) not in ('Cc', 'Cf') or ch == '\n'
    )

    if transliterate and HAS_UNIDECODE:
        # only transliterate if text contains non-ASCII
        if any(ord(c) > 127 for c in text):
            text = unidecode(text)

    return text.strip()


def safe_str(val, transliterate=False, maxlen=None):
    """Normalize + optionally truncate."""
    s = normalize_text(val, transliterate=transliterate)
    if maxlen and len(s) > maxlen:
        s = s[:maxlen] + '…'
    return s


# ══════════════════════════════════════════════════════════════════════════════
#  COUNTRY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_country_flag(country):
    if not country:
        return ''
    c = country.lower().strip()
    codes = {
        'india':'IN','pakistan':'PK','bangladesh':'BD','nepal':'NP',
        'sri lanka':'LK','afghanistan':'AF','bhutan':'BT','maldives':'MV',
        'saudi arabia':'SA','uae':'AE','united arab emirates':'AE',
        'iran':'IR','iraq':'IQ','syria':'SY','jordan':'JO',
        'lebanon':'LB','israel':'IL','palestine':'PS','yemen':'YE',
        'oman':'OM','qatar':'QA','kuwait':'KW','bahrain':'BH',
        'turkey':'TR','turkiye':'TR','china':'CN','japan':'JP',
        'south korea':'KR','korea':'KR','taiwan':'TW',
        'indonesia':'ID','malaysia':'MY','philippines':'PH',
        'thailand':'TH','vietnam':'VN','myanmar':'MM','singapore':'SG',
        'kazakhstan':'KZ','uzbekistan':'UZ','azerbaijan':'AZ',
        'united kingdom':'GB','uk':'GB','england':'GB',
        'germany':'DE','france':'FR','italy':'IT','spain':'ES',
        'portugal':'PT','netherlands':'NL','belgium':'BE',
        'switzerland':'CH','austria':'AT','sweden':'SE',
        'norway':'NO','denmark':'DK','finland':'FI','ireland':'IE',
        'russia':'RU','ukraine':'UA','poland':'PL',
        'united states':'US','usa':'US','us':'US','america':'US',
        'canada':'CA','mexico':'MX','brazil':'BR','argentina':'AR',
        'colombia':'CO','chile':'CL','australia':'AU','new zealand':'NZ',
        'egypt':'EG','nigeria':'NG','south africa':'ZA','kenya':'KE',
        'ethiopia':'ET','morocco':'MA','algeria':'DZ',
    }
    for key, code in codes.items():
        if key in c:
            return f'[{code}]'
    return '[--]'


def is_valid_country(country):
    """Return True if country is a real identified country (not Unknown/null)."""
    if not country:
        return False
    c = country.strip().lower()
    return c not in ('unknown', 'null', 'none', '', 'n/a', 'not identified')


def first_valid_country(country_list):
    """Return first valid country from a sorted list."""
    for c in country_list:
        if is_valid_country(c):
            return c
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  STYLES
# ══════════════════════════════════════════════════════════════════════════════

def make_styles():
    return {
        'cover_title': ParagraphStyle('cover_title', fontName='Helvetica-Bold',
            fontSize=36, textColor=C_DARK, alignment=TA_CENTER, spaceAfter=6, leading=42),
        'cover_quote': ParagraphStyle('cover_quote', fontName='Helvetica-Oblique',
            fontSize=14, textColor=C_SUBTEXT, alignment=TA_CENTER, spaceAfter=8, leading=22),
        'section_title': ParagraphStyle('section_title', fontName='Helvetica-Bold',
            fontSize=22, textColor=C_ACCENT, spaceAfter=10, spaceBefore=4, leading=26),
        'sub_title': ParagraphStyle('sub_title', fontName='Helvetica-Bold',
            fontSize=16, textColor=C_ACCENT2, spaceAfter=6, spaceBefore=10, leading=20),
        'body': ParagraphStyle('body', fontName='Helvetica',
            fontSize=13, textColor=C_TEXT, spaceAfter=5, leading=19),
        'body_sub': ParagraphStyle('body_sub', fontName='Helvetica',
            fontSize=13, textColor=C_SUBTEXT, spaceAfter=4, leading=18),
        'dev': ParagraphStyle('dev', fontName='Helvetica',
            fontSize=13, textColor=C_SUBTEXT, alignment=TA_CENTER),
        'confidential': ParagraphStyle('confidential', fontName='Helvetica-Bold',
            fontSize=14, textColor=C_ACCENT, alignment=TA_CENTER, spaceAfter=4),
        'table_header': ParagraphStyle('table_header', fontName='Helvetica-Bold',
            fontSize=12, textColor=C_WHITE, alignment=TA_CENTER),
        'table_cell': ParagraphStyle('table_cell', fontName='Helvetica',
            fontSize=12, textColor=C_TEXT, alignment=TA_LEFT),
        'table_cell_c': ParagraphStyle('table_cell_c', fontName='Helvetica',
            fontSize=12, textColor=C_TEXT, alignment=TA_CENTER),
        'stat_label': ParagraphStyle('stat_label', fontName='Helvetica-Bold',
            fontSize=12, textColor=C_SUBTEXT, alignment=TA_CENTER),
        'stat_value': ParagraphStyle('stat_value', fontName='Helvetica-Bold',
            fontSize=28, textColor=C_DARK, alignment=TA_CENTER),
        'meta_label': ParagraphStyle('meta_label', fontName='Helvetica-Bold',
            fontSize=13, textColor=C_ACCENT, alignment=TA_RIGHT),
        'meta_value': ParagraphStyle('meta_value', fontName='Helvetica',
            fontSize=13, textColor=C_TEXT, alignment=TA_LEFT),
        'card_name': ParagraphStyle('card_name', fontName='Helvetica-Bold',
            fontSize=14, textColor=C_DARK, spaceAfter=3),
        'card_country': ParagraphStyle('card_country', fontName='Helvetica',
            fontSize=13, textColor=C_SUBTEXT, spaceAfter=3),
        'card_detail': ParagraphStyle('card_detail', fontName='Helvetica',
            fontSize=12, textColor=C_SUBTEXT, leading=16),
        'highlight_label': ParagraphStyle('highlight_label', fontName='Helvetica-Bold',
            fontSize=11, textColor=C_WHITE, alignment=TA_CENTER),
        'highlight_value': ParagraphStyle('highlight_value', fontName='Helvetica-Bold',
            fontSize=18, textColor=C_WHITE, alignment=TA_CENTER),
        'highlight_sub': ParagraphStyle('highlight_sub', fontName='Helvetica',
            fontSize=10, textColor=colors.HexColor('#ecf0f1'), alignment=TA_CENTER),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE CANVAS
# ══════════════════════════════════════════════════════════════════════════════

def on_cover(c, doc):
    c.saveState()
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    c.setFillColor(C_ACCENT)
    c.rect(0, 0, 10, H, fill=1, stroke=0)
    c.setFillColor(C_DARK)
    c.rect(0, H-10, W, 10, fill=1, stroke=0)
    c.restoreState()


def on_page(c, doc):
    c.saveState()
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    c.setFillColor(C_ACCENT)
    c.rect(0, H-6, W, 6, fill=1, stroke=0)
    c.setFillColor(C_LIGHT_BG)
    c.rect(0, 0, W, 24, fill=1, stroke=0)
    c.setFillColor(C_SUBTEXT)
    c.setFont('Helvetica', 9)
    c.drawString(2*cm, 8, "BIRDY-EDWARDS  |  Infiltrate & Expose  |  CONFIDENTIAL")
    c.drawRightString(W - 2*cm, 8, f"Page {doc.page}")
    c.restoreState()


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_score_label(score):
    if score > 0.5:   return 'Strong Supporter', C_GREEN
    if score > 0.1:   return 'Supporter',        C_BLUE
    if score > -0.1:  return 'Neutral',           C_SILVER
    if score > -0.5:  return 'Low Interaction',   C_YELLOW
    return 'Critical Voice', C_RED


def get_score_hex(score):
    if score > 0.5:   return '#27ae60'
    if score > 0.1:   return '#2980b9'
    if score > -0.1:  return '#7f8c8d'
    if score > -0.5:  return '#f39c12'
    return '#c0392b'


def chart_buf(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight',
                facecolor='white', edgecolor='none', dpi=180)
    buf.seek(0)
    plt.close(fig)
    return buf


def sec_hdr(title, styles):
    return [
        Paragraph(title, styles['section_title']),
        HRFlowable(width='100%', thickness=2, color=C_ACCENT, spaceAfter=12),
    ]


def hyperlink_url(url, label='url'):
    """Return a ReportLab hyperlink Paragraph for a profile URL."""
    if not url:
        return Paragraph('—', ParagraphStyle('na', fontName='Helvetica',
            fontSize=11, textColor=C_SUBTEXT, alignment=TA_CENTER))
    safe = safe_str(url, maxlen=120)
    return Paragraph(
        f'<link href="{safe}" color="#2980b9"><u>{label}</u></link>',
        ParagraphStyle('url_link', fontName='Helvetica', fontSize=11,
                       textColor=C_BLUE, alignment=TA_CENTER)
    )


# ══════════════════════════════════════════════════════════════════════════════
#  CHARTS
# ══════════════════════════════════════════════════════════════════════════════

def chart_countries(country_data):
    countries = sorted(country_data.items(), key=lambda x: -x[1])[:12]
    if not countries:
        return None
    labels = [safe_str(c[0], transliterate=True) for c in countries]
    values = [c[1] for c in countries]
    total  = sum(values)

    if HAS_SEABORN: sns.set_style("whitegrid")
    h = max(6, len(labels) * 0.7)
    fig, ax = plt.subplots(figsize=(13, h))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#fafbfc')
    clrs = [CHART_PALETTE[0]] + [CHART_PALETTE[1]] * (len(labels)-1)
    bars = ax.barh(labels, values, color=clrs, height=0.55, edgecolor='white', linewidth=2)
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                f'{val}  ({val/total*100:.1f}%)', va='center', ha='left',
                color='#444', fontsize=15, fontweight='bold')
    ax.set_xlabel('Number of Commentors', fontsize=16, color='#555', labelpad=8)
    ax.set_title('Network Country Distribution', fontsize=16, fontweight='bold',
                 color='#c0392b', pad=14)
    ax.tick_params(colors='#555', labelsize=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xlim(0, max(values)*1.38)
    plt.tight_layout(pad=1.5)
    return chart_buf(fig)


def chart_tiers(tier_data):
    if not tier_data or not any(tier_data.values()):
        return None
    labels = list(tier_data.keys())
    values = list(tier_data.values())
    clrs   = [SCORE_COLORS.get(l, '#95a5a6') for l in labels]

    if HAS_SEABORN: sns.set_style("white")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    fig.patch.set_facecolor('white')

    ax1.set_facecolor('white')
    wedges, _, autotexts = ax1.pie(
        values, colors=clrs, autopct='%1.1f%%', startangle=90,
        pctdistance=0.78,
        wedgeprops={'width': 0.55, 'edgecolor': 'white', 'linewidth': 3}
    )
    for at in autotexts:
        at.set_fontsize(12); at.set_color('white'); at.set_fontweight('bold')
    ax1.set_title('Tier Distribution', fontsize=15, fontweight='bold', color='#333', pad=16)
    handles = [mpatches.Patch(color=c, label=f'{l}  ({v})')
               for l, v, c in zip(labels, values, clrs)]
    ax1.legend(handles=handles, loc='lower center', bbox_to_anchor=(0.5, -0.2),
               ncol=2, fontsize=14, frameon=False)

    ax2.set_facecolor('#fafbfc')
    bars = ax2.barh(labels, values, color=clrs, height=0.5, edgecolor='white', linewidth=2)
    for bar, val in zip(bars, values):
        ax2.text(bar.get_width()+0.05, bar.get_y()+bar.get_height()/2,
                 str(val), va='center', ha='left', fontsize=16,
                 fontweight='bold', color='#555')
    ax2.set_xlabel('Commentors', fontsize=16, color='#555')
    ax2.set_title('Commentors by Category', fontsize=15, fontweight='bold', color='#333', pad=16)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.tick_params(labelsize=12)
    ax2.set_xlim(0, max(values)*1.35)
    plt.tight_layout(pad=2)
    return chart_buf(fig)


def chart_sentiment(sd):
    if not sd or not any(sd.values()):
        return None
    labels = ['Positive', 'Neutral', 'Negative']
    values = [sd.get('positive', 0), sd.get('neutral', 0), sd.get('negative', 0)]
    clrs   = ['#27ae60', '#7f8c8d', '#c0392b']

    if HAS_SEABORN: sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(7, 5))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#fafbfc')
    bars = ax.bar(labels, values, color=clrs, width=0.45, edgecolor='white', linewidth=2)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.15,
                str(val), ha='center', va='bottom', fontsize=14,
                fontweight='bold', color='#555')
    ax.set_ylabel('Comments', fontsize=16, color='#555')
    ax.set_title('Sentiment Distribution', fontsize=15, fontweight='bold', color='#333', pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(labelsize=13)
    plt.tight_layout()
    return chart_buf(fig)


def chart_language(ld):
    if not ld:
        return None
    langs  = sorted(ld.items(), key=lambda x: -x[1])[:8]
    labels = [safe_str(l[0], transliterate=True) for l in langs]
    values = [l[1] for l in langs]

    if HAS_SEABORN: sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(7, 5))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#fafbfc')
    clrs = [CHART_PALETTE[0]] + [CHART_PALETTE[1]]*(len(labels)-1)
    ax.barh(labels, values, color=clrs, height=0.45, edgecolor='white', linewidth=2)
    for i, val in enumerate(values):
        ax.text(val+0.05, i, str(val), va='center', ha='left',
                fontsize=16, fontweight='bold', color='#555')
    ax.set_xlabel('Comments', fontsize=16, color='#555')
    ax.set_title('Language Distribution', fontsize=15, fontweight='bold', color='#333', pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(labelsize=13)
    ax.set_xlim(0, max(values)*1.28)
    plt.tight_layout()
    return chart_buf(fig)


def chart_top7(profiles):
    if not profiles:
        return None
    if HAS_SEABORN: sns.set_style("white")
    fig, ax = plt.subplots(figsize=(12, 10))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    ax.set_xlim(-1.6, 1.6)
    ax.set_ylim(-1.6, 1.6)
    ax.axis('off')

    ax.add_patch(plt.Circle((0, 0), 0.18, color='#c0392b', zorder=5))
    ax.text(0, -0.24, profiles[0].get('owner_name', 'Target') if 'owner_name' in profiles[0]
            else 'Target', ha='center', va='top', fontsize=14, fontweight='bold',
            color='#333', zorder=6)
    ax.text(0, 0, '🎯', ha='center', va='center', fontsize=18, zorder=7)

    n      = len(profiles)
    angles = [2 * np.pi * i / n for i in range(n)]
    radius = 1.15

    for i, (c, angle) in enumerate(zip(profiles, angles)):
        x     = radius * np.cos(angle)
        y     = radius * np.sin(angle)
        score = c.get('total_score', c.get('score', 0.0))
        color = get_score_hex(score)

        ax.plot([0, x*0.82], [0, y*0.82], color=color, linewidth=2.5, alpha=0.7, zorder=2)
        ax.add_patch(plt.Circle((x, y), 0.13, color=color, zorder=4, alpha=0.9))
        ax.add_patch(plt.Circle((x, y), 0.13, fill=False,
                                edgecolor='white', linewidth=2, zorder=5))
        ax.text(x, y, str(i+1), ha='center', va='center',
                fontsize=15, fontweight='bold', color='white', zorder=6)

        name_short = safe_str(c['name'], transliterate=True)[:18]
        country    = safe_str(c.get('country') or 'Unknown', transliterate=True)
        lx = x * 1.38; ly = y * 1.38
        ax.text(lx, ly, name_short, ha='center', va='center',
                fontsize=13, fontweight='bold', color='#333', zorder=6,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                          edgecolor=color, linewidth=1.5, alpha=0.95))
        ax.text(lx, ly-0.12, country, ha='center', va='center',
                fontsize=12, color='#777', zorder=6)
        ax.text(lx, ly-0.22, f"{score:+.2f}", ha='center', va='center',
                fontsize=12, color=color, fontweight='bold', zorder=6)

    legend_items = [
        ('Strong Supporter', '#27ae60'), ('Supporter', '#2980b9'),
        ('Neutral', '#7f8c8d'), ('Low Interaction', '#f39c12'), ('Critical Voice', '#c0392b')
    ]
    for j, (name, color) in enumerate(legend_items):
        ax.add_patch(plt.Circle((-1.55, 1.4 - j*0.18), 0.04, color=color, zorder=5))
        ax.text(-1.45, 1.4 - j*0.18, name, va='center', fontsize=12, color='#444')

    ax.set_title('Top 7 Close Network', fontsize=16, fontweight='bold', color='#333', pad=16)
    plt.tight_layout()
    return chart_buf(fig)


def chart_post_timeline(timeline_data):
    if not timeline_data:
        return None
    posts    = sorted(timeline_data, key=lambda x: x.get('sort_key', 0))
    labels   = [safe_str(p['label'], transliterate=True) for p in posts]
    totals   = [p['total']    for p in posts]
    pos_vals = [p['positive'] for p in posts]
    neg_vals = [p['negative'] for p in posts]
    neu_vals = [p['neutral']  for p in posts]
    cumsum   = np.cumsum(totals).tolist()

    if HAS_SEABORN: sns.set_style("whitegrid")
    x = np.arange(len(labels))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10),
                                    gridspec_kw={'height_ratios': [2, 1]})
    fig.patch.set_facecolor('white')

    ax1.set_facecolor('#fafbfc')
    ax1.bar(x, pos_vals, color='#27ae60', label='Positive', width=0.55,
            edgecolor='white', linewidth=1.5)
    ax1.bar(x, neu_vals, bottom=pos_vals, color='#7f8c8d', label='Neutral',
            width=0.55, edgecolor='white', linewidth=1.5)
    ax1.bar(x, neg_vals, bottom=[p+n for p, n in zip(pos_vals, neu_vals)],
            color='#c0392b', label='Negative', width=0.55, edgecolor='white', linewidth=1.5)
    for i, total in enumerate(totals):
        if total > 0:
            ax1.text(i, total + 0.15, str(total), ha='center', va='bottom',
                     fontsize=13, fontweight='bold', color='#333')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=35, ha='right', fontsize=12)
    ax1.set_ylabel('Comment Count', fontsize=15, color='#555', labelpad=8)
    ax1.set_title('Post Activity Timeline — Comment Frequency & Sentiment',
                  fontsize=17, fontweight='bold', color='#c0392b', pad=14)
    ax1.legend(fontsize=13, frameon=False, loc='upper right')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)

    ax2.set_facecolor('#fafbfc')
    ax2.plot(x, cumsum, color='#2980b9', linewidth=2.5, marker='o', markersize=7, zorder=3)
    ax2.fill_between(x, cumsum, alpha=0.15, color='#2980b9')
    for i, val in enumerate(cumsum):
        ax2.text(i, val + max(cumsum)*0.02, str(val), ha='center', va='bottom',
                 fontsize=11, color='#2980b9', fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, rotation=35, ha='right', fontsize=12)
    ax2.set_ylabel('Cumulative', fontsize=14, color='#555', labelpad=8)
    ax2.set_title('Cumulative Comment Growth', fontsize=15, fontweight='bold', color='#333', pad=10)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    plt.tight_layout(pad=2)
    return chart_buf(fig)


def download_image_thumb(url, max_size=(200, 200)):
    try:
        import urllib.request
        from PIL import Image as PILImage
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as resp:
            tmp.write(resp.read())
        tmp.close()
        img = PILImage.open(tmp.name)
        img.thumbnail(max_size, PILImage.LANCZOS)
        img.save(tmp.name, 'JPEG', quality=85)
        return tmp.name
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  NEW: CO-COMMENTOR PAIRS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_cocommentor_pairs(profile_id_or_batch, db_file, is_batch=False):
    """
    Fetch top 10 co-commentor pairs (people who commented on the same posts).
    Returns list of {name_a, url_a, name_b, url_b, shared_posts}
    """
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    pairs = []

    try:
        if is_batch:
            # Manual batch: photo_comments joined by post_id
            query = """
                SELECT
                    ca.name, ca.profile_url,
                    cb.name, cb.profile_url,
                    COUNT(DISTINCT c1.post_id) as shared
                FROM comments c1
                JOIN comments c2 ON c2.post_id = c1.post_id AND c2.commentor_id > c1.commentor_id
                JOIN commentors ca ON ca.id = c1.commentor_id
                JOIN commentors cb ON cb.id = c2.commentor_id
                JOIN manual_posts mp ON mp.id = c1.post_id
                WHERE mp.batch_id = ?
                GROUP BY c1.commentor_id, c2.commentor_id
                ORDER BY shared DESC
                LIMIT 10
            """
            cur.execute(query, (profile_id_or_batch,))
        else:
            # Profile: union all post types then count shared posts
            # This matches the UI which counts across photo + reel + text
            query = """
                WITH all_comments AS (
                    SELECT pc.commentor_id, pc.photo_post_id AS post_id,
                           pp.photo_url AS post_url, 'photo' AS ptype
                    FROM photo_comments pc
                    JOIN photo_posts pp ON pp.id = pc.photo_post_id
                    WHERE pp.profile_id = ?
                    UNION ALL
                    SELECT rc.commentor_id, rc.reel_post_id,
                           rp.reel_url, 'reel'
                    FROM reel_comments rc
                    JOIN reel_posts rp ON rp.id = rc.reel_post_id
                    WHERE rp.profile_id = ?
                    UNION ALL
                    SELECT tc.commentor_id, tc.text_post_id,
                           tp.post_url, 'text'
                    FROM text_comments tc
                    JOIN text_posts tp ON tp.id = tc.text_post_id
                    WHERE tp.profile_id = ?
                )
                SELECT
                    ca.name, ca.profile_url,
                    cb.name, cb.profile_url,
                    COUNT(DISTINCT a1.post_id) AS shared,
                    GROUP_CONCAT(DISTINCT a1.post_url) AS post_urls
                FROM all_comments a1
                JOIN all_comments a2 ON a2.post_id = a1.post_id
                    AND a2.commentor_id > a1.commentor_id
                JOIN commentors ca ON ca.id = a1.commentor_id
                JOIN commentors cb ON cb.id = a2.commentor_id
                GROUP BY a1.commentor_id, a2.commentor_id
                ORDER BY shared DESC
                LIMIT 10
            """
            cur.execute(query, (profile_id_or_batch, profile_id_or_batch, profile_id_or_batch,))

        for r in cur.fetchall():
            # batch query returns 5 cols, profile query returns 6 cols
            post_urls_raw = r[5] if len(r) > 5 else ''
            post_urls = [u.strip() for u in (post_urls_raw or '').split(',') if u.strip()][:3]
            pairs.append({
                'name_a':       safe_str(r[0] or 'Unknown'),
                'url_a':        r[1] or '',
                'name_b':       safe_str(r[2] or 'Unknown'),
                'url_b':        r[3] or '',
                'shared_posts': r[4] or 0,
                'post_urls':    post_urls,
            })
    except Exception as e:
        print(f'  [CO-COMMENTOR] Error: {e}')
    finally:
        con.close()

    return pairs


def build_cocommentor_section(pairs, S, section_num='03b'):
    """Build co-commentor table section for profile report."""
    if not pairs:
        return []

    story = []
    story += sec_hdr(f"{section_num} / CO-COMMENTOR ANALYSIS", S)
    story.append(Paragraph(
        "Users who repeatedly comment together across multiple posts — "
        "indicating potential coordination or close relationship.",
        S['body_sub']))
    story.append(Spacer(1, 0.4*cm))

    headers = ['#', 'PERSON A', 'PROFILE', 'PERSON B', 'PROFILE', 'SHARED POSTS']
    col_w   = [0.8*cm, 4.2*cm, 1.8*cm, 4.2*cm, 1.8*cm, 2.5*cm]
    rows    = [[Paragraph(h, S['table_header']) for h in headers]]

    for i, p in enumerate(pairs, 1):
        shared    = p['shared_posts']
        post_urls = p.get('post_urls', [])
        color     = C_RED if shared >= 5 else C_BLUE if shared >= 3 else C_SILVER
        rows.append([
            Paragraph(str(i), ParagraphStyle('cc_n', fontName='Helvetica',
                fontSize=12, textColor=C_SUBTEXT, alignment=TA_CENTER)),
            unicode_para(safe_str(p['name_a'], maxlen=28), font_size=12),
            hyperlink_url(p['url_a']),
            unicode_para(safe_str(p['name_b'], maxlen=28), font_size=12),
            hyperlink_url(p['url_b']),
            Paragraph(str(shared), ParagraphStyle('cc_s', fontName='Helvetica-Bold',
                fontSize=13, textColor=color, alignment=TA_CENTER)),
        ])
        # shared post URLs row
        if post_urls:
            url_parts = '  |  '.join(
                f'<link href="{u}" color="#2980b9"><u>post {j+1}</u></link>'
                for j, u in enumerate(post_urls)
            )
            rows.append([
                Paragraph('', S['table_cell']),
                Paragraph(
                    f'<font size="10" color="#6c757d">Shared posts: {url_parts}</font>',
                    ParagraphStyle('cc_urls', fontName='Helvetica', fontSize=10,
                                   textColor=C_SUBTEXT, leading=14)),
                '', '', '', '',
            ])

    t = Table(rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,0),  C_ACCENT2),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_WHITE, C_LIGHT_BG]),
        ('TOPPADDING',    (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING',   (0,0), (-1,-1), 6),
        ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
        ('LINEBELOW',     (0,0), (-1,0),  2, C_ACCENT),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


# ══════════════════════════════════════════════════════════════════════════════
#  NEW: TOP 5 MOST FREQUENT + COUNTRY BANNER
# ══════════════════════════════════════════════════════════════════════════════

def build_top5_banner(commentors, S):
    """
    Top 5 most frequent interactors — vertical colored cards.
    Each card is full width with colored background (same style as before).
    """
    if not commentors:
        return []

    top5 = sorted(commentors, key=lambda x: x['comment_count'], reverse=True)[:5]

    story = []
    story.append(Paragraph("Top 5 Most Frequent Interactors", S['sub_title']))
    story.append(Spacer(1, 0.2*cm))

    bg_colors = ['#c0392b', '#2c3e50', '#2980b9', '#27ae60', '#8e44ad']

    for i, c in enumerate(top5):
        country = safe_str(c.get('country') or 'Unknown', transliterate=True)
        flag    = get_country_flag(c.get('country'))
        name    = safe_str(c['name'], transliterate=True, maxlen=40)
        count   = c['comment_count']
        lbl, _  = get_score_label(c['total_score'])
        bg_c    = colors.HexColor(bg_colors[i % len(bg_colors)])

        # each card: full width, colored bg
        # left side: rank + name + country  |  right side: count + category
        card = Table([[
            # left cell
            Table([
                [Paragraph(f'<b>#{i+1}</b>', ParagraphStyle('v5_rank',
                    fontName='Helvetica-Bold', fontSize=13,
                    textColor=colors.HexColor('#ffffff'), leading=16))],
                [unicode_para(name, font_size=16, bold=True, color=colors.white, leading=20, space_after=2)],
                [Paragraph(f'{flag}  {country}', ParagraphStyle('v5_cntry',
                    fontName='Helvetica', fontSize=12,
                    textColor=colors.HexColor('#ecf0f1'), leading=16))],
            ], colWidths=[11*cm]),

            # right cell
            Table([
                [Paragraph(str(count), ParagraphStyle('v5_cnt',
                    fontName='Helvetica-Bold', fontSize=28,
                    textColor=colors.white, alignment=TA_CENTER, leading=32))],
                [Paragraph('interactions', ParagraphStyle('v5_int',
                    fontName='Helvetica', fontSize=11,
                    textColor=colors.HexColor('#ecf0f1'), alignment=TA_CENTER))],
                [Paragraph(lbl, ParagraphStyle('v5_lbl',
                    fontName='Helvetica-Bold', fontSize=11,
                    textColor=colors.white, alignment=TA_CENTER,
                    spaceAfter=0))],
            ], colWidths=[6*cm]),
        ]], colWidths=[11*cm, 6*cm])

        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0), (-1,-1), bg_c),
            ('TOPPADDING',   (0,0), (-1,-1), 12),
            ('BOTTOMPADDING',(0,0), (-1,-1), 12),
            ('LEFTPADDING',  (0,0), (-1,-1), 16),
            ('RIGHTPADDING', (0,0), (-1,-1), 12),
            ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
            ('LINEBELOW',    (0,0), (-1,-1), 1, colors.HexColor('#ffffff33')),
        ]))

        story.append(card)
        story.append(Spacer(1, 0.15*cm))

    story.append(Spacer(1, 0.4*cm))
    return story


# ══════════════════════════════════════════════════════════════════════════════
#  NEW: FETCH COMMENTOR COMMENTS + STANCE + SENTIMENT
# ══════════════════════════════════════════════════════════════════════════════

def fetch_commentor_details(profile_id, db_file):
    """
    For each commentor: fetch up to 3 sample comments with stance + sentiment + post_url.
    Returns dict: {commentor_id: [{comment, stance, sentiment, post_url}]}
    """
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    details = {}

    try:
        for tbl, src, id_col, post_tbl, url_col in [
            ('photo_comments', 'photo', 'photo_post_id', 'photo_posts', 'photo_url'),
            ('reel_comments',  'reel',  'reel_post_id',  'reel_posts',  'reel_url'),
            ('text_comments',  'text',  'text_post_id',  'text_posts',  'post_url'),
        ]:
            cur.execute(f"""
                SELECT
                    c.commentor_id,
                    c.comment_text,
                    ca.sentiment,
                    ca.stance,
                    pp.{url_col}
                FROM {tbl} c
                JOIN {post_tbl} pp ON pp.id = c.{id_col}
                LEFT JOIN comment_analysis ca
                    ON ca.comment_id = c.id AND ca.db_source = '{src}'
                WHERE pp.profile_id = ?
                  AND c.comment_text IS NOT NULL
                  AND c.comment_text != ''
                ORDER BY c.commentor_id
            """, (profile_id,))

            for row in cur.fetchall():
                cid      = row[0]
                text     = safe_str(row[1])   # full comment text — no truncation
                sent     = safe_str(row[2] or 'N/A')
                stan     = safe_str(row[3] or 'N/A')
                post_url = row[4] or ''
                if cid not in details:
                    details[cid] = []
                details[cid].append({
                        'comment':   text,
                        'sentiment': sent,
                        'stance':    stan,
                        'post_url':  post_url,
                    })
    except Exception as e:
        print(f'  [COMMENTOR DETAILS] Error: {e}')
    finally:
        con.close()

    return details


def fetch_batch_commentor_details(batch_id, db_file):
    """Same as fetch_commentor_details but for manual batch — includes post_url."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    details = {}

    try:
        cur.execute("""
            SELECT
                c.commentor_id,
                c.comment_text,
                ca.sentiment,
                ca.stance,
                mp.url
            FROM comments c
            JOIN manual_posts mp ON mp.id = c.post_id
            LEFT JOIN comment_analysis ca
                ON ca.comment_id = c.id AND ca.db_source = 'manual'
            WHERE mp.batch_id = ?
              AND c.comment_text IS NOT NULL
              AND c.comment_text != ''
            ORDER BY c.commentor_id
        """, (batch_id,))

        for row in cur.fetchall():
            cid      = row[0]
            text     = safe_str(row[1])   # full comment text — no truncation
            sent     = safe_str(row[2] or 'N/A')
            stan     = safe_str(row[3] or 'N/A')
            post_url = row[4] or ''
            if cid not in details:
                details[cid] = []
            details[cid].append({
                    'comment':   text,
                    'sentiment': sent,
                    'stance':    stan,
                    'post_url':  post_url,
                })
    except Exception as e:
        print(f'  [BATCH COMMENTOR DETAILS] Error: {e}')
    finally:
        con.close()

    return details


# ══════════════════════════════════════════════════════════════════════════════
#  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_profile_data(profile_url, db_file=DB_FILE):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute(
        "SELECT id,owner_name,profile_url,is_locked FROM profiles WHERE profile_url=?",
        (profile_url,))
    row = cur.fetchone()
    if not row:
        con.close()
        return None

    pid    = row[0]
    owner  = safe_str(row[1] or 'Unknown')
    locked = row[3]

    cur.execute("SELECT section,label,value FROM profile_fields WHERE profile_id=?", (pid,))
    seen_fields = set()
    pfields = []
    for r in cur.fetchall():
        key = (r[1], r[2])
        if key not in seen_fields:
            seen_fields.add(key)
            pfields.append({
                'section': safe_str(r[0]),
                'label':   safe_str(r[1]),
                'value':   safe_str(r[2]),   # keep original language
            })

    cur.execute("SELECT COUNT(*) FROM photo_posts WHERE profile_id=?", (pid,))
    pc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM reel_posts WHERE profile_id=?",  (pid,))
    rc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM text_posts WHERE profile_id=?",  (pid,))
    tc = cur.fetchone()[0]

    cur.execute("""
        SELECT co.id,co.name,co.profile_url,cs.total_score,cs.comment_count,cs.tier,
               cs.sentiment_score,cc.identified_country,cc.current_city,cc.employer,cc.education
        FROM commentor_scores cs
        JOIN commentors co ON co.id=cs.commentor_id
        LEFT JOIN commentor_country cc ON cc.commentor_id=co.id
        WHERE cs.main_profile_id=? ORDER BY cs.total_score DESC
    """, (pid,))
    commentors = [{
        'id':            r[0],
        'name':          safe_str(r[1] or ''),
        'profile_url':   r[2] or '',
        'total_score':   r[3] or 0.0,
        'comment_count': r[4] or 0,
        'tier':          r[5] or 'neutral',
        'sentiment_score': r[6] or 0.0,
        'country':       safe_str(r[7] or ''),
        'city':          safe_str(r[8] or ''),
        'employer':      safe_str(r[9] or ''),
        'education':     safe_str(r[10] or ''),
    } for r in cur.fetchall()]

    cur.execute("""
        SELECT sp.name,sp.profile_url,sp.relationship_type,sp.score,
               GROUP_CONCAT(spf.label||': '||spf.value,' | '),cc.identified_country
        FROM secondary_profiles sp
        LEFT JOIN secondary_profile_fields spf ON spf.secondary_profile_id=sp.id
        LEFT JOIN commentor_country cc ON cc.commentor_id=sp.commentor_id
        WHERE sp.main_profile_id=? GROUP BY sp.id ORDER BY sp.score DESC LIMIT 7
    """, (pid,))
    secondary = [{
        'name':        safe_str(r[0] or 'Unknown'),
        'profile_url': r[1] or '',
        'tier':        r[2] or 'neutral',
        'score':       r[3] or 0.0,
        'fields':      safe_str(r[4] or ''),
        'country':     safe_str(r[5] or ''),
    } for r in cur.fetchall()]

    cur.execute("""
        SELECT ia.scene_type,ia.objects,ia.activity,ia.political_symbols,ia.religious_symbols,
               ia.weapons_visible,ia.estimated_location,ia.text_in_image,ia.confidence,
               pp.image_src, pp.caption, pp.date_text
        FROM image_analysis ia JOIN photo_posts pp ON pp.id=ia.photo_post_id WHERE pp.profile_id=?
    """, (pid,))
    image_analysis = [{
        'scene_type':        safe_str(r[0]),
        'objects':           safe_str(r[1]),
        'activity':          safe_str(r[2]),
        'political_symbols': safe_str(r[3]),
        'religious_symbols': safe_str(r[4]),
        'weapons_visible':   r[5],
        'estimated_location':safe_str(r[6]),
        'text_in_image':     safe_str(r[7]),
        'confidence':        r[8],
        'image_src':         r[9],
        'caption':           safe_str(r[10]),
        'date':              safe_str(r[11]),
    } for r in cur.fetchall()]

    cur.execute("""
        SELECT tpa.topic,tpa.sentiment,tpa.narrative_type,tpa.key_entities,
               tpa.threat_indicators,tpa.text_language,tp.screenshot_path
        FROM text_post_analysis tpa JOIN text_posts tp ON tp.id=tpa.text_post_id WHERE tp.profile_id=?
    """, (pid,))
    text_posts = [{
        'topic':            safe_str(r[0]),
        'sentiment':        safe_str(r[1]),
        'narrative_type':   safe_str(r[2]),
        'key_entities':     safe_str(r[3]),
        'threat_indicators':safe_str(r[4]),
        'text_language':    safe_str(r[5]),
        'screenshot_path':  r[6],
    } for r in cur.fetchall()]

    # Reel intelligence
    reel_intel = []
    try:
        cur.execute("""
            SELECT rp.reel_url, rp.caption, rp.caption_context,
                   rp.caption_entities, rp.caption_hashtags, rp.caption_topic,
                   COUNT(DISTINCT rc.commentor_id) as comment_count
            FROM reel_posts rp
            LEFT JOIN reel_comments rc ON rc.reel_post_id = rp.id
            WHERE rp.profile_id = ?
              AND rp.caption IS NOT NULL
              AND rp.caption != ''
            GROUP BY rp.id
            ORDER BY comment_count DESC
        """, (pid,))
        for r in cur.fetchall():
            reel_intel.append({
                'reel_url':        r[0],
                'caption':         r[1],
                'caption_context': r[2],
                'caption_entities':r[3],
                'caption_hashtags':r[4],
                'caption_topic':   r[5],
                'comment_count':   r[6],
            })
    except Exception as e:
        print(f"  Reel intel fetch error: {e}")

    lang_counts     = {}
    sentiment_counts = {'positive': 0, 'neutral': 0, 'negative': 0}

    for tbl, src in [('photo_comments','photo'),('reel_comments','reel'),('text_comments','text')]:
        pt = tbl.replace('_comments','_posts')
        try:
            cur.execute(f"SELECT id FROM {pt} WHERE profile_id=?", (pid,))
            pids = [r[0] for r in cur.fetchall()]
            if not pids:
                continue
            cur.execute(f"""
                SELECT ca.language,ca.sentiment FROM comment_analysis ca
                JOIN {tbl} t ON t.id=ca.comment_id AND ca.db_source='{src}'
                WHERE t.{src}_post_id IN ({','.join('?'*len(pids))})
            """, pids)
            for lang, sent in cur.fetchall():
                l = safe_str(lang)
                if l and l.lower() not in ('unknown','null','none',''):
                    lang_counts[l] = lang_counts.get(l, 0) + 1
                if sent:
                    s = sent.lower()
                    if s == 'positive':   sentiment_counts['positive'] += 1
                    elif s == 'negative': sentiment_counts['negative'] += 1
                    else:                 sentiment_counts['neutral']  += 1
        except Exception:
            pass

    country_dist = {}
    for c in commentors:
        ct = c['country'] or 'Unknown'
        country_dist[ct] = country_dist.get(ct, 0) + 1

    # timeline
    timeline_data = []
    for tbl, src, url_col, id_col in [
        ('photo_posts','photo','photo_url','photo_post_id'),
        ('reel_posts', 'reel', 'reel_url', 'reel_post_id'),
        ('text_posts', 'text', 'post_url', 'text_post_id'),
    ]:
        comment_tbl = src + '_comments'
        try:
            cur.execute(f"""
                SELECT pp.id, pp.date_text,
                       COUNT(DISTINCT c.id),
                       COUNT(DISTINCT CASE WHEN ca.sentiment='positive' THEN c.id END),
                       COUNT(DISTINCT CASE WHEN ca.sentiment='negative' THEN c.id END),
                       COUNT(DISTINCT CASE WHEN ca.sentiment IS NOT NULL
                                AND ca.sentiment NOT IN ('positive','negative')
                                THEN c.id END)
                FROM {tbl} pp
                LEFT JOIN {comment_tbl} c ON c.{id_col} = pp.id
                LEFT JOIN comment_analysis ca ON ca.comment_id = c.id
                WHERE pp.profile_id = ?
                GROUP BY pp.id ORDER BY pp.date_text
            """, (pid,))
            for r in cur.fetchall():
                date_str = safe_str(r[1] or 'Unknown')
                short    = date_str[:12]
                timeline_data.append({
                    'label':    f"[{src[:1].upper()}] {short}",
                    'date':     date_str,
                    'type':     src,
                    'total':    r[2] or 0,
                    'positive': r[3] or 0,
                    'negative': r[4] or 0,
                    'neutral':  r[5] or 0,
                    'sort_key': r[1] or '',
                })
        except Exception:
            pass

    con.close()
    return {
        'type': 'profile', 'owner_name': owner,
        'profile_url': profile_url, 'is_locked': locked,
        'profile_id': pid,
        'profile_fields': pfields,
        'photo_count': pc, 'reel_count': rc, 'text_count': tc,
        'commentors': commentors, 'secondary': secondary,
        'image_analysis': image_analysis, 'text_posts': text_posts,
        'reel_intel': reel_intel,
        'lang_counts': lang_counts, 'sentiment_counts': sentiment_counts,
        'country_dist': country_dist, 'timeline_data': timeline_data,
    }


def fetch_batch_data(batch_id, db_file=MANUAL_DB_FILE):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("SELECT label FROM batches WHERE batch_id=?", (batch_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        return None
    label = safe_str(row[0])

    cur.execute("SELECT COUNT(*),type FROM manual_posts WHERE batch_id=? GROUP BY type", (batch_id,))
    pc = {'photo': 0, 'post': 0, 'reel': 0}
    for r in cur.fetchall():
        pc[r[1]] = r[0]

    cur.execute("""
        SELECT co.id,co.name,co.profile_url,bcs.total_score,bcs.comment_count,bcs.tier,
               bcs.sentiment_score,cc.identified_country,cc.current_city,cc.employer,cc.education
        FROM batch_commentor_scores bcs
        JOIN commentors co ON co.id=bcs.commentor_id
        LEFT JOIN commentor_country cc ON cc.commentor_id=co.id
        WHERE bcs.batch_id=? ORDER BY bcs.total_score DESC
    """, (batch_id,))
    commentors = [{
        'id':            r[0],
        'name':          safe_str(r[1] or ''),
        'profile_url':   r[2] or '',
        'total_score':   r[3] or 0.0,
        'comment_count': r[4] or 0,
        'tier':          r[5] or 'neutral',
        'sentiment_score': r[6] or 0.0,
        'country':       safe_str(r[7] or ''),
        'city':          safe_str(r[8] or ''),
        'employer':      safe_str(r[9] or ''),
        'education':     safe_str(r[10] or ''),
    } for r in cur.fetchall()]

    cur.execute("""
        SELECT sp.name,sp.profile_url,sp.relationship_type,sp.score,
               GROUP_CONCAT(spf.label||': '||spf.value,' | '),cc.identified_country
        FROM secondary_profiles sp
        LEFT JOIN secondary_profile_fields spf ON spf.secondary_profile_id=sp.id
        LEFT JOIN commentor_country cc ON cc.commentor_id=sp.commentor_id
        WHERE sp.batch_id=? GROUP BY sp.id ORDER BY sp.score DESC LIMIT 7
    """, (batch_id,))
    secondary = [{
        'name':        safe_str(r[0] or 'Unknown'),
        'profile_url': r[1] or '',
        'tier':        r[2] or 'neutral',
        'score':       r[3] or 0.0,
        'fields':      safe_str(r[4] or ''),
        'country':     safe_str(r[5] or ''),
    } for r in cur.fetchall()]

    reel_intel = []
    try:
        cur.execute("""
            SELECT mp.url as reel_url, mp.caption, mp.caption_context,
                   mp.caption_entities, mp.caption_hashtags, mp.caption_topic,
                   COUNT(DISTINCT c.commentor_id) as comment_count
            FROM manual_posts mp
            LEFT JOIN comments c ON c.post_id = mp.id
            WHERE mp.batch_id = ?
              AND mp.type = 'reel'
              AND mp.caption IS NOT NULL
              AND mp.caption != ''
            GROUP BY mp.id
            ORDER BY comment_count DESC
        """, (batch_id,))
        for r in cur.fetchall():
            reel_intel.append({
                'reel_url':        r[0],
                'caption':         r[1],
                'caption_context': r[2],
                'caption_entities':r[3],
                'caption_hashtags':r[4],
                'caption_topic':   r[5],
                'comment_count':   r[6],
            })
    except Exception as e:
        print(f"  Reel intel fetch error: {e}")

    lang_counts      = {}
    sentiment_counts = {'positive': 0, 'neutral': 0, 'negative': 0}
    cur.execute("""
        SELECT ca.language,ca.sentiment FROM comment_analysis ca
        JOIN comments c ON c.id=ca.comment_id AND ca.db_source='manual'
        JOIN manual_posts mp ON mp.id=c.post_id WHERE mp.batch_id=?
    """, (batch_id,))
    for lang, sent in cur.fetchall():
        l = safe_str(lang)
        if l and l.lower() not in ('unknown','null','none',''):
            lang_counts[l] = lang_counts.get(l, 0) + 1
        if sent:
            s = sent.lower()
            if s == 'positive':   sentiment_counts['positive'] += 1
            elif s == 'negative': sentiment_counts['negative'] += 1
            else:                 sentiment_counts['neutral']  += 1

    country_dist = {}
    for c in commentors:
        ct = c['country'] or 'Unknown'
        country_dist[ct] = country_dist.get(ct, 0) + 1

    cur.execute("""
        SELECT url, type, date_text, caption
        FROM manual_posts WHERE batch_id = ? ORDER BY type, id
    """, (batch_id,))
    manual_urls = [{
        'url':     r[0],
        'type':    r[1] or 'unknown',
        'date':    safe_str(r[2] or 'N/A'),
        'caption': safe_str(r[3] or ''),
    } for r in cur.fetchall()]

    # ── NEW: stance by country ────────────────────────────────────────────────
    # Most supporting country — skip Unknown, fallback to next valid
    support_by_country = {}
    oppose_by_country  = {}
    try:
        cur.execute("""
            SELECT cc.identified_country, ca.stance
            FROM comment_analysis ca
            JOIN comments c ON c.id = ca.comment_id AND ca.db_source = 'manual'
            JOIN manual_posts mp ON mp.id = c.post_id
            JOIN commentors co ON co.id = c.commentor_id
            LEFT JOIN commentor_country cc ON cc.commentor_id = co.id
            WHERE mp.batch_id = ?
              AND ca.stance IS NOT NULL
        """, (batch_id,))
        for country, stance in cur.fetchall():
            if not is_valid_country(country):
                continue
            ct = safe_str(country)
            s  = (stance or '').lower()
            if 'support' in s:
                support_by_country[ct] = support_by_country.get(ct, 0) + 1
            elif 'oppose' in s:
                oppose_by_country[ct] = oppose_by_country.get(ct, 0) + 1
    except Exception as e:
        print(f'  [STANCE COUNTRY] Error: {e}')

    most_support_country = max(support_by_country, key=support_by_country.get) \
        if support_by_country else None
    most_oppose_country  = max(oppose_by_country, key=oppose_by_country.get) \
        if oppose_by_country else None

    # ── NEW: multiple appeared users ─────────────────────────────────────────
    # Commentors who appear in >= 2 posts, sorted by frequency
    multi_appeared = []
    try:
        cur.execute("""
            SELECT co.id, co.name, co.profile_url,
                   COUNT(DISTINCT c.post_id) as post_count,
                   COUNT(c.id) as comment_count,
                   cc.identified_country
            FROM comments c
            JOIN manual_posts mp ON mp.id = c.post_id
            JOIN commentors co ON co.id = c.commentor_id
            LEFT JOIN commentor_country cc ON cc.commentor_id = co.id
            WHERE mp.batch_id = ?
            GROUP BY co.id
            HAVING post_count >= 2
            ORDER BY post_count DESC, comment_count DESC
        """, (batch_id,))
        for r in cur.fetchall():
            multi_appeared.append({
                'id':            r[0],
                'name':          safe_str(r[1] or 'Unknown'),
                'profile_url':   r[2] or '',
                'post_count':    r[3] or 0,
                'comment_count': r[4] or 0,
                'country':       safe_str(r[5] or 'Unknown'),
            })
    except Exception as e:
        print(f'  [MULTI APPEARED] Error: {e}')

    # timeline
    timeline_data = []
    try:
        cur.execute("""
            SELECT mp.id, mp.date_text, mp.type,
                   COUNT(DISTINCT c.id),
                   COUNT(DISTINCT CASE WHEN ca.sentiment='positive' THEN c.id END),
                   COUNT(DISTINCT CASE WHEN ca.sentiment='negative' THEN c.id END),
                   COUNT(DISTINCT CASE WHEN ca.sentiment IS NOT NULL
                            AND ca.sentiment NOT IN ('positive','negative')
                            THEN c.id END)
            FROM manual_posts mp
            LEFT JOIN comments c ON c.post_id = mp.id
            LEFT JOIN comment_analysis ca ON ca.comment_id = c.id
            WHERE mp.batch_id = ?
            GROUP BY mp.id ORDER BY mp.date_text
        """, (batch_id,))
        for r in cur.fetchall():
            date_str = safe_str(r[1] or 'Unknown')
            short    = date_str[:12]
            ptype    = r[2] or 'post'
            timeline_data.append({
                'label':    f"[{ptype[:1].upper()}] {short}",
                'date':     date_str,
                'type':     ptype,
                'total':    r[3] or 0,
                'positive': r[4] or 0,
                'negative': r[5] or 0,
                'neutral':  r[6] or 0,
                'sort_key': r[1] or '',
            })
    except Exception:
        pass

    
    # Batch image analysis — photo type manual posts with AI results
    batch_image_analysis = []
    try:
        cur.execute("""
            SELECT mp.url, mp.date_text, mp.image_src, mp.caption,
                   ia.scene_type, ia.activity, ia.estimated_location,
                   ia.political_symbols, ia.religious_symbols, ia.weapons_visible,
                   ia.text_in_image, ia.confidence
            FROM manual_posts mp
            JOIN image_analysis ia ON ia.photo_post_id = mp.id
            WHERE mp.batch_id = ? AND mp.type = 'photo'
            ORDER BY mp.date_text DESC
        """, (batch_id,))
        for r in cur.fetchall():
            batch_image_analysis.append({
                'photo_url':        r[0],
                'date':             r[1],
                'image_src':        r[2],
                'caption':          r[3],
                'scene_type':       r[4],
                'activity':         r[5],
                'estimated_location': r[6],
                'political_symbols':  r[7],
                'religious_symbols':  r[8],
                'weapons_visible':    r[9],
                'text_in_image':      r[10],
                'confidence':         r[11],
            })
    except Exception as e:
        print(f"  Batch image analysis fetch error: {e}")

    # Batch text post analysis — post type manual posts with AI results
    batch_text_posts = []
    try:
        cur.execute("""
            SELECT mp.url, mp.date_text, mp.caption,
                   tpa.topic, tpa.sentiment, tpa.narrative_type,
                   tpa.key_entities, tpa.threat_indicators,
                   tpa.text_language, tpa.extracted_text
            FROM manual_posts mp
            JOIN text_post_analysis tpa ON tpa.text_post_id = mp.id
            WHERE mp.batch_id = ? AND mp.type = 'post'
            ORDER BY mp.date_text DESC
        """, (batch_id,))
        for r in cur.fetchall():
            batch_text_posts.append({
                'post_url':          r[0],
                'date_text':         r[1],
                'caption':           r[2],
                'topic':             r[3],
                'sentiment':         r[4],
                'narrative_type':    r[5],
                'key_entities':      r[6],
                'threat_indicators': r[7],
                'text_language':     r[8],
                'screenshot_path':   None,
            })
    except Exception as e:
        print(f"  Batch text post analysis fetch error: {e}")

    con.close()

    return {
        'type': 'batch', 'owner_name': label,
        'profile_url': batch_id, 'is_locked': False,
        'batch_id': batch_id,
        'profile_fields': [],
        'photo_count': pc.get('photo', 0),
        'reel_count':  pc.get('reel', 0),
        'text_count':  pc.get('post', 0),
        'commentors': commentors, 'secondary': secondary,
        'image_analysis': batch_image_analysis,
        'text_posts':     batch_text_posts,
        'reel_intel': reel_intel,
        'lang_counts': lang_counts, 'sentiment_counts': sentiment_counts,
        'country_dist': country_dist, 'timeline_data': timeline_data,
        'manual_urls': manual_urls,
        'most_support_country': most_support_country,
        'most_oppose_country':  most_oppose_country,
        'support_by_country':   support_by_country,
        'oppose_by_country':    oppose_by_country,
        'multi_appeared':       multi_appeared,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def build_cover(data, S):
    story = []
    story.append(Spacer(1, 1.5*cm))
    # Logo — path is resolved at module level relative to script location
    _logo = LOGO_PATH
    print(f"  Logo path: {_logo}")
    print(f"  Logo exists: {os.path.exists(_logo)}")
    if os.path.exists(_logo):
        try:
            from PIL import Image as PILImage
            pil = PILImage.open(_logo)
            pil = pil.convert('RGB')
            import tempfile
            _tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            pil.save(_tmp.name, 'JPEG', quality=92)
            img = Image(_tmp.name, width=7.5*cm, height=7.5*cm)
            img.hAlign = 'CENTER'
            story.append(img)
            print("  ✅ Logo loaded")
        except Exception as e:
            print(f"  ⚠️  Logo load error: {e}")
    else:
        print(f"  ⚠️  Logo not found at: {_logo}")
    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph("BIRDY-EDWARDS WRAITH", S['cover_title']))
    story.append(HRFlowable(width='65%', thickness=2.5, color=C_ACCENT,
                             spaceAfter=14, spaceBefore=4))
    story.append(Paragraph(
        '"The truth is never buried deep enough<br/>to escape the right set of eyes."',
        S['cover_quote']))
    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width='100%', thickness=0.8, color=C_BORDER, spaceAfter=14))
    story.append(Paragraph("SOCMINT  INVESTIGATION  REPORT", ParagraphStyle(
        'rt', fontName='Helvetica-Bold', fontSize=17, textColor=C_ACCENT2,
        alignment=TA_CENTER, spaceAfter=16)))

    inv_type = 'Automated Profile' if data['type'] == 'profile' else 'Manual Batch'
    meta = [
        ('TARGET', data['owner_name']),
        ('TYPE',   inv_type),
        ('ID',     (data['profile_url'][:55] +
                    ('...' if len(data['profile_url']) > 55 else ''))),
        ('DATE',   datetime.now().strftime('%B %d, %Y  at  %H:%M')),
    ]
    tbl = Table(
        [[Paragraph(k, S['meta_label']), Paragraph(v, S['meta_value'])]
         for k, v in meta],
        colWidths=[3.5*cm, 12*cm]
    )
    tbl.setStyle(TableStyle([
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [C_LIGHT_BG, C_WHITE]),
        ('TOPPADDING',    (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 9),
        ('LEFTPADDING',   (0,0), (-1,-1), 12),
        ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
        ('LINEBEFORE',    (0,0), (0,-1),  3, C_ACCENT),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width='100%', thickness=0.5, color=C_BORDER, spaceAfter=10))
    story.append(Paragraph("Developed for:  Investigators", S['dev']))
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph("CONFIDENTIAL  —  FOR  AUTHORIZED  USE  ONLY", S['confidential']))
    story.append(PageBreak())
    return story


def build_profile_summary(data, S):
    story = []
    story += sec_hdr("01 / PROFILE SUMMARY", S)

    stats = [
        ('Photos',     str(data['photo_count'])),
        ('Reels',      str(data['reel_count'])),
        ('Text Posts', str(data['text_count'])),
        ('Commentors', str(len(data['commentors']))),
        ('Locked',     'Yes' if data['is_locked'] else 'No'),
    ]
    st = Table(
        [[Paragraph(s[0], S['stat_label']) for s in stats],
         [Paragraph(s[1], S['stat_value']) for s in stats]],
        colWidths=[3.2*cm]*5
    )
    st.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), C_LIGHT_BG),
        ('TOPPADDING',    (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
        ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
        ('LINEABOVE',     (0,0), (-1,0),  3, C_ACCENT),
        ('ALIGN',         (0,0), (-1,-1), 'CENTER'),
    ]))
    story.append(st)
    story.append(Spacer(1, 0.5*cm))

    if data['profile_fields']:
        story.append(Paragraph("About Information", S['sub_title']))
        rows = [[
            Paragraph(f['label'], ParagraphStyle('fl', fontName='Helvetica-Bold',
                fontSize=13, textColor=C_ACCENT2)),
            Paragraph(f['value'], S['body']),
        ] for f in data['profile_fields'][:25]]
        ft = Table(rows, colWidths=[5*cm, 11*cm])
        ft.setStyle(TableStyle([
            ('ROWBACKGROUNDS', (0,0), (-1,-1), [C_WHITE, C_LIGHT_BG]),
            ('TOPPADDING',    (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
            ('LINEBEFORE',    (0,0), (0,-1),  3, C_ACCENT2),
        ]))
        story.append(ft)

    story.append(PageBreak())
    return story


def build_network_overview(data, S):
    story = []
    story += sec_hdr("02 / NETWORK OVERVIEW", S)

    cb = chart_countries(data['country_dist'])
    if cb:
        n = len(data['country_dist'])
        h = max(7*cm, min(n * 0.9*cm, 16*cm))
        img = Image(cb, width=17*cm, height=h)
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.6*cm))

    tier_counts = {}
    for c in data['commentors']:
        lbl, _ = get_score_label(c['total_score'])
        tier_counts[lbl] = tier_counts.get(lbl, 0) + 1

    tb = chart_tiers(tier_counts)
    if tb:
        img = Image(tb, width=17*cm, height=9*cm)
        img.hAlign = 'CENTER'
        story.append(img)

    story.append(PageBreak())
    return story


def build_commentors_table(data, S, commentor_details=None):
    """
    Updated commentors table:
    - Top 5 banner at top
    - Table columns: # | NAME | COUNTRY | PROFILE URL | SCORE | COMMENTS | CATEGORY
    - Extended sub-table per commentor showing up to 3 comments + stance + sentiment
    """
    story = []
    story += sec_hdr("03 / ALL COMMENTORS", S)

    # ── Top 5 banner ──────────────────────────────────────────────────────────
    story += build_top5_banner(data['commentors'], S)

    # ── Main table ────────────────────────────────────────────────────────────
    headers = ['#', 'NAME', 'COUNTRY', 'PROFILE', 'SCORE', 'CMT', 'CATEGORY']
    col_w   = [0.8*cm, 4.2*cm, 2.8*cm, 1.6*cm, 2.0*cm, 1.2*cm, 3.2*cm]
    rows    = [[Paragraph(h, S['table_header']) for h in headers]]

    for i, c in enumerate(data['commentors'], 1):
        lbl, _ = get_score_label(c['total_score'])
        sc     = colors.HexColor(get_score_hex(c['total_score']))
        flag   = get_country_flag(c.get('country'))
        country_str = f"{flag} {c.get('country') or 'Unknown'}"

        rows.append([
            Paragraph(str(i), ParagraphStyle('n', fontName='Helvetica', fontSize=11,
                textColor=C_SUBTEXT, alignment=TA_CENTER)),
            Paragraph(safe_str(c['name'], maxlen=28), S['table_cell']),
            Paragraph(safe_str(country_str, transliterate=True, maxlen=22), S['table_cell']),
            hyperlink_url(c['profile_url']),
            Paragraph(f"{c['total_score']:+.3f}", ParagraphStyle('sc',
                fontName='Helvetica-Bold', fontSize=11, textColor=sc, alignment=TA_CENTER)),
            Paragraph(str(c['comment_count']), S['table_cell_c']),
            Paragraph(lbl, ParagraphStyle('cl', fontName='Helvetica-Bold', fontSize=10,
                textColor=sc, alignment=TA_CENTER)),
        ])

    t = Table(rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,0),  C_ACCENT2),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_WHITE, C_LIGHT_BG]),
        ('TOPPADDING',    (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('LEFTPADDING',   (0,0), (-1,-1), 5),
        ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
        ('LINEBELOW',     (0,0), (-1,0),  2, C_ACCENT),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(t)
    story.append(PageBreak())

    # ── Comments + Stance + Sentiment per commentor ───────────────────────────
    if commentor_details:
        story += sec_hdr("03a / COMMENTOR ACTIVITY — COMMENTS, STANCE & SENTIMENT", S)
        story.append(Paragraph(
            "Sample comments (up to 3 per person) with AI-detected stance and sentiment.",
            S['body_sub']))
        story.append(Spacer(1, 0.3*cm))

        for c in data['commentors']:
            cid     = c['id']
            details = commentor_details.get(cid, [])
            if not details:
                continue

            lbl, _ = get_score_label(c['total_score'])
            sc_hex = get_score_hex(c['total_score'])
            flag   = get_country_flag(c.get('country'))

            # commentor header row
            header_tbl = Table([[
                unicode_para(
                    safe_str(c['name'], maxlen=40) + "  " +
                    flag + " " + safe_str(c.get('country') or 'Unknown', transliterate=True),
                    font_size=13, bold=True, color=C_DARK),
                Paragraph(
                    f"{c['comment_count']} interactions  |  {lbl}  |  score {c['total_score']:+.3f}",
                    ParagraphStyle('cs_h', fontName='Helvetica', fontSize=11,
                                   textColor=colors.HexColor(sc_hex), alignment=TA_RIGHT)),
            ]], colWidths=[10*cm, 7*cm])
            header_tbl.setStyle(TableStyle([
                ('BACKGROUND',   (0,0), (-1,-1), colors.HexColor('#f0f3f7')),
                ('TOPPADDING',   (0,0), (-1,-1), 6),
                ('BOTTOMPADDING',(0,0), (-1,-1), 6),
                ('LEFTPADDING',  (0,0), (-1,-1), 10),
                ('LINEBEFORE',   (0,0), (0,-1),  4, colors.HexColor(sc_hex)),
                ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
            ]))
            story.append(header_tbl)

            # comment rows
            c_headers = ['COMMENT', 'POST', 'STANCE', 'SENTIMENT']
            c_col_w   = [8.5*cm, 3*cm, 2.5*cm, 2.5*cm]
            c_rows    = [[Paragraph(h, ParagraphStyle('dh', fontName='Helvetica-Bold',
                fontSize=11, textColor=C_WHITE, alignment=TA_CENTER)) for h in c_headers]]

            for d in details:
                # stance color
                stance_s = (d['stance'] or '').lower()
                if 'support' in stance_s:
                    s_color = C_GREEN
                elif 'oppose' in stance_s:
                    s_color = C_RED
                else:
                    s_color = C_SILVER

                # sentiment color
                sent_s = (d['sentiment'] or '').lower()
                if 'positive' in sent_s:
                    sent_color = C_GREEN
                elif 'negative' in sent_s:
                    sent_color = C_RED
                else:
                    sent_color = C_SILVER

                # post url cell
                post_url = d.get('post_url', '')
                if post_url:
                    post_cell = Paragraph(
                        f'<link href="{post_url}" color="#2980b9"><u>view post</u></link>',
                        ParagraphStyle('dp', fontName='Helvetica', fontSize=10,
                                       textColor=C_BLUE, alignment=TA_CENTER))
                else:
                    post_cell = Paragraph('—', ParagraphStyle('dp2', fontName='Helvetica',
                        fontSize=10, textColor=C_SUBTEXT, alignment=TA_CENTER))

                c_rows.append([
                    unicode_para(d['comment'], font_size=11, leading=16, max_width_pt=240, max_len=350),
                    post_cell,
                    Paragraph(d['stance'], ParagraphStyle('dst', fontName='Helvetica-Bold',
                        fontSize=10, textColor=s_color, alignment=TA_CENTER)),
                    Paragraph(d['sentiment'], ParagraphStyle('dse', fontName='Helvetica-Bold',
                        fontSize=10, textColor=sent_color, alignment=TA_CENTER)),
                ])

            ct = Table(c_rows, colWidths=c_col_w, repeatRows=1, splitByRow=1)
            ct.setStyle(TableStyle([
                ('BACKGROUND',    (0,0), (-1,0),  C_DARK),
                ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_WHITE, C_LIGHT_BG]),
                ('TOPPADDING',    (0,0), (-1,-1), 6),
                ('BOTTOMPADDING', (0,0), (-1,-1), 6),
                ('LEFTPADDING',   (0,0), (-1,-1), 8),
                ('GRID',          (0,0), (-1,-1), 0.4, C_BORDER),
                ('VALIGN',        (0,0), (-1,-1), 'TOP'),
            ]))
            story.append(ct)
            story.append(Spacer(1, 0.3*cm))

        story.append(PageBreak())

    return story


def build_top7(data, S):
    story = []
    story += sec_hdr("04 / TOP 7 CLOSE NETWORK", S)

    profiles = data['secondary'] if data['secondary'] else data['commentors'][:7]

    cb = chart_top7(profiles[:7])
    if cb:
        img = Image(cb, width=14*cm, height=11*cm)
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.5*cm))

    cards = []
    for i, c in enumerate(profiles[:7]):
        rank   = i + 1
        score  = c.get('total_score', c.get('score', 0.0))
        lbl, _ = get_score_label(score)
        country = safe_str(c.get('country') or 'Unknown', transliterate=True)
        bc     = colors.HexColor(get_score_hex(score))

        if c.get('fields'):
            ftext = safe_str(c['fields'], maxlen=140)
        else:
            parts = []
            if c.get('city'):      parts.append(f"City: {safe_str(c['city'])}")
            if c.get('employer'):  parts.append(f"Work: {safe_str(c['employer'])}")
            if c.get('education'): parts.append(f"Edu: {safe_str(c['education'])}")
            ftext = '  |  '.join(parts) if parts else 'No additional data'

        card = Table([
            [unicode_para(f"#{rank}  {safe_str(c['name'], maxlen=28)}", font_size=14, bold=True, color=C_DARK, space_after=3)],
            [Paragraph(f"{get_country_flag(c.get('country'))} {country}", S['card_country'])],
            [Paragraph(f"Score: {score:+.3f}  |  {lbl}", ParagraphStyle(
                'cs3', fontName='Helvetica-Bold', fontSize=13,
                textColor=bc, spaceAfter=3))],
            [Paragraph(ftext[:150], S['card_detail'])],
        ], colWidths=[8.2*cm])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0), (-1,-1), C_WHITE),
            ('TOPPADDING',   (0,0), (-1,-1), 7),
            ('BOTTOMPADDING',(0,0), (-1,-1), 7),
            ('LEFTPADDING',  (0,0), (-1,-1), 10),
            ('BOX',          (0,0), (-1,-1), 0.5, C_BORDER),
            ('LINEABOVE',    (0,0), (-1,0),  3, bc),
        ]))
        cards.append(card)

    for i in range(0, len(cards), 2):
        row_c = cards[i:i+2]
        pair  = [row_c[0], row_c[1]] if len(row_c) == 2 else [row_c[0], '']
        row   = Table([pair], colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN',       (0,0), (-1,-1), 'TOP'),
            ('LEFTPADDING',  (0,0), (-1,-1), 2),
            ('RIGHTPADDING', (0,0), (-1,-1), 2),
            ('TOPPADDING',   (0,0), (-1,-1), 3),
            ('BOTTOMPADDING',(0,0), (-1,-1), 3),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.2*cm))

    story.append(PageBreak())
    return story


# ══════════════════════════════════════════════════════════════════════════════
#  NEW: BATCH-SPECIFIC INTELLIGENCE SECTION
# ══════════════════════════════════════════════════════════════════════════════

def build_batch_intelligence(data, S):
    """
    Manual batch only:
    - Most supporting stance country
    - Most opposing stance country
    - Multiple time appeared users (≥2 posts) sorted by frequency
    """
    if data.get('type') != 'batch':
        return []

    story = []
    story += sec_hdr("03b / BATCH INTELLIGENCE", S)

    # ── Stance country cards ──────────────────────────────────────────────────
    support_c = data.get('most_support_country')
    oppose_c  = data.get('most_oppose_country')
    sup_count = data.get('support_by_country', {}).get(support_c, 0) if support_c else 0
    opp_count = data.get('oppose_by_country',  {}).get(oppose_c,  0) if oppose_c  else 0

    story.append(Paragraph("Stance Geography", S['sub_title']))
    story.append(Paragraph(
        "Countries with highest supporting and opposing comment activity. "
        "Unknown/unidentified countries are excluded.",
        S['body_sub']))
    story.append(Spacer(1, 0.3*cm))

    def stance_card(title, country, count, bg_color, icon):
        if not country:
            content = [
                [Paragraph(f"{icon}  {title}", ParagraphStyle('sc_t',
                    fontName='Helvetica-Bold', fontSize=13,
                    textColor=colors.white, alignment=TA_CENTER))],
                [Paragraph("No data identified", ParagraphStyle('sc_nd',
                    fontName='Helvetica', fontSize=12,
                    textColor=colors.HexColor('#ecf0f1'), alignment=TA_CENTER))],
            ]
        else:
            flag = get_country_flag(country)
            content = [
                [Paragraph(f"{icon}  {title}", ParagraphStyle('sc_t2',
                    fontName='Helvetica-Bold', fontSize=13,
                    textColor=colors.white, alignment=TA_CENTER))],
                [Paragraph(f"{flag} {safe_str(country, transliterate=True)}",
                    ParagraphStyle('sc_c', fontName='Helvetica-Bold', fontSize=20,
                    textColor=colors.white, alignment=TA_CENTER))],
                [Paragraph(f"{count} comments", ParagraphStyle('sc_n',
                    fontName='Helvetica', fontSize=13,
                    textColor=colors.HexColor('#ecf0f1'), alignment=TA_CENTER))],
            ]
        card = Table(content, colWidths=[8*cm])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0), (-1,-1), colors.HexColor(bg_color)),
            ('TOPPADDING',   (0,0), (-1,-1), 14),
            ('BOTTOMPADDING',(0,0), (-1,-1), 14),
            ('ALIGN',        (0,0), (-1,-1), 'CENTER'),
            ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
        ]))
        return card

    sup_card = stance_card("MOST SUPPORTING", support_c, sup_count, '#27ae60', '👍')
    opp_card = stance_card("MOST OPPOSING",   oppose_c,  opp_count, '#c0392b', '👎')

    pair = Table([[sup_card, opp_card]], colWidths=[8.5*cm, 8.5*cm])
    pair.setStyle(TableStyle([
        ('VALIGN',      (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 2),
        ('RIGHTPADDING',(0,0), (-1,-1), 2),
    ]))
    story.append(pair)
    story.append(Spacer(1, 0.6*cm))

    # ── Multiple appeared users ───────────────────────────────────────────────
    multi = data.get('multi_appeared', [])
    if multi:
        story.append(Paragraph("Multiple-Post Interactors", S['sub_title']))
        story.append(Paragraph(
            "Users who appeared in 2 or more posts in this batch — "
            "sorted by post frequency. High frequency may indicate coordinated activity.",
            S['body_sub']))
        story.append(Spacer(1, 0.3*cm))

        headers = ['#', 'NAME', 'COUNTRY', 'PROFILE', 'POSTS', 'TOTAL COMMENTS']
        col_w   = [0.8*cm, 4.5*cm, 3*cm, 1.8*cm, 2*cm, 3*cm]
        rows    = [[Paragraph(h, S['table_header']) for h in headers]]

        for i, u in enumerate(multi, 1):
            pc     = u['post_count']
            color  = C_RED if pc >= 5 else C_YELLOW if pc >= 3 else C_BLUE
            flag   = get_country_flag(u.get('country'))
            rows.append([
                Paragraph(str(i), ParagraphStyle('mu_n', fontName='Helvetica',
                    fontSize=11, textColor=C_SUBTEXT, alignment=TA_CENTER)),
                unicode_para(safe_str(u['name'], maxlen=32), font_size=12),
                Paragraph(
                    f"{flag} {safe_str(u.get('country') or 'Unknown', transliterate=True, maxlen=20)}",
                    S['table_cell']),
                hyperlink_url(u['profile_url']),
                Paragraph(str(pc), ParagraphStyle('mu_p', fontName='Helvetica-Bold',
                    fontSize=13, textColor=color, alignment=TA_CENTER)),
                Paragraph(str(u['comment_count']), S['table_cell_c']),
            ])

        t = Table(rows, colWidths=col_w, repeatRows=1)
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,0),  C_ACCENT2),
            ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_WHITE, C_LIGHT_BG]),
            ('TOPPADDING',    (0,0), (-1,-1), 7),
            ('BOTTOMPADDING', (0,0), (-1,-1), 7),
            ('LEFTPADDING',   (0,0), (-1,-1), 5),
            ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
            ('LINEBELOW',     (0,0), (-1,0),  2, C_ACCENT),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(t)
    else:
        story.append(Paragraph(
            "No users appeared in more than one post in this batch.",
            S['body_sub']))

    story.append(PageBreak())
    return story


# ── Keep existing builders (content analysis, comment intelligence, etc.) ─────
# These are unchanged from original — included here for completeness

def build_reel_intelligence_section(data, S):
    """Section 05b — Reel Intelligence: caption + AI verdict per reel."""
    import json as _json
    reels = data.get('reel_intel', [])
    if not reels:
        return []

    story = []
    story += sec_hdr("05b / REEL INTELLIGENCE", S)
    story.append(Paragraph(
        f"AI-analyzed captions from {len(reels)} reel(s) with caption data.",
        S['table_cell']
    ))
    story.append(Spacer(1, 0.3*cm))

    for i, r in enumerate(reels, 1):
        url     = safe_str(r.get('reel_url') or 'N/A', maxlen=80)
        caption = safe_str(r.get('caption') or 'N/A', maxlen=300)
        context = safe_str(r.get('caption_context') or '—', maxlen=200)
        topic   = safe_str(r.get('caption_topic') or '—', maxlen=60)
        count   = str(r.get('comment_count') or 0)

        # Parse entities and hashtags
        try:
            entities = ', '.join(_json.loads(r.get('caption_entities') or '[]')) or '—'
        except Exception:
            entities = '—'
        try:
            hashtags = ' '.join(_json.loads(r.get('caption_hashtags') or '[]')) or '—'
        except Exception:
            hashtags = '—'

        # Reel header
        story.append(Paragraph(
            f"Reel #{i}",
            ParagraphStyle('rh', fontName='Helvetica-Bold', fontSize=13,
                           textColor=C_ACCENT2, spaceBefore=10, spaceAfter=4)
        ))

        lbl = ParagraphStyle('rl', fontName='Helvetica-Bold', fontSize=11,
                             textColor=C_ACCENT2)

        rows = [
            [Paragraph('URL',           lbl), Paragraph(url,     S['table_cell'])],
            [Paragraph('Caption',       lbl), Paragraph(caption, S['table_cell'])],
            [Paragraph('AI Context',    lbl), Paragraph(context, S['table_cell'])],
            [Paragraph('Topic',         lbl), Paragraph(topic,   S['table_cell'])],
            [Paragraph('Named Entities',lbl), Paragraph(entities,S['table_cell'])],
            [Paragraph('Hashtags',      lbl), Paragraph(hashtags,S['table_cell'])],
            [Paragraph('Interactors',   lbl), Paragraph(count,   S['table_cell'])],
        ]

        tbl = Table(rows, colWidths=[3.5*cm, 13.5*cm])
        tbl.setStyle(TableStyle([
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [C_WHITE, C_LIGHT_BG]),
            ('TOPPADDING',     (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING',  (0, 0), (-1, -1), 6),
            ('LEFTPADDING',    (0, 0), (-1, -1), 8),
            ('GRID',           (0, 0), (-1, -1), 0.5, C_BORDER),
            ('LINEBEFORE',     (0, 0), (0, -1),  3,   C_ACCENT2),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.3*cm))

    return story

def build_content_analysis(data, S):
    if not data['image_analysis'] and not data['text_posts']:
        return []
    story = []
    story += sec_hdr("05 / CONTENT ANALYSIS", S)

    if data['image_analysis']:
        story.append(Paragraph("Image Intelligence", S['sub_title']))
        for i, img_data in enumerate(data['image_analysis'], 1):
            story.append(Paragraph(f"Photo {i}", ParagraphStyle(
                'ph', fontName='Helvetica-Bold', fontSize=13, textColor=C_ACCENT2,
                spaceBefore=8, spaceAfter=4)))
            thumb_cell = ''
            image_src  = img_data.get('image_src')
            if image_src:
                thumb_path = download_image_thumb(image_src)
                if thumb_path:
                    try:
                        thumb_cell = Image(thumb_path, width=4*cm, height=4*cm)
                    except Exception:
                        pass

            def flag(v):
                has = v and str(v).lower() not in ('none','null','false','no','')
                return Paragraph('YES' if has else 'NO', ParagraphStyle(
                    'if3', fontName='Helvetica-Bold', fontSize=12, alignment=TA_CENTER,
                    textColor=C_RED if has else C_SILVER))

            info_rows = [
                [Paragraph('Date',     ParagraphStyle('il',  fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(img_data.get('date') or 'N/A', maxlen=30), S['table_cell'])],
                [Paragraph('Caption',  ParagraphStyle('il2', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(img_data.get('caption') or 'N/A', maxlen=100), S['table_cell'])],
                [Paragraph('Scene',    ParagraphStyle('il3', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(img_data.get('scene_type') or 'N/A'), S['table_cell'])],
                [Paragraph('Activity', ParagraphStyle('il4', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(img_data.get('activity') or 'N/A', maxlen=80), S['table_cell'])],
                [Paragraph('Location', ParagraphStyle('il5', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(img_data.get('estimated_location') or 'Unknown', maxlen=60), S['table_cell'])],
                [Paragraph('Political',ParagraphStyle('il6', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 flag(img_data.get('political_symbols'))],
                [Paragraph('Weapons',  ParagraphStyle('il7', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 flag(img_data.get('weapons_visible'))],
                [Paragraph('Religious',ParagraphStyle('il8', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 flag(img_data.get('religious_symbols'))],
            ]
            info_tbl = Table(info_rows, colWidths=[2.5*cm, 8*cm])
            info_tbl.setStyle(TableStyle([
                ('ROWBACKGROUNDS',(0,0),(-1,-1),[C_WHITE,C_LIGHT_BG]),
                ('TOPPADDING',   (0,0),(-1,-1),5), ('BOTTOMPADDING',(0,0),(-1,-1),5),
                ('LEFTPADDING',  (0,0),(-1,-1),8),
                ('GRID',         (0,0),(-1,-1),0.5,C_BORDER),
                ('LINEBEFORE',   (0,0),(0,-1),3,C_ACCENT2),
            ]))
            if thumb_cell:
                outer = Table([[thumb_cell, info_tbl]], colWidths=[4.5*cm, 12.5*cm])
                outer.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP')]))
                story.append(outer)
            else:
                story.append(info_tbl)
            story.append(Spacer(1, 0.3*cm))

    if data['text_posts']:
        story.append(Paragraph("Text Post Intelligence", S['sub_title']))
        for i, tp in enumerate(data['text_posts'], 1):
            threat = safe_str(tp.get('threat_indicators') or 'None', maxlen=80)
            has_t  = threat.lower() not in ('none','null','n/a','')
            sc_path = tp.get('screenshot_path')
            thumb_cell = ''
            if sc_path and os.path.exists(sc_path):
                try:
                    from PIL import Image as PILImage
                    import tempfile
                    pil = PILImage.open(sc_path)
                    pil.thumbnail((200, 200), PILImage.LANCZOS)
                    tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
                    pil.save(tmp.name)
                    thumb_cell = Image(tmp.name, width=4*cm, height=4*cm)
                except Exception:
                    pass

            info_rows = [
                [Paragraph('Post',      ParagraphStyle('tp1', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(f"Post {i}", S['table_cell'])],
                [Paragraph('Topic',     ParagraphStyle('tp2', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(tp.get('topic') or 'N/A'), S['table_cell'])],
                [Paragraph('Language',  ParagraphStyle('tp3', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(tp.get('text_language') or 'N/A'), S['table_cell'])],
                [Paragraph('Narrative', ParagraphStyle('tp4', fontName='Helvetica-Bold', fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(safe_str(tp.get('narrative_type') or 'N/A'), S['table_cell'])],
                [Paragraph('Threats',   ParagraphStyle('tp5', fontName='Helvetica-Bold', fontSize=11,
                    textColor=C_RED if has_t else C_ACCENT2)),
                 Paragraph(threat, ParagraphStyle('tv', fontName='Helvetica', fontSize=12,
                    textColor=C_RED if has_t else C_TEXT))],
            ]
            info_tbl = Table(info_rows, colWidths=[2.5*cm, 8*cm])
            info_tbl.setStyle(TableStyle([
                ('ROWBACKGROUNDS',(0,0),(-1,-1),[C_WHITE,C_LIGHT_BG]),
                ('TOPPADDING',   (0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
                ('LEFTPADDING',  (0,0),(-1,-1),8),
                ('GRID',         (0,0),(-1,-1),0.5,C_BORDER),
                ('LINEBEFORE',   (0,0),(0,-1),3,C_RED if has_t else C_ACCENT2),
            ]))
            if thumb_cell:
                outer = Table([[thumb_cell, info_tbl]], colWidths=[4.5*cm, 12.5*cm])
                outer.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP')]))
                story.append(outer)
            else:
                story.append(info_tbl)
            story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())
    return story


def build_comment_intelligence(data, S):
    total = sum(data['sentiment_counts'].values())
    if total == 0 and not data['lang_counts']:
        return []

    story = []
    story += sec_hdr("06 / COMMENT INTELLIGENCE", S)

    sb = chart_sentiment(data['sentiment_counts'])
    lb = chart_language(data['lang_counts'])

    if sb and lb:
        row = Table([[Image(sb, width=8*cm, height=6*cm),
                      Image(lb, width=8*cm, height=6*cm)]],
                    colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('ALIGN',  (0,0), (-1,-1), 'CENTER'),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.5*cm))
    elif sb:
        img = Image(sb, width=12*cm, height=7*cm)
        img.hAlign = 'CENTER'
        story.append(img)

    pos = data['sentiment_counts'].get('positive', 0)
    neg = data['sentiment_counts'].get('negative', 0)
    dom = (max(data['lang_counts'], key=data['lang_counts'].get)
           if data['lang_counts'] else 'Unknown')

    stats = [
        ('Total Commentors',        str(len(data['commentors']))),
        ('Total Comments Analyzed', str(total)),
        ('Positive Comments',       f"{pos}  ({pos/total*100:.1f}%)" if total else '0'),
        ('Negative Comments',       f"{neg}  ({neg/total*100:.1f}%)" if total else '0'),
        ('Languages Detected',      str(len(data['lang_counts']))),
        ('Primary Language',        safe_str(dom)),
    ]
    for k, v in stats:
        r = Table([[
            Paragraph(k, ParagraphStyle('sk3', fontName='Helvetica-Bold',
                fontSize=13, textColor=C_ACCENT2)),
            Paragraph(v, S['body']),
        ]], colWidths=[8*cm, 9*cm])
        r.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('LEFTPADDING',   (0,0), (-1,-1), 12),
            ('LINEBELOW',     (0,0), (-1,-1), 0.5, C_BORDER),
            ('LINEBEFORE',    (0,0), (0,-1),  3, C_ACCENT),
        ]))
        story.append(r)

    return story


def build_post_timeline(data, S):
    td = data.get('timeline_data', [])
    if not td or all(p['total'] == 0 for p in td):
        return []

    story = []
    story += sec_hdr("07 / POST ACTIVITY TIMELINE", S)
    story.append(Paragraph(
        "Comment frequency per post with sentiment breakdown and cumulative growth.",
        S['body_sub']))
    story.append(Spacer(1, 0.3*cm))

    buf = chart_post_timeline(td)
    if buf:
        img = Image(buf, width=17*cm, height=13*cm)
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("Post Activity Breakdown", S['sub_title']))
    headers = ['POST DATE', 'TYPE', 'TOTAL', 'POSITIVE', 'NEUTRAL', 'NEGATIVE']
    rows = [[Paragraph(h, S['table_header']) for h in headers]]
    for p in sorted(td, key=lambda x: -x['total']):
        rows.append([
            Paragraph(safe_str(p['date'], maxlen=20), S['table_cell']),
            Paragraph(p['type'].upper(), ParagraphStyle('pt', fontName='Helvetica-Bold',
                fontSize=12, textColor=C_ACCENT2, alignment=TA_CENTER)),
            Paragraph(str(p['total']), ParagraphStyle('ptot', fontName='Helvetica-Bold',
                fontSize=12, textColor=C_DARK, alignment=TA_CENTER)),
            Paragraph(str(p['positive']), ParagraphStyle('ppos', fontName='Helvetica-Bold',
                fontSize=12, textColor=C_GREEN, alignment=TA_CENTER)),
            Paragraph(str(p['neutral']), ParagraphStyle('pneu', fontName='Helvetica',
                fontSize=12, textColor=C_SILVER, alignment=TA_CENTER)),
            Paragraph(str(p['negative']), ParagraphStyle('pneg', fontName='Helvetica-Bold',
                fontSize=12, textColor=C_RED, alignment=TA_CENTER)),
        ])
    t = Table(rows, colWidths=[5*cm, 2.5*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm], repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,0),  C_ACCENT2),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_WHITE, C_LIGHT_BG]),
        ('TOPPADDING',    (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('LEFTPADDING',   (0,0), (-1,-1), 6),
        ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
        ('LINEBELOW',     (0,0), (-1,0),  2, C_ACCENT),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


def build_manual_urls(data, S):
    if data.get('type') != 'batch':
        return []
    manual_urls = data.get('manual_urls', [])
    if not manual_urls:
        return []

    story = []
    story += sec_hdr("08 / INVESTIGATED URLS", S)
    story.append(Paragraph(
        f"This investigation analyzed {len(manual_urls)} URL(s) from batch: "
        f"<b>{safe_str(data['owner_name'])}</b>", S['body']))
    story.append(Spacer(1, 0.4*cm))

    type_icons = {'photo': '📸', 'reel': '🎬', 'post': '📝', 'unknown': '🔗'}
    for i, u in enumerate(manual_urls, 1):
        ptype   = u.get('type', 'unknown')
        icon    = type_icons.get(ptype, '🔗')
        caption = safe_str(u.get('caption', ''), maxlen=120)
        date    = safe_str(u.get('date', 'N/A'), maxlen=30)
        url     = u.get('url', '')

        row_data = [
            [Paragraph(f"{icon}  #{i}  {ptype.upper()}", ParagraphStyle(
                'ut', fontName='Helvetica-Bold', fontSize=13, textColor=C_ACCENT2))],
            [Paragraph(f"URL: {url[:80]}{'...' if len(url) > 80 else ''}", ParagraphStyle(
                'uu', fontName='Helvetica', fontSize=11, textColor=C_BLUE))],
            [Paragraph(f"Date: {date}", ParagraphStyle(
                'ud', fontName='Helvetica', fontSize=12, textColor=C_SUBTEXT))],
        ]
        if caption:
            row_data.append([Paragraph(
                f"Caption: {caption}",
                ParagraphStyle('uc', fontName='Helvetica-Oblique',
                               fontSize=12, textColor=C_TEXT))])

        card = Table(row_data, colWidths=[17*cm])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0), (-1,-1), C_WHITE),
            ('TOPPADDING',   (0,0), (-1,-1), 6),
            ('BOTTOMPADDING',(0,0), (-1,-1), 6),
            ('LEFTPADDING',  (0,0), (-1,-1), 12),
            ('BOX',          (0,0), (-1,-1), 0.5, C_BORDER),
            ('LINEABOVE',    (0,0), (-1,0),  3, C_ACCENT2),
        ]))
        story.append(card)
        story.append(Spacer(1, 0.2*cm))

    return story


# ══════════════════════════════════════════════════════════════════════════════
#  FACE INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_face_data(profile_id, db_file):
    """Fetch face clusters + detected faces for a profile from DB."""
    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    results = []

    try:
        cur.execute("""
            SELECT
                fc.id, fc.person_label, fc.representative_face,
                fc.appearance_count, fc.post_ids, fc.created_at,
                COUNT(df.id) AS face_count
            FROM face_clusters fc
            LEFT JOIN detected_faces df ON df.person_id = fc.id
            LEFT JOIN photo_posts pp ON pp.id = df.photo_post_id
            WHERE pp.profile_id = ?
            GROUP BY fc.id
            ORDER BY fc.appearance_count DESC
        """, (profile_id,))
        clusters = [dict(r) for r in cur.fetchall()]

        for cl in clusters:
            cid = cl['id']
            # fetch up to 6 face crop paths for this cluster
            cur.execute("""
                SELECT df.face_image_path, pp.photo_url
                FROM detected_faces df
                JOIN photo_posts pp ON pp.id = df.photo_post_id
                WHERE df.person_id = ?
                  AND df.face_image_path IS NOT NULL
                  AND df.face_image_path != ''
                ORDER BY df.id LIMIT 6
            """, (cid,))
            face_paths = [{'path': r[0], 'post_url': r[1] or ''} for r in cur.fetchall()]

            # parse post_ids JSON
            post_ids = []
            try:
                import json
                post_ids = json.loads(cl['post_ids'] or '[]')
            except Exception:
                pass

            results.append({
                'id':           cid,
                'label':        safe_str(cl['person_label'] or f'Person {cid}'),
                'repr_face':    cl['representative_face'] or '',
                'appearance_count': cl['appearance_count'] or 0,
                'face_count':   cl['face_count'] or 0,
                'post_ids':     post_ids,
                'face_paths':   face_paths,
            })
    except Exception as e:
        print(f'  [FACE DATA] Error: {e}')
    finally:
        con.close()

    return results


def build_face_intelligence(data, S):
    """Face intelligence section — most appeared + all detected faces."""
    if data.get('type') != 'profile':
        return []

    profile_id = data.get('profile_id')
    if not profile_id:
        return []

    results = fetch_face_data(profile_id, DB_FILE)
    results = [r for r in results if r['face_count'] > 0]
    if not results:
        return []

    story = []
    story += sec_hdr("08 / FACE INTELLIGENCE", S)

    total_faces = sum(r['face_count'] for r in results)
    multi_post  = sum(1 for r in results if len(r['post_ids']) > 1)
    high_freq   = [r for r in results if len(r['post_ids']) >= 3]

    story.append(Paragraph(
        f"Detected <b>{len(results)}</b> unique person(s) across all photo posts "
        f"using CNN face recognition.",
        S['body']))
    story.append(Spacer(1, 0.4*cm))

    # ── Summary stats ─────────────────────────────────────────────────────────
    st = Table([[
        Paragraph("Unique Persons", S['stat_label']),
        Paragraph("Total Faces",    S['stat_label']),
        Paragraph("Repeat Persons", S['stat_label']),
        Paragraph("High Frequency", S['stat_label']),
    ],[
        Paragraph(str(len(results)), S['stat_value']),
        Paragraph(str(total_faces),  S['stat_value']),
        Paragraph(str(multi_post),   S['stat_value']),
        Paragraph(str(len(high_freq)), ParagraphStyle(
            'hf', fontName='Helvetica-Bold', fontSize=28,
            textColor=C_RED if high_freq else C_DARK, alignment=TA_CENTER)),
    ]], colWidths=[4.2*cm]*4)
    st.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), C_LIGHT_BG),
        ('TOPPADDING',    (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ('GRID',          (0,0), (-1,-1), 0.5, C_BORDER),
        ('LINEABOVE',     (0,0), (-1,0),  3, C_ACCENT),
        ('ALIGN',         (0,0), (-1,-1), 'CENTER'),
    ]))
    story.append(st)
    story.append(Spacer(1, 0.6*cm))

    def make_face_card(r, big=True):
        img_size   = 4.5*cm if big else 3.5*cm
        info_w     = 3.8*cm if big else 3.2*cm
        post_count = len(r['post_ids'])
        lc         = C_RED if post_count >= 3 else C_BLUE if post_count >= 2 else C_SILVER

        face_elem = Paragraph("No\nImage", ParagraphStyle(
            'ni', fontName='Helvetica', fontSize=11,
            textColor=C_SUBTEXT, alignment=TA_CENTER))

        repr_path = r.get('repr_face', '')
        if repr_path and os.path.exists(repr_path):
            try:
                face_elem = Image(repr_path, width=img_size, height=img_size)
            except Exception:
                pass
        elif r['face_paths']:
            fp = r['face_paths'][0]['path']
            if fp and os.path.exists(fp):
                try:
                    face_elem = Image(fp, width=img_size, height=img_size)
                except Exception:
                    pass

        info_rows = [
            [Paragraph(r['label'].replace('_', ' ').title(), ParagraphStyle(
                'fn3', fontName='Helvetica-Bold',
                fontSize=14 if big else 12, textColor=C_DARK, spaceAfter=3))],
            [Paragraph(f"Posts: {post_count}", ParagraphStyle(
                'fp3', fontName='Helvetica-Bold',
                fontSize=12 if big else 11, textColor=lc, spaceAfter=2))],
            [Paragraph(f"Faces detected: {r['face_count']}", ParagraphStyle(
                'ff3', fontName='Helvetica',
                fontSize=11 if big else 10, textColor=C_SUBTEXT, spaceAfter=2))],
        ]

        # post URLs as hyperlinks
        if r['face_paths']:
            seen_urls = []
            url_parts = []
            for j, fp in enumerate(r['face_paths'][:3]):
                pu = fp.get('post_url', '')
                if pu and pu not in seen_urls:
                    seen_urls.append(pu)
                    url_parts.append(
                        f'<link href="{pu}" color="#2980b9"><u>post {len(url_parts)+1}</u></link>'
                    )
            if url_parts:
                info_rows.append([Paragraph(
                    'Seen in: ' + '  '.join(url_parts),
                    ParagraphStyle('fp_u', fontName='Helvetica', fontSize=10,
                                   textColor=C_SUBTEXT, leading=14))])

        info_tbl = Table(info_rows, colWidths=[info_w])
        info_tbl.setStyle(TableStyle([
            ('TOPPADDING',   (0,0), (-1,-1), 3),
            ('BOTTOMPADDING',(0,0), (-1,-1), 3),
            ('LEFTPADDING',  (0,0), (-1,-1), 8),
            ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
        ]))

        card = Table([[face_elem, info_tbl]],
                     colWidths=[img_size + 0.2*cm, info_w])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0), (-1,-1), C_WHITE),
            ('TOPPADDING',   (0,0), (-1,-1), 10),
            ('BOTTOMPADDING',(0,0), (-1,-1), 10),
            ('LEFTPADDING',  (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
            ('BOX',          (0,0), (-1,-1), 0.5, C_BORDER),
            ('LINEBEFORE',   (0,0), (0,-1),  4, lc),
            ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
        ]))
        return card

    # ── Most appeared (top 6) ─────────────────────────────────────────────────
    story.append(Paragraph("Most Appeared Persons", S['sub_title']))
    story.append(Paragraph(
        "Persons appearing most frequently across posts — likely close associates.",
        S['body_sub']))
    story.append(Spacer(1, 0.3*cm))

    top_cards = [make_face_card(r, big=True) for r in results[:6]]
    for i in range(0, len(top_cards), 2):
        row_c = top_cards[i:i+2]
        while len(row_c) < 2: row_c.append('')
        row = Table([row_c], colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('LEFTPADDING',  (0,0), (-1,-1), 2),
            ('RIGHTPADDING', (0,0), (-1,-1), 2),
            ('TOPPADDING',   (0,0), (-1,-1), 4),
            ('BOTTOMPADDING',(0,0), (-1,-1), 4),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())

    # ── All faces ─────────────────────────────────────────────────────────────
    story += sec_hdr("08b / ALL DETECTED FACES", S)
    story.append(Paragraph(
        f"Complete gallery of all <b>{len(results)}</b> unique persons detected.",
        S['body']))
    story.append(Spacer(1, 0.4*cm))

    all_cards = [make_face_card(r, big=False) for r in results]
    for i in range(0, len(all_cards), 2):
        row_c = all_cards[i:i+2]
        while len(row_c) < 2: row_c.append('')
        row = Table([row_c], colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('LEFTPADDING',  (0,0), (-1,-1), 2),
            ('RIGHTPADDING', (0,0), (-1,-1), 2),
            ('TOPPADDING',   (0,0), (-1,-1), 4),
            ('BOTTOMPADDING',(0,0), (-1,-1), 4),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.3*cm))

    # high frequency alert
    if high_freq:
        story.append(Spacer(1, 0.4*cm))
        story.append(HRFlowable(width='100%', thickness=1, color=C_ACCENT, spaceAfter=8))
        story.append(Paragraph("High Frequency Alert", S['sub_title']))
        for r in high_freq:
            pc = len(r['post_ids'])
            story.append(Paragraph(
                f"• <b>{r['label'].replace('_',' ').title()}</b> — "
                f"appeared in <b>{pc} posts</b> with {r['face_count']} face detection(s). "
                f"Likely a close associate or frequent contact.",
                S['body']))

    return story


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN REPORT GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def generate_report(profile_url=None, batch_id=None):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    print(f"\n{'═'*65}\n📄  Birdy-Edwards — Report Generator\n{'═'*65}")

    if profile_url:
        print(f"  Profile : {profile_url}")
        data = fetch_profile_data(profile_url)
        if not data:
            print("  ⚠️  Profile not found")
            return
        safe = data['owner_name'].replace(' ','_').replace('/','_')[:30]
        out  = os.path.join(REPORTS_DIR, f"report_{safe}.pdf")
        is_batch = False
        db_file  = DB_FILE

    elif batch_id:
        print(f"  Batch   : {batch_id}")
        data = fetch_batch_data(batch_id)
        if not data:
            print("  ⚠️  Batch not found")
            return
        safe = batch_id.replace(' ','_')[:30]
        out  = os.path.join(REPORTS_DIR, f"report_{safe}.pdf")
        is_batch = True
        db_file  = MANUAL_DB_FILE
    else:
        print("  ⚠️  Provide profile_url or batch_id")
        return

    print(f"  Target  : {data['owner_name']}\n  Output  : {out}")

    S   = make_styles()
    doc = SimpleDocTemplate(out, pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)

    print("\n  Building pages...")
    story = []

    # ── Cover ─────────────────────────────────────────────────────────────────
    story += build_cover(data, S)
    print("  ✅ Cover")

    # ── Profile summary (profile only) ────────────────────────────────────────
    if not is_batch:
        story += build_profile_summary(data, S)
        print("  ✅ Profile Summary")

    # ── Network overview ──────────────────────────────────────────────────────
    story += build_network_overview(data, S)
    print("  ✅ Network Overview")

    # ── Fetch commentor details (comments + stance + sentiment) ───────────────
    print("  ⏳ Fetching commentor details...")
    if is_batch:
        commentor_details = fetch_batch_commentor_details(batch_id, db_file)
    else:
        commentor_details = fetch_commentor_details(data['profile_id'], db_file)
    print(f"  ✅ Commentor details fetched ({len(commentor_details)} persons)")

    # ── All commentors table (with top5 banner + comments section) ────────────
    story += build_commentors_table(data, S, commentor_details=commentor_details)
    print("  ✅ All Commentors + Comments/Stance/Sentiment")

    # ── Batch-specific intelligence (batch only) ──────────────────────────────
    if is_batch:
        bi = build_batch_intelligence(data, S)
        if bi:
            story += bi
            print("  ✅ Batch Intelligence (Stance Countries + Multi-Post Users)")
    else:
        # ── Co-commentor analysis (profile only) ─────────────────────────────
        print("  ⏳ Fetching co-commentor pairs...")
        pairs = fetch_cocommentor_pairs(data['profile_id'], db_file, is_batch=False)
        if pairs:
            story += build_cocommentor_section(pairs, S, section_num='03b')
            print(f"  ✅ Co-Commentor Analysis ({len(pairs)} pairs)")
        else:
            print("  ⏭️  Co-Commentor — skipped (no pairs found)")

    # ── Top 7 ─────────────────────────────────────────────────────────────────
    story += build_top7(data, S)
    print("  ✅ Top 7 Network")

    # ── Content analysis (profile + batch) ───────────────────────────────────
    cp = build_content_analysis(data, S)
    if cp:
        story += cp
        print("  ✅ Content Analysis")
    else:
        print("  ⏭️  Content Analysis — skipped (no data)")

    # ── Reel intelligence (both profile and batch) ────────────────────────────
    rp = build_reel_intelligence_section(data, S)
    if rp:
        story += rp
        print(f"  ✅ Reel Intelligence ({len(data.get('reel_intel', []))} reels)")
    else:
        print("  ⏭️  Reel Intelligence — skipped (no caption data)")

    # ── Comment intelligence ──────────────────────────────────────────────────
    ip = build_comment_intelligence(data, S)
    if ip:
        story += ip
        print("  ✅ Comment Intelligence")
    else:
        print("  ⏭️  Comment Intelligence — skipped (no data)")

    # ── Post timeline ─────────────────────────────────────────────────────────
    tp = build_post_timeline(data, S)
    if tp:
        story += tp
        print("  ✅ Post Activity Timeline")
    else:
        print("  ⏭️  Post Timeline — skipped (no data)")

    # ── Investigated URLs (batch only) ────────────────────────────────────────
    if is_batch:
        up = build_manual_urls(data, S)
        if up:
            story += up
            print("  ✅ Investigated URLs")

    # ── Face intelligence (profile only) ─────────────────────────────────────
    if not is_batch:
        fp = build_face_intelligence(data, S)
        if fp:
            story += fp
            print("  ✅ Face Intelligence")
        else:
            print("  ⏭️  Face Intelligence — skipped (no face data)")

    doc.build(story, onFirstPage=on_cover, onLaterPages=on_page)
    print(f"\n{'═'*65}\n✅ Report saved: {out}\n{'═'*65}")
    return out


if __name__ == "__main__":
    print("Birdy-Edwards — SOCMINT Report Generator\n1 → Automated profile\n2 → Manual batch")
    choice = input("Choice: ").strip()
    if choice == "1":
        generate_report(profile_url=input("Enter profile URL: ").strip())
    elif choice == "2":
        generate_report(batch_id=input("Enter batch ID: ").strip())
    else:
        print("Invalid choice")