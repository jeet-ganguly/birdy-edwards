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

#  CONFIG

DB_FILE        = "socmint.db"
MANUAL_DB_FILE = "socmint_manual.db"
REPORTS_DIR    = "reports"
LOGO_PATH      = "icons/logo.jpeg"

W, H = A4

# White Theme Colors
C_WHITE     = colors.white
C_DARK      = colors.HexColor('#1a1a2e')
C_ACCENT    = colors.HexColor('#c0392b')
C_ACCENT2   = colors.HexColor('#2c3e50')
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

CHART_PALETTE = ['#c0392b','#2980b9','#27ae60','#f39c12','#8e44ad','#16a085','#d35400','#2c3e50']

SCORE_COLORS = {
    'Strong Supporter': '#27ae60',
    'Supporter':        '#2980b9',
    'Neutral':          '#7f8c8d',
    'Low Interaction':  '#f39c12',
    'Critical Voice':   '#c0392b',
}

# Country Flag Lookup 
def get_country_flag(country):
    """Return 2-letter ISO code styled as [XX] — works in all PDF fonts."""
    if not country: return ""
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
        if key in c: return f"[{code}]"
    return '[--]'




#  STYLES  (min 13pt body)

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
    }


#  PAGE CANVAS

def on_cover(c, doc):
    c.saveState()
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    c.setFillColor(C_ACCENT)
    c.rect(0, 0, 10, H, fill=1, stroke=0)   # left red stripe
    c.setFillColor(C_DARK)
    c.rect(0, H-10, W, 10, fill=1, stroke=0) # top dark bar
    c.restoreState()


def on_page(c, doc):
    c.saveState()
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    c.setFillColor(C_ACCENT)
    c.rect(0, H-6, W, 6, fill=1, stroke=0)  # top red bar
    c.setFillColor(C_LIGHT_BG)
    c.rect(0, 0, W, 24, fill=1, stroke=0)   # footer bg
    c.setFillColor(C_SUBTEXT)
    c.setFont('Helvetica', 9)
    c.drawString(2*cm, 8, "BIRDY-EDWARDS  |  Infiltrate & Expose  |  CONFIDENTIAL")
    c.drawRightString(W - 2*cm, 8, f"Page {doc.page}")
    c.restoreState()


#  HELPERS

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


#  CHARTS  (seaborn style)

def chart_countries(country_data):
    countries = sorted(country_data.items(), key=lambda x: -x[1])[:12]
    if not countries: return None
    labels = [c[0] for c in countries]
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
    ax.spines['left'].set_color('#ddd')
    ax.spines['bottom'].set_color('#ddd')
    ax.set_xlim(0, max(values)*1.38)
    plt.tight_layout(pad=1.5)
    return chart_buf(fig)


def chart_tiers(tier_data):
    if not tier_data or not any(tier_data.values()): return None
    labels = list(tier_data.keys())
    values = list(tier_data.values())
    clrs   = [SCORE_COLORS.get(l,'#95a5a6') for l in labels]

    if HAS_SEABORN: sns.set_style("white")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    fig.patch.set_facecolor('white')

    # Donut
    ax1.set_facecolor('white')
    wedges, _, autotexts = ax1.pie(
        values, colors=clrs, autopct='%1.1f%%', startangle=90,
        pctdistance=0.78,
        wedgeprops={'width':0.55, 'edgecolor':'white', 'linewidth':3}
    )
    for at in autotexts:
        at.set_fontsize(12)
        at.set_color('white')
        at.set_fontweight('bold')
    ax1.set_title('Tier Distribution', fontsize=15, fontweight='bold', color='#333', pad=16)
    handles = [mpatches.Patch(color=c, label=f'{l}  ({v})') for l,v,c in zip(labels,values,clrs)]
    ax1.legend(handles=handles, loc='lower center', bbox_to_anchor=(0.5,-0.2),
               ncol=2, fontsize=14, frameon=False)

    # Bar
    ax2.set_facecolor('#fafbfc')
    bars = ax2.barh(labels, values, color=clrs, height=0.5, edgecolor='white', linewidth=2)
    for bar, val in zip(bars, values):
        ax2.text(bar.get_width()+0.05, bar.get_y()+bar.get_height()/2,
                 str(val), va='center', ha='left', fontsize=16, fontweight='bold', color='#555')
    ax2.set_xlabel('Commentors', fontsize=16, color='#555')
    ax2.set_title('Commentors by Category', fontsize=15, fontweight='bold', color='#333', pad=16)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.tick_params(labelsize=12)
    ax2.set_xlim(0, max(values)*1.35)
    plt.tight_layout(pad=2)
    return chart_buf(fig)


def chart_sentiment(sd):
    if not sd or not any(sd.values()): return None
    labels = ['Positive','Neutral','Negative']
    values = [sd.get('positive',0), sd.get('neutral',0), sd.get('negative',0)]
    clrs   = ['#27ae60','#7f8c8d','#c0392b']

    if HAS_SEABORN: sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(7,5))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#fafbfc')
    bars = ax.bar(labels, values, color=clrs, width=0.45, edgecolor='white', linewidth=2)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.15,
                str(val), ha='center', va='bottom', fontsize=14, fontweight='bold', color='#555')
    ax.set_ylabel('Comments', fontsize=16, color='#555')
    ax.set_title('Sentiment Distribution', fontsize=15, fontweight='bold', color='#333', pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.tick_params(labelsize=13)
    plt.tight_layout()
    return chart_buf(fig)


def chart_language(ld):
    if not ld: return None
    langs  = sorted(ld.items(), key=lambda x:-x[1])[:8]
    labels = [l[0] for l in langs]
    values = [l[1] for l in langs]

    if HAS_SEABORN: sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(7,5))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('#fafbfc')
    clrs = [CHART_PALETTE[0]] + [CHART_PALETTE[1]]*(len(labels)-1)
    ax.barh(labels, values, color=clrs, height=0.45, edgecolor='white', linewidth=2)
    for i,val in enumerate(values):
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
    """Build top 7 network graph using matplotlib — star layout."""
    if not profiles: return None

    if HAS_SEABORN: sns.set_style("white")

    fig, ax = plt.subplots(figsize=(12, 10))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    ax.set_xlim(-1.6, 1.6)
    ax.set_ylim(-1.6, 1.6)
    ax.axis('off')

    # Center node
    ax.add_patch(plt.Circle((0,0), 0.18, color='#c0392b', zorder=5))
    ax.text(0, -0.24, profiles[0].get('owner_name','Target') if 'owner_name' in profiles[0]
            else 'Target', ha='center', va='top', fontsize=14, fontweight='bold',
            color='#333', zorder=6)
    ax.text(0, 0, '🎯', ha='center', va='center', fontsize=18, zorder=7)

    n = len(profiles)
    angles = [2 * np.pi * i / n for i in range(n)]
    radius = 1.15

    for i, (c, angle) in enumerate(zip(profiles, angles)):
        x = radius * np.cos(angle)
        y = radius * np.sin(angle)
        score = c.get('total_score', c.get('score', 0.0))
        lbl,_ = get_score_label(score)
        color = get_score_hex(score)

        # Edge
        ax.plot([0, x*0.82], [0, y*0.82], color=color, linewidth=2.5,
                alpha=0.7, zorder=2)

        # Node circle
        ax.add_patch(plt.Circle((x, y), 0.13, color=color, zorder=4, alpha=0.9))
        ax.add_patch(plt.Circle((x, y), 0.13, fill=False,
                                edgecolor='white', linewidth=2, zorder=5))

        # Rank number
        ax.text(x, y, str(i+1), ha='center', va='center',
                fontsize=15, fontweight='bold', color='white', zorder=6)

        # Name label
        name_short = c['name'][:18] if len(c['name']) > 18 else c['name']
        country = c.get('country') or 'Unknown'

        # Offset label away from center
        lx = x * 1.38
        ly = y * 1.38
        ax.text(lx, ly, name_short, ha='center', va='center',
                fontsize=13, fontweight='bold', color='#333', zorder=6,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                          edgecolor=color, linewidth=1.5, alpha=0.95))
        ax.text(lx, ly-0.12, country, ha='center', va='center',
                fontsize=12, color='#777', zorder=6)
        ax.text(lx, ly-0.22, f"{score:+.2f}", ha='center', va='center',
                fontsize=12, color=color, fontweight='bold', zorder=6)

    # Legend
    legend_items = [
        ('Strong Supporter', '#27ae60'), ('Supporter', '#2980b9'),
        ('Neutral', '#7f8c8d'), ('Low Interaction', '#f39c12'), ('Critical Voice', '#c0392b')
    ]
    for j, (name, color) in enumerate(legend_items):
        ax.add_patch(plt.Circle((-1.55, 1.4 - j*0.18), 0.04, color=color, zorder=5))
        ax.text(-1.45, 1.4 - j*0.18, name, va='center', fontsize=12, color='#444')

    ax.set_title('Top 7 Close Network', fontsize=16, fontweight='bold',
                 color='#333', pad=16)
    plt.tight_layout()
    return chart_buf(fig)


def download_image_thumb(url, max_size=(200, 200)):
    """Download image from URL and return as PIL Image, resized."""
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
    except Exception as e:
        return None


def chart_post_timeline(timeline_data):
    """
    Post timeline chart:
    Bars = total comment count per post (colored by dominant sentiment)
    Line = cumulative comments over time
    """
    if not timeline_data: return None

    if HAS_SEABORN: sns.set_style("whitegrid")

    # Sort by date
    posts = sorted(timeline_data, key=lambda x: x.get('sort_key', 0))
    labels   = [p['label'] for p in posts]
    totals   = [p['total'] for p in posts]
    pos_vals = [p['positive'] for p in posts]
    neg_vals = [p['negative'] for p in posts]
    neu_vals = [p['neutral']  for p in posts]
    cumsum   = np.cumsum(totals).tolist()

    x      = np.arange(len(labels))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10),
                                    gridspec_kw={'height_ratios': [2, 1]})
    fig.patch.set_facecolor('white')

    # ── Top chart — stacked bars by sentiment ─────────────────────────────────
    ax1.set_facecolor('#fafbfc')
    bar_pos = ax1.bar(x, pos_vals, color='#27ae60', label='Positive',
                      width=0.55, edgecolor='white', linewidth=1.5)
    bar_neu = ax1.bar(x, neu_vals, bottom=pos_vals, color='#7f8c8d',
                      label='Neutral', width=0.55, edgecolor='white', linewidth=1.5)
    bar_neg = ax1.bar(x, neg_vals,
                      bottom=[p+n for p,n in zip(pos_vals, neu_vals)],
                      color='#c0392b', label='Negative',
                      width=0.55, edgecolor='white', linewidth=1.5)

    # Total label on top of each bar
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
    ax1.tick_params(axis='y', labelsize=12)

    # Bottom chart — cumulative line
    ax2.set_facecolor('#fafbfc')
    ax2.plot(x, cumsum, color='#2980b9', linewidth=2.5,
             marker='o', markersize=7, zorder=3)
    ax2.fill_between(x, cumsum, alpha=0.15, color='#2980b9')
    for i, val in enumerate(cumsum):
        ax2.text(i, val + max(cumsum)*0.02, str(val), ha='center', va='bottom',
                 fontsize=11, color='#2980b9', fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, rotation=35, ha='right', fontsize=12)
    ax2.set_ylabel('Cumulative', fontsize=14, color='#555', labelpad=8)
    ax2.set_title('Cumulative Comment Growth', fontsize=15,
                  fontweight='bold', color='#333', pad=10)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.tick_params(axis='y', labelsize=12)

    plt.tight_layout(pad=2)
    return chart_buf(fig)


#  SCREENSHOT

def screenshot_html(html_path, crop_top=55):
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from PIL import Image as PILImage
        import tempfile, time

        opts = Options()
        opts.add_argument('--headless')
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--window-size=1400,820')

        drv = webdriver.Chrome(options=opts)
        drv.get(f"file://{os.path.abspath(html_path)}")
        time.sleep(4)

        tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        drv.save_screenshot(tmp.name)
        drv.quit()

        pil = PILImage.open(tmp.name)
        w, h = pil.size
        pil.crop((0, crop_top, w, h)).save(tmp.name)
        return tmp.name
    except Exception as e:
        print(f"  ⚠️  Screenshot failed: {e}")
        return None


#  DATA FETCHERS

def fetch_profile_data(profile_url, db_file=DB_FILE):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("SELECT id,owner_name,profile_url,is_locked FROM profiles WHERE profile_url=?", (profile_url,))
    row = cur.fetchone()
    if not row: con.close(); return None

    pid = row[0]; owner = row[1] or 'Unknown'; locked = row[3]
    cur.execute("SELECT section,label,value FROM profile_fields WHERE profile_id=?", (pid,))
    seen_fields = set()
    pfields = []
    for r in cur.fetchall():
        key = (r[1], r[2])  # label + value dedup
        if key not in seen_fields:
            seen_fields.add(key)
            pfields.append({'section':r[0],'label':r[1],'value':r[2]})

    cur.execute("SELECT COUNT(*) FROM photo_posts WHERE profile_id=?", (pid,)); pc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM reel_posts WHERE profile_id=?",  (pid,)); rc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM text_posts WHERE profile_id=?",  (pid,)); tc = cur.fetchone()[0]

    cur.execute("""
        SELECT co.id,co.name,co.profile_url,cs.total_score,cs.comment_count,cs.tier,
               cs.sentiment_score,cc.identified_country,cc.current_city,cc.employer,cc.education
        FROM commentor_scores cs
        JOIN commentors co ON co.id=cs.commentor_id
        LEFT JOIN commentor_country cc ON cc.commentor_id=co.id
        WHERE cs.main_profile_id=? ORDER BY cs.total_score DESC
    """, (pid,))
    commentors = [{'id':r[0],'name':r[1] or '','profile_url':r[2] or '',
                   'total_score':r[3] or 0.0,'comment_count':r[4] or 0,'tier':r[5] or 'neutral',
                   'sentiment_score':r[6] or 0.0,'country':r[7],'city':r[8],
                   'employer':r[9],'education':r[10]} for r in cur.fetchall()]

    cur.execute("""
        SELECT sp.name,sp.profile_url,sp.relationship_type,sp.score,
               GROUP_CONCAT(spf.label||': '||spf.value,' | '),cc.identified_country
        FROM secondary_profiles sp
        LEFT JOIN secondary_profile_fields spf ON spf.secondary_profile_id=sp.id
        LEFT JOIN commentor_country cc ON cc.commentor_id=sp.commentor_id
        WHERE sp.main_profile_id=? GROUP BY sp.id ORDER BY sp.score DESC LIMIT 7
    """, (pid,))
    secondary = [{'name':r[0] or 'Unknown','profile_url':r[1] or '',
                  'tier':r[2] or 'neutral','score':r[3] or 0.0,
                  'fields':r[4] or '','country':r[5]} for r in cur.fetchall()]

    cur.execute("""
        SELECT ia.scene_type,ia.objects,ia.activity,ia.political_symbols,ia.religious_symbols,
               ia.weapons_visible,ia.estimated_location,ia.text_in_image,ia.confidence,
               pp.image_src, pp.caption, pp.date_text
        FROM image_analysis ia JOIN photo_posts pp ON pp.id=ia.photo_post_id WHERE pp.profile_id=?
    """, (pid,))
    image_analysis = [{'scene_type':r[0],'objects':r[1],'activity':r[2],
                        'political_symbols':r[3],'religious_symbols':r[4],
                        'weapons_visible':r[5],'estimated_location':r[6],
                        'text_in_image':r[7],'confidence':r[8],
                        'image_src':r[9],'caption':r[10],'date':r[11]} for r in cur.fetchall()]

    cur.execute("""
        SELECT tpa.topic,tpa.sentiment,tpa.narrative_type,tpa.key_entities,
               tpa.threat_indicators,tpa.text_language,tp.screenshot_path
        FROM text_post_analysis tpa JOIN text_posts tp ON tp.id=tpa.text_post_id WHERE tp.profile_id=?
    """, (pid,))
    text_posts = [{'topic':r[0],'sentiment':r[1],'narrative_type':r[2],
                   'key_entities':r[3],'threat_indicators':r[4],'text_language':r[5],
                   'screenshot_path':r[6]} for r in cur.fetchall()]

    lang_counts = {}; sentiment_counts = {'positive':0,'neutral':0,'negative':0}
    for tbl, src in [('photo_comments','photo'),('reel_comments','reel'),('text_comments','text')]:
        pt = tbl.replace('_comments','_posts')
        try:
            cur.execute(f"SELECT id FROM {pt} WHERE profile_id=?", (pid,))
            pids = [r[0] for r in cur.fetchall()]
            if not pids: continue
            cur.execute(f"""
                SELECT ca.language,ca.sentiment FROM comment_analysis ca
                JOIN {tbl} t ON t.id=ca.comment_id AND ca.db_source='{src}'
                WHERE t.{src}_post_id IN ({','.join('?'*len(pids))})
            """, pids)
            for lang,sent in cur.fetchall():
                if lang and lang.lower() not in ('unknown','null','none',''):
                    lang_counts[lang] = lang_counts.get(lang,0)+1
                if sent:
                    s = sent.lower()
                    if s=='positive': sentiment_counts['positive']+=1
                    elif s=='negative': sentiment_counts['negative']+=1
                    else: sentiment_counts['neutral']+=1
        except: pass

    country_dist = {}
    for c in commentors:
        ct = c['country'] or 'Unknown'
        country_dist[ct] = country_dist.get(ct,0)+1

    # Post timeline — comments per post with sentiment breakdown
    timeline_data = []
    for tbl, src, url_col, id_col in [
        ('photo_posts', 'photo', 'photo_url', 'photo_post_id'),
        ('reel_posts',  'reel',  'reel_url',  'reel_post_id'),
        ('text_posts',  'text',  'post_url',  'text_post_id'),
    ]:
        comment_tbl = src + '_comments'
        try:
            cur.execute(f"""
                SELECT pp.id, pp.date_text,
                       COUNT(DISTINCT c.id) as total,
                       COUNT(DISTINCT CASE WHEN ca.sentiment='positive' THEN c.id END),
                       COUNT(DISTINCT CASE WHEN ca.sentiment='negative' THEN c.id END),
                       COUNT(DISTINCT CASE WHEN ca.sentiment IS NOT NULL
                                AND ca.sentiment NOT IN ('positive','negative')
                                THEN c.id END)
                FROM {tbl} pp
                LEFT JOIN {comment_tbl} c ON c.{id_col} = pp.id
                LEFT JOIN comment_analysis ca ON ca.comment_id = c.id
                WHERE pp.profile_id = ?
                GROUP BY pp.id
                ORDER BY pp.date_text
            """, (pid,))
            for r in cur.fetchall():
                date_str = r[1] or 'Unknown'
                short    = date_str[:12] if len(date_str) > 12 else date_str
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
        except Exception: pass

    con.close()
    return {'type':'profile','owner_name':owner,'profile_url':profile_url,'is_locked':locked,
            'profile_fields':pfields,'photo_count':pc,'reel_count':rc,'text_count':tc,
            'commentors':commentors,'secondary':secondary,'image_analysis':image_analysis,
            'text_posts':text_posts,'lang_counts':lang_counts,'sentiment_counts':sentiment_counts,
            'country_dist':country_dist, 'timeline_data':timeline_data}


def fetch_batch_data(batch_id, db_file=MANUAL_DB_FILE):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("SELECT label FROM batches WHERE batch_id=?", (batch_id,))
    row = cur.fetchone()
    if not row: con.close(); return None
    label = row[0]

    cur.execute("SELECT COUNT(*),type FROM manual_posts WHERE batch_id=? GROUP BY type", (batch_id,))
    pc = {'photo':0,'post':0,'reel':0}
    for r in cur.fetchall(): pc[r[1]] = r[0]

    cur.execute("""
        SELECT co.id,co.name,co.profile_url,bcs.total_score,bcs.comment_count,bcs.tier,
               bcs.sentiment_score,cc.identified_country,cc.current_city,cc.employer,cc.education
        FROM batch_commentor_scores bcs
        JOIN commentors co ON co.id=bcs.commentor_id
        LEFT JOIN commentor_country cc ON cc.commentor_id=co.id
        WHERE bcs.batch_id=? ORDER BY bcs.total_score DESC
    """, (batch_id,))
    commentors = [{'id':r[0],'name':r[1] or '','profile_url':r[2] or '',
                   'total_score':r[3] or 0.0,'comment_count':r[4] or 0,'tier':r[5] or 'neutral',
                   'sentiment_score':r[6] or 0.0,'country':r[7],'city':r[8],
                   'employer':r[9],'education':r[10]} for r in cur.fetchall()]

    cur.execute("""
        SELECT sp.name,sp.profile_url,sp.relationship_type,sp.score,
               GROUP_CONCAT(spf.label||': '||spf.value,' | '),cc.identified_country
        FROM secondary_profiles sp
        LEFT JOIN secondary_profile_fields spf ON spf.secondary_profile_id=sp.id
        LEFT JOIN commentor_country cc ON cc.commentor_id=sp.commentor_id
        WHERE sp.batch_id=? GROUP BY sp.id ORDER BY sp.score DESC LIMIT 7
    """, (batch_id,))
    secondary = [{'name':r[0] or 'Unknown','profile_url':r[1] or '',
                  'tier':r[2] or 'neutral','score':r[3] or 0.0,
                  'fields':r[4] or '','country':r[5]} for r in cur.fetchall()]

    lang_counts = {}; sentiment_counts = {'positive':0,'neutral':0,'negative':0}
    cur.execute("""
        SELECT ca.language,ca.sentiment FROM comment_analysis ca
        JOIN comments c ON c.id=ca.comment_id AND ca.db_source='manual'
        JOIN manual_posts mp ON mp.id=c.post_id WHERE mp.batch_id=?
    """, (batch_id,))
    for lang,sent in cur.fetchall():
        if lang and lang.lower() not in ('unknown','null','none',''):
            lang_counts[lang] = lang_counts.get(lang,0)+1
        if sent:
            s = sent.lower()
            if s=='positive': sentiment_counts['positive']+=1
            elif s=='negative': sentiment_counts['negative']+=1
            else: sentiment_counts['neutral']+=1

    country_dist = {}
    for c in commentors:
        ct = c['country'] or 'Unknown'
        country_dist[ct] = country_dist.get(ct,0)+1

    # Manual URLs investigated in this batch
    cur.execute("""
        SELECT url, type, date_text, caption
        FROM manual_posts WHERE batch_id = ?
        ORDER BY type, id
    """, (batch_id,))
    manual_urls = [{'url': r[0], 'type': r[1] or 'unknown',
                    'date': r[2] or 'N/A', 'caption': r[3] or ''}
                   for r in cur.fetchall()]

    # Timeline per post
    timeline_data = []
    try:
        cur.execute("""
            SELECT mp.id, mp.date_text, mp.type,
                   COUNT(DISTINCT c.id) as total,
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
            date_str = r[1] or 'Unknown'
            short    = date_str[:12] if len(date_str) > 12 else date_str
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
    except Exception: pass

    con.close()
    return {'type':'batch','owner_name':label,'profile_url':batch_id,'is_locked':False,
            'profile_fields':[],'photo_count':pc.get('photo',0),
            'reel_count':pc.get('reel',0),'text_count':pc.get('post',0),
            'commentors':commentors,'secondary':secondary,'image_analysis':[],'text_posts':[],
            'lang_counts':lang_counts,'sentiment_counts':sentiment_counts,
            'country_dist':country_dist, 'timeline_data':timeline_data,
            'manual_urls':manual_urls}


#  PAGE BUILDERS

def build_cover(data, S):
    story = []
    story.append(Spacer(1, 1.5*cm))

    if os.path.exists(LOGO_PATH):
        try:
            img = Image(LOGO_PATH, width=7.5*cm, height=7.5*cm)
            img.hAlign = 'CENTER'
            story.append(img)
        except: pass

    story.append(Spacer(1, 0.6*cm))
    story.append(Paragraph("BIRDY-EDWARDS", S['cover_title']))
    story.append(HRFlowable(width='65%', thickness=2.5, color=C_ACCENT, spaceAfter=14, spaceBefore=4))
    story.append(Paragraph(
        '"The truth is never buried deep enough<br/>to escape the right set of eyes."',
        S['cover_quote']))
    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width='100%', thickness=0.8, color=C_BORDER, spaceAfter=14))

    story.append(Paragraph("SOCMINT  INVESTIGATION  REPORT", ParagraphStyle(
        'rt', fontName='Helvetica-Bold', fontSize=17, textColor=C_ACCENT2,
        alignment=TA_CENTER, spaceAfter=16)))

    inv_type = 'Automated Profile' if data['type']=='profile' else 'Manual Batch'
    meta = [
        ('TARGET', data['owner_name']),
        ('TYPE',   inv_type),
        ('ID',     data['profile_url'][:55]+('...' if len(data['profile_url'])>55 else '')),
        ('DATE',   datetime.now().strftime('%B %d, %Y  at  %H:%M')),
    ]
    tbl = Table(
        [[Paragraph(k, S['meta_label']), Paragraph(v, S['meta_value'])] for k,v in meta],
        colWidths=[3.5*cm, 12*cm]
    )
    tbl.setStyle(TableStyle([
        ('ROWBACKGROUNDS', (0,0),(-1,-1), [C_LIGHT_BG, C_WHITE]),
        ('TOPPADDING',    (0,0),(-1,-1), 9),
        ('BOTTOMPADDING', (0,0),(-1,-1), 9),
        ('LEFTPADDING',   (0,0),(-1,-1), 12),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('LINEBEFORE',    (0,0),(0,-1),  3, C_ACCENT),
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

    stats = [('Photos',str(data['photo_count'])),('Reels',str(data['reel_count'])),
             ('Text Posts',str(data['text_count'])),('Commentors',str(len(data['commentors']))),
             ('Locked','Yes' if data['is_locked'] else 'No')]

    st = Table(
        [[Paragraph(s[0],S['stat_label']) for s in stats],
         [Paragraph(s[1],S['stat_value']) for s in stats]],
        colWidths=[3.2*cm]*5
    )
    st.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), C_LIGHT_BG),
        ('TOPPADDING',    (0,0),(-1,-1), 12),
        ('BOTTOMPADDING', (0,0),(-1,-1), 12),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('LINEABOVE',     (0,0),(-1,0),  3, C_ACCENT),
        ('ALIGN',         (0,0),(-1,-1), 'CENTER'),
    ]))
    story.append(st)
    story.append(Spacer(1, 0.5*cm))

    if data['profile_fields']:
        story.append(Paragraph("About Information", S['sub_title']))
        rows = [[
            Paragraph(str(f.get('label','')), ParagraphStyle('fl',fontName='Helvetica-Bold',
                fontSize=13, textColor=C_ACCENT2)),
            Paragraph(str(f.get('value','')), S['body']),
        ] for f in data['profile_fields'][:25]]
        ft = Table(rows, colWidths=[5*cm, 11*cm])
        ft.setStyle(TableStyle([
            ('ROWBACKGROUNDS',(0,0),(-1,-1),[C_WHITE,C_LIGHT_BG]),
            ('TOPPADDING',   (0,0),(-1,-1), 8),
            ('BOTTOMPADDING',(0,0),(-1,-1), 8),
            ('LEFTPADDING',  (0,0),(-1,-1), 10),
            ('GRID',         (0,0),(-1,-1), 0.5, C_BORDER),
            ('LINEBEFORE',   (0,0),(0,-1),  3, C_ACCENT2),
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
        lbl,_ = get_score_label(c['total_score'])
        tier_counts[lbl] = tier_counts.get(lbl,0)+1

    tb = chart_tiers(tier_counts)
    if tb:
        img = Image(tb, width=17*cm, height=9*cm)
        img.hAlign = 'CENTER'
        story.append(img)

    story.append(PageBreak())
    return story


def build_commentors_table(data, S):
    story = []
    story += sec_hdr("03 / ALL COMMENTORS", S)

    headers = ['#','NAME','COUNTRY','SCORE','COMMENTS','CATEGORY']
    col_w   = [1*cm, 5*cm, 3.5*cm, 2.2*cm, 2*cm, 3.8*cm]
    rows    = [[Paragraph(h, S['table_header']) for h in headers]]

    for i,c in enumerate(data['commentors'],1):
        lbl,_ = get_score_label(c['total_score'])
        sc    = colors.HexColor(get_score_hex(c['total_score']))
        rows.append([
            Paragraph(str(i), ParagraphStyle('n',fontName='Helvetica',fontSize=12,
                                              textColor=C_SUBTEXT,alignment=TA_CENTER)),
            Paragraph(c['name'][:30], S['table_cell']),
            Paragraph(
                f"{get_country_flag(c.get('country'))} {c.get('country') or 'Unknown'}",
                S['table_cell']),
            Paragraph(f"{c['total_score']:+.3f}", ParagraphStyle('sc',fontName='Helvetica-Bold',
                fontSize=12, textColor=sc, alignment=TA_CENTER)),
            Paragraph(str(c['comment_count']), S['table_cell_c']),
            Paragraph(lbl, ParagraphStyle('cl',fontName='Helvetica-Bold',fontSize=11,
                textColor=sc, alignment=TA_CENTER)),
        ])

    t = Table(rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  C_ACCENT2),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ('TOPPADDING',    (0,0),(-1,-1), 8),
        ('BOTTOMPADDING', (0,0),(-1,-1), 8),
        ('LEFTPADDING',   (0,0),(-1,-1), 6),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('LINEBELOW',     (0,0),(-1,0),  2, C_ACCENT),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


def build_top7(data, S):
    story = []
    story += sec_hdr("04 / TOP 7 CLOSE NETWORK", S)

    # Matplotlib star graph
    profiles = data['secondary'] if data['secondary'] else data['commentors'][:7]
    graph_profiles = profiles[:7]

    # Add owner_name to first entry for center label
    graph_data = [{'owner_name': data['owner_name']}] + [{}]  # dummy
    cb = chart_top7(graph_profiles)
    if cb:
        img = Image(cb, width=14*cm, height=11*cm)
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.5*cm))

    # Cards
    cards = []
    for i, c in enumerate(profiles[:7]):
        rank   = i+1
        score  = c.get('total_score', c.get('score', 0.0))
        lbl,_  = get_score_label(score)
        country= c.get('country') or 'Unknown'
        bc     = colors.HexColor(get_score_hex(score))

        if c.get('fields'):
            ftext = c['fields'][:140]
        else:
            parts = []
            if c.get('city'):      parts.append(f"City: {c['city']}")
            if c.get('employer'):  parts.append(f"Work: {c['employer']}")
            if c.get('education'): parts.append(f"Edu: {c['education']}")
            ftext = '  |  '.join(parts) if parts else 'No additional data'

        card = Table([
            [Paragraph(f"#{rank}  {c['name'][:28]}", S['card_name'])],
            [Paragraph(country, S['card_country'])],
            [Paragraph(f"Score: {score:+.3f}  |  {lbl}", ParagraphStyle(
                'cs3', fontName='Helvetica-Bold', fontSize=13, textColor=bc, spaceAfter=3))],
            [Paragraph(ftext[:150], S['card_detail'])],
        ], colWidths=[8.2*cm])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0),(-1,-1), C_WHITE),
            ('TOPPADDING',   (0,0),(-1,-1), 7),
            ('BOTTOMPADDING',(0,0),(-1,-1), 7),
            ('LEFTPADDING',  (0,0),(-1,-1), 10),
            ('BOX',          (0,0),(-1,-1), 0.5, C_BORDER),
            ('LINEABOVE',    (0,0),(-1,0),  3, bc),
        ]))
        cards.append(card)

    for i in range(0, len(cards), 2):
        row_c = cards[i:i+2]
        pair  = [row_c[0], row_c[1]] if len(row_c)==2 else [row_c[0], '']
        row   = Table([pair], colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN',       (0,0),(-1,-1), 'TOP'),
            ('LEFTPADDING',  (0,0),(-1,-1), 2),
            ('RIGHTPADDING', (0,0),(-1,-1), 2),
            ('TOPPADDING',   (0,0),(-1,-1), 3),
            ('BOTTOMPADDING',(0,0),(-1,-1), 3),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.2*cm))

    story.append(PageBreak())
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

            # Try to download and embed thumbnail
            thumb_cell = ''
            image_src = img_data.get('image_src')
            if image_src:
                thumb_path = download_image_thumb(image_src, max_size=(180, 180))
                if thumb_path:
                    try:
                        thumb = Image(thumb_path, width=4*cm, height=4*cm)
                        thumb_cell = thumb
                    except Exception:
                        thumb_cell = Paragraph('Image\nUnavailable',
                            ParagraphStyle('na', fontName='Helvetica', fontSize=10,
                                          textColor=C_SUBTEXT, alignment=TA_CENTER))

            # Info table
            def flag(v):
                has = v and str(v).lower() not in ('none','null','false','no','')
                return Paragraph('YES' if has else 'NO', ParagraphStyle(
                    'if3', fontName='Helvetica-Bold', fontSize=12, alignment=TA_CENTER,
                    textColor=C_RED if has else C_SILVER))

            caption = img_data.get('caption') or 'N/A'
            date    = img_data.get('date') or 'N/A'

            info_rows = [
                [Paragraph('Date', ParagraphStyle('il', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(date, S['table_cell'])],
                [Paragraph('Caption', ParagraphStyle('il2', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(caption)[:100], S['table_cell'])],
                [Paragraph('Scene', ParagraphStyle('il3', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(img_data.get('scene_type') or 'N/A'), S['table_cell'])],
                [Paragraph('Activity', ParagraphStyle('il4', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(img_data.get('activity') or 'N/A')[:80], S['table_cell'])],
                [Paragraph('Location', ParagraphStyle('il5', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(img_data.get('estimated_location') or 'Unknown')[:60], S['table_cell'])],
                [Paragraph('Political', ParagraphStyle('il6', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 flag(img_data.get('political_symbols'))],
                [Paragraph('Weapons', ParagraphStyle('il7', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 flag(img_data.get('weapons_visible'))],
                [Paragraph('Religious', ParagraphStyle('il8', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 flag(img_data.get('religious_symbols'))],
            ]

            info_tbl = Table(info_rows, colWidths=[2.5*cm, 8*cm])
            info_tbl.setStyle(TableStyle([
                ('ROWBACKGROUNDS', (0,0),(-1,-1), [C_WHITE, C_LIGHT_BG]),
                ('TOPPADDING',    (0,0),(-1,-1), 5),
                ('BOTTOMPADDING', (0,0),(-1,-1), 5),
                ('LEFTPADDING',   (0,0),(-1,-1), 8),
                ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
                ('LINEBEFORE',    (0,0),(0,-1),  3, C_ACCENT2),
            ]))

            # Side by side: thumbnail + info
            if thumb_cell:
                outer = Table([[thumb_cell, info_tbl]], colWidths=[4.5*cm, 12.5*cm])
                outer.setStyle(TableStyle([
                    ('VALIGN', (0,0),(-1,-1), 'TOP'),
                    ('LEFTPADDING',  (0,0),(-1,-1), 0),
                    ('RIGHTPADDING', (0,0),(-1,-1), 6),
                ]))
                story.append(outer)
            else:
                story.append(info_tbl)

            story.append(Spacer(1, 0.3*cm))

    if data['text_posts']:
        story.append(Paragraph("Text Post Intelligence", S['sub_title']))
        for i, tp in enumerate(data['text_posts'], 1):
            threat = tp.get('threat_indicators') or 'None'
            has_t  = threat.lower() not in ('none','null','n/a','')

            # Screenshot thumbnail
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
                    thumb_cell = ''

            info_rows = [
                [Paragraph('Post', ParagraphStyle('tp1', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(f"Post {i}", S['table_cell'])],
                [Paragraph('Topic', ParagraphStyle('tp2', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(tp.get('topic') or 'N/A'), S['table_cell'])],
                [Paragraph('Language', ParagraphStyle('tp3', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(tp.get('text_language') or 'N/A'), S['table_cell'])],
                [Paragraph('Narrative', ParagraphStyle('tp4', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_ACCENT2)),
                 Paragraph(str(tp.get('narrative_type') or 'N/A'), S['table_cell'])],
                [Paragraph('Threats', ParagraphStyle('tp5', fontName='Helvetica-Bold',
                    fontSize=11, textColor=C_RED if has_t else C_ACCENT2)),
                 Paragraph(threat[:80], ParagraphStyle('tv', fontName='Helvetica',
                    fontSize=12, textColor=C_RED if has_t else C_TEXT))],
            ]

            info_tbl = Table(info_rows, colWidths=[2.5*cm, 8*cm])
            info_tbl.setStyle(TableStyle([
                ('ROWBACKGROUNDS', (0,0),(-1,-1), [C_WHITE, C_LIGHT_BG]),
                ('TOPPADDING',    (0,0),(-1,-1), 5),
                ('BOTTOMPADDING', (0,0),(-1,-1), 5),
                ('LEFTPADDING',   (0,0),(-1,-1), 8),
                ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
                ('LINEBEFORE',    (0,0),(0,-1),  3, C_RED if has_t else C_ACCENT2),
            ]))

            if thumb_cell:
                outer = Table([[thumb_cell, info_tbl]], colWidths=[4.5*cm, 12.5*cm])
                outer.setStyle(TableStyle([
                    ('VALIGN', (0,0),(-1,-1), 'TOP'),
                    ('LEFTPADDING',  (0,0),(-1,-1), 0),
                    ('RIGHTPADDING', (0,0),(-1,-1), 6),
                ]))
                story.append(outer)
            else:
                story.append(info_tbl)
            story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())
    return story


def build_comment_intelligence(data, S):
    total = sum(data['sentiment_counts'].values())
    if total==0 and not data['lang_counts']:
        return []

    story = []
    story += sec_hdr("06 / COMMENT INTELLIGENCE", S)

    sb = chart_sentiment(data['sentiment_counts'])
    lb = chart_language(data['lang_counts'])

    if sb and lb:
        row = Table([[Image(sb,width=8*cm,height=6*cm), Image(lb,width=8*cm,height=6*cm)]],
                    colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'),('ALIGN',(0,0),(-1,-1),'CENTER')]))
        story.append(row)
        story.append(Spacer(1, 0.5*cm))
    elif sb:
        img = Image(sb, width=12*cm, height=7*cm); img.hAlign='CENTER'; story.append(img)

    pos = data['sentiment_counts'].get('positive',0)
    neg = data['sentiment_counts'].get('negative',0)
    dom = max(data['lang_counts'], key=data['lang_counts'].get) if data['lang_counts'] else 'Unknown'

    stats = [
        ('Total Commentors',       str(len(data['commentors']))),
        ('Total Comments Analyzed', str(total)),
        ('Positive Comments',      f"{pos}  ({pos/total*100:.1f}%)" if total else '0'),
        ('Negative Comments',      f"{neg}  ({neg/total*100:.1f}%)" if total else '0'),
        ('Languages Detected',     str(len(data['lang_counts']))),
        ('Primary Language',       dom),
    ]

    for k,v in stats:
        r = Table([[
            Paragraph(k, ParagraphStyle('sk3',fontName='Helvetica-Bold',fontSize=13,textColor=C_ACCENT2)),
            Paragraph(v, S['body']),
        ]], colWidths=[8*cm, 9*cm])
        r.setStyle(TableStyle([
            ('TOPPADDING',    (0,0),(-1,-1), 8),
            ('BOTTOMPADDING', (0,0),(-1,-1), 8),
            ('LEFTPADDING',   (0,0),(-1,-1), 12),
            ('LINEBELOW',     (0,0),(-1,-1), 0.5, C_BORDER),
            ('LINEBEFORE',    (0,0),(0,-1),  3, C_ACCENT),
        ]))
        story.append(r)

    return story


#  MAIN

def build_post_timeline(data, S):
    """Post activity timeline — new page."""
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

    # Summary table
    story.append(Paragraph("Post Activity Breakdown", S['sub_title']))
    headers = ['POST DATE', 'TYPE', 'TOTAL', 'POSITIVE', 'NEUTRAL', 'NEGATIVE']
    rows = [[Paragraph(h, S['table_header']) for h in headers]]
    for p in sorted(td, key=lambda x: -x['total']):
        rows.append([
            Paragraph(p['date'][:20], S['table_cell']),
            Paragraph(p['type'].upper(), ParagraphStyle('pt',fontName='Helvetica-Bold',
                fontSize=12, textColor=C_ACCENT2, alignment=TA_CENTER)),
            Paragraph(str(p['total']), ParagraphStyle('ptot',fontName='Helvetica-Bold',
                fontSize=12, textColor=C_DARK, alignment=TA_CENTER)),
            Paragraph(str(p['positive']), ParagraphStyle('ppos',fontName='Helvetica-Bold',
                fontSize=12, textColor=C_GREEN, alignment=TA_CENTER)),
            Paragraph(str(p['neutral']), ParagraphStyle('pneu',fontName='Helvetica',
                fontSize=12, textColor=C_SILVER, alignment=TA_CENTER)),
            Paragraph(str(p['negative']), ParagraphStyle('pneg',fontName='Helvetica-Bold',
                fontSize=12, textColor=C_RED, alignment=TA_CENTER)),
        ])
    t = Table(rows, colWidths=[5*cm, 2.5*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm], repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  C_ACCENT2),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [C_WHITE, C_LIGHT_BG]),
        ('TOPPADDING',    (0,0),(-1,-1), 7),
        ('BOTTOMPADDING', (0,0),(-1,-1), 7),
        ('LEFTPADDING',   (0,0),(-1,-1), 6),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('LINEBELOW',     (0,0),(-1,0),  2, C_ACCENT),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


def build_manual_urls(data, S):
    """Manual batch — list of investigated URLs. Only for batch type."""
    if data.get('type') != 'batch':
        return []
    manual_urls = data.get('manual_urls', [])
    if not manual_urls:
        return []

    story = []
    story += sec_hdr("08 / INVESTIGATED URLS", S)

    story.append(Paragraph(
        f"This investigation analyzed {len(manual_urls)} URL(s) from batch: "
        f"<b>{data['owner_name']}</b>",
        S['body']))
    story.append(Spacer(1, 0.4*cm))

    # Group by type
    type_icons = {'photo': '📸', 'reel': '🎬', 'post': '📝', 'unknown': '🔗'}

    for i, u in enumerate(manual_urls, 1):
        ptype   = u.get('type', 'unknown')
        icon    = type_icons.get(ptype, '🔗')
        caption = u.get('caption', '') or ''
        date    = u.get('date', 'N/A') or 'N/A'
        url     = u.get('url', '')

        row_data = [
            [Paragraph(f"{icon}  #{i}  {ptype.upper()}", ParagraphStyle(
                'ut', fontName='Helvetica-Bold', fontSize=13, textColor=C_ACCENT2))],
            [Paragraph(f"URL: {url[:80]}{'...' if len(url)>80 else ''}", ParagraphStyle(
                'uu', fontName='Helvetica', fontSize=11, textColor=C_BLUE))],
            [Paragraph(f"Date: {date}", ParagraphStyle(
                'ud', fontName='Helvetica', fontSize=12, textColor=C_SUBTEXT))],
        ]
        if caption:
            row_data.append([Paragraph(
                f"Caption: {caption[:120]}{'...' if len(caption)>120 else ''}",
                ParagraphStyle('uc', fontName='Helvetica-Oblique',
                               fontSize=12, textColor=C_TEXT))])

        card = Table(row_data, colWidths=[17*cm])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0),(-1,-1), C_WHITE),
            ('TOPPADDING',   (0,0),(-1,-1), 6),
            ('BOTTOMPADDING',(0,0),(-1,-1), 6),
            ('LEFTPADDING',  (0,0),(-1,-1), 12),
            ('BOX',          (0,0),(-1,-1), 0.5, C_BORDER),
            ('LINEABOVE',    (0,0),(-1,0),  3, C_ACCENT2),
        ]))
        story.append(card)
        story.append(Spacer(1, 0.2*cm))

    return story


def build_face_intelligence(data, S):
    """Face intelligence page — most appeared + all detected faces."""
    if data.get('type') != 'profile':
        return []

    try:
        import face_intelligence as fi
        results = fi.fetch_face_results(data['profile_url'])
    except Exception:
        return []

    if not results:
        return []

    # Filter only real detections
    results = [r for r in results if r['count'] > 0 and r['repr_face']]
    if not results:
        return []

    # Sort by appearance count descending
    results = sorted(results, key=lambda x: len(x['post_ids']), reverse=True)

    story = []
    story += sec_hdr("08 / FACE INTELLIGENCE", S)

    story.append(Paragraph(
        f"Detected <b>{len(results)}</b> unique person(s) across all photo posts "
        f"using face recognition analysis.",
        S['body']))
    story.append(Spacer(1, 0.4*cm))

    # Summary stats
    total_faces = sum(r['count'] for r in results)
    multi_post  = sum(1 for r in results if len(r['post_ids']) > 1)
    high_freq   = [r for r in results if len(r['post_ids']) >= 3]

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
        ('BACKGROUND',    (0,0),(-1,-1), C_LIGHT_BG),
        ('TOPPADDING',    (0,0),(-1,-1), 10),
        ('BOTTOMPADDING', (0,0),(-1,-1), 10),
        ('GRID',          (0,0),(-1,-1), 0.5, C_BORDER),
        ('LINEABOVE',     (0,0),(-1,0),  3, C_ACCENT),
        ('ALIGN',         (0,0),(-1,-1), 'CENTER'),
    ]))
    story.append(st)
    story.append(Spacer(1, 0.6*cm))

    # SECTION 1 — MOST APPEARED FACES (top 6 by post count)
    story.append(Paragraph("Most Appeared Persons", S['sub_title']))
    story.append(Paragraph(
        "Persons appearing most frequently across posts — likely close associates.",
        S['body_sub']))
    story.append(Spacer(1, 0.3*cm))

    top_results = results[:6]  # top 6

    def make_face_card(r, big=True):
        """Build a single person card — face left, info right (horizontal)."""
        img_size    = 4.5*cm if big else 3.5*cm
        post_count  = len(r['post_ids'])
        label_color = C_RED if post_count >= 3 else C_BLUE if post_count >= 2 else C_SILVER
        info_w      = 3.8*cm if big else 3.2*cm
        card_w      = img_size + info_w + 0.4*cm

        face_elem = Paragraph("No\nImage", ParagraphStyle(
            'ni', fontName='Helvetica', fontSize=11, textColor=C_SUBTEXT,
            alignment=TA_CENTER))
        if r['repr_face'] and os.path.exists(r['repr_face']):
            try:
                face_elem = Image(r['repr_face'], width=img_size, height=img_size)
            except Exception:
                pass

        info_rows = [
            [Paragraph(r['label'].replace('_',' ').title(), ParagraphStyle(
                'fn3', fontName='Helvetica-Bold',
                fontSize=14 if big else 12, textColor=C_DARK, spaceAfter=3))],
            [Paragraph(f"Posts:  {post_count}", ParagraphStyle(
                'fp3', fontName='Helvetica-Bold',
                fontSize=12 if big else 11, textColor=label_color, spaceAfter=2))],
            [Paragraph(f"Faces:  {r['count']}", ParagraphStyle(
                'ff3', fontName='Helvetica',
                fontSize=11 if big else 10, textColor=C_SUBTEXT, spaceAfter=2))],
        ]
        if big and r.get('face_paths'):
            dates = list(set(f['date'] for f in r['face_paths'] if f.get('date')))[:2]
            if dates:
                info_rows.append([Paragraph(
                    "Seen: " + ", ".join(d[:15] for d in dates),
                    ParagraphStyle('fd2', fontName='Helvetica', fontSize=10,
                                   textColor=C_SUBTEXT))])

        info_tbl = Table(info_rows, colWidths=[info_w])
        info_tbl.setStyle(TableStyle([
            ('TOPPADDING',   (0,0),(-1,-1), 3),
            ('BOTTOMPADDING',(0,0),(-1,-1), 3),
            ('LEFTPADDING',  (0,0),(-1,-1), 8),
            ('VALIGN',       (0,0),(-1,-1), 'MIDDLE'),
        ]))

        # Horizontal: [face_img | info_text]
        card = Table([[face_elem, info_tbl]],
                     colWidths=[img_size + 0.2*cm, info_w])
        card.setStyle(TableStyle([
            ('BACKGROUND',   (0,0),(-1,-1), C_WHITE),
            ('TOPPADDING',   (0,0),(-1,-1), 10),
            ('BOTTOMPADDING',(0,0),(-1,-1), 10),
            ('LEFTPADDING',  (0,0),(-1,-1), 8),
            ('RIGHTPADDING', (0,0),(-1,-1), 8),
            ('BOX',          (0,0),(-1,-1), 0.5, C_BORDER),
            ('LINEBEFORE',   (0,0),(0,-1),  4, label_color),
            ('VALIGN',       (0,0),(-1,-1), 'MIDDLE'),
        ]))
        return card

    # Top cards — 2 per row
    top_cards = [make_face_card(r, big=True) for r in top_results]
    for i in range(0, len(top_cards), 2):
        row_c = top_cards[i:i+2]
        while len(row_c) < 2: row_c.append('')
        row = Table([row_c], colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN',       (0,0),(-1,-1), 'TOP'),
            ('LEFTPADDING',  (0,0),(-1,-1), 2),
            ('RIGHTPADDING', (0,0),(-1,-1), 2),
            ('TOPPADDING',   (0,0),(-1,-1), 4),
            ('BOTTOMPADDING',(0,0),(-1,-1), 4),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.3*cm))

    story.append(PageBreak())

    # SECTION 2 — ALL DETECTED FACES
    story += sec_hdr("08b / ALL DETECTED FACES", S)
    story.append(Paragraph(
        f"Complete gallery of all <b>{len(results)}</b> unique persons "
        f"detected across the investigation.",
        S['body']))
    story.append(Spacer(1, 0.4*cm))

    # All faces — 2 per row (consistent with top section)
    all_cards = [make_face_card(r, big=False) for r in results]
    for i in range(0, len(all_cards), 2):
        row_c = all_cards[i:i+2]
        while len(row_c) < 2: row_c.append('')
        row = Table([row_c], colWidths=[8.5*cm, 8.5*cm])
        row.setStyle(TableStyle([
            ('VALIGN',       (0,0),(-1,-1), 'TOP'),
            ('LEFTPADDING',  (0,0),(-1,-1), 2),
            ('RIGHTPADDING', (0,0),(-1,-1), 2),
            ('TOPPADDING',   (0,0),(-1,-1), 4),
            ('BOTTOMPADDING',(0,0),(-1,-1), 4),
        ]))
        story.append(row)
        story.append(Spacer(1, 0.3*cm))

    # High frequency alert box
    if high_freq:
        story.append(Spacer(1, 0.4*cm))
        story.append(HRFlowable(width='100%', thickness=1, color=C_ACCENT, spaceAfter=8))
        story.append(Paragraph("High Frequency Alert", S['sub_title']))
        for r in high_freq:
            post_count = len(r['post_ids'])
            story.append(Paragraph(
                f"• <b>{r['label'].replace('_',' ').title()}</b> — "
                f"appeared in <b>{post_count} posts</b> with {r['count']} face detection(s). "
                f"Likely a close associate or frequent contact.",
                S['body']))

    return story


def generate_report(profile_url=None, batch_id=None):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    print(f"\n{'═'*65}\n📄  Birdy-Edwards — Report Generator\n{'═'*65}")

    if profile_url:
        print(f"  Profile : {profile_url}")
        data = fetch_profile_data(profile_url)
        if not data: print("  ⚠️  Profile not found"); return
        safe = data['owner_name'].replace(' ','_').replace('/','_')[:30]
        out  = os.path.join(REPORTS_DIR, f"report_{safe}.pdf")
    elif batch_id:
        print(f"  Batch   : {batch_id}")
        data = fetch_batch_data(batch_id)
        if not data: print("  ⚠️  Batch not found"); return
        safe = batch_id.replace(' ','_')[:30]
        out  = os.path.join(REPORTS_DIR, f"report_{safe}.pdf")
    else:
        print("  ⚠️  Provide profile_url or batch_id"); return

    print(f"  Target  : {data['owner_name']}\n  Output  : {out}")

    S   = make_styles()
    doc = SimpleDocTemplate(out, pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)

    print("\n  Building pages...")
    story = []

    story += build_cover(data, S);             print("  ✅ Cover")
    story += build_profile_summary(data, S);   print("  ✅ Profile Summary")
    story += build_network_overview(data, S);  print("  ✅ Network Overview")
    story += build_commentors_table(data, S);  print("  ✅ All Commentors")
    story += build_top7(data, S);              print("  ✅ Top 7 Network")

    cp = build_content_analysis(data, S)
    if cp: story += cp; print("  ✅ Content Analysis")
    else: print("  ⏭️  Content Analysis — skipped (no data)")

    ip = build_comment_intelligence(data, S)
    if ip: story += ip; print("  ✅ Comment Intelligence")
    else: print("  ⏭️  Comment Intelligence — skipped (no data)")

    tp = build_post_timeline(data, S)
    if tp: story += tp; print("  ✅ Post Activity Timeline")
    else: print("  ⏭️  Post Timeline — skipped (no data)")

    up = build_manual_urls(data, S)
    if up: story += up; print("  ✅ Investigated URLs (batch)")

    fp = build_face_intelligence(data, S)
    if fp: story += fp; print("  ✅ Face Intelligence")
    else: print("  ⏭️  Face Intelligence — skipped (no data or not profile type)")

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