import os
import re
import sys
import json
import sqlite3
import subprocess
import threading
import urllib.request
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, Response, send_file

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY') or os.urandom(32)

#  PATHS — all relative to socmint/ root (where app.py lives)

BASE_DIR       = os.path.dirname(os.path.abspath(__file__))

# Ollama host — reads from environment for Docker compatibility

OLLAMA_HOST = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
DB_FILE        = os.path.join(BASE_DIR, 'socmint.db')
MANUAL_DB_FILE = os.path.join(BASE_DIR, 'socmint_manual.db')
STATUS_DIR     = os.path.join(BASE_DIR, 'status')
REPORTS_DIR    = os.path.join(BASE_DIR, 'reports')
ICONS_DIR      = os.path.join(BASE_DIR, 'icons')
MODEL_FILE     = os.path.join(BASE_DIR, '.ollama_model')

# Available models — verified on Ollama library
AVAILABLE_MODELS = {
    '8GB RAM (4B–7B Models)': [
        ('gemma3:4b',       'Gemma 3 4B       — recommended for 8GB'),
        ('llava:7b',        'LLaVA 7B         — vision capable'),
        ('qwen2-vl:7b',     'Qwen2-VL 7B      — strong vision'),
    ],
    '16GB RAM (8B–13B Models)': [
        ('gemma3:12b',      'Gemma 3 12B      — recommended for 16GB'),
        ('minicpm-v:8b',    'MiniCPM-V 8B     — best OCR + vision'),
        ('qwen2.5-vl:7b',   'Qwen2.5-VL 7B   — flagship vision'),
        ('llava:13b',       'LLaVA 13B        — high quality vision'),
    ],
    '32GB RAM (27B+ Models)': [
        ('gemma3:27b',      'Gemma 3 27B      — high accuracy'),
        ('qwen2.5-vl:32b',  'Qwen2.5-VL 32B  — premium vision'),
        ('llava:34b',       'LLaVA 34B        — top vision quality'),
    ],
}

os.makedirs(STATUS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)


#  SYSTEM CHECK — runs on startup

def system_check():
    """Check RAM, set Ollama model, verify dependencies."""
    try:
        import psutil
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    except ImportError:
        ram_gb = 8.0

    # Check for user-selected model first
    user_model = None
    if os.path.exists(MODEL_FILE):
        try:
            user_model = open(MODEL_FILE).read().strip()
        except Exception:
            pass

    if user_model:
        model        = user_model
        model_reason = f'User selected: {model}'
    elif ram_gb >= 14:
        model        = 'gemma3:12b'
        model_reason = f'14GB+ RAM detected ({ram_gb:.1f}GB) → using gemma3:12b'
    else:
        model        = 'gemma3:4b'
        model_reason = f'{ram_gb:.1f}GB RAM → using gemma3:4b (safe mode)'

    app.config['OLLAMA_MODEL'] = model
    app.config['RAM_GB']       = round(ram_gb, 1)

    # Check Ollama running
    try:
        req  = urllib.request.urlopen(f'{OLLAMA_HOST}/api/tags', timeout=3)
        data = json.loads(req.read())
        models      = [m['name'] for m in data.get('models', [])]
        models_base = [m.split(':')[0] for m in models]
        model_base  = model.split(':')[0]
        app.config['OLLAMA_OK']    = True
        app.config['MODEL_PULLED'] = model in models or model_base in models_base
    except Exception:
        app.config['OLLAMA_OK']    = False
        app.config['MODEL_PULLED'] = False

    # Check session cookies
    app.config['COOKIES_OK'] = os.path.exists(os.path.join(BASE_DIR, 'fb_cookies.pkl'))

    print(f"\n{'═'*60}")
    print(f"  BIRDY-EDWARDS — System Check")
    print(f"{'═'*60}")
    print(f"  RAM      : {ram_gb:.1f} GB")
    print(f"  Model    : {model}  ←  {model_reason}")
    print(f"  Ollama   : {' Running' if app.config['OLLAMA_OK'] else ' Not running — start with: ollama serve'}")
    print(f"  Pulled   : {' Ready' if app.config['MODEL_PULLED'] else f' Run: ollama pull {model}'}")
    print(f"  Cookies  : {' Found' if app.config['COOKIES_OK'] else ' Missing — run: python3 refresh_cookies.py'}")
    print(f"{'═'*60}\n")


#  COOKIE VALIDITY CHECK


def check_cookies_valid():
    """
    Check if fb_cookies.pkl works on Facebook using SeleniumBase.
    Mirrors exact login() pattern used in all scrapers.
    """
    cookie_path = os.path.join(BASE_DIR, 'fb_cookies.pkl')

    if not os.path.exists(cookie_path):
        return False, 'Cookie file not found — refresh session first'

    try:
        import pickle
        import time
        from seleniumbase import SB

        cookies = pickle.load(open(cookie_path, 'rb'))
        if not cookies:
            return False, 'Cookie file is empty — refresh session'

        with SB(uc=True, headless=True, xvfb=True,
                window_size="1280,900") as sb:

            sb.open("https://www.facebook.com")
            time.sleep(3)

            for c in cookies:
                try:
                    sb.driver.add_cookie(c)
                except Exception:
                    pass

            sb.driver.refresh()
            time.sleep(5)

            # Handle profile selection screen — click Continue if it appears
            # If profile selection screen appears — cookies need refresh
            try:
                page_source = sb.get_page_source()
                if 'Use another profile' in page_source:
                    return False, 'Session requires re-authentication — please refresh cookies'
            except Exception:
                pass

            current_url = sb.get_current_url()
            page_source = sb.get_page_source()

            # Expired — redirected to login
            if 'login' in current_url or 'checkpoint' in current_url:
                return False, 'Session expired — redirected to login page'

            # Expired — login form in page
            logged_out_signals = [
                'id="loginbutton"',
                'name="login"',
                '"isLoggedIn":false',
            ]
            for sig in logged_out_signals:
                if sig in page_source:
                    return False, 'Session expired — login page detected'

            logged_in_signals = [
                'c_user',
                '"USER_ID"',
                'id="mount_0_0_',
            ]
            for sig in logged_in_signals:
                if sig in page_source:
                    try:
                        proof_path = os.path.join(BASE_DIR, 'status', 'session_proof.png')
                        sb.save_screenshot(proof_path)
                    except Exception as e:
                        print(f"    Screenshot save error: {e}")
                    return True, 'Session is active'

            return True, 'Session appears valid'

    except Exception as e:
        print(f"    Cookie check error: {e}")
        return True, 'Could not verify — proceeding anyway'

#  STATUS HELPERS

def write_status(key, data):
    path = os.path.join(STATUS_DIR, f'{key}.json')
    with open(path, 'w') as f:
        json.dump(data, f)

def read_status(key):
    path = os.path.join(STATUS_DIR, f'{key}.json')
    if os.path.exists(path):
        try:
            with open(path) as f:
                content = f.read().strip()
                if not content:
                    return None
                return json.loads(content)
        except (json.JSONDecodeError, Exception):
            return None
    return None

def init_status(key, profile_url=None, batch_id=None, scan_type=None):
    write_status(key, {
        'phase':           'gathering',
        'gathering_done':  False,
        'numbers_ready':   False,
        'analysis_done':   False,
        'network_done':    False,
        'report_done':     False,
        'posts_count':     0,
        'comments_count':  0,
        'commentors_count':0,
        'faces_count':     0,
        'profile_url':     profile_url,
        'batch_id':        batch_id,
        'scan_type':       scan_type,
        'error':           None,
        'started_at':      datetime.now().isoformat(),
    })

#  DB HELPERS

def db_connect(manual=False):
    path = MANUAL_DB_FILE if manual else DB_FILE
    if not os.path.exists(path):
        return None
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con

def get_all_profiles():
    con = db_connect()
    if not con:
        return []
    cur = con.cursor()
    try:
        cur.execute("SELECT id, profile_url, owner_name, scraped_at FROM profiles ORDER BY scraped_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        rows = []
    con.close()
    return rows

def get_all_batches():
    con = db_connect(manual=True)
    if not con:
        return []
    cur = con.cursor()
    try:
        cur.execute("SELECT id, batch_id, label, created_at FROM batches ORDER BY created_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        rows = []
    con.close()
    return rows

def get_profile_stats(profile_id):
    con = db_connect()
    if not con:
        return {}
    cur = con.cursor()
    stats = {}
    try:
        cur.execute("SELECT COUNT(*) FROM photo_posts WHERE profile_id=?", (profile_id,))
        photos = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM reel_posts WHERE profile_id=?", (profile_id,))
        reels = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM text_posts WHERE profile_id=?", (profile_id,))
        texts = cur.fetchone()[0]
        stats['posts_count'] = photos + reels + texts
        stats['photos']      = photos
        stats['reels']       = reels
        stats['texts']       = texts

        # Comments
        photo_ids = [r[0] for r in cur.execute("SELECT id FROM photo_posts WHERE profile_id=?", (profile_id,)).fetchall()]
        reel_ids  = [r[0] for r in cur.execute("SELECT id FROM reel_posts WHERE profile_id=?", (profile_id,)).fetchall()]
        text_ids  = [r[0] for r in cur.execute("SELECT id FROM text_posts WHERE profile_id=?", (profile_id,)).fetchall()]

        pc = rc = tc = 0
        if photo_ids:
            cur.execute(f"SELECT COUNT(*) FROM photo_comments WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids)
            pc = cur.fetchone()[0]
        if reel_ids:
            cur.execute(f"SELECT COUNT(*) FROM reel_comments WHERE reel_post_id IN ({','.join('?'*len(reel_ids))})", reel_ids)
            rc = cur.fetchone()[0]
        if text_ids:
            cur.execute(f"SELECT COUNT(*) FROM text_comments WHERE text_post_id IN ({','.join('?'*len(text_ids))})", text_ids)
            tc = cur.fetchone()[0]
        stats['comments_count'] = pc + rc + tc
        stats['photo_comments'] = pc
        stats['reel_comments']  = rc
        stats['text_comments']  = tc
        # Commentors
        all_post_ids = photo_ids + reel_ids + text_ids
        commentor_ids = set()
        if photo_ids:
            rows = cur.execute(f"SELECT DISTINCT commentor_id FROM photo_comments WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids).fetchall()
            commentor_ids.update(r[0] for r in rows)
        if reel_ids:
            rows = cur.execute(f"SELECT DISTINCT commentor_id FROM reel_comments WHERE reel_post_id IN ({','.join('?'*len(reel_ids))})", reel_ids).fetchall()
            commentor_ids.update(r[0] for r in rows)
        if text_ids:
            rows = cur.execute(f"SELECT DISTINCT commentor_id FROM text_comments WHERE text_post_id IN ({','.join('?'*len(text_ids))})", text_ids).fetchall()
            commentor_ids.update(r[0] for r in rows)
        stats['commentors_count'] = len(commentor_ids)

        # Faces — only count clusters belonging to THIS profile
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT fc.id) FROM face_clusters fc
                JOIN detected_faces df ON df.person_id = fc.id
                JOIN photo_posts pp ON pp.id = df.photo_post_id
                WHERE pp.profile_id = ?
            """, (profile_id,))
            stats['faces_count'] = cur.fetchone()[0]
        except Exception:
            stats['faces_count'] = 0

    except Exception as e:
        print(f"Stats error: {e}")
    con.close()
    return stats

def get_profile_about(profile_id):
    con = db_connect()
    if not con:
        return {}
    cur = con.cursor()
    data = {'fields': []}
    try:
        cur.execute("SELECT * FROM profiles WHERE id=?", (profile_id,))
        row = cur.fetchone()
        if row:
            data.update(dict(row))
        cur.execute("SELECT * FROM profile_fields WHERE profile_id=?", (profile_id,))
        data['fields'] = [dict(r) for r in cur.fetchall()]

        # Fix: owner_name is null when profile was pre-created by app.py before scraping
        # After scraping, socmint_db_import updates it — we just need to re-read it
        if not data.get('owner_name') or data['owner_name'] in ('Unknown','unknown','',None):
            # Try intro/name field from profile_fields
            for ft in ('name','display_name','full_name','intro'):
                nf = next((f for f in data['fields'] if f.get('field_type')==ft), None)
                if nf and nf.get('value'):
                    data['owner_name'] = nf['value']
                    try:
                        cur.execute("UPDATE profiles SET owner_name=? WHERE id=?", (nf['value'], profile_id))
                        con.commit()
                    except Exception:
                        pass
                    break
            else:
                # Last resort: humanize slug from URL
                url = data.get('profile_url','')
                if url and 'facebook.com/' in url and 'profile.php' not in url:
                    slug = url.rstrip('/').split('/')[-1]
                    if slug and not slug.isdigit():
                        data['owner_name'] = slug.replace('.',' ').replace('_',' ').title()
    except Exception as e:
        print(f"get_profile_about error: {e}")
    con.close()
    return data

def get_commentors(profile_id=None, batch_id=None):
    if batch_id:
        con = db_connect(manual=True)
        if not con: return []
        cur = con.cursor()
        try:
            cur.execute("""
                SELECT DISTINCT c.id, c.name, c.profile_url,
                       cc.identified_country,
                       cc.country_confidence,
                       cc.identification_basis,
                       COALESCE(bcs.total_score, 0) as total_score,
                       COALESCE(bcs.tier, 'Unknown') as tier,
                       COALESCE(bcs.comment_count, 0) as comment_count
                FROM commentors c
                JOIN comments cm ON cm.commentor_id = c.id
                JOIN manual_posts mp ON mp.id = cm.post_id AND mp.batch_id = ?
                LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
                LEFT JOIN batch_commentor_scores bcs ON bcs.commentor_id = c.id AND bcs.batch_id = ?
                ORDER BY total_score DESC
            """, (batch_id, batch_id))
            rows = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            print(f"Commentors error: {e}")
            rows = []
        con.close()
        return rows
    else:
        con = db_connect()
        if not con: return []
        cur = con.cursor()
        try:
            # CRITICAL: Only return commentors who actually commented on THIS profile's posts
            # Otherwise all commentors from all profiles appear (commentors table is shared)
            cur.execute("""
                SELECT DISTINCT c.id, c.name, c.profile_url,
                       cc.identified_country,
                       cc.country_confidence,
                       cc.identification_basis,
                       COALESCE(cs.total_score, 0) as total_score,
                       COALESCE(cs.tier, 'Unknown') as tier,
                       COALESCE(cs.comment_count, 0) as comment_count
                FROM commentors c
                -- Only commentors who commented on this profile's posts
                JOIN (
                    SELECT DISTINCT commentor_id FROM photo_comments pc
                    JOIN photo_posts pp ON pp.id = pc.photo_post_id
                    WHERE pp.profile_id = ?
                    UNION
                    SELECT DISTINCT commentor_id FROM reel_comments rc
                    JOIN reel_posts rp ON rp.id = rc.reel_post_id
                    WHERE rp.profile_id = ?
                    UNION
                    SELECT DISTINCT commentor_id FROM text_comments tc
                    JOIN text_posts tp ON tp.id = tc.text_post_id
                    WHERE tp.profile_id = ?
                ) mine ON mine.commentor_id = c.id
                LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
                LEFT JOIN commentor_scores cs ON cs.commentor_id = c.id AND cs.main_profile_id = ?
                ORDER BY total_score DESC
            """, (profile_id, profile_id, profile_id, profile_id))
            rows = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            print(f"Commentors error: {e}")
            rows = []
        con.close()
        return rows

def get_country_distribution(profile_id=None, batch_id=None):
    con = db_connect(manual=bool(batch_id))
    if not con: return []
    cur = con.cursor()
    try:
        if batch_id:
            cur.execute("""
                SELECT cc.identified_country, COUNT(DISTINCT c.id) as count
                FROM commentors c
                JOIN comments cm ON cm.commentor_id = c.id
                JOIN manual_posts mp ON mp.id = cm.post_id AND mp.batch_id = ?
                JOIN commentor_country cc ON cc.commentor_id = c.id
                GROUP BY cc.identified_country
                ORDER BY count DESC
            """, (batch_id,))
        else:
            cur.execute("""
                SELECT cc.identified_country, COUNT(*) as count
                FROM commentors c
                JOIN commentor_country cc ON cc.commentor_id = c.id
                JOIN (
                    SELECT DISTINCT commentor_id FROM photo_comments pc
                    JOIN photo_posts pp ON pp.id = pc.photo_post_id
                    WHERE pp.profile_id = ?
                    UNION
                    SELECT DISTINCT commentor_id FROM reel_comments rc
                    JOIN reel_posts rp ON rp.id = rc.reel_post_id
                    WHERE rp.profile_id = ?
                    UNION
                    SELECT DISTINCT commentor_id FROM text_comments tc
                    JOIN text_posts tp ON tp.id = tc.text_post_id
                    WHERE tp.profile_id = ?
                ) x ON x.commentor_id = c.id
                GROUP BY cc.identified_country
                ORDER BY count DESC
            """, (profile_id, profile_id, profile_id))
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"Country dist error: {e}")
        rows = []
    con.close()
    return rows

def get_tier_distribution(profile_id=None, batch_id=None):
    con = db_connect(manual=bool(batch_id))
    if not con: return {}
    cur = con.cursor()
    tiers = {'Strong Supporter':0,'Supporter':0,'Neutral':0,'Low Interaction':0,'Critical Voice':0}
    try:
        if batch_id:
            cur.execute("SELECT tier, COUNT(*) FROM batch_commentor_scores WHERE batch_id=? GROUP BY tier", (batch_id,))
        else:
            cur.execute("SELECT tier, COUNT(*) FROM commentor_scores WHERE main_profile_id=? GROUP BY tier", (profile_id,))
        for row in cur.fetchall():
            if row[0] in tiers:
                tiers[row[0]] = row[1]
    except Exception as e:
        print(f"Tier error: {e}")
    con.close()
    return tiers

def get_top7(profile_id=None, batch_id=None):
    con = db_connect(manual=bool(batch_id))
    if not con: return []
    cur = con.cursor()
    try:
        if batch_id:
            cur.execute("""
                SELECT c.id as commentor_id, c.name, c.profile_url, bcs.total_score, bcs.tier,
                       cc.identified_country, cc.current_city, cc.hometown, cc.employer, cc.education,
                       sp.id as sp_id
                FROM batch_commentor_scores bcs
                JOIN commentors c ON c.id = bcs.commentor_id
                LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
                LEFT JOIN secondary_profiles sp ON sp.commentor_id = c.id
                WHERE bcs.batch_id = ?
                ORDER BY bcs.total_score DESC LIMIT 7
            """, (batch_id,))
        else:
            cur.execute("""
                SELECT c.id as commentor_id, c.name, c.profile_url, cs.total_score, cs.tier,
                       cc.identified_country, cc.current_city, cc.hometown, cc.employer, cc.education,
                       sp.id as sp_id
                FROM commentor_scores cs
                JOIN commentors c ON c.id = cs.commentor_id
                LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
                LEFT JOIN secondary_profiles sp ON sp.commentor_id = c.id
                    AND sp.main_profile_id = ?
                WHERE cs.main_profile_id = ?
                ORDER BY cs.total_score DESC LIMIT 7
            """, (profile_id, profile_id))
        top7 = []
        for row in cur.fetchall():
            person = dict(row)

            # Step 1: Use commentor_country fields from JOIN (always available)
            person['city']      = person.pop('current_city', None) or '—'
            person['hometown']  = person.pop('hometown', None) or '—'
            person['work']      = person.pop('employer', None) or '—'
            person['education'] = person.pop('education', None) or '—'

            # Step 2: Override with secondary_profile_fields if available (medium/deep)
            if person.get('sp_id'):
                cur.execute("SELECT field_type, value FROM secondary_profile_fields WHERE secondary_profile_id=?", (person['sp_id'],))
                fields = {r[0]: r[1] for r in cur.fetchall()}
                if fields.get('current_city'): person['city']      = fields['current_city']
                if fields.get('work') or fields.get('employer'): person['work'] = fields.get('work') or fields.get('employer')
                if fields.get('college') or fields.get('education'): person['education'] = fields.get('college') or fields.get('education')
                if fields.get('hometown'): person['hometown'] = fields['hometown']

            top7.append(person)
    except Exception as e:
        print(f"Top7 error: {e}")
        top7 = []
    con.close()
    return top7

def get_post_activity(profile_id):
    con = db_connect()
    if not con: return []
    cur = con.cursor()
    rows = []
    try:
        cur.execute("""
            SELECT pp.date_text, 'photo' as type, pp.id,
                   COUNT(pc.id) as total,
                   SUM(CASE WHEN ca.sentiment='positive' THEN 1 ELSE 0 END) as positive,
                   SUM(CASE WHEN ca.sentiment='neutral' THEN 1 ELSE 0 END) as neutral,
                   SUM(CASE WHEN ca.sentiment='negative' THEN 1 ELSE 0 END) as negative
            FROM photo_posts pp
            LEFT JOIN photo_comments pc ON pc.photo_post_id = pp.id
            LEFT JOIN comment_analysis ca ON ca.comment_id = pc.id AND ca.db_source='photo'
            WHERE pp.profile_id = ?
            GROUP BY pp.id
            ORDER BY pp.date_text DESC
        """, (profile_id,))
        rows += [dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT tp.date_text, 'text' as type, tp.id,
                   COUNT(tc.id) as total,
                   SUM(CASE WHEN ca.sentiment='positive' THEN 1 ELSE 0 END) as positive,
                   SUM(CASE WHEN ca.sentiment='neutral' THEN 1 ELSE 0 END) as neutral,
                   SUM(CASE WHEN ca.sentiment='negative' THEN 1 ELSE 0 END) as negative
            FROM text_posts tp
            LEFT JOIN text_comments tc ON tc.text_post_id = tp.id
            LEFT JOIN comment_analysis ca ON ca.comment_id = tc.id AND ca.db_source='text'
            WHERE tp.profile_id = ?
            GROUP BY tp.id
            ORDER BY tp.date_text DESC
        """, (profile_id,))
        rows += [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"Post activity error: {e}")
    con.close()
    return sorted(rows, key=lambda x: x.get('date_text') or '', reverse=True)

def get_comment_intelligence(profile_id=None, batch_id=None):
    con = db_connect(manual=bool(batch_id))
    if not con: return {}
    cur = con.cursor()
    data = {'positive':0,'neutral':0,'negative':0,'total':0,'languages':{},'primary_language':'Unknown'}
    try:
        if batch_id:
            cur.execute("""
                SELECT ca.sentiment, ca.language, COUNT(*) as cnt
                FROM comment_analysis ca
                JOIN comments c ON c.id = ca.comment_id
                JOIN manual_posts mp ON mp.id = c.post_id
                WHERE mp.batch_id = ? AND ca.db_source = 'manual'
                GROUP BY ca.sentiment, ca.language
            """, (batch_id,))
        else:
            cur.execute("""
                SELECT ca.sentiment, ca.language, COUNT(*) as cnt
                FROM comment_analysis ca
                WHERE (
                    (ca.db_source = 'photo' AND ca.comment_id IN (
                        SELECT pc.id FROM photo_comments pc
                        JOIN photo_posts pp ON pp.id = pc.photo_post_id
                        WHERE pp.profile_id = ?
                    ))
                    OR
                    (ca.db_source = 'reel' AND ca.comment_id IN (
                        SELECT rc.id FROM reel_comments rc
                        JOIN reel_posts rp ON rp.id = rc.reel_post_id
                        WHERE rp.profile_id = ?
                    ))
                    OR
                    (ca.db_source = 'text' AND ca.comment_id IN (
                        SELECT tc.id FROM text_comments tc
                        JOIN text_posts tp ON tp.id = tc.text_post_id
                        WHERE tp.profile_id = ?
                    ))
                )
                GROUP BY ca.sentiment, ca.language
            """, (profile_id, profile_id, profile_id))
        for row in cur.fetchall():
            s, lang, cnt = row
            if s in data: data[s] += cnt
            data['total'] += cnt
            data['languages'][lang] = data['languages'].get(lang, 0) + cnt
        if data['languages']:
            data['primary_language'] = max(data['languages'], key=data['languages'].get)
    except Exception as e:
        print(f"Comment intel error: {e}")
    con.close()
    return data

def _resolve_face_url(rep):
    """Resolve a stored face path to a working /face_image/ URL."""
    if not rep:
        return None
    if os.path.exists(os.path.join(BASE_DIR, rep)):
        return f'/face_image/{rep}'
    alt = os.path.join(BASE_DIR, 'face_data', rep)
    if os.path.exists(alt):
        return f'/face_image/{rep}'
    return None

def get_faces(profile_id):
    con = db_connect()
    if not con: return {'clusters':[], 'stats':{}}
    cur = con.cursor()
    clusters = []
    stats = {'unique':0,'total':0,'repeat':0,'high_freq':0}
    try:
        # Get all clusters for this profile
        cur.execute("""
            SELECT DISTINCT fc.*
            FROM face_clusters fc
            JOIN detected_faces df ON df.person_id = fc.id
            JOIN photo_posts pp ON pp.id = df.photo_post_id
            WHERE pp.profile_id = ?
            ORDER BY fc.appearance_count DESC
        """, (profile_id,))
        raw = [dict(r) for r in cur.fetchall()]

        for c in raw:
            # Resolve representative face URL
            c['face_url'] = _resolve_face_url(c.get('representative_face') or '')

            # Fetch ALL individual detected face crops for this cluster
            cur.execute("""
                SELECT df.face_image_path, df.id as face_id,
                       pp.photo_url, pp.date_text
                FROM detected_faces df
                JOIN photo_posts pp ON pp.id = df.photo_post_id
                WHERE df.person_id = ?
                  AND pp.profile_id = ?
                ORDER BY df.id ASC
            """, (c['id'], profile_id))
            all_faces = []
            for row in cur.fetchall():
                r = dict(row)
                r['face_url'] = _resolve_face_url(r.get('face_image_path') or '')
                all_faces.append(r)
            c['all_faces'] = all_faces

            # Also scan face_data folder on disk as fallback
            # (in case detected_faces table paths are stale)
            if not any(f['face_url'] for f in all_faces):
                folder = None
                face_dir = os.path.join(BASE_DIR, 'face_data')
                person_label = c.get('person_label', '')
                if person_label and os.path.exists(face_dir):
                    for root_folder in os.listdir(face_dir):
                        p = os.path.join(face_dir, root_folder, 'persons', person_label)
                        if os.path.exists(p):
                            folder = p
                            break
                if folder:
                    disk_faces = []
                    for fname in sorted(os.listdir(folder)):
                        if fname.lower().endswith(('.jpg','.jpeg','.png')):
                            rel = os.path.relpath(
                                os.path.join(folder, fname), BASE_DIR
                            )
                            disk_faces.append({
                                'face_id': None,
                                'face_url': f'/face_image/{rel}',
                                'face_image_path': rel,
                                'photo_url': None,
                                'date_text': None,
                            })
                    if disk_faces:
                        c['all_faces'] = disk_faces
                        if not c['face_url']:
                            c['face_url'] = disk_faces[0]['face_url']

            clusters.append(c)

        stats['unique']    = len(clusters)
        stats['total']     = sum(len(c['all_faces']) for c in clusters)
        stats['repeat']    = sum(1 for c in clusters if c['appearance_count'] > 1)
        stats['high_freq'] = sum(1 for c in clusters if c['appearance_count'] >= 3)
    except Exception as e:
        print(f"Faces error: {e}")
    con.close()
    return {'clusters': clusters, 'stats': stats}

def get_image_analysis(profile_id):
    con = db_connect()
    if not con: return []
    cur = con.cursor()
    rows = []
    try:
        cur.execute("""
            SELECT pp.photo_url, pp.date_text, pp.image_src, pp.caption,
                   ia.scene_type, ia.activity, ia.estimated_location,
                   ia.political_symbols, ia.religious_symbols, ia.weapons_visible,
                   ia.text_in_image, ia.confidence
            FROM photo_posts pp
            LEFT JOIN image_analysis ia ON ia.photo_post_id = pp.id
            WHERE pp.profile_id = ?
            ORDER BY pp.date_text DESC
        """, (profile_id,))
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"Image analysis error: {e}")
    con.close()
    return rows

def get_text_post_analysis(profile_id):
    con = db_connect()
    if not con: return []
    cur = con.cursor()
    rows = []
    try:
        cur.execute("""
            SELECT tp.post_url, tp.date_text, tp.screenshot_path,
                   tpa.topic, tpa.sentiment, tpa.narrative_type,
                   tpa.key_entities, tpa.threat_indicators, tpa.text_language
            FROM text_posts tp
            LEFT JOIN text_post_analysis tpa ON tpa.text_post_id = tp.id
            WHERE tp.profile_id = ?
            ORDER BY tp.date_text DESC
        """, (profile_id,))
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"Text post analysis error: {e}")
    con.close()
    return rows

def get_report_file(prefix, name, ext, exact_only=False):
    """
    Find a report file. Tries exact name first.
    If exact_only=False, scans for any match with that prefix+ext.
    """
    if not os.path.exists(REPORTS_DIR):
        return None
    exact = os.path.join(REPORTS_DIR, f'{prefix}_{name}{ext}')
    if os.path.exists(exact):
        return exact
    if exact_only:
        return None
    # Fuzzy scan — only used for profile reports where name may vary
    for fname in os.listdir(REPORTS_DIR):
        if fname.startswith(f'{prefix}_') and fname.endswith(ext):
            return os.path.join(REPORTS_DIR, fname)
    return None

def get_network_graph_path(owner_name, batch_id=None):
    """Return path to top7 pyvis HTML file if it exists."""
    if batch_id:
        return get_report_file('top7', batch_id, '.html')
    name = (owner_name or 'Target').replace(' ', '_')[:30]
    return get_report_file('top7', name, '.html')

def get_batch_post_activity(batch_id):
    """Returns post list with comment counts and date for batch posts page."""
    con = db_connect(manual=True)
    if not con: return []
    cur = con.cursor()
    rows = []
    try:
        cur.execute("""
            SELECT mp.id, mp.url, mp.type, mp.date_text, mp.caption, mp.image_src,
                   COUNT(c.id) as comment_count,
                   SUM(CASE WHEN ca.sentiment='positive' THEN 1 ELSE 0 END) as positive,
                   SUM(CASE WHEN ca.sentiment='negative' THEN 1 ELSE 0 END) as negative,
                   SUM(CASE WHEN ca.sentiment='neutral'  THEN 1 ELSE 0 END) as neutral
            FROM manual_posts mp
            LEFT JOIN comments c  ON c.post_id = mp.id
            LEFT JOIN comment_analysis ca ON ca.comment_id = c.id
            WHERE mp.batch_id = ?
            GROUP BY mp.id
            ORDER BY mp.date_text DESC
        """, (batch_id,))
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"Batch post activity error: {e}")
    con.close()
    return rows

def get_batch_stats(batch_id):
    con = db_connect(manual=True)
    if not con: return {}
    cur = con.cursor()
    stats = {}
    try:
        cur.execute("SELECT COUNT(*) FROM manual_posts WHERE batch_id=?", (batch_id,))
        stats['posts_count'] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM comments c
            JOIN manual_posts mp ON mp.id = c.post_id
            WHERE mp.batch_id=?
        """, (batch_id,))
        stats['comments_count'] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(DISTINCT c.commentor_id) FROM comments c
            JOIN manual_posts mp ON mp.id = c.post_id
            WHERE mp.batch_id=?
        """, (batch_id,))
        stats['commentors_count'] = cur.fetchone()[0]
        stats['faces_count'] = 0
    except Exception as e:
        print(f"Batch stats error: {e}")
    con.close()
    return stats


#  PIPELINE RUNNER


SCAN_LIMITS = {
    'light':  {'photos': 5,  'reels': 5,  'posts': 5},
    'medium': {'photos': 10, 'reels': 10, 'posts': 10},
    'deep':   {'photos': 20, 'reels': 20, 'posts': 20},
}

def run_pipeline_auto(profile_url, profile_id, scan_type):
    """Run automated pipeline in background thread — no input() anywhere."""
    key    = f'profile_{profile_id}'
    limits = SCAN_LIMITS.get(scan_type, SCAN_LIMITS['medium'])
    model  = app.config.get('OLLAMA_MODEL', 'gemma3:4b')

    try:
        # Phase 1 — Data Gathering 
        write_status(key, {**read_status(key), 'phase': 'gathering'})

        sys.path.insert(0, BASE_DIR)

        import multiprocessing
        import fb_about_sb, fb_photos_sb, fb_reels_sb, fb_posts_sb

        p1 = multiprocessing.Process(target=fb_about_sb.main,  args=(profile_url,))
        p2 = multiprocessing.Process(target=fb_reels_sb.main,  args=(profile_url, limits['reels']))
        p3 = multiprocessing.Process(target=fb_photos_sb.main, args=(profile_url, limits['photos']))
        p4 = multiprocessing.Process(target=fb_posts_sb.main,  args=(profile_url, limits['posts']))

        for p in [p1, p2, p3, p4]: p.start()
        for p in [p1, p2, p3, p4]: p.join()

        write_status(key, {**read_status(key), 'gathering_done': True})

        # Phase 2 — DB Import 
        import socmint_db_import
        socmint_db_import.import_all(
            profile_json='fb_about.json',
            photos_json='fb_photos.json',
            reels_json='fb_reels.json',
            posts_json='fb_posts.json'
        )

        # Unlock stat numbers on dashboard immediately after DB import
        stats = get_profile_stats(profile_id)
        write_status(key, {
            **read_status(key),
            'numbers_ready':    True,
            'posts_count':      stats.get('posts_count', 0),
            'comments_count':   stats.get('comments_count', 0),
            'commentors_count': stats.get('commentors_count', 0),
            'faces_count':      0,
        })

        # Phase 3 — AI Analysis (inject correct model) 
        write_status(key, {**read_status(key), 'phase': 'analyzing'})

        import comment_intelligence_offline
        comment_intelligence_offline.OLLAMA_MODEL = model
        comment_intelligence_offline.analyze_comments(
            socmint_db_import.DB_FILE, label='socmint'
        )

        import image_intelligence
        image_intelligence.OLLAMA_MODEL = model
        image_intelligence.analyze_images(socmint_db_import.DB_FILE)

        import text_post_intelligence
        text_post_intelligence.OLLAMA_MODEL = model
        text_post_intelligence.analyze_text_posts(socmint_db_import.DB_FILE)

        import face_intelligence
        face_intelligence.analyze_faces(
            profile_url, db_file=socmint_db_import.DB_FILE, rerun=True
        )

        write_status(key, {**read_status(key), 'analysis_done': True})

        # Phase 4 — Intelligence 
        write_status(key, {**read_status(key), 'phase': 'building'})

        import commentor_scoring
        commentor_scoring.run_scoring(
            profile_url, db_file=socmint_db_import.DB_FILE
        )
        if scan_type in ('medium', 'deep'):
            commentor_scoring.scrape_top14_about(
                profile_url, db_file=socmint_db_import.DB_FILE
            )

        import commentor_country
        commentor_country.OLLAMA_MODEL = model
        commentor_country.run_for_profile(
            profile_url, db_file=socmint_db_import.DB_FILE
        )

        import network_graph
        network_graph.run_for_profile(
            profile_url, db_file=socmint_db_import.DB_FILE
        )

        # Update face count after full analysis
        stats2 = get_profile_stats(profile_id)
        write_status(key, {
            **read_status(key),
            'network_done': True,
            'faces_count':  stats2.get('faces_count', 0),
        })

        # Phase 5 — Report
        write_status(key, {**read_status(key), 'phase': 'report'})

        import threat_report
        threat_report.generate_report(profile_url=profile_url)

        write_status(key, {**read_status(key), 'report_done': True, 'phase': 'complete'})

    except Exception as e:
        write_status(key, {**read_status(key), 'error': str(e), 'phase': 'error'})
        print(f"[Pipeline Error] profile_{profile_id}: {e}")


def run_pipeline_manual(batch_id, label, urls, scan_type='medium'):
    """Run manual batch pipeline in background thread — no input() anywhere."""
    key   = f'batch_{batch_id}'
    model = app.config.get('OLLAMA_MODEL', 'gemma3:4b')

    try:
        # Phase 1 — Data Gathering 
        write_status(key, {**read_status(key), 'phase': 'gathering'})

        sys.path.insert(0, BASE_DIR)

        import fb_manual_unified_sb
        # Pass URLs directly — no input() blocking
        fb_manual_unified_sb.main(MAX_URLS=len(urls), urls=urls)

        write_status(key, {**read_status(key), 'gathering_done': True})

        # Phase 2 — DB Import
        import socmint_manual_db
        socmint_manual_db.import_manual(
            'fb_manual_scrape.json', batch_id=batch_id, label=label
        )

        stats = get_batch_stats(batch_id)
        write_status(key, {
            **read_status(key),
            'numbers_ready':    True,
            'posts_count':      stats.get('posts_count', 0),
            'comments_count':   stats.get('comments_count', 0),
            'commentors_count': stats.get('commentors_count', 0),
        })

        # Phase 3 — AI Analysis (inject correct model)
        write_status(key, {**read_status(key), 'phase': 'analyzing'})

        import comment_intelligence_offline
        comment_intelligence_offline.OLLAMA_MODEL = model
        comment_intelligence_offline.analyze_comments(
            socmint_manual_db.DB_FILE, label='manual', batch_id=batch_id
        )

        write_status(key, {**read_status(key), 'analysis_done': True})

        # Phase 4 — Intelligence
        write_status(key, {**read_status(key), 'phase': 'building'})

        import commentor_scoring
        commentor_scoring.run_batch_scoring(
            batch_id, db_file=socmint_manual_db.DB_FILE
        )
        # Enrich top14 for medium and deep scans
        if scan_type in ('medium', 'deep'):
            commentor_scoring.scrape_top14_about_batch(
                batch_id, db_file=socmint_manual_db.DB_FILE
            )

        import commentor_country
        commentor_country.OLLAMA_MODEL = model
        commentor_country.run_for_batch(
            batch_id, db_file=socmint_manual_db.DB_FILE
        )

        import network_graph
        network_graph.run_for_batch(
            batch_id, db_file=socmint_manual_db.DB_FILE
        )

        write_status(key, {**read_status(key), 'network_done': True})

        # Phase 5 — Report
        write_status(key, {**read_status(key), 'phase': 'report'})

        import threat_report
        threat_report.generate_report(batch_id=batch_id)

        write_status(key, {**read_status(key), 'report_done': True, 'phase': 'complete'})

    except Exception as e:
        write_status(key, {**read_status(key), 'error': str(e), 'phase': 'error'})
        print(f"[Pipeline Error] batch_{batch_id}: {e}")

#  ROUTES

@app.route('/')
def home():
    profiles = get_all_profiles()
    batches  = get_all_batches()
    system   = {
        'ram':         app.config.get('RAM_GB', 8),
        'model':       app.config.get('OLLAMA_MODEL', 'gemma3:4b'),
        'ollama_ok':   app.config.get('OLLAMA_OK', False),
        'cookies_ok':  app.config.get('COOKIES_OK', False),
        'model_pulled':app.config.get('MODEL_PULLED', False),
    }
    return render_template('home.html',
        profiles=profiles, batches=batches,
        system=system, available_models=AVAILABLE_MODELS)


@app.route('/api/change-model', methods=['POST'])
def change_model():
    """User selects a different Ollama model from home page."""
    data  = request.get_json() or {}
    model = data.get('model', '').strip()
    if not model:
        return jsonify({'ok': False, 'error': 'No model specified'})

    # Validate model is in our list
    all_models = [m[0] for group in AVAILABLE_MODELS.values() for m in group]
    if model not in all_models:
        return jsonify({'ok': False, 'error': 'Invalid model'})

    # Save to file immediately
    try:
        open(MODEL_FILE, 'w').write(model)
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

    # Update app config — pipeline will use this model
    app.config['OLLAMA_MODEL'] = model
    system_check()

    return jsonify({'ok': True, 'model': model})


@app.route('/api/pull-model')
def pull_model_stream():
    """SSE endpoint — streams ollama pull progress to browser."""
    model = request.args.get('model', '').strip()

    all_models = [m[0] for group in AVAILABLE_MODELS.values() for m in group]
    if not model or model not in all_models:
        def bad():
            yield 'data: {"status":"error","msg":"Invalid model"}\n\n'
        return Response(bad(), mimetype='text/event-stream')

    def generate():
        import json as _json
        import ollama as _ollama

        try:
            yield f"data: {_json.dumps({'status':'progress','msg':f'📥 Starting pull: {model}','pct':2})}\n\n"

            client = _ollama.Client(host=OLLAMA_HOST)
            last_pct = 2

            for progress in client.pull(model, stream=True):
                status_str = getattr(progress, 'status', '') or ''
                total      = getattr(progress, 'total', 0) or 0
                completed  = getattr(progress, 'completed', 0) or 0

                if 'manifest' in status_str:
                    yield f"data: {_json.dumps({'status':'progress','msg':'📋 Pulling manifest...','pct':5})}\n\n"

                elif 'download' in status_str or 'pulling' in status_str:
                    if total > 0:
                        pct = int((completed / total) * 85) + 5
                        mb  = completed // (1024 * 1024)
                        tmb = total // (1024 * 1024)
                        last_pct = pct
                        yield f"data: {_json.dumps({'status':'progress','msg':f'📥 Downloading {mb}MB / {tmb}MB','pct':pct})}\n\n"
                    else:
                        yield f"data: {_json.dumps({'status':'progress','msg':'📥 Downloading...','pct':last_pct})}\n\n"

                elif 'verif' in status_str:
                    yield f"data: {_json.dumps({'status':'progress','msg':'🔍 Verifying...','pct':92})}\n\n"

                elif 'writing' in status_str:
                    yield f"data: {_json.dumps({'status':'progress','msg':'✍️ Writing manifest...','pct':96})}\n\n"

                elif 'success' in status_str:
                    yield f"data: {_json.dumps({'status':'done','msg':f' {model} ready!','pct':100})}\n\n"
                    return

            # Stream ended without explicit success
            yield f"data: {_json.dumps({'status':'done','msg':f' {model} ready!','pct':100})}\n\n"

        except Exception as e:
            yield f"data: {_json.dumps({'status':'error','msg':f' {str(e)[:120]}'})}\n\n"

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/scan/new', methods=['GET', 'POST'])
def scan_new():
    if request.method == 'POST':
        profile_url = request.form.get('profile_url', '').strip()
        scan_type   = request.form.get('scan_type', 'medium')
        enrich      = request.form.get('enrich_top14', 'n')

        if not profile_url:
            return render_template('scan_new.html', error='Please enter a Facebook profile URL.')

        # Create profile record in DB
        # If DB doesn't exist yet, create schema first
        import socmint_db_import
        socmint_db_import.init_db(DB_FILE)
        con = db_connect()
        cur = con.cursor()
        try:
            cur.execute("INSERT OR IGNORE INTO profiles (profile_url) VALUES (?)", (profile_url,))
            con.commit()
            cur.execute("SELECT id FROM profiles WHERE profile_url=?", (profile_url,))
            profile_id = cur.fetchone()[0]
        except Exception as e:
            con.close()
            return render_template('scan_new.html', error=f'DB error: {e}')
        con.close()

        # Cookie check before launching pipeline ──
        cookies_ok, cookie_err = check_cookies_valid()
        if not cookies_ok:
            con.close() if hasattr(con, 'close') else None
            return render_template('scan_new.html',
                error=f' Session check failed: {cookie_err}. '
                      f'Go to System Tools → Refresh Session Cookies first.')

        # Init status
        init_status(f'profile_{profile_id}', profile_url=profile_url, scan_type=scan_type)

        # Launch pipeline in background
        t = threading.Thread(
            target=run_pipeline_auto,
            args=(profile_url, profile_id, scan_type),
            daemon=True
        )
        t.start()

        return redirect(url_for('dashboard_profile', profile_id=profile_id))

    return render_template('scan_new.html')

@app.route('/api/check-cookies')
def api_check_cookies():
    """Live cookie validity check — called from scan UI before user submits."""
    ok, reason = check_cookies_valid()
    return jsonify({'ok': ok, 'reason': reason or 'Session is active'})

@app.route('/api/session-screenshot')
def session_screenshot():
    path = os.path.join(STATUS_DIR, 'session_proof.png')
    if os.path.exists(path):
        return send_file(path, mimetype='image/png')
    return '', 404

@app.route('/batch/new', methods=['GET', 'POST'])
def batch_new():
    if request.method == 'POST':
        label    = request.form.get('label', '').strip()
        urls_raw = request.form.get('urls', '').strip()
        urls     = [u.strip() for u in urls_raw.splitlines() if u.strip()]

        if not urls:
            return render_template('scan_new.html', mode='batch', error='Please enter at least one URL.')

        batch_id = label.replace(' ', '_').lower() if label else f'batch_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        label    = label or batch_id

        # Create batch record
        import socmint_manual_db
        socmint_manual_db.init_db(MANUAL_DB_FILE)
        con = db_connect(manual=True)
        if not con:
            con = db_connect(manual=True)

        cur = con.cursor()
        try:
            cur.execute("INSERT OR IGNORE INTO batches (batch_id, label) VALUES (?,?)", (batch_id, label))
            con.commit()
        except Exception:
            pass
        con.close()

        scan_type = request.form.get('scan_type', 'medium')

        # Cookie check before launching pipeline ──
        cookies_ok, cookie_err = check_cookies_valid()
        if not cookies_ok:
            return render_template('scan_new.html', mode='batch',
                error=f' Session check failed: {cookie_err}. '
                      f'Go to System Tools → Refresh Session Cookies first.')

        init_status(f'batch_{batch_id}', batch_id=batch_id)
        # Store URLs in status so dashboard can display them
        write_status(f'batch_{batch_id}', {
            **read_status(f'batch_{batch_id}'),
            'urls': urls,
            'label': label,
        })

        t = threading.Thread(
            target=run_pipeline_manual,
            args=(batch_id, label, urls, scan_type),
            daemon=True
        )
        t.start()

        return redirect(url_for('dashboard_batch', batch_id=batch_id))

    return render_template('scan_new.html', mode='batch')


# DASHBOARDS ─────────────────────────────────────────────────────────────

@app.route('/profile/<int:profile_id>')
def dashboard_profile(profile_id):
    con = db_connect()
    if not con:
        return redirect(url_for('home'))
    cur = con.cursor()
    cur.execute("SELECT * FROM profiles WHERE id=?", (profile_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return redirect(url_for('home'))

    profile  = dict(row)
    status   = read_status(f'profile_{profile_id}') or {}
    model    = app.config.get('OLLAMA_MODEL', 'gemma3:4b')
    about    = get_profile_about(profile_id)

    # Get network graph path for iframe
    graph_path = get_network_graph_path(profile.get('owner_name'))
    graph_url  = f'/reports/top7/{profile_id}' if graph_path else None

    return render_template('dashboard_profile.html',
        profile=profile,
        about=about,
        status=status,
        model=model,
        graph_url=graph_url,
        profile_id=profile_id,
    )


@app.route('/batch/<batch_id>')
def dashboard_batch(batch_id):
    con = db_connect(manual=True)
    if not con:
        return redirect(url_for('home'))
    cur = con.cursor()
    cur.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return redirect(url_for('home'))

    batch    = dict(row)
    status   = read_status(f'batch_{batch_id}') or {}
    model    = app.config.get('OLLAMA_MODEL', 'gemma3:4b')
    graph_path = get_network_graph_path(None, batch_id=batch_id)
    graph_url  = f'/reports/top7_batch/{batch_id}' if graph_path else None

    return render_template('dashboard_batch.html',
        batch=batch,
        status=status,
        model=model,
        graph_url=graph_url,
        batch_id=batch_id,
    )


# DETAIL PAGES

@app.route('/profile/<int:profile_id>/posts')
def detail_posts(profile_id):
    about    = get_profile_about(profile_id)
    activity = get_post_activity(profile_id)
    images   = get_image_analysis(profile_id)
    texts    = get_text_post_analysis(profile_id)
    return render_template('detail_posts.html',
        profile=about, activity=activity, images=images, texts=texts, profile_id=profile_id)

@app.route('/profile/<int:profile_id>/interactions')
def detail_interactions(profile_id):
    about = get_profile_about(profile_id)
    intel = get_comment_intelligence(profile_id=profile_id)
    return render_template('detail_comments.html',
        profile=about, intel=intel, profile_id=profile_id)

@app.route('/batch/<batch_id>/posts')
def detail_batch_posts(batch_id):
    posts    = get_batch_post_activity(batch_id)
    activity = [{'date_text': p['date_text'], 'total': p['comment_count'],
                 'positive': p['positive'], 'negative': p['negative'],
                 'neutral': p['neutral'], 'type': p['type']} for p in posts]
    return render_template('detail_batch_posts.html',
        posts=posts, activity=activity, batch_id=batch_id)

@app.route('/batch/<batch_id>/interactions')
def detail_batch_interactions(batch_id):
    intel = get_comment_intelligence(batch_id=batch_id)
    return render_template('detail_comments.html',
        intel=intel, batch_id=batch_id)

@app.route('/profile/<int:profile_id>/faces')
def detail_faces(profile_id):
    about   = get_profile_about(profile_id)
    faces   = get_faces(profile_id)
    return render_template('detail_faces.html',
        profile=about, faces=faces, profile_id=profile_id)

@app.route('/profile/<int:profile_id>/network')
def detail_network(profile_id):
    about      = get_profile_about(profile_id)
    commentors = get_commentors(profile_id=profile_id)
    return render_template('detail_commentors.html',
        profile=about, commentors=commentors, profile_id=profile_id)

@app.route('/batch/<batch_id>/network')
def detail_batch_network(batch_id):
    commentors = get_commentors(batch_id=batch_id)
    return render_template('detail_commentors.html',
        commentors=commentors, batch_id=batch_id)


# API ENDPOINTS

@app.route('/api/status/profile/<int:profile_id>')
def api_status_profile(profile_id):
    status = read_status(f'profile_{profile_id}') or {'phase': 'unknown'}

    # Auto-detect completion from DB + reports dir when status.json is missing/stale
    con = db_connect()
    if con:
        cur = con.cursor()
        cur.execute("SELECT owner_name FROM profiles WHERE id=?", (profile_id,))
        row = cur.fetchone()
        if row:
            name = (row[0] or 'Unknown').replace(' ', '_')[:30]

            # Check if data exists in DB tables
            try:
                cur.execute("SELECT COUNT(*) FROM photo_posts WHERE profile_id=?", (profile_id,))
                has_posts = cur.fetchone()[0] > 0
                cur.execute("SELECT COUNT(*) FROM commentor_scores WHERE main_profile_id=?", (profile_id,))
                has_scores = cur.fetchone()[0] > 0
                cur.execute("SELECT COUNT(*) FROM commentor_country WHERE commentor_id IN (SELECT DISTINCT commentor_id FROM photo_comments pc JOIN photo_posts pp ON pp.id=pc.photo_post_id WHERE pp.profile_id=?)", (profile_id,))
                has_country = cur.fetchone()[0] > 0
                cur.execute("""
                    SELECT COUNT(*) FROM comment_analysis ca
                    JOIN photo_comments pc ON pc.id=ca.comment_id AND ca.db_source='photo'
                    JOIN photo_posts pp ON pp.id=pc.photo_post_id
                    WHERE pp.profile_id=?
                """, (profile_id,))
                has_analysis = cur.fetchone()[0] > 0
                if not has_analysis:
                    # Try reel comments
                    cur.execute("""
                        SELECT COUNT(*) FROM comment_analysis ca
                        JOIN reel_comments rc ON rc.id=ca.comment_id AND ca.db_source='reel'
                        JOIN reel_posts rp ON rp.id=rc.reel_post_id
                        WHERE rp.profile_id=?
                    """, (profile_id,))
                    has_analysis = cur.fetchone()[0] > 0
            except Exception:
                has_posts = has_scores = has_country = has_analysis = False

            # Set flags based on actual data presence
            if has_posts and not status.get('numbers_ready'):
                status['numbers_ready'] = True
            if has_analysis and not status.get('analysis_done'):
                status['analysis_done'] = True
            if has_scores and not status.get('network_done'):
                status['network_done'] = True

            # Check report file
            has_pdf = bool(get_report_file('report', name, '.pdf'))
            if has_pdf:
                status['report_done'] = True
                status['phase'] = 'complete'
                status['numbers_ready'] = True
                status['analysis_done'] = True
                status['network_done']  = True

            # Save updated status
            if any([has_posts, has_scores, has_country, has_pdf]):
                write_status(f'profile_{profile_id}', status)
        con.close()

    return jsonify(status)

@app.route('/api/status/batch/<batch_id>')
def api_status_batch(batch_id):
    status = read_status(f'batch_{batch_id}') or {'phase': 'unknown'}

    # Auto-detect from DB — only upgrade flags, never downgrade
    # (don't overwrite flags already set by pipeline)
    con = db_connect(manual=True)
    if con:
        cur = con.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM manual_posts WHERE batch_id=?", (batch_id,))
            has_posts = cur.fetchone()[0] > 0
            cur.execute("SELECT COUNT(*) FROM batch_commentor_scores WHERE batch_id=?", (batch_id,))
            has_scores = cur.fetchone()[0] > 0
            try:
                cur.execute("""SELECT COUNT(*) FROM comment_analysis ca
                    JOIN comments c ON c.id=ca.comment_id
                    JOIN manual_posts mp ON mp.id=c.post_id
                    WHERE mp.batch_id=?""", (batch_id,))
                has_analysis = cur.fetchone()[0] > 0
            except Exception:
                has_analysis = False
        except Exception:
            has_posts = has_scores = has_analysis = False
        con.close()

        changed = False
        if has_posts  and not status.get('numbers_ready'): status['numbers_ready'] = True; changed = True
        if has_analysis and not status.get('analysis_done'): status['analysis_done'] = True; changed = True
        if has_scores and not status.get('network_done'):  status['network_done']  = True; changed = True

        safe_bid = batch_id.replace(' ','_')[:30]
        has_pdf = bool(get_report_file('report', safe_bid, '.pdf', exact_only=True))
        if has_pdf and not status.get('report_done'):
            status.update({'report_done':True,'phase':'complete',
                           'numbers_ready':True,'analysis_done':True,'network_done':True})
            changed = True

        if changed:
            write_status(f'batch_{batch_id}', status)

    return jsonify(status)

@app.route('/api/data/profile/<int:profile_id>')
def api_data_profile(profile_id):
    """Return all dashboard data as JSON for live updates."""
    stats      = get_profile_stats(profile_id)
    countries  = get_country_distribution(profile_id=profile_id)
    tiers      = get_tier_distribution(profile_id=profile_id)
    top7       = get_top7(profile_id=profile_id)
    commentors = get_commentors(profile_id=profile_id)
    about      = get_profile_about(profile_id)
    activity   = get_post_activity(profile_id)

    return jsonify({
        'stats':      stats,
        'countries':  countries,
        'tiers':      tiers,
        'top7':       top7,
        'commentors': commentors[:50],  # first 50 for dashboard
        'about':      about,
        'activity':   activity[:20],
    })

@app.route('/api/data/batch/<batch_id>')
def api_data_batch(batch_id):
    stats      = get_batch_stats(batch_id)
    countries  = get_country_distribution(batch_id=batch_id)
    tiers      = get_tier_distribution(batch_id=batch_id)
    top7       = get_top7(batch_id=batch_id)
    commentors = get_commentors(batch_id=batch_id)

    # Get batch info for dashboard card
    batch_info = {}
    con = db_connect(manual=True)
    if con:
        cur = con.cursor()
        try:
            cur.execute("SELECT batch_id, label, created_at FROM batches WHERE batch_id=?", (batch_id,))
            row = cur.fetchone()
            if row: batch_info = dict(row)
        except Exception: pass
        con.close()

    # Get activity (comment counts per post date)
    activity = []
    con2 = db_connect(manual=True)
    if con2:
        cur2 = con2.cursor()
        try:
            cur2.execute("""
                SELECT mp.date_text, COUNT(cm.id) as total
                FROM manual_posts mp
                LEFT JOIN comments cm ON cm.post_id = mp.id
                WHERE mp.batch_id=?
                GROUP BY mp.date_text ORDER BY mp.date_text
            """, (batch_id,))
            activity = [dict(r) for r in cur2.fetchall()]
        except Exception: pass
        con2.close()

    # analyzed count
    stats['analyzed_count'] = sum(tiers.values()) if tiers else 0

    return jsonify({
        'stats':      stats,
        'batch':      batch_info,
        'countries':  countries,
        'tiers':      tiers,
        'top7':       top7,
        'commentors': commentors[:50],
        'activity':   activity,
    })

#Detail commentors page graph
@app.route('/api/comment-graph/profile/<int:profile_id>')
def api_comment_graph_profile(profile_id):
    """
    Returns all commentors with their individual comments,
    sentiment, and post URLs for the network graph view.
    """
    con = db_connect()
    if not con:
        return jsonify({'commentors': []})
 
    cur = con.cursor()
    results = []
 
    try:
        # Get all commentors for this profile with scores
        cur.execute("""
            SELECT DISTINCT c.id as commentor_id, c.name, c.profile_url,
                   COALESCE(cs.total_score, 0) as total_score,
                   COALESCE(cs.tier, 'Unknown') as tier,
                   COALESCE(cs.comment_count, 0) as comment_count,
                   cc.identified_country
            FROM commentors c
            JOIN (
                SELECT DISTINCT commentor_id FROM photo_comments pc
                JOIN photo_posts pp ON pp.id = pc.photo_post_id
                WHERE pp.profile_id = ?
                UNION
                SELECT DISTINCT commentor_id FROM reel_comments rc
                JOIN reel_posts rp ON rp.id = rc.reel_post_id
                WHERE rp.profile_id = ?
                UNION
                SELECT DISTINCT commentor_id FROM text_comments tc
                JOIN text_posts tp ON tp.id = tc.text_post_id
                WHERE tp.profile_id = ?
            ) mine ON mine.commentor_id = c.id
            LEFT JOIN commentor_scores cs ON cs.commentor_id = c.id
                AND cs.main_profile_id = ?
            LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
            ORDER BY total_score DESC
        """, (profile_id, profile_id, profile_id, profile_id))
 
        commentors = [dict(r) for r in cur.fetchall()]
 
        for cm in commentors:
            cid = cm['commentor_id']
            comments = []
 
            # Photo comments
            try:
                cur.execute("""
                    SELECT pc.comment_text, pp.photo_url as post_url,
                           ca.sentiment, ca.emotion, ca.stance,
                           'photo' as db_source
                    FROM photo_comments pc
                    JOIN photo_posts pp ON pp.id = pc.photo_post_id
                    LEFT JOIN comment_analysis ca ON ca.comment_id = pc.id
                        AND ca.db_source = 'photo'
                    WHERE pc.commentor_id = ? AND pp.profile_id = ?
                """, (cid, profile_id))
                comments += [dict(r) for r in cur.fetchall()]
            except Exception:
                pass
 
            # Reel comments
            try:
                cur.execute("""
                    SELECT rc.comment_text, rp.reel_url as post_url,
                           ca.sentiment, ca.emotion, ca.stance,
                           'reel' as db_source
                    FROM reel_comments rc
                    JOIN reel_posts rp ON rp.id = rc.reel_post_id
                    LEFT JOIN comment_analysis ca ON ca.comment_id = rc.id
                        AND ca.db_source = 'reel'
                    WHERE rc.commentor_id = ? AND rp.profile_id = ?
                """, (cid, profile_id))
                comments += [dict(r) for r in cur.fetchall()]
            except Exception:
                pass
 
            # Text comments
            try:
                cur.execute("""
                    SELECT tc.comment_text, tp.post_url,
                           ca.sentiment, ca.emotion, ca.stance,
                           'text' as db_source
                    FROM text_comments tc
                    JOIN text_posts tp ON tp.id = tc.text_post_id
                    LEFT JOIN comment_analysis ca ON ca.comment_id = tc.id
                        AND ca.db_source = 'text'
                    WHERE tc.commentor_id = ? AND tp.profile_id = ?
                """, (cid, profile_id))
                comments += [dict(r) for r in cur.fetchall()]
            except Exception:
                pass
 
            cm['comments'] = comments
            results.append(cm)
 
    except Exception as e:
        print(f"Comment graph error: {e}")
    finally:
        con.close()
 
    return jsonify({'commentors': results})
 
 
@app.route('/api/comment-graph/batch/<batch_id>')
def api_comment_graph_batch(batch_id):
    """Comment graph data for manual batch."""
    con = db_connect(manual=True)
    if not con:
        return jsonify({'commentors': []})
 
    cur = con.cursor()
    results = []
 
    try:
        cur.execute("""
            SELECT DISTINCT c.id as commentor_id, c.name, c.profile_url,
                   COALESCE(bcs.total_score, 0) as total_score,
                   COALESCE(bcs.tier, 'Unknown') as tier,
                   COALESCE(bcs.comment_count, 0) as comment_count,
                   cc.identified_country
            FROM commentors c
            JOIN comments cm ON cm.commentor_id = c.id
            JOIN manual_posts mp ON mp.id = cm.post_id AND mp.batch_id = ?
            LEFT JOIN batch_commentor_scores bcs ON bcs.commentor_id = c.id
                AND bcs.batch_id = ?
            LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
            ORDER BY total_score DESC
        """, (batch_id, batch_id))
 
        commentors = [dict(r) for r in cur.fetchall()]
 
        for cm in commentors:
            cid = cm['commentor_id']
            try:
                cur.execute("""
                    SELECT c.comment_text, mp.url as post_url,
                           ca.sentiment, ca.emotion, ca.stance,
                           'manual' as db_source
                    FROM comments c
                    JOIN manual_posts mp ON mp.id = c.post_id
                    LEFT JOIN comment_analysis ca ON ca.comment_id = c.id
                        AND ca.db_source = 'manual'
                    WHERE c.commentor_id = ? AND mp.batch_id = ?
                """, (cid, batch_id))
                cm['comments'] = [dict(r) for r in cur.fetchall()]
            except Exception:
                cm['comments'] = []
            results.append(cm)
 
    except Exception as e:
        print(f"Batch comment graph error: {e}")
    finally:
        con.close()
 
    return jsonify({'commentors': results})


#CO-COMMENTOR graph
@app.route('/api/cocomment-graph/profile/<int:profile_id>')
def api_cocomment_graph_profile(profile_id):
    """
    Returns co-commentor pairs — people who commented on same posts.
    Edge weight = number of shared posts across photo + reel + text.
    """
    con = db_connect()
    if not con:
        return jsonify({'nodes': [], 'edges': []})
 
    cur = con.cursor()
 
    try:
        # Get all commentors with scores
        cur.execute("""
            SELECT DISTINCT c.id as commentor_id, c.name, c.profile_url,
                   COALESCE(cs.total_score, 0) as total_score,
                   COALESCE(cs.tier, 'Unknown') as tier,
                   COALESCE(cs.comment_count, 0) as comment_count,
                   cc.identified_country
            FROM commentors c
            JOIN (
                SELECT DISTINCT commentor_id FROM photo_comments pc
                JOIN photo_posts pp ON pp.id = pc.photo_post_id
                WHERE pp.profile_id = ?
                UNION
                SELECT DISTINCT commentor_id FROM reel_comments rc
                JOIN reel_posts rp ON rp.id = rc.reel_post_id
                WHERE rp.profile_id = ?
                UNION
                SELECT DISTINCT commentor_id FROM text_comments tc
                JOIN text_posts tp ON tp.id = tc.text_post_id
                WHERE tp.profile_id = ?
            ) mine ON mine.commentor_id = c.id
            LEFT JOIN commentor_scores cs ON cs.commentor_id = c.id
                AND cs.main_profile_id = ?
            LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
            ORDER BY total_score DESC
        """, (profile_id, profile_id, profile_id, profile_id))
 
        commentors = [dict(r) for r in cur.fetchall()]
 
        # Find co-commentor pairs across all 3 post types
        edge_map = {}  # (id1, id2) -> shared count
 
        # Photo post pairs
        try:
            cur.execute("""
                SELECT pc1.commentor_id as c1, pc2.commentor_id as c2, COUNT(*) as shared
                FROM photo_comments pc1
                JOIN photo_comments pc2
                    ON pc1.photo_post_id = pc2.photo_post_id
                    AND pc1.commentor_id < pc2.commentor_id
                JOIN photo_posts pp ON pp.id = pc1.photo_post_id
                WHERE pp.profile_id = ?
                GROUP BY pc1.commentor_id, pc2.commentor_id
            """, (profile_id,))
            for row in cur.fetchall():
                key = (row[0], row[1])
                edge_map[key] = edge_map.get(key, 0) + row[2]
        except Exception as e:
            print(f"  Photo cocomment error: {e}")
 
        # Reel post pairs
        try:
            cur.execute("""
                SELECT rc1.commentor_id as c1, rc2.commentor_id as c2, COUNT(*) as shared
                FROM reel_comments rc1
                JOIN reel_comments rc2
                    ON rc1.reel_post_id = rc2.reel_post_id
                    AND rc1.commentor_id < rc2.commentor_id
                JOIN reel_posts rp ON rp.id = rc1.reel_post_id
                WHERE rp.profile_id = ?
                GROUP BY rc1.commentor_id, rc2.commentor_id
            """, (profile_id,))
            for row in cur.fetchall():
                key = (row[0], row[1])
                edge_map[key] = edge_map.get(key, 0) + row[2]
        except Exception as e:
            print(f"  Reel cocomment error: {e}")
 
        # Text post pairs
        try:
            cur.execute("""
                SELECT tc1.commentor_id as c1, tc2.commentor_id as c2, COUNT(*) as shared
                FROM text_comments tc1
                JOIN text_comments tc2
                    ON tc1.text_post_id = tc2.text_post_id
                    AND tc1.commentor_id < tc2.commentor_id
                JOIN text_posts tp ON tp.id = tc1.text_post_id
                WHERE tp.profile_id = ?
                GROUP BY tc1.commentor_id, tc2.commentor_id
            """, (profile_id,))
            for row in cur.fetchall():
                key = (row[0], row[1])
                edge_map[key] = edge_map.get(key, 0) + row[2]
        except Exception as e:
            print(f"  Text cocomment error: {e}")
 
        # Build edges list
        edges = [
            {'source': k[0], 'target': k[1], 'weight': v}
            for k, v in edge_map.items()
        ]
 
        return jsonify({'nodes': commentors, 'edges': edges})
 
    except Exception as e:
        print(f"Co-commentor graph error: {e}")
        return jsonify({'nodes': [], 'edges': []})
    finally:
        con.close()
 
 
@app.route('/api/cocomment-graph/batch/<batch_id>')
def api_cocomment_graph_batch(batch_id):
    """Co-commentor graph for manual batch."""
    con = db_connect(manual=True)
    if not con:
        return jsonify({'nodes': [], 'edges': []})
 
    cur = con.cursor()
 
    try:
        # Get commentors
        cur.execute("""
            SELECT DISTINCT c.id as commentor_id, c.name, c.profile_url,
                   COALESCE(bcs.total_score, 0) as total_score,
                   COALESCE(bcs.tier, 'Unknown') as tier,
                   COALESCE(bcs.comment_count, 0) as comment_count,
                   cc.identified_country
            FROM commentors c
            JOIN comments cm ON cm.commentor_id = c.id
            JOIN manual_posts mp ON mp.id = cm.post_id AND mp.batch_id = ?
            LEFT JOIN batch_commentor_scores bcs ON bcs.commentor_id = c.id
                AND bcs.batch_id = ?
            LEFT JOIN commentor_country cc ON cc.commentor_id = c.id
            ORDER BY total_score DESC
        """, (batch_id, batch_id))
 
        commentors = [dict(r) for r in cur.fetchall()]
 
        # Find pairs
        edge_map = {}
        try:
            cur.execute("""
                SELECT c1.commentor_id, c2.commentor_id, COUNT(*) as shared
                FROM comments c1
                JOIN comments c2
                    ON c1.post_id = c2.post_id
                    AND c1.commentor_id < c2.commentor_id
                JOIN manual_posts mp ON mp.id = c1.post_id
                WHERE mp.batch_id = ?
                GROUP BY c1.commentor_id, c2.commentor_id
            """, (batch_id,))
            for row in cur.fetchall():
                key = (row[0], row[1])
                edge_map[key] = edge_map.get(key, 0) + row[2]
        except Exception as e:
            print(f"  Batch cocomment error: {e}")
 
        edges = [
            {'source': k[0], 'target': k[1], 'weight': v}
            for k, v in edge_map.items()
        ]
 
        return jsonify({'nodes': commentors, 'edges': edges})
 
    except Exception as e:
        print(f"Batch co-commentor graph error: {e}")
        return jsonify({'nodes': [], 'edges': []})
    finally:
        con.close()
 
# SERVE NETWORK GRAPH

@app.route('/reports/top7/<int:profile_id>')
def serve_top7_profile(profile_id):
    con = db_connect()
    if not con: return 'Not found', 404
    cur = con.cursor()
    cur.execute("SELECT owner_name FROM profiles WHERE id=?", (profile_id,))
    row = cur.fetchone()
    con.close()
    if not row: return 'Not found', 404
    name = (row[0] or 'Target').replace(' ', '_')[:30]
    path = get_report_file('top7', name, '.html')
    if not path: return 'Graph not ready yet', 404
    with open(path, 'r', encoding='utf-8') as f:
        return Response(f.read(), mimetype='text/html')

@app.route('/reports/top7_batch/<batch_id>')
def serve_top7_batch(batch_id):
    path = os.path.join(REPORTS_DIR, f'top7_{batch_id}.html')
    if not os.path.exists(path): return 'Graph not ready yet', 404
    with open(path, 'r', encoding='utf-8') as f:
        return Response(f.read(), mimetype='text/html')

@app.route('/reports/pdf/<int:profile_id>')
def serve_pdf_profile(profile_id):
    from flask import send_file
    con = db_connect()
    if not con: return 'Not found', 404
    cur = con.cursor()
    cur.execute("SELECT owner_name FROM profiles WHERE id=?", (profile_id,))
    row = cur.fetchone()
    con.close()
    if not row: return 'Not found', 404
    raw_name = (row[0] or 'Unknown')
    name = raw_name.replace(' ', '_')[:30]
    path = get_report_file('report', name, '.pdf')
    if not path: return 'Report not ready yet', 404
    # Sanitize download_name: remove all non-alphanumeric except underscore/hyphen/dot
    safe_dl = re.sub(r'[^a-zA-Z0-9_\-]', '', name) + '.pdf'
    return send_file(path, as_attachment=True, download_name=safe_dl)

@app.route('/reports/pdf/batch/<batch_id>')
def serve_pdf_batch(batch_id):
    from flask import send_file
    safe_name = batch_id.replace(' ','_')[:30]
    path = get_report_file('report', safe_name, '.pdf', exact_only=True)
    if not path: return 'Report not ready yet', 404
    safe_dl = re.sub(r'[^a-zA-Z0-9_\-]', '', safe_name) + '.pdf'
    return send_file(path, as_attachment=True, download_name=safe_dl)

# Serve face images
# face_intelligence.py stores paths RELATIVE to CWD (socmint/ dir):
#   representative_face = "face_data/{name_safe}/persons/person_N/face_N.jpg"
# So correct full path = BASE_DIR + "/" + filepath
@app.route('/face_image/<path:filepath>')
def serve_face_image(filepath):
    from flask import send_file
    import urllib.parse

    # Decode URL-encoded characters
    filepath = urllib.parse.unquote(filepath)

    # Normalize non-breaking spaces and other unicode spaces to regular space
    filepath = filepath.replace('\xa0', ' ').replace('\u202f', ' ').replace('\u2009', ' ')
    base_real = os.path.realpath(BASE_DIR)

    def _safe_send(path):
        """Return send_file only if path stays within BASE_DIR."""
        real = os.path.realpath(path)
        if real.startswith(base_real + os.sep) or real == base_real:
            return send_file(real)
        return None

    # Try 1: direct path from BASE_DIR
    full = os.path.join(BASE_DIR, filepath)
    resp = _safe_send(full)
    if resp and os.path.exists(os.path.realpath(full)):
        return resp

    # Try 2: double face_data prefix
    if filepath.startswith('face_data/face_data/'):
        full2 = os.path.join(BASE_DIR, filepath.replace('face_data/face_data/', 'face_data/'))
        resp = _safe_send(full2)
        if resp and os.path.exists(os.path.realpath(full2)):
            return resp

    # Try 3: the owner_name folder may have spaces/parens — scan all subdirs
    parts = filepath.replace('\\', '/').split('/')
    if len(parts) >= 4 and parts[0] == 'face_data':
        face_dir = os.path.join(BASE_DIR, 'face_data')
        fname_tail = '/'.join(parts[2:])  # persons/person_N/face_N.jpg
        if os.path.exists(face_dir):
            for folder in os.listdir(face_dir):
                candidate = os.path.join(face_dir, folder, fname_tail)
                resp = _safe_send(candidate)
                if resp and os.path.exists(os.path.realpath(candidate)):
                    return resp

    return 'Not found', 404

# Serve post screenshots
@app.route('/screenshot/<path:filepath>')
def serve_screenshot(filepath):
    from flask import send_file
    screenshots_dir = os.path.realpath(os.path.join(BASE_DIR, 'post_screenshots'))
    full_path = os.path.realpath(os.path.join(screenshots_dir, filepath))
    if not full_path.startswith(screenshots_dir + os.sep):
        return 'Forbidden', 403
    if not os.path.exists(full_path):
        return 'Not found', 404
    return send_file(full_path)

# Serve logo
@app.route('/logo')
def serve_logo():
    from flask import send_file
    path = os.path.join(ICONS_DIR, 'logo.jpeg')
    if os.path.exists(path):
        return send_file(path)
    return 'Not found', 404

@app.route('/icons/<filename>')
def serve_icon(filename):
    from flask import send_file
    path = os.path.join(ICONS_DIR, filename)
    if os.path.exists(path):
        return send_file(path)
    return 'Not found', 404

#  COOKIE REFRESH

cookie_refresh_status = {'running': False, 'done': False, 'error': None, 'seconds_left': 0}

@app.route('/tools/refresh-cookies', methods=['GET', 'POST'])
def refresh_cookies():
    return render_template('tool_cookies.html',
        status=cookie_refresh_status,
        cookies_ok=app.config.get('COOKIES_OK', False))

@app.route('/api/tools/refresh-cookies/start', methods=['POST'])
def start_cookie_refresh():
    global cookie_refresh_status
    if cookie_refresh_status['running']:
        return jsonify({'ok': False, 'msg': 'Already running'})
    cookie_refresh_status = {'running': True, 'done': False, 'error': None, 'seconds_left': 60}

    def _run():
        global cookie_refresh_status
        try:
            import pickle, subprocess, time as _time
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            options = Options()
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            driver = webdriver.Chrome(options=options)
            driver.get("https://www.facebook.com")
            for i in range(60, 0, -1):
                cookie_refresh_status['seconds_left'] = i
                _time.sleep(1)
            pickle.dump(driver.get_cookies(),
                open(os.path.join(BASE_DIR, 'fb_cookies.pkl'), 'wb'))
            driver.quit()
            app.config['COOKIES_OK'] = True
            cookie_refresh_status.update({'running': False, 'done': True, 'seconds_left': 0})
        except Exception as e:
            cookie_refresh_status.update({'running': False, 'done': False,
                                          'error': str(e), 'seconds_left': 0})

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/tools/refresh-cookies/status')
def cookie_refresh_status_api():
    return jsonify(cookie_refresh_status)


@app.route('/tools/import-cookies', methods=['GET'])
def import_cookies():
    """Cookie-Editor import page."""
    return render_template('tool_import_cookies.html',
        cookies_ok=app.config.get('COOKIES_OK', False))

@app.route('/api/tools/import-cookies', methods=['POST'])
def api_import_cookies():
    """
    Receive cookie JSON from Cookie-Editor extension,
    convert to pickle format and save as fb_cookies.pkl.
    """
    import json as _json
    import pickle
 
    try:
        data = request.get_json()
        if not data:
            return jsonify({'ok': False, 'error': 'No data received'})
 
        cookies_json = data.get('cookies', '')
        if not cookies_json:
            return jsonify({'ok': False, 'error': 'No cookies provided'})
 
        # Parse JSON — Cookie-Editor exports as array
        if isinstance(cookies_json, str):
            cookies = _json.loads(cookies_json)
        else:
            cookies = cookies_json  # already parsed
 
        if not isinstance(cookies, list):
            return jsonify({'ok': False, 'error': 'Invalid format — expected JSON array'})
 
        if len(cookies) == 0:
            return jsonify({'ok': False, 'error': 'Cookie array is empty'})
 
        # Validate — must contain c_user (core FB session cookie)
        names = [c.get('name', '') for c in cookies]
        if 'c_user' not in names:
            return jsonify({
                'ok': False,
                'error': 'Missing c_user cookie — make sure you are logged into Facebook before exporting'
            })
 
        # Convert Cookie-Editor format to Selenium format
        # Cookie-Editor uses: name, value, domain, path, expires, httpOnly, secure, sameSite
        # Selenium uses:      name, value, domain, path, expiry, httpOnly, secure
        converted = []
        for c in cookies:
            selenium_cookie = {
                'name':     c.get('name', ''),
                'value':    c.get('value', ''),
                'domain':   c.get('domain', '.facebook.com'),
                'path':     c.get('path', '/'),
                'httpOnly': c.get('httpOnly', False),
                'secure':   c.get('secure', False),
            }
            # expiry field
            expires = c.get('expirationDate') or c.get('expires') or c.get('expiry')
            if expires and isinstance(expires, (int, float)):
                selenium_cookie['expiry'] = int(expires)
 
            # sameSite
            same_site = c.get('sameSite', 'None')
            if same_site in ('Strict', 'Lax', 'None'):
                selenium_cookie['sameSite'] = same_site
 
            converted.append(selenium_cookie)
 
        # Save to fb_cookies.pkl
        cookie_path = os.path.join(BASE_DIR, 'fb_cookies.pkl')
        pickle.dump(converted, open(cookie_path, 'wb'))
 
        # Update app config
        app.config['COOKIES_OK'] = True
 
        return jsonify({
            'ok': True,
            'count': len(converted),
            'message': f'{len(converted)} cookies imported successfully'
        })
 
    except _json.JSONDecodeError as e:
        return jsonify({'ok': False, 'error': f'Invalid JSON: {str(e)}'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

#  DB CLEANER

SOCMINT_TABLES = [
    "secondary_profile_fields","secondary_profiles","commentor_scores",
    "commentor_country","comment_analysis","image_analysis","text_post_analysis",
    "detected_faces","face_clusters","photo_comments","reel_comments",
    "text_comments","photo_posts","reel_posts","text_posts",
    "profile_fields","profiles","commentors",
]
MANUAL_TABLES = [
    "secondary_profile_fields","secondary_profiles","batch_commentor_scores",
    "commentor_country","comment_analysis","comments",
    "manual_posts","batches","commentors",
]

def _wipe_db(db_path, tables):
    if not os.path.exists(db_path):
        return 0
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = OFF")
    total = 0
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            n = cur.fetchone()[0]
            cur.execute(f"DELETE FROM {t}")
            total += n
        except Exception:
            pass
    cur.execute("PRAGMA foreign_keys = ON")
    try: cur.execute("DELETE FROM sqlite_sequence")
    except Exception: pass
    con.commit()
    con.execute("VACUUM")
    con.close()
    return total

def _wipe_faces():
    """Delete face_data contents — preserves mount point for Docker compatibility."""
    import shutil
    face_dir = os.path.join(BASE_DIR, 'face_data')
    if os.path.exists(face_dir):
        for item in os.listdir(face_dir):
            item_path = os.path.join(face_dir, item)
            try:
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            except Exception as e:
                print(f"   Could not delete {item_path}: {e}")
        return True
    return False

@app.route('/tools/db-cleaner')
def db_cleaner():
    # Get DB sizes
    def db_size(path):
        if os.path.exists(path):
            return round(os.path.getsize(path) / 1024 / 1024, 2)
        return 0
    info = {
        'socmint_size':  db_size(DB_FILE),
        'manual_size':   db_size(MANUAL_DB_FILE),
        'profiles':      len(get_all_profiles()),
        'batches':       len(get_all_batches()),
        'faces_exist':   os.path.exists(os.path.join(BASE_DIR, 'face_data')),
    }
    return render_template('tool_dbcleaner.html', info=info)

@app.route('/api/tools/clean-db', methods=['POST'])
def api_clean_db():
    target = request.json.get('target', 'both')
    results = {}
    if target in ('both', 'profile'):
        results['profile_rows'] = _wipe_db(DB_FILE, SOCMINT_TABLES)
        _wipe_faces()
        results['faces_wiped'] = True
        # Delete status files for profiles
        for f in os.listdir(STATUS_DIR):
            if f.startswith('profile_'):
                try: os.remove(os.path.join(STATUS_DIR, f))
                except: pass
    if target in ('both', 'manual'):
        results['manual_rows'] = _wipe_db(MANUAL_DB_FILE, MANUAL_TABLES)
        for f in os.listdir(STATUS_DIR):
            if f.startswith('batch_'):
                try: os.remove(os.path.join(STATUS_DIR, f))
                except: pass
    return jsonify({'ok': True, 'results': results})

#  DELETE SINGLE INVESTIGATION

@app.route('/api/delete/profile/<int:profile_id>', methods=['POST'])
def delete_profile(profile_id):
    con = db_connect()
    if not con: return jsonify({'ok': False})
    cur = con.cursor()
    try:
        # Get photo_post_ids for face cleanup
        cur.execute("SELECT id FROM photo_posts WHERE profile_id=?", (profile_id,))
        pp_ids = [r[0] for r in cur.fetchall()]

        # Delete in FK order
        if pp_ids:
            ph = ','.join('?'*len(pp_ids))
            cur.execute(f"DELETE FROM detected_faces WHERE photo_post_id IN ({ph})", pp_ids)
            cur.execute(f"DELETE FROM image_analysis WHERE photo_post_id IN ({ph})", pp_ids)
            cur.execute(f"DELETE FROM photo_comments WHERE photo_post_id IN ({ph})", pp_ids)

        cur.execute("SELECT id FROM reel_posts WHERE profile_id=?", (profile_id,))
        rp_ids = [r[0] for r in cur.fetchall()]
        if rp_ids:
            ph = ','.join('?'*len(rp_ids))
            cur.execute(f"DELETE FROM reel_comments WHERE reel_post_id IN ({ph})", rp_ids)

        cur.execute("SELECT id FROM text_posts WHERE profile_id=?", (profile_id,))
        tp_ids = [r[0] for r in cur.fetchall()]
        if tp_ids:
            ph = ','.join('?'*len(tp_ids))
            cur.execute(f"DELETE FROM text_comments WHERE text_post_id IN ({ph})", tp_ids)
            cur.execute(f"DELETE FROM text_post_analysis WHERE text_post_id IN ({ph})", tp_ids)

        cur.execute("DELETE FROM face_clusters WHERE id NOT IN (SELECT DISTINCT person_id FROM detected_faces WHERE person_id IS NOT NULL)")
        cur.execute("DELETE FROM photo_posts WHERE profile_id=?", (profile_id,))
        cur.execute("DELETE FROM reel_posts  WHERE profile_id=?", (profile_id,))
        cur.execute("DELETE FROM text_posts  WHERE profile_id=?", (profile_id,))

        cur.execute("SELECT id FROM secondary_profiles WHERE main_profile_id=?", (profile_id,))
        sp_ids = [r[0] for r in cur.fetchall()]
        if sp_ids:
            ph = ','.join('?'*len(sp_ids))
            cur.execute(f"DELETE FROM secondary_profile_fields WHERE secondary_profile_id IN ({ph})", sp_ids)
        cur.execute("DELETE FROM secondary_profiles WHERE main_profile_id=?", (profile_id,))
        cur.execute("DELETE FROM commentor_scores WHERE main_profile_id=?", (profile_id,))
        cur.execute("DELETE FROM profile_fields WHERE profile_id=?", (profile_id,))
        cur.execute("DELETE FROM profiles WHERE id=?", (profile_id,))
        con.commit()
    except Exception as e:
        con.close()
        return jsonify({'ok': False, 'error': str(e)})
    con.close()

    # Delete status file
    sf = os.path.join(STATUS_DIR, f'profile_{profile_id}.json')
    try: os.remove(sf)
    except: pass

    return jsonify({'ok': True})

@app.route('/api/delete/batch/<batch_id>', methods=['POST'])
def delete_batch(batch_id):
    con = db_connect(manual=True)
    if not con: return jsonify({'ok': False})
    cur = con.cursor()
    try:
        cur.execute("SELECT id FROM manual_posts WHERE batch_id=?", (batch_id,))
        post_ids = [r[0] for r in cur.fetchall()]
        if post_ids:
            ph = ','.join('?'*len(post_ids))
            cur.execute(f"DELETE FROM comments WHERE post_id IN ({ph})", post_ids)
        cur.execute("DELETE FROM batch_commentor_scores WHERE batch_id=?", (batch_id,))
        cur.execute("DELETE FROM manual_posts WHERE batch_id=?", (batch_id,))
        cur.execute("DELETE FROM batches WHERE batch_id=?", (batch_id,))
        con.commit()
    except Exception as e:
        con.close()
        return jsonify({'ok': False, 'error': str(e)})
    con.close()

    sf = os.path.join(STATUS_DIR, f'batch_{batch_id}.json')
    try: os.remove(sf)
    except: pass

    return jsonify({'ok': True})


if __name__ == '__main__':
    system_check()
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)