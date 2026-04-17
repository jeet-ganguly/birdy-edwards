import sqlite3
import json
import time
import os
import base64

OLLAMA_HOST = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')


OLLAMA_MODEL    = "gemma3:4b"   # text analysis after OCR extraction

# Tesseract language codes — all in one call

TESS_LANG       = "eng+urd+hin+ara+ben"

DB_FILE         = "socmint.db"
DELAY           = 1.0
MIN_TEXT_LENGTH = 10  # skip analysis if extracted text too short


#  SCHEMA

TEXT_POST_ANALYSIS_SCHEMA = """
CREATE TABLE IF NOT EXISTS text_post_analysis (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    text_post_id      INTEGER UNIQUE NOT NULL,
    extracted_text    TEXT,
    text_language     TEXT,
    topic             TEXT,
    sentiment         TEXT,
    narrative_type    TEXT,
    key_entities      TEXT,
    threat_indicators TEXT,
    ocr_used          INTEGER DEFAULT 1,
    analyzed_at       TEXT DEFAULT (datetime('now'))
);
"""

def robust_json_parse(raw):
    """Robustly extract JSON from LLM response."""
    import re
    raw = re.sub(r'```json\s*', '', raw)
    raw = re.sub(r'```\s*', '', raw)
    raw = raw.strip()
    try:
        return json.loads(raw)
    except Exception:
        pass
    match = re.search(r'(\{.*\})', raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass
    cleaned = re.sub(r',\s*([}\]])', r'\1', raw)
    try:
        return json.loads(cleaned)
    except Exception:
        return None

#  PYTESSERACT TEXT EXTRACTION

def extract_text_tesseract(screenshot_path):
    """
    Extract text from screenshot using pytesseract.
    Supports English, Urdu, Hindi, Arabic, Bengali in one call.
    Returns (extracted_text, detected_language) or (None, None)
    """
    import re

    try:
        import pytesseract
        from PIL import Image

        img = Image.open(screenshot_path)

        # Extract text with all languages
        raw = pytesseract.image_to_string(img, lang=TESS_LANG, config='--psm 6')

        if not raw or not raw.strip():
            return None, None

        # Filter out UI elements and timestamps
        ui_skip = {
            'like', 'comment', 'share', 'follow', 'reply',
            'see more', 'hide', 'edit', 'delete', 'report',
            'just now', 'yesterday', 'today', 'write a comment'
        }

        lines = []
        for line in raw.splitlines():
            line = line.strip()
            if not line or len(line) < 2:
                continue
            if line.lower() in ui_skip:
                continue
            # Skip timestamps
            if re.match(r'^\d+\s*(min|hr|h|d|w|days?|hours?|minutes?)\s*(ago)?$', line, re.I):
                continue
            if re.match(r'^\d+\s+(January|February|March|April|May|June|July|August|September|October|November|December)', line, re.I):
                continue
            lines.append(line)

        if not lines:
            return None, None

        extracted = '\n'.join(lines)
        lang      = detect_language(extracted)

        return extracted, lang

    except ImportError:
        print("    pytesseract not installed — run: pip install pytesseract")
        return None, None
    except Exception as e:
        print(f"    OCR error: {e}")
        return None, None


def detect_language(text):
    """Simple language detection based on character ranges."""
    if not text:
        return None

    counts = {
        'Bengali':  sum(1 for c in text if '\u0980' <= c <= '\u09FF'),
        'Arabic':   sum(1 for c in text if '\u0600' <= c <= '\u06FF'),
        'Urdu':     sum(1 for c in text if '\u0600' <= c <= '\u06FF' or '\uFB50' <= c <= '\uFDFF'),
        'Hindi':    sum(1 for c in text if '\u0900' <= c <= '\u097F'),
        'Chinese':  sum(1 for c in text if '\u4E00' <= c <= '\u9FFF'),
        'Russian':  sum(1 for c in text if '\u0400' <= c <= '\u04FF'),
        'English':  sum(1 for c in text if c.isascii() and c.isalpha()),
    }

    total = sum(counts.values())
    if total == 0:
        return 'Unknown'

    dominant = max(counts, key=counts.get)
    ratio    = counts[dominant] / total

    if ratio > 0.3:
        return dominant
    return 'Mixed'


#  OLLAMA CONTEXT ANALYSIS

def check_ollama():
    import urllib.request
    try:
        req  = urllib.request.urlopen(f'{OLLAMA_HOST}/api/tags', timeout=5)
        data = json.loads(req.read())
        models = [m['name'].split(':')[0] for m in data.get('models', [])]
        if OLLAMA_MODEL.split(':')[0] not in models:
            print(f"    Model '{OLLAMA_MODEL}' not found")
            return False
        print(f"   Ollama running — model: {OLLAMA_MODEL}")
        return True
    except Exception as e:
        print(f"   Ollama not reachable: {e}")
        return False


def build_analysis_prompt(extracted_text, post_date, language):
    return f"""You are a SOCMINT analyst. Analyze the following Facebook post text.

Post Date     : {post_date or 'N/A'}
Text Language : {language or 'Unknown'}
Post Text     :
{extracted_text}

Return ONLY a valid JSON object:

{{
  "topic": "brief one-line summary of what this post is about",
  "sentiment": "one of: positive / negative / neutral",
  "narrative_type": "one of: political / religious / social / personal / propaganda / news / humor / other",
  "key_entities": ["list of people, places, organizations, events mentioned"],
  "threat_indicators": "any threatening language, hate speech, calls to violence, incitement — or null"
}}

Rules:
- Analyze in the original language — do NOT translate before analyzing
- key_entities must be a JSON array
- threat_indicators is null if no threats detected
- Return ONLY the JSON object, no explanation, no markdown, no backticks"""


def analyze_with_ollama(extracted_text, post_date, language):
    """Send extracted text to Ollama for context analysis."""
    import ollama

    prompt = build_analysis_prompt(extracted_text, post_date, language)

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': prompt}],
            options={'temperature': 0.1}
        )

        raw = response['message']['content'].strip()

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        return robust_json_parse(raw)

    except json.JSONDecodeError as e:
        print(f"      JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"      Ollama error: {e}")
        return None


#  FETCH UNANALYZED TEXT POSTS

def fetch_unanalyzed_text_posts(db_file):
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.executescript(TEXT_POST_ANALYSIS_SCHEMA)
    con.commit()

    posts = []
    try:
        cur.execute("""
            SELECT tp.id, tp.post_url, tp.screenshot_path, tp.date_text
            FROM text_posts tp
            WHERE tp.screenshot_path IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM text_post_analysis tpa
                WHERE tpa.text_post_id = tp.id
            )
        """)
        for row in cur.fetchall():
            posts.append({
                'id':              row[0],
                'post_url':        row[1],
                'screenshot_path': row[2],
                'date':            row[3]
            })
    except Exception as e:
        print(f"    Fetch error: {e}")

    con.close()
    return posts


#  SAVE RESULT

def save_result(db_file, text_post_id, extracted_text, language, analysis):
    con = sqlite3.connect(db_file)
    con.execute("PRAGMA foreign_keys = OFF")
    cur = con.cursor()

    try:
        entities_json = json.dumps(
            analysis.get('key_entities', []) if analysis else [],
            ensure_ascii=False
        )
        cur.execute("""
            INSERT OR REPLACE INTO text_post_analysis
                (text_post_id, extracted_text, text_language, topic,
                 sentiment, narrative_type, key_entities, threat_indicators, ocr_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            text_post_id,
            extracted_text,
            language,
            analysis.get('topic')             if analysis else None,
            analysis.get('sentiment')         if analysis else None,
            analysis.get('narrative_type')    if analysis else None,
            entities_json,
            analysis.get('threat_indicators') if analysis else None,
        ))
        con.commit()
        return True
    except Exception as e:
        print(f"      Save error: {e}")
        return False
    finally:
        con.close()


#  MAIN ANALYZER

def analyze_text_posts(db_file=DB_FILE):
    print(f"\n{'═'*65}")
    print(f"Text Post Intelligence")
    print(f"OCR     : pytesseract ({TESS_LANG})")
    print(f"Analysis: {OLLAMA_MODEL}")
    print(f"DB      : {db_file}")
    print("═"*65)

    if not check_ollama():
        return

    posts   = fetch_unanalyzed_text_posts(db_file)
    total   = len(posts)
    print(f"  {total} unanalyzed text posts found")

    if total == 0:
        print("   All text posts already analyzed")
        return

    success = 0
    skipped = 0

    for i, post in enumerate(posts, 1):
        print(f"\n  [{i}/{total}] {post['post_url'][:70]}")
        print(f"     date       : {post['date'] or 'N/A'}")
        print(f"     screenshot : {post['screenshot_path']}")

        # Step 1 — EasyOCR extract text
        print(f"     Extracting text with EasyOCR...")
        extracted_text, language = extract_text_tesseract(post['screenshot_path'])

        if not extracted_text or len(extracted_text.strip()) < MIN_TEXT_LENGTH:
            print(f" No meaningful text extracted — skipping analysis")
            save_result(db_file, post['id'], extracted_text, language, None)
            skipped += 1
            continue

        print(f"      Extracted ({language}): {extracted_text[:80]}...")

        # Step 2 — Ollama context analysis
        print(f"     Analyzing context with {OLLAMA_MODEL}...")
        analysis = analyze_with_ollama(extracted_text, post['date'], language)

        if analysis:
            save_result(db_file, post['id'], extracted_text, language, analysis)
            success += 1
            print(f"      topic       : {analysis.get('topic') or 'None'}")
            print(f"      sentiment   : {analysis.get('sentiment') or 'None'}")
            print(f"      narrative   : {analysis.get('narrative_type') or 'None'}")
            print(f"      entities    : {', '.join(analysis.get('key_entities', []))}")
            threat = analysis.get('threat_indicators')
            if threat:
                print(f"     threat      : {threat[:80]}")
            else:
                print(f"     threat      : None")
        else:
            # Save OCR result even if LLM fails
            save_result(db_file, post['id'], extracted_text, language, None)
            print(f"       LLM analysis failed — saved OCR text only")
            skipped += 1

        time.sleep(DELAY)

    print(f"\n{'═'*65}")
    print(f" Done — {success} analyzed, {skipped} skipped out of {total}")


def fetch_unanalyzed_batch_text_posts(db_file, batch_id):
    """Fetch post type URLs from manual batch that have screenshots."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.executescript(TEXT_POST_ANALYSIS_SCHEMA)
    con.commit()
    posts = []
    try:
        cur.execute("""
            SELECT mp.id, mp.url, mp.screenshot_path, mp.date_text
            FROM manual_posts mp
            WHERE mp.batch_id = ? AND mp.type = 'post'
            AND mp.screenshot_path IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM text_post_analysis tpa
                WHERE tpa.text_post_id = mp.id
            )
        """, (batch_id,))
        for row in cur.fetchall():
            posts.append({
                'id': row[0], 'post_url': row[1],
                'screenshot_path': row[2], 'date': row[3]
            })
    except Exception as e:
        print(f"    Batch text post fetch error: {e}")
    con.close()
    return posts


def analyze_batch_text_posts(db_file, batch_id):
    """Analyze post type URLs from manual batch investigation."""
    print(f"\n{'═'*65}")
    print(f"Text Post Intelligence — Manual Batch")
    print(f"Batch : {batch_id}")
    print(f"Model : {OLLAMA_MODEL}")
    print("═"*65)

    if not check_ollama():
        return

    posts = fetch_unanalyzed_batch_text_posts(db_file, batch_id)
    total = len(posts)
    print(f"   {total} unanalyzed batch text posts found")

    if total == 0:
        print("   All batch text posts already analyzed")
        return

    success = skipped = 0

    for i, post in enumerate(posts, 1):
        print(f"\n   [{i}/{total}] {post['post_url'][:70]}")
        extracted_text, language = extract_text_tesseract(post['screenshot_path'])

        if not extracted_text or len(extracted_text.strip()) < MIN_TEXT_LENGTH:
            save_result(db_file, post['id'], extracted_text, language, None)
            skipped += 1
            continue

        analysis = analyze_with_ollama(extracted_text, post['date'], language)
        save_result(db_file, post['id'], extracted_text, language, analysis)

        if analysis:
            success += 1
            print(f"      topic: {analysis.get('topic') or 'None'}")
        else:
            skipped += 1

        time.sleep(DELAY)

    print(f"\n Done — {success} analyzed, {skipped} skipped")

if __name__ == "__main__":
    analyze_text_posts()