"""
reel_caption_intelligence.py
─────────────────────────────
Reads reel captions from reel_posts, sends each to the LLM,
extracts structured context (summary, named entities, hashtags,
topic, sentiment) and saves back to DB.

Run order in pipeline:
  DB Import → reel_caption_intelligence → image_intelligence
                                        → text_post_intelligence
                                        → comment_intelligence_offline
"""

import os
import json
import sqlite3
import requests

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DB_FILE       = os.path.join(BASE_DIR, 'socmint.db')
OLLAMA_HOST   = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
OLLAMA_MODEL  = ''   # overridden by app.py at runtime


# ── DB helpers ────────────────────────────────────────────────────────────────

def init_columns(db_file):
    """Add caption intelligence columns to reel_posts if missing."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    for col, typedef in [
        ('caption',          'TEXT'),
        ('caption_context',  'TEXT'),
        ('caption_entities', 'TEXT'),   # JSON array  e.g. ["Messi","Argentina"]
        ('caption_hashtags', 'TEXT'),   # JSON array  e.g. ["#worldcup","#messi"]
        ('caption_topic',    'TEXT'),
        ('caption_sentiment','TEXT'),
    ]:
        try:
            cur.execute(f"ALTER TABLE reel_posts ADD COLUMN {col} {typedef}")
            print(f"  ✓ Added column reel_posts.{col}")
        except Exception:
            pass  # already exists
    con.commit()
    con.close()


def fetch_unprocessed(db_file):
    """Fetch reel posts that have a caption but no context yet."""
    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT id, reel_url, caption
            FROM reel_posts
            WHERE caption IS NOT NULL
              AND caption != ''
              AND caption_context IS NULL
            ORDER BY id
        """)
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"  fetch error: {e}")
        rows = []
    con.close()
    return rows


def save_result(db_file, reel_id, result):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute("""
        UPDATE reel_posts SET
            caption_context   = ?,
            caption_entities  = ?,
            caption_hashtags  = ?,
            caption_topic     = ?
        WHERE id = ?
    """, (
        result.get('context'),
        json.dumps(result.get('tagged_names', []),  ensure_ascii=False),
        json.dumps(result.get('hashtags', []),      ensure_ascii=False),
        result.get('topic'),
        reel_id,
    ))
    con.commit()
    con.close()


# ── LLM ───────────────────────────────────────────────────────────────────────

def build_prompt(caption):
    return f"""You are a SOCMINT analyst. Analyze this Facebook Reel caption and return structured JSON.

Reel Caption:
{caption}

Return ONLY a valid JSON object:
{{
  "context": "1-2 sentence summary of what this reel is about — who/what/why",
  "topic": "single topic label e.g. politics / sports / travel / religion / military / celebration / protest / personal",
  "tagged_names": ["list of real person names or organization names mentioned — empty array if none"],
  "hashtags": ["all hashtags exactly as written e.g. #ladakh #travel — empty array if none"],
}}

Rules:
- tagged_names: real names only — not generic words. Include @mentions and names in text.
- hashtags: extract exactly from caption text including the # symbol
- context: be specific about subjects — name the person/event/place if identifiable
- Return ONLY the JSON object, no markdown, no backticks"""


def call_ollama(prompt):
    try:
        r = requests.post(
            f'{OLLAMA_HOST}/api/generate',
            json={
                'model':  OLLAMA_MODEL,
                'prompt': prompt,
                'stream': False,
                'options': {'temperature': 0.1, 'num_predict': 300},
            },
            timeout=60,
        )
        if r.status_code != 200:
            return None
        raw = r.json().get('response', '').strip()

        # Strip markdown fences if present
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
            raw = raw.strip()

        return json.loads(raw)
    except Exception as e:
        print(f"    LLM error: {e}")
        return None


# ── Fallback: extract hashtags without LLM ───────────────────────────────────

def extract_hashtags_simple(caption):
    """Extract hashtags from caption text without LLM."""
    import re
    return re.findall(r'#\w+', caption)


def extract_context_simple(caption):
    """Return first 200 chars as context fallback."""
    return caption[:200].strip()


# ── Main ──────────────────────────────────────────────────────────────────────

def analyze_reel_captions(db_file=DB_FILE):
    print(f"\n{'═'*65}")
    print(f"  Reel Caption Intelligence")
    print(f"  Model : {OLLAMA_MODEL}")
    print(f"  DB    : {db_file}")
    print(f"{'═'*65}")

    init_columns(db_file)

    reels = fetch_unprocessed(db_file)
    total = len(reels)
    print(f"\n  {total} reel(s) with unprocessed captions\n")

    if total == 0:
        print("  All reel captions already processed")
        return

    success = 0
    fallback = 0

    for i, reel in enumerate(reels, 1):
        print(f"  [{i}/{total}] {reel['reel_url'][:70]}")
        print(f"    caption : {reel['caption'][:80]}{'…' if len(reel['caption'])>80 else ''}")

        prompt = build_prompt(reel['caption'])
        result = call_ollama(prompt)

        if result and isinstance(result, dict):
            # Validate and clean
            result.setdefault('context',      extract_context_simple(reel['caption']))
            result.setdefault('topic',        'unknown')
            result.setdefault('tagged_names', [])
            result.setdefault('hashtags',     extract_hashtags_simple(reel['caption']))
            #result.setdefault('sentiment',    'neutral')

            # Ensure hashtags list is actually a list
            if not isinstance(result.get('hashtags'), list):
                result['hashtags'] = extract_hashtags_simple(reel['caption'])
            if not isinstance(result.get('tagged_names'), list):
                result['tagged_names'] = []

            save_result(db_file, reel['id'], result)
            success += 1
            print(f"    ✓ topic     : {result.get('topic')}")
            print(f"      sentiment : {result.get('sentiment')}")
            print(f"      entities  : {result.get('tagged_names')}")
            print(f"      hashtags  : {result.get('hashtags')}")
            print(f"      context   : {(result.get('context') or '')[:80]}")
        else:
            # LLM failed — use simple extraction as fallback
            fallback_result = {
                'context':      extract_context_simple(reel['caption']),
                'topic':        'unknown',
                'tagged_names': [],
                'hashtags':     extract_hashtags_simple(reel['caption']),
                'sentiment':    'neutral',
            }
            save_result(db_file, reel['id'], fallback_result)
            fallback += 1
            print(f"    ⚠ LLM failed — used fallback extraction")
            print(f"      hashtags  : {fallback_result['hashtags']}")

    print(f"\n{'─'*65}")
    print(f"  Done — {success} LLM processed, {fallback} fallback")
    print(f"{'─'*65}\n")


# ── Run standalone ────────────────────────────────────────────────────────────

def analyze_batch_reel_captions(db_file, batch_id):
    """Same as analyze_reel_captions but for manual_posts table."""
    print(f"\n{'═'*65}")
    print(f"  Reel Caption Intelligence (Batch: {batch_id})")
    print(f"  Model : {OLLAMA_MODEL}")
    print(f"{'═'*65}")

    # Ensure columns exist
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    for col in ['caption_context', 'caption_entities',
                'caption_hashtags', 'caption_topic', 'caption_sentiment']:
        try:
            cur.execute(f"ALTER TABLE manual_posts ADD COLUMN {col} TEXT")
        except Exception:
            pass
    con.commit()
    con.close()

    # Fetch unprocessed reel posts from this batch
    con = sqlite3.connect(db_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT id, url, caption
            FROM manual_posts
            WHERE batch_id = ?
              AND type = 'reel'
              AND caption IS NOT NULL
              AND caption != ''
              AND caption_context IS NULL
        """, (batch_id,))
        reels = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"  fetch error: {e}")
        reels = []
    con.close()

    total = len(reels)
    print(f"\n  {total} reel(s) with unprocessed captions\n")
    if total == 0:
        return

    success = 0
    for i, reel in enumerate(reels, 1):
        print(f"  [{i}/{total}] {reel['url'][:70]}")
        result = call_ollama(build_prompt(reel['caption']))

        if result and isinstance(result, dict):
            result.setdefault('context',      extract_context_simple(reel['caption']))
            result.setdefault('topic',        'unknown')
            result.setdefault('tagged_names', [])
            result.setdefault('hashtags',     extract_hashtags_simple(reel['caption']))
            #result.setdefault('sentiment',    'neutral')
            if not isinstance(result.get('hashtags'), list):
                result['hashtags'] = extract_hashtags_simple(reel['caption'])
            if not isinstance(result.get('tagged_names'), list):
                result['tagged_names'] = []
        else:
            result = {
                'context':      extract_context_simple(reel['caption']),
                'topic':        'unknown',
                'tagged_names': [],
                'hashtags':     extract_hashtags_simple(reel['caption']),
                'sentiment':    'neutral',
            }

        # Save to manual_posts
        con = sqlite3.connect(db_file)
        cur = con.cursor()
        cur.execute("""
            UPDATE manual_posts SET
                caption_context   = ?,
                caption_entities  = ?,
                caption_hashtags  = ?,
                caption_topic     = ?,
                caption_sentiment = ?
            WHERE id = ?
        """, (
            result.get('context'),
            json.dumps(result.get('tagged_names', []), ensure_ascii=False),
            json.dumps(result.get('hashtags', []),     ensure_ascii=False),
            result.get('topic'),
            reel['id'],
        ))
        con.commit()
        con.close()
        success += 1
        print(f"    ✓ topic: {result.get('topic')} | entities: {result.get('tagged_names')}")

    print(f"\n  Done — {success}/{total} processed\n")
    
if __name__ == '__main__':
    analyze_reel_captions()