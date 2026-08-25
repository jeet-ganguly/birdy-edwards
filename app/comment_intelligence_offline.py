import sqlite3
import json
import time
import os


OLLAMA_HOST = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
OLLAMA_MODEL   = ""          # It can be overwrite from web panel not need to change from here


DB_FILE        = "socmint.db"
MANUAL_DB_FILE = "socmint_manual.db"
BATCH_SIZE     = 2
DELAY          = 0.5            # ollama is local — no rate limit needed

#  SCHEMA

ANALYSIS_SCHEMA = """
CREATE TABLE IF NOT EXISTS comment_analysis (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    comment_id        INTEGER NOT NULL,
    db_source         TEXT NOT NULL,
    sentiment         TEXT,
    emotion           TEXT,
    stance            TEXT,
    language          TEXT,
    analyzed_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(comment_id, db_source)
);
"""

#  OLLAMA CHECK

def check_ollama():
    """Check if Ollama is running and model is available."""
    try:
        import urllib.request
        req = urllib.request.urlopen(f"{OLLAMA_HOST}/api/tags", timeout=5)
        data = json.loads(req.read())
        # Check full name AND base name — both valid
        model_names = [m['name'] for m in data.get('models', [])]
        base_names  = [m['name'].split(':')[0] for m in data.get('models', [])]
        if OLLAMA_MODEL not in model_names and OLLAMA_MODEL.split(':')[0] not in base_names:
            print(f"    Model '{OLLAMA_MODEL}' not found in Ollama")
            print(f"    Run: ollama pull {OLLAMA_MODEL}")
            return False
        print(f"   Ollama running — model: {OLLAMA_MODEL}")
        return True
    except Exception as e:
        print(f"   Ollama not reachable: {e}")
        print(f"   Run: ollama serve")
        return False

#  PROMPT

def build_prompt(comments_batch):
    comments_text = ""
    for i, c in enumerate(comments_batch):
        comments_text += f"""
#{i+1}
  Commentor Name    : {c['name']}
  Post Caption      : {c['post_caption'] or 'N/A'}
  Post Context      : {c.get('post_context') or 'N/A'}
  Comment Text      : {c['comment_text']}
---"""

    return f"""You are a multilingual SOCMINT analyst. Analyze these Facebook comments.

For each comment return:
- sentiment : positive | negative | neutral
- emotion   : support | anger | sarcasm | aggressive
- stance    : support_post | oppose_post | neutral_discussion
- language  : full language name (English, Bengali, Hindi, Arabic, Urdu etc.)

Rules:
- Analyze in the ORIGINAL language, do not translate
- [Emoji] [Sticker] [GIF] → sentiment=neutral, emotion=support, stance=neutral_discussion
- Aggressive/hateful → emotion=aggressive, sentiment=negative
- Detect sarcasm in all languages

Stance = does the commentor AGREE or DISAGREE with the post author's message:
- support_post      = agrees with what the post is saying
- oppose_post       = disagrees, OR praises a rival, OR undermines the post subject
- neutral_discussion = off-topic, general, unrelated to the post

Examples:
Post "Ronaldo GOAT" + Comment "Ronaldo GOAT" → support_post
Post "Ronaldo GOAT" + Comment "Messi is better" → oppose_post
Post "Our party wins" + Comment "Great leader!" → support_post
Post "Our party wins" + Comment "Opposition is stronger" → oppose_post
Post "Beautiful sunset" + Comment "Nice photo" → neutral_discussion

{comments_text}

Return ONLY a JSON array with exactly {len(comments_batch)} objects:
[{{"index":1,"sentiment":"...","emotion":"...","stance":"...","language":"..."}}]
No explanation. No markdown. JSON only."""

    return prompt


#  OLLAMA API CALL


def extract_json_from_response(raw):
    """Robustly extract JSON array from LLM response — handles all model quirks."""
    import re

    # 1. Strip markdown fences
    raw = re.sub(r'```json\s*', '', raw)
    raw = re.sub(r'```\s*', '', raw)
    raw = raw.strip()

    # 2. Remove control characters that break JSON parsing
    raw = re.sub(r'[--]', '', raw)

    # 3. Try direct parse first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # 4. Extract first [...] block
    match = re.search(r'(\[.*?\])', raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # 5. Try fixing common issues — trailing commas, single quotes
    cleaned = re.sub(r',\s*([}\]])', r'', raw)   # trailing commas
    cleaned = cleaned.replace("'", '"')              # single to double quotes
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # 6. Extract individual {...} objects and rebuild array
    objects = re.findall(r'\{[^{}]+\}', raw, re.DOTALL)
    if objects:
        try:
            arr = '[' + ','.join(objects) + ']'
            return json.loads(arr)
        except json.JSONDecodeError:
            pass

    return None


def analyze_single_ollama(comment):
    """Fallback: analyze one comment at a time when batch fails."""
    import ollama
    prompt = f"""Analyze this Facebook comment and return ONLY a JSON object.

Comment: {comment['comment_text']}
Commentor: {comment['name']}

Return ONLY this JSON, nothing else:
{{"sentiment": "positive|negative|neutral", "emotion": "support|anger|sarcasm|aggressive", "stance": "support_post|oppose_post|neutral_discussion", "language": "English"}}"""

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.1, "top_p": 0.9}
        )
        raw = response["message"]["content"].strip()
        result = extract_json_from_response(raw)
        if isinstance(result, list) and result:
            result = result[0]
        if isinstance(result, dict):
            return {
                "index": 1,
                "sentiment": result.get("sentiment", "neutral"),
                "emotion":   result.get("emotion",   "support"),
                "stance":    result.get("stance",    "neutral_discussion"),
                "language":  result.get("language",  "Unknown"),
            }
    except Exception:
        pass
    return None


def analyze_batch_ollama(comments_batch):
    """Send batch to Ollama and return analysis results."""
    import ollama

    prompt = build_prompt(comments_batch)

    try:
        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.1, "top_p": 0.9}
        )
        raw = response["message"]["content"].strip()
        result = extract_json_from_response(raw)

        if result and isinstance(result, list):
            return result

        # Batch JSON failed — fall back to one-by-one
        print(f"      Batch JSON failed — trying one-by-one fallback")
        results = []
        for i, c in enumerate(comments_batch):
            single = analyze_single_ollama(c)
            if single:
                single["index"] = i + 1
                results.append(single)
            else:
                results.append({
                    "index": i + 1,
                    "sentiment": "neutral",
                    "emotion": "support",
                    "stance": "neutral_discussion",
                    "language": "Unknown"
                })
        return results

    except Exception as e:
        print(f"      Ollama error: {e}")
        return None


#  FETCH COMMENTS


def fetch_unanalyzed_comments(db_file):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.executescript(ANALYSIS_SCHEMA)
    con.commit()

    comments = []

    # Photo comments — with image analysis context
    try:
        cur.execute("""
            SELECT pc.id, 'photo', co.name, co.profile_url,
                   pp.caption, pp.date_text, pc.comment_text,
                   ia.scene_type, ia.activity, ia.political_symbols,
                   ia.text_in_image
            FROM photo_comments pc
            JOIN commentors co ON co.id = pc.commentor_id
            JOIN photo_posts pp ON pp.id = pc.photo_post_id
            LEFT JOIN image_analysis ia ON ia.photo_post_id = pp.id
            WHERE NOT EXISTS (
                SELECT 1 FROM comment_analysis ca
                WHERE ca.comment_id = pc.id AND ca.db_source = 'photo'
            )
        """)
        for row in cur.fetchall():
            context_parts = []
            if row[7]: context_parts.append(f"Scene: {row[7]}")
            if row[8]: context_parts.append(f"Activity: {row[8]}")
            if row[9]: context_parts.append(f"Political symbols: {row[9]}")
            if row[10]: context_parts.append(f"Text in image: {row[10]}")
            comments.append({
                'id': row[0], 'db_source': row[1],
                'name': row[2] or '', 'profile_url': row[3] or '',
                'post_caption': row[4] or '', 'post_date': row[5] or '',
                'comment_text': row[6] or '',
                'post_context': ' | '.join(context_parts) if context_parts else None
            })
    except Exception as e:
        print(f"    photo_comments fetch error: {e}")

    # Reel comments — no image context available
    try:
        try:
            cur.execute("""
                SELECT rc.id, 'reel', co.name, co.profile_url,
                       rp.caption, rp.scraped_at, rc.comment_text,
                       rp.caption_context, rp.caption_entities,
                       rp.caption_hashtags, rp.caption_topic
                FROM reel_comments rc
                JOIN commentors co ON co.id = rc.commentor_id
                JOIN reel_posts rp ON rp.id = rc.reel_post_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM comment_analysis ca
                    WHERE ca.comment_id = rc.id AND ca.db_source = 'reel'
                )
            """)
        except Exception:
            # Caption columns not yet migrated — use base query
            cur.execute("""
                SELECT rc.id, 'reel', co.name, co.profile_url,
                       NULL, NULL, rc.comment_text,
                       NULL, NULL, NULL, NULL
                FROM reel_comments rc
                JOIN commentors co ON co.id = rc.commentor_id
                JOIN reel_posts rp ON rp.id = rc.reel_post_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM comment_analysis ca
                    WHERE ca.comment_id = rc.id AND ca.db_source = 'reel'
                )
            """)
        for row in cur.fetchall():
            caption         = row[4] or ''
            caption_context = row[7] or ''
            entities        = row[8] or '[]'
            hashtags        = row[9] or '[]'
            topic           = row[10] or ''
            context_parts = []
            if caption_context:
                context_parts.append(caption_context)
            if topic and topic != 'unknown':
                context_parts.append(f"Topic: {topic}")
            try:
                names = json.loads(entities)
                if names: context_parts.append(f"People/Orgs: {', '.join(names)}")
            except Exception:
                pass
            try:
                tags = json.loads(hashtags)
                if tags: context_parts.append(f"Hashtags: {' '.join(tags)}")
            except Exception:
                pass
            post_context = ' | '.join(context_parts) if context_parts else (caption or None)
            comments.append({
                'id': row[0], 'db_source': row[1],
                'name': row[2] or '', 'profile_url': row[3] or '',
                'post_caption': caption,
                'post_date':    row[5] or '',
                'comment_text': row[6] or '',
                'post_context': post_context,
            })
    except Exception as e:
        print(f"    reel_comments fetch error: {e}")

    # Text post comments — with text post analysis context
    try:
        cur.execute("""
            SELECT tc.id, 'text', co.name, co.profile_url,
                   NULL, tp.date_text, tc.comment_text,
                   tpa.topic, tpa.narrative_type
            FROM text_comments tc
            JOIN commentors co ON co.id = tc.commentor_id
            JOIN text_posts tp ON tp.id = tc.text_post_id
            LEFT JOIN text_post_analysis tpa ON tpa.text_post_id = tp.id
            WHERE NOT EXISTS (
                SELECT 1 FROM comment_analysis ca
                WHERE ca.comment_id = tc.id AND ca.db_source = 'text'
            )
        """)
        for row in cur.fetchall():
            context_parts = []
            if row[7]: context_parts.append(f"Post topic: {row[7]}")
            if row[8]: context_parts.append(f"Narrative: {row[8]}")
            comments.append({
                'id': row[0], 'db_source': row[1],
                'name': row[2] or '', 'profile_url': row[3] or '',
                'post_caption': row[4] or '', 'post_date': row[5] or '',
                'comment_text': row[6] or '',
                'post_context': ' | '.join(context_parts) if context_parts else None
            })
    except Exception as e:
        print(f"    text_comments fetch error: {e}")

    con.close()
    return comments


def fetch_unanalyzed_manual_comments(db_file, batch_id=None):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.executescript(ANALYSIS_SCHEMA)
    con.commit()

    comments = []
    try:
        if batch_id:
            cur.execute("""
                SELECT c.id, 'manual', co.name, co.profile_url,
                       mp.caption, mp.date_text, c.comment_text,
                       mp.type,
                       ia.scene_type, ia.activity, ia.political_symbols,
                       ia.text_in_image,
                       tpa.topic, tpa.narrative_type
                FROM comments c
                JOIN commentors co ON co.id = c.commentor_id
                JOIN manual_posts mp ON mp.id = c.post_id
                LEFT JOIN image_analysis ia ON ia.photo_post_id = mp.id
                LEFT JOIN text_post_analysis tpa ON tpa.text_post_id = mp.id
                WHERE mp.batch_id = ?
                AND c.id NOT IN (
                    SELECT comment_id FROM comment_analysis
                    WHERE db_source = 'manual'
                )
            """, (batch_id,))
        else:
            cur.execute("""
                SELECT c.id, 'manual', co.name, co.profile_url,
                       mp.caption, mp.date_text, c.comment_text
                FROM comments c
                JOIN commentors co ON co.id = c.commentor_id
                JOIN manual_posts mp ON mp.id = c.post_id
                WHERE c.id NOT IN (
                    SELECT comment_id FROM comment_analysis
                    WHERE db_source = 'manual'
                )
            """)
        for row in cur.fetchall():
            # Build post context from image/text analysis
            post_type = row[7] or ''
            context_parts = []
            if row[8]: context_parts.append(f"Scene: {row[8]}")
            if row[9]: context_parts.append(f"Activity: {row[9]}")
            if row[10]: context_parts.append(f"Political symbols: {row[10]}")
            if row[11]: context_parts.append(f"Text in image: {row[11]}")
            if row[12]: context_parts.append(f"Post topic: {row[12]}")
            if row[13]: context_parts.append(f"Narrative: {row[13]}")
            post_context = ' | '.join(context_parts) if context_parts else None

            comments.append({
                'id': row[0], 'db_source': row[1],
                'name': row[2] or '', 'profile_url': row[3] or '',
                'post_caption': row[4] or '', 'post_date': row[5] or '',
                'comment_text': row[6] or '',
                'post_context': post_context
            })
    except Exception as e:
        print(f"    manual comments fetch error: {e}")

    con.close()
    return comments


#  SAVE RESULTS

def save_results(db_file, batch, results):
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    saved = 0
    for i, result in enumerate(results):
        if i >= len(batch):
            break
        comment = batch[i]
        try:
            cur.execute("""
                INSERT OR REPLACE INTO comment_analysis
                    (comment_id, db_source, sentiment, emotion, stance, language)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                comment['id'], comment['db_source'],
                result.get('sentiment') or 'neutral',
                result.get('emotion')   or 'support',
                result.get('stance')    or 'neutral_discussion',
                result.get('language')  or 'Unknown'
            ))
            saved += 1
        except Exception as e:
            print(f"      Save error: {e}")
    con.commit()
    con.close()
    return saved


#  MAIN ANALYZER


def fetch_analyzed_comments(db_file, label="socmint", batch_id=None):
    """Fetch already analyzed comments to display existing results."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    results = []

    try:
        if label == "manual" and batch_id:
            cur.execute("""
                SELECT c.id, co.name, c.comment_text,
                       ca.sentiment, ca.emotion, ca.stance, ca.language
                FROM comments c
                JOIN commentors co ON co.id = c.commentor_id
                JOIN manual_posts mp ON mp.id = c.post_id
                JOIN comment_analysis ca ON ca.comment_id = c.id
                    AND ca.db_source = 'manual'
                WHERE mp.batch_id = ?
                ORDER BY c.id
            """, (batch_id,))
        elif label == "manual":
            cur.execute("""
                SELECT c.id, co.name, c.comment_text,
                       ca.sentiment, ca.emotion, ca.stance, ca.language
                FROM comments c
                JOIN commentors co ON co.id = c.commentor_id
                JOIN comment_analysis ca ON ca.comment_id = c.id
                    AND ca.db_source = 'manual'
                ORDER BY c.id
            """)
        else:
            cur.execute("""
                SELECT t.id, co.name, t.comment_text,
                       ca.sentiment, ca.emotion, ca.stance, ca.language
                FROM (
                    SELECT id, commentor_id, comment_text, 'photo' AS src
                    FROM photo_comments
                    UNION ALL
                    SELECT id, commentor_id, comment_text, 'reel' AS src
                    FROM reel_comments
                    UNION ALL
                    SELECT id, commentor_id, comment_text, 'text' AS src
                    FROM text_comments
                ) t
                JOIN commentors co ON co.id = t.commentor_id
                JOIN comment_analysis ca ON ca.comment_id = t.id
                    AND ca.db_source = t.src
                ORDER BY t.id
            """)

        for row in cur.fetchall():
            results.append({
                'id':           row[0],
                'name':         row[1] or '',
                'comment_text': row[2] or '',
                'sentiment':    row[3] or 'N/A',
                'emotion':      row[4] or 'N/A',
                'stance':       row[5] or 'N/A',
                'language':     row[6] or 'N/A'
            })
    except Exception as e:
        print(f"    fetch analyzed error: {e}")

    con.close()
    return results


def analyze_comments(db_file, label="socmint", batch_id=None):
    print(f"\n{'═'*65}")
    print(f"Comment Intelligence (Offline) — {label}")
    if batch_id:
        print(f"Batch ID : {batch_id}")
    print(f"Model    : {OLLAMA_MODEL}")
    print(f"DB       : {db_file}")
    print("═"*65)

    if label == "manual":
        comments = fetch_unanalyzed_manual_comments(db_file, batch_id=batch_id)
    else:
        comments = fetch_unanalyzed_comments(db_file)

    total = len(comments)
    print(f"   {total} unanalyzed comments found")

    if total == 0:
        # Print existing results instead
        existing = fetch_analyzed_comments(db_file, label=label, batch_id=batch_id)
        if existing:
            print(f"   Showing {len(existing)} previously analyzed comments:\n")
            for c in existing:
                snippet = c['comment_text'][:50] + ('…' if len(c['comment_text']) > 50 else '')
                print(f"     {c['name']:20s} → {snippet}")
                print(f"       sentiment={c['sentiment']:8s} emotion={c['emotion']:10s} "
                      f"stance={c['stance']:20s} lang={c['language']}")
        else:
            print("   No comments found for this batch")
        return

    processed   = 0
    saved_total = 0

    for i in range(0, total, BATCH_SIZE):
        batch = comments[i:i+BATCH_SIZE]
        print(f"\n   Batch {i//BATCH_SIZE + 1} — comments {i+1} to {min(i+BATCH_SIZE, total)}")

        for c in batch:
            snippet = c['comment_text'][:50] + ('…' if len(c['comment_text']) > 50 else '')
            print(f"     [{c['db_source']}] {c['name']:20s} → {snippet}")

        results = analyze_batch_ollama(batch)

        if results:
            saved = save_results(db_file, batch, results)
            saved_total += saved
            processed   += len(batch)

            for r in results:
                print(f"     #{r['index']} → sentiment={str(r.get('sentiment') or 'N/A'):8s} "
                      f"emotion={str(r.get('emotion') or 'N/A'):10s} "
                      f"stance={str(r.get('stance') or 'N/A'):20s} "
                      f"lang={r.get('language') or 'N/A'}")
        else:
            print(f"       Batch failed — skipping")

        time.sleep(DELAY)

    print(f"\n{'═'*65}")
    print(f" Done — {saved_total}/{total} comments analyzed")


def run_all(manual_batch_id=None):
    """Analyze comments from both DBs using Ollama."""
    print("\n" + "═"*65)
    print("SOCMINT — Offline Comment Intelligence")
    print(f"Model: {OLLAMA_MODEL}  Host: {OLLAMA_HOST}")
    print("═"*65)

    if not check_ollama():
        print("\n Ollama not available. Exiting.")
        return

    if os.path.exists(DB_FILE):
        analyze_comments(DB_FILE, label="socmint")
    else:
        print(f"  {DB_FILE} not found — skipping")

    if os.path.exists(MANUAL_DB_FILE):
        analyze_comments(MANUAL_DB_FILE, label="manual", batch_id=manual_batch_id)
    else:
        print(f"  {MANUAL_DB_FILE} not found — skipping")


if __name__ == "__main__":
    run_all()