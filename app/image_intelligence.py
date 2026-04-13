import sqlite3
import json
import time
import os
import base64
import urllib.request
import urllib.error

OLLAMA_HOST = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
OLLAMA_MODEL = "gemma3:4b"   # vision capable model

DB_FILE      = "socmint.db"
DELAY        = 1.0           # seconds between API calls


#  SCHEMA

IMAGE_ANALYSIS_SCHEMA = """
CREATE TABLE IF NOT EXISTS image_analysis (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_post_id       INTEGER UNIQUE NOT NULL,
    scene_type          TEXT,
    objects             TEXT,       -- JSON array
    activity            TEXT,
    crowd_size          TEXT,
    political_symbols   TEXT,
    religious_symbols   TEXT,
    weapons_visible     TEXT,
    cultural_context    TEXT,
    text_in_image       TEXT,       -- from gemma3:4b
    text_language       TEXT,
    ocr_text            TEXT,       -- from EasyOCR (more accurate for Urdu/Arabic)
    location_clues      TEXT,
    estimated_location  TEXT,
    confidence          INTEGER,
    analyzed_at         TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (photo_post_id) REFERENCES photo_posts(id)
);
"""


def extract_text_ocr(image_bytes):
    """Extract text from image using EasyOCR — accurate for Urdu/Arabic/Hindi/Bengali."""
    if not OCR_AVAILABLE:
        return None
    try:
        import numpy as np
        import io
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes)).convert('RGB')
        img_array = np.array(img)

        results = OCR_READER.readtext(img_array)
        if not results:
            return None

        # Combine all detected text with confidence > 0.3
        texts = [r[1] for r in results if r[2] > 0.3]
        return ' '.join(texts).strip() if texts else None
    except Exception as e:
        print(f"      OCR error: {e}")
        return None


#  OLLAMA CHECK

def check_ollama():
    try:
        req  = urllib.request.urlopen(f"{OLLAMA_HOST}/api/tags", timeout=5)
        data = json.loads(req.read())
        models = [m['name'].split(':')[0] for m in data.get('models', [])]
        if OLLAMA_MODEL.split(':')[0] not in models:
            print(f"    Model '{OLLAMA_MODEL}' not found")
            print(f"   Run: ollama pull {OLLAMA_MODEL}")
            return False
        print(f"   Ollama running — model: {OLLAMA_MODEL}")
        return True
    except Exception as e:
        print(f"   Ollama not reachable: {e}")
        print(f"   Run: ollama serve")
        return False


#  IMAGE DOWNLOAD

def download_image_base64(url):
    """Download image from URL and return as base64 string."""
    try:
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            }
        )
        response = urllib.request.urlopen(req, timeout=15)
        image_data = response.read()

        # Detect mime type from content
        content_type = response.headers.get('Content-Type', 'image/jpeg')
        mime_type = content_type.split(';')[0].strip()

        b64 = base64.b64encode(image_data).decode('utf-8')
        return b64, mime_type

    except urllib.error.HTTPError as e:
        print(f"      HTTP {e.code} — image download failed")
        return None, None
    except Exception as e:
        print(f"      Download error: {e}")
        return None, None


#  PROMPT

def build_image_prompt(caption, post_date):
    return f"""You are a SOCMINT image analyst. Analyze this image carefully for intelligence gathering purposes.

Post Caption : {caption or 'N/A'}
Post Date    : {post_date or 'N/A'}

IMPORTANT RULES BEFORE ANALYZING:
- If the image is primarily TEXT, QUOTE, TYPOGRAPHY or CALLIGRAPHY — set scene_type="text_image", transcribe the text carefully, detect the language, and set all other fields to null
- Do NOT hallucinate objects, crowds, flags or scenes that are not clearly visible
- If you are not sure about something set it to null and lower the confidence
- Only describe what you can clearly see

Analyze the image and return ONLY a valid JSON object:

{{
  "scene_type": "one of: text_image / personal_photo / family_gathering / religious_ceremony / political_rally / protest / military_activity / celebration / funeral / wedding / outdoor_activity / indoor_activity / other",
  "objects": ["specific objects clearly visible — flags, banners, weapons, vehicles, religious_items, uniforms, crowd etc. Empty array if text_image"],
  "activity": "specific activity — marching, praying, celebrating, protesting, posing, mourning, displaying text etc.",
  "crowd_size": "one of: none / small(2-10) / medium(10-50) / large(50-200) / massive(200+)",
  "political_symbols": "any political flags, party symbols, banners with slogans — describe or null",
  "religious_symbols": "any religious items, symbols, attire — describe or null",
  "weapons_visible": "describe any weapons, uniforms, military gear visible — or null",
  "cultural_context": "cultural clues — type of clothing, festival, ceremony, region-specific items — or null",
  "text_in_image": "transcribe ALL text visible in image as accurately as possible — or null if no text",
  "text_language": "language of text found in image (e.g. Urdu, Arabic, Hindi, Bengali, English) — or null",
  "location_clues": "landmarks, architecture style, vegetation, geography, signboards — or null",
  "estimated_location": "best guess city/state/country based on visual clues — or null",
  "confidence": 0
}}

Rules:
- confidence is 0-100 integer for location estimate confidence
- objects must be a JSON array — empty array [] if scene_type is text_image
- For text_image: focus on accurate text transcription and language detection
- Return ONLY the JSON object, no explanation, no markdown, no backticks"""


#  TEXT RATIO DETECTION — pre-filter heavy text images

def detect_text_ratio(image_b64):
    """
    Detect if image is heavily text-based using pytesseract.
    Returns ratio 0.0-1.0 based on how much text is found.
    """
    try:
        from PIL import Image, ImageFilter
        import io
        import pytesseract

        image_bytes = base64.b64decode(image_b64)
        img = Image.open(io.BytesIO(image_bytes))

        # Get OCR data with confidence scores
        data = pytesseract.image_to_data(
            img,
            lang='eng+urd+hin+ara+ben',
            output_type=pytesseract.Output.DICT
        )

        # Count words with confidence > 30
        total_words = 0
        confident_words = 0
        for i, conf in enumerate(data['conf']):
            try:
                c = int(conf)
                text = data['text'][i].strip()
                if not text:
                    continue
                total_words += 1
                if c > 30:
                    confident_words += 1
            except (ValueError, TypeError):
                continue

        if total_words == 0:
            return 0.0

        # Also check total text area vs image area
        img_area = img.width * img.height
        text_area = 0
        for i in range(len(data['text'])):
            try:
                if int(data['conf'][i]) > 30 and data['text'][i].strip():
                    w = data['width'][i]
                    h = data['height'][i]
                    text_area += w * h
            except (ValueError, TypeError):
                continue

        area_ratio = min(text_area / img_area if img_area > 0 else 0, 1.0)
        word_ratio = confident_words / total_words if total_words > 0 else 0

        # Combine both signals
        return (area_ratio * 0.6 + word_ratio * 0.4)

    except ImportError:
        # pytesseract not available — use PIL edge detection fallback
        try:
            from PIL import Image, ImageFilter
            import io
            image_bytes = base64.b64decode(image_b64)
            img = Image.open(io.BytesIO(image_bytes)).convert('L')
            img_small = img.resize((200, 200))
            edges = img_small.filter(ImageFilter.FIND_EDGES)
            edge_arr = list(edges.getdata())
            return sum(1 for p in edge_arr if p > 30) / len(edge_arr)
        except Exception:
            return 0.0
    except Exception as e:
        return 0.0


TEXT_RATIO_THRESHOLD  = 0.75  # ≥ 75% → heavily text, skip gemma
TEXT_MID_THRESHOLD    = 0.40  # 40-74% → OCR + text-aware gemma prompt


def extract_text_from_image(b64):
    """Extract text from image using pytesseract."""
    try:
        from PIL import Image
        import pytesseract
        import io
        image_bytes = base64.b64decode(b64)
        img = Image.open(io.BytesIO(image_bytes))
        raw = pytesseract.image_to_string(img, lang='eng+urd+hin+ara+ben', config='--psm 6')
        lines = [l.strip() for l in raw.splitlines() if l.strip() and len(l.strip()) > 2]
        return '\n'.join(lines) if lines else None
    except Exception:
        return None


def build_text_aware_prompt(ocr_text, caption, post_date):
    """Prompt for images with significant text content (40-74% text ratio)."""
    return f"""You are a SOCMINT image analyst. This image contains both visual content and text.

Post Caption : {caption or 'N/A'}
Post Date    : {post_date or 'N/A'}
OCR Extracted Text:
{ocr_text or 'N/A'}

Analyze the image combining both visual and text content. Return ONLY a valid JSON object:

{{
  "scene_type": "one of: text_image / personal_photo / family_gathering / religious_ceremony / political_rally / protest / military_activity / celebration / funeral / wedding / outdoor_activity / indoor_activity / other",
  "objects": ["specific objects clearly visible"],
  "activity": "main activity in the image",
  "crowd_size": "one of: none / small(2-10) / medium(10-50) / large(50-200) / massive(200+)",
  "political_symbols": "any political flags, party symbols, slogans — describe or null",
  "religious_symbols": "any religious items, symbols, attire — describe or null",
  "weapons_visible": "describe any weapons, uniforms, military gear — or null",
  "cultural_context": "cultural clues from both image and text — or null",
  "text_in_image": "the most meaningful text from the image (use OCR text above as reference)",
  "text_language": "language of text found",
  "location_clues": "location clues from image or text — or null",
  "estimated_location": "best guess location — or null",
  "confidence": 0
}}

Rules:
- Use the OCR text to understand the message/meaning of the image
- Do NOT hallucinate objects not visible
- confidence is 0-100 integer
- Return ONLY the JSON object, no markdown, no backticks"""


def build_vision_only_prompt(caption, post_date):
    """Simple focused prompt for scene images — only what matters for SOCMINT."""
    return f"""You are a SOCMINT image analyst. Analyze this image.

Post Caption : {caption or 'N/A'}
Post Date    : {post_date or 'N/A'}

Return ONLY a valid JSON object:

{{
  "scene_type": "one of: personal_photo / family_gathering / religious_ceremony / political_rally / protest / military_activity / celebration / outdoor_activity / indoor_activity / other",
  "objects": ["key objects visible — flags, banners, weapons, uniforms, crowd, religious items etc."],
  "activity": "what is happening in this image",
  "text_in_image": "any text visible in the image — or null",
  "location_clues": "landmarks, architecture, signs, vegetation, geography that hint at location — or null",
  "estimated_location": "best guess country/city based on visual clues — or null",
  "confidence": 0
}}

Rules:
- confidence is 0-100 for location estimate
- objects must be a JSON array
- Only describe what is clearly visible
- Return ONLY the JSON object, no markdown, no backticks"""

def analyze_image_ollama(image_b64, mime_type, prompt):
    """Send image to Ollama vision model and return analysis."""
    import ollama

    try:
        image_bytes = base64.b64decode(image_b64)

        response = ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{
                'role': 'user',
                'content': prompt,
                'images': [image_bytes]
            }],
            options={'temperature': 0.1}
        )

        raw = response['message']['content'].strip()

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        return json.loads(raw)

    except json.JSONDecodeError as e:
        print(f"      JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"      Ollama vision error: {e}")
        return None


#  FETCH UNANALYZED PHOTO POSTS

def fetch_unanalyzed_photos(db_file):
    """Fetch photo posts that have image_src but no image analysis yet."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.executescript(IMAGE_ANALYSIS_SCHEMA)
    con.commit()

    photos = []
    try:
        cur.execute("""
            SELECT
                pp.id,
                pp.photo_url,
                pp.image_src,
                pp.caption,
                pp.date_text
            FROM photo_posts pp
            WHERE pp.image_src IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM image_analysis ia
                WHERE ia.photo_post_id = pp.id
            )
        """)
        for row in cur.fetchall():
            photos.append({
                'id':        row[0],
                'photo_url': row[1],
                'image_src': row[2],
                'caption':   row[3],
                'date':      row[4]
            })
    except Exception as e:
        print(f"    Fetch error: {e}")

    con.close()
    return photos


#  SAVE RESULT

def save_image_result(db_file, photo_id, result):
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    try:
        objects_json = json.dumps(result.get('objects', []), ensure_ascii=False)

        # Fix — text_in_image might come back as list from LLM
        text_in_image = result.get('text_in_image')
        if isinstance(text_in_image, list):
            text_in_image = ' | '.join(text_in_image)

        cur.execute("""
            INSERT OR REPLACE INTO image_analysis
                (photo_post_id, scene_type, objects, activity,
                 crowd_size, political_symbols, religious_symbols,
                 weapons_visible, cultural_context,
                 text_in_image, text_language, location_clues,
                 estimated_location, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            photo_id,
            result.get('scene_type'),
            objects_json,
            result.get('activity'),
            result.get('crowd_size'),
            result.get('political_symbols'),
            result.get('religious_symbols'),
            result.get('weapons_visible'),
            result.get('cultural_context'),
            text_in_image,
            result.get('text_language'),
            result.get('location_clues'),
            result.get('estimated_location'),
            result.get('confidence', 0)
        ))
        con.commit()
        return True
    except Exception as e:
        print(f"      Save error: {e}")
        return False
    finally:
        con.close()


#  MAIN ANALYZER

def analyze_images(db_file=DB_FILE):
    print(f"\n{'═'*65}")
    print(f"Image Intelligence")
    print(f"Model : {OLLAMA_MODEL}")
    print(f"DB    : {db_file}")
    print("═"*65)

    if not check_ollama():
        return

    photos = fetch_unanalyzed_photos(db_file)
    total  = len(photos)
    print(f"   {total} unanalyzed photo posts found")

    if total == 0:
        print("   All images already analyzed")
        return

    success = 0
    skipped = 0

    for i, photo in enumerate(photos, 1):
        print(f"\n   [{i}/{total}] {photo['photo_url'][:70]}")
        print(f"     caption : {photo['caption'][:60] if photo['caption'] else 'N/A'}")
        print(f"     date    : {photo['date'] or 'N/A'}")

        # Download image
        print(f"      Downloading image...")
        b64, mime = download_image_base64(photo['image_src'])

        if not b64:
            print(f"       Skipping — download failed")
            skipped += 1
            continue

        # Pre-filter — check text ratio
        text_ratio = detect_text_ratio(b64)
        print(f"      Text ratio: {text_ratio:.2f}")

        if text_ratio >= TEXT_RATIO_THRESHOLD:
            # ≥ 75% — heavily text, skip gemma
            print(f"      Heavily text-based — skipping gemma")
            result = {
                'scene_type': 'text_image', 'objects': [],
                'activity': 'displaying text', 'crowd_size': 'none',
                'political_symbols': None, 'religious_symbols': None,
                'weapons_visible': None, 'cultural_context': None,
                'text_in_image': 'Heavily OCR found', 'text_language': None,
                'location_clues': None, 'estimated_location': None, 'confidence': 0
            }
            save_image_result(db_file, photo['id'], result)
            success += 1
            print(f"      Saved as text_image — Heavily OCR found")
            continue

        elif text_ratio >= TEXT_MID_THRESHOLD:
            # 40-74% — extract OCR text + text-aware gemma prompt
            print(f"      Mixed text+image — using OCR + text-aware prompt")
            ocr_text = extract_text_from_image(b64)
            if ocr_text:
                print(f"      OCR: {ocr_text[:60]}...")
            prompt = build_text_aware_prompt(ocr_text, photo['caption'], photo['date'])

        else:
            # < 40% — pure scene analysis prompt
            print(f"       Scene analysis prompt")
            prompt = build_vision_only_prompt(photo['caption'], photo['date'])

        # Analyze with gemma
        print(f"      Analyzing with {OLLAMA_MODEL}...")
        result = analyze_image_ollama(b64, mime, prompt)

        if result:
            save_image_result(db_file, photo['id'], result)
            success += 1
            print(f"      scene          : {result.get('scene_type')}")
            print(f"      objects        : {', '.join(result.get('objects', []))}")
            print(f"      activity       : {result.get('activity')}")
            print(f"      crowd_size     : {result.get('crowd_size')}")
            print(f"      political      : {result.get('political_symbols') or 'None'}")
            print(f"      religious      : {result.get('religious_symbols') or 'None'}")
            print(f"      weapons        : {result.get('weapons_visible') or 'None'}")
            print(f"      cultural       : {result.get('cultural_context') or 'None'}")
            print(f"      text           : {result.get('text_in_image') or 'None'}")
            print(f"      text_lang      : {result.get('text_language') or 'None'}")
            print(f"      location_clues : {result.get('location_clues') or 'None'}")
            print(f"      location       : {result.get('estimated_location') or 'Unknown'}")
            print(f"      confidence     : {result.get('confidence', 0)}%")
        else:
            print(f"       Analysis failed — skipping")
            skipped += 1

        time.sleep(DELAY)

    print(f"\n{'═'*65}")
    print(f" Done — {success} analyzed, {skipped} skipped out of {total}")


if __name__ == "__main__":
    analyze_images()