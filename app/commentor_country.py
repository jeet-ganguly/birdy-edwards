import sqlite3
import json
import time
import pickle
import os

OLLAMA_HOST = os.environ.get('OLLAMA_HOST', 'http://localhost:11434')
COOKIE_FILE = "fb_cookies.pkl"
DB_FILE     = "socmint.db"
OLLAMA_MODEL = "gemma3:4b"

QUICK_SECTIONS = [
    "directory_personal_details",
    "directory_work",
    "directory_education",
]

#  SCHEMA

COUNTRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS commentor_country (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    commentor_id            INTEGER UNIQUE NOT NULL,
    current_city            TEXT,
    hometown                TEXT,
    employer                TEXT,
    education               TEXT,
    identified_country      TEXT,
    country_confidence      INTEGER DEFAULT 0,
    identification_basis    TEXT,
    identified_at           TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (commentor_id) REFERENCES commentors(id)
);
"""


#  FETCH COMMENTORS FROM DB

def fetch_all_commentors(con, main_profile_id):
    """Fetch all commentors for a given main profile — skips already identified."""
    cur = con.cursor()
    results = {}

    for table, id_col in [
        ('photo_comments', 'photo_post_id'),
        ('reel_comments',  'reel_post_id'),
        ('text_comments',  'text_post_id'),
    ]:
        post_table = table.replace('_comments', '_posts')
        try:
            cur.execute(f"SELECT id FROM {post_table} WHERE profile_id = ?", (main_profile_id,))
            post_ids = [r[0] for r in cur.fetchall()]
            if not post_ids:
                continue

            cur.execute(f"""
                SELECT c.id, c.name, c.profile_url
                FROM {table} t
                JOIN commentors c ON c.id = t.commentor_id
                WHERE t.{id_col} IN ({','.join('?'*len(post_ids))})
                AND NOT EXISTS (
                    SELECT 1 FROM commentor_country cc
                    WHERE cc.commentor_id = c.id
                )
            """, post_ids)

            for row in cur.fetchall():
                cid = row[0]
                if cid not in results:
                    results[cid] = {
                        'commentor_id': cid,
                        'name':         row[1] or '',
                        'profile_url':  row[2] or ''
                    }
        except Exception as e:
            print(f"    fetch error ({table}): {e}")

    return list(results.values())


def fetch_batch_commentors(con, batch_id):
    """Fetch all commentors for a manual batch — skips already identified."""
    cur = con.cursor()
    results = {}

    try:
        cur.execute("""
            SELECT DISTINCT co.id, co.name, co.profile_url
            FROM comments c
            JOIN commentors co ON co.id = c.commentor_id
            JOIN manual_posts mp ON mp.id = c.post_id
            WHERE mp.batch_id = ?
            AND NOT EXISTS (
                SELECT 1 FROM commentor_country cc
                WHERE cc.commentor_id = co.id
            )
        """, (batch_id,))

        for row in cur.fetchall():
            cid = row[0]
            results[cid] = {
                'commentor_id': cid,
                'name':         row[1] or '',
                'profile_url':  row[2] or ''
            }
    except Exception as e:
        print(f"    fetch error: {e}")

    return list(results.values())


def get_comment_language(con, commentor_id):
    """Get dominant comment language for a commentor across all comment tables."""
    cur = con.cursor()

    for source, table in [('photo', 'photo_comments'), ('reel', 'reel_comments'), ('text', 'text_comments')]:
        try:
            cur.execute(f"""
                SELECT ca.language, COUNT(*) as cnt
                FROM comment_analysis ca
                JOIN {table} t ON t.id = ca.comment_id AND ca.db_source = '{source}'
                WHERE t.commentor_id = ?
                GROUP BY ca.language ORDER BY cnt DESC LIMIT 1
            """, (commentor_id,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
        except Exception:
            continue

    return None


#  QUICK SCRAPE — 3 sections, 3 profiles parallel using multiprocessing

BATCH_SIZE = 3  # parallel browser sessions — safe on 8GB RAM


def _scrape_single_profile(args):
    """
    Worker function for multiprocessing.
    Scrapes 3 sections for ONE profile and returns result dict.
    """
    import re
    commentor, cookie_file, sections = args
    cid  = commentor['commentor_id']
    purl = commentor['profile_url']
    name = commentor['name']

    if not purl or purl == 'unknown':
        return cid, {'current_city': None, 'hometown': None,
                     'employer': None, 'education': None}

    def get_section_url(p, section):
        p = p.rstrip('/')
        if 'profile.php' in p:
            return p + f'&sk={section}'
        return p + f'/{section}'

    def parse_section(source):
        fields  = {}
        pattern = re.compile(
            r'"field_type"\s*:\s*"([^"]+)"'
            r'.{0,300}?'
            r'"title"\s*:\s*\{'
            r'[^}]{0,300}?'
            r'"text"\s*:\s*"([^"]+)"',
            re.DOTALL
        )
        for m in pattern.finditer(source):
            ft  = m.group(1).lower()
            val = m.group(2)
            if ft in ('medium', 'high', 'low') or len(ft) > 50:
                continue
            if ft not in fields:
                fields[ft] = val
        return fields

    try:
        from seleniumbase import SB
        import pickle

        all_fields = {}

        with SB(uc=True, headless=False, xvfb=True, window_size="1024,768") as sb:
            sb.driver.set_page_load_timeout(15)  # max 15 sec per page
            sb.open("https://www.facebook.com")
            time.sleep(3)
            for c in pickle.load(open(cookie_file, "rb")):
                try: sb.driver.add_cookie(c)
                except: pass
            sb.driver.refresh()
            time.sleep(5)

            for section in sections:
                url = get_section_url(purl, section)
                try:
                    sb.open(url)
                    time.sleep(4)
                    source = sb.get_page_source()
                    fields = parse_section(source)
                    all_fields.update(fields)
                except Exception:
                    # Page timed out or failed — skip this section
                    print(f"      Skipped slow section: {section}")
                    pass

        current_city = all_fields.get('current_city')
        hometown     = all_fields.get('hometown')
        employer     = all_fields.get('work') or all_fields.get('employer')
        education    = (all_fields.get('college') or
                       all_fields.get('university') or
                       all_fields.get('high_school') or
                       all_fields.get('education'))

        print(f"   {name[:30]:30s} → city={current_city or 'N/A'}  "
              f"work={employer or 'N/A'}  edu={education or 'N/A'}")

        return cid, {
            'current_city': current_city,
            'hometown':     hometown,
            'employer':     employer,
            'education':    education
        }

    except Exception as e:
        print(f"    {name[:30]} scrape failed: {e}")
        return cid, {'current_city': None, 'hometown': None,
                     'employer': None, 'education': None}


def scrape_personal_details_batch(commentor_list):
    """
    Scrape 3 sections for all commentors using multiprocessing batches of 3.
    Each batch runs 3 browser sessions in parallel then merges results.
    Returns dict: {commentor_id: {current_city, hometown, employer, education}}
    """
    from multiprocessing import Pool
    import os

    results = {}
    total   = len(commentor_list)

    print(f"   Scraping {total} profiles in batches of {BATCH_SIZE}...")

    for i in range(0, total, BATCH_SIZE):
        batch = commentor_list[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\n   Batch {batch_num}/{total_batches} — profiles {i+1} to {min(i+BATCH_SIZE, total)}")
        for c in batch:
            print(f"     → {c['name'][:40]}")

        # Prepare args for each worker
        args = [(c, COOKIE_FILE, QUICK_SECTIONS) for c in batch]

        try:
            with Pool(processes=len(batch)) as pool:
                # 90 second timeout per batch — kills stuck workers
                async_result = pool.map_async(_scrape_single_profile, args)
                try:
                    batch_results = async_result.get(timeout=90)
                except Exception:
                    print(f"    Batch {batch_num} timed out — skipping stuck profiles")
                    pool.terminate()
                    # Use empty results for timed-out profiles
                    batch_results = [(c['id'], {'current_city': None, 'hometown': None,
                                                'employer': None, 'education': None})
                                     for c in batch]

            for cid, data in batch_results:
                results[cid] = data

        except Exception as e:
            print(f"    Batch {batch_num} failed: {e} — trying sequential fallback")
            # Sequential fallback with individual timeout
            for c in batch:
                try:
                    cid, data = _scrape_single_profile((c, COOKIE_FILE, QUICK_SECTIONS))
                    results[cid] = data
                except Exception as fe:
                    print(f"      Skipped {c['name'][:30]}: {fe}")
                    results[c['id']] = {'current_city': None, 'hometown': None,
                                        'employer': None, 'education': None}

        time.sleep(2)  # brief pause between batches

    print(f"\n   Scraping complete — {len(results)}/{total} profiles done")
    return results

#  COUNTRY IDENTIFICATION — using LLM


def identify_country_llm(name, current_city, hometown, employer, education, comment_language):
    """
    Using LLM to identify country from all available signals.
    Returns (country, confidence, basis) or (None, 0, None)
    """
    import ollama

    if not any([current_city, hometown, employer, education, name]):
        return None, 0, None

    prompt = f"""You are a SOCMINT analyst. Based on the following information about a Facebook user, identify which country they are most likely from.

Name          : {name or 'N/A'}
Current City  : {current_city or 'N/A'}
Hometown      : {hometown or 'N/A'}
Employer      : {employer or 'N/A'}
Education     : {education or 'N/A'}
Comment Lang  : {comment_language or 'N/A'}

Return ONLY a valid JSON object:
{{
  "country": "country name in English — or null if cannot determine",
  "confidence": 0,
  "basis": "signals used — city / hometown / employer / education / name / language / combined"
}}

Rules:
- confidence is 0-100 integer
- City or hometown clearly matches → confidence 80-95
- Employer or university clearly matches → confidence 70-85
- Only name → confidence 30-50
- Diaspora case (Indian name + London city) → state both origin and residence
- No useful data → country null, confidence 0
- Return ONLY the JSON object, no markdown, no backticks"""

    try:
        client = ollama.Client(host=OLLAMA_HOST)
        response = client.chat(
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
        result  = json.loads(raw)
        country = result.get('country')

        # Normalize unknown/null responses to None
        if country:
            null_values = {
                'null', 'none', 'unknown', 'not determined',
                'n/a', 'na', 'not available', 'cannot determine',
                'undetermined', 'unclear', 'unidentified'
            }
            if country.lower().strip() in null_values:
                country = None

        return (
            country,
            result.get('confidence', 0),
            result.get('basis')
        )
    except Exception as e:
        print(f"        LLM error: {e}")
        return None, 0, None


#  SAVE RESULT


def save_country_identification(con, commentor_id, current_city, hometown,
                                 employer, education, country, confidence, basis):
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO commentor_country
                (commentor_id, current_city, hometown, employer, education,
                 identified_country, country_confidence, identification_basis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (commentor_id, current_city, hometown, employer, education,
              country, confidence, basis))
        con.commit()
    except Exception as e:
        print(f"        Save error: {e}")


#  MAIN RUN FUNCTIONS


def run_country_identification(con, commentor_list, personal_details):
    """
    Run LLM country identification for all commentors.
    Works for both automated profile and manual batch.
    """
    print(f"\n{'═'*65}")
    print(f"  Country Identification — {len(commentor_list)} commentors")
    print("═"*65)

    country_summary = {}
    identified      = 0

    for commentor in commentor_list:
        cid  = commentor['commentor_id']
        name = commentor['name']

        details      = personal_details.get(cid, {})
        current_city = details.get('current_city')
        hometown     = details.get('hometown')
        employer     = details.get('employer')
        education    = details.get('education')
        comment_lang = get_comment_language(con, cid)

        print(f"\n   {name[:40]:40s}")
        print(f"     city={current_city or 'N/A'}  hometown={hometown or 'N/A'}")
        print(f"     work={employer or 'N/A'}  edu={education or 'N/A'}  lang={comment_lang or 'N/A'}")

        country, confidence, basis = identify_country_llm(
            name, current_city, hometown, employer, education, comment_lang
        )

        save_country_identification(
            con, cid, current_city, hometown, employer, education,
            country, confidence, basis
        )

        if country:
            identified += 1
            country_summary[country] = country_summary.get(country, 0) + 1
            print(f"      {country} ({confidence}% — {basis})")
        else:
            print(f"      Unknown")

    # Print distribution chart
    print(f"\n{'═'*65}")
    print(f"  NETWORK COUNTRY DISTRIBUTION")
    print("═"*65)
    print(f"  Identified : {identified}/{len(commentor_list)}")
    print(f"  Unknown    : {len(commentor_list) - identified}")
    print()
    for country, count in sorted(country_summary.items(), key=lambda x: -x[1]):
        pct = count / len(commentor_list) * 100
        bar = '█' * int(pct / 5)
        print(f"  {country:20s} {count:3d} ({pct:4.1f}%) {bar}")

    return country_summary


# How many scrape batches to collect before running LLM identification
LLM_FLUSH_EVERY = 5  # flush after every 5 scrape batches = 15 profiles

def _scrape_and_identify(con, commentor_list):
    """
    Interleaved pipeline:
    - Scrape BATCH_SIZE profiles in parallel
    - After every LLM_FLUSH_EVERY scrape batches, run LLM and save to DB
    - UI map and flags update progressively after each flush
    """
    from multiprocessing import Pool

    total          = len(commentor_list)
    pending        = {}   # cid -> scraped details, waiting for LLM
    scrape_batches = 0

    print(f"   Scraping {total} profiles — flushing to LLM every {LLM_FLUSH_EVERY} batches")

    for i in range(0, total, BATCH_SIZE):
        batch     = commentor_list[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\n   Scrape batch {batch_num}/{total_batches}")
        for c in batch:
            print(f"     → {c['name'][:40]}")

        args = [(c, COOKIE_FILE, QUICK_SECTIONS) for c in batch]

        try:
            with Pool(processes=len(batch)) as pool:
                async_result = pool.map_async(_scrape_single_profile, args)
                try:
                    batch_results = async_result.get(timeout=90)
                except Exception:
                    print(f"    Batch {batch_num} timed out")
                    pool.terminate()
                    batch_results = [
                        (c['commentor_id'], {'current_city': None, 'hometown': None,
                                             'employer': None, 'education': None})
                        for c in batch
                    ]
            for cid, data in batch_results:
                pending[cid] = data

        except Exception as e:
            print(f"    Batch {batch_num} failed: {e}")
            for c in batch:
                try:
                    cid, data = _scrape_single_profile((c, COOKIE_FILE, QUICK_SECTIONS))
                    pending[cid] = data
                except Exception:
                    pending[c['commentor_id']] = {
                        'current_city': None, 'hometown': None,
                        'employer': None, 'education': None
                    }

        scrape_batches += 1
        time.sleep(2)

        # Flush to LLM every LLM_FLUSH_EVERY scrape batches
        if scrape_batches % LLM_FLUSH_EVERY == 0 or (i + BATCH_SIZE) >= total:
            if pending:
                print(f"\n  🤖 Running LLM on {len(pending)} pending profiles...")
                _identify_and_save(con, commentor_list, pending)
                pending = {}  # clear after flush

    # Final flush for any remaining
    if pending:
        print(f"\n  🤖 Final LLM flush — {len(pending)} profiles...")
        _identify_and_save(con, commentor_list, pending)


def _identify_and_save(con, commentor_list, personal_details):
    """Run LLM country identification for pending profiles and save immediately."""
    cid_to_commentor = {c['commentor_id']: c for c in commentor_list}

    for cid, details in personal_details.items():
        commentor    = cid_to_commentor.get(cid, {})
        name         = commentor.get('name', '')
        current_city = details.get('current_city')
        hometown     = details.get('hometown')
        employer     = details.get('employer')
        education    = details.get('education')
        comment_lang = get_comment_language(con, cid)

        print(f"   {name[:40]:40s} → ", end='', flush=True)

        country, confidence, basis = identify_country_llm(
            name, current_city, hometown, employer, education, comment_lang
        )

        save_country_identification(
            con, cid, current_city, hometown, employer, education,
            country, confidence, basis
        )

        if country:
            print(f" {country} ({confidence}%)")
        else:
            print(f" Unknown")
            

def run_for_profile(profile_url, db_file=DB_FILE):
    print(f"\n{'═'*65}")
    print(f"Commentor Country Detection — Profile")
    print(f"Profile : {profile_url}")
    print("═"*65)

    con = sqlite3.connect(db_file)
    con.executescript(COUNTRY_SCHEMA)
    con.commit()

    cur = con.cursor()
    cur.execute("SELECT id FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if not row:
        print(f"    Profile not found: {profile_url}")
        con.close()
        return

    main_profile_id = row[0]
    commentor_list  = fetch_all_commentors(con, main_profile_id)
    print(f"   {len(commentor_list)} unidentified commentors found")

    if not commentor_list:
        print("   All commentors already identified")
        con.close()
        return

    # Interleaved: scrape N batches then identify, repeat
    _scrape_and_identify(con, commentor_list)

    con.close()
    print(f"\n Done → {db_file}")


def run_for_batch(batch_id, db_file="socmint_manual.db"):
    print(f"\n{'═'*65}")
    print(f"Commentor Country Detection — Batch")
    print(f"Batch ID : {batch_id}")
    print("═"*65)

    con = sqlite3.connect(db_file)
    con.executescript(COUNTRY_SCHEMA)
    con.commit()

    cur = con.cursor()
    cur.execute("SELECT label FROM batches WHERE batch_id = ?", (batch_id,))
    row = cur.fetchone()
    if not row:
        print(f"    Batch not found: {batch_id}")
        con.close()
        return

    print(f"   Label: {row[0]}")
    commentor_list = fetch_batch_commentors(con, batch_id)
    print(f"   {len(commentor_list)} unidentified commentors found")

    if not commentor_list:
        print("   All commentors already identified")
        con.close()
        return

    # Interleaved: scrape N batches then identify, repeat
    _scrape_and_identify(con, commentor_list)

    con.close()
    print(f"\n Done → {db_file}")


if __name__ == "__main__":
    print("Commentor Country Detection")
    print("1 -> Automated profile")
    print("2 -> Manual batch")
    choice = input("Choice: ").strip()

    if choice == "1":
        profile = input("Enter profile URL: ").strip()
        run_for_profile(profile)
    elif choice == "2":
        batch = input("Enter batch ID: ").strip()
        run_for_batch(batch)
    else:
        print("Invalid choice")