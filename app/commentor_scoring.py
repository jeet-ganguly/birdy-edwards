import sqlite3
import json
import os
import time
import pickle

DB_FILE     = "socmint.db"
COOKIE_FILE = "fb_cookies.pkl"
TOP_N       = 7   # top N supporters and opposition

#  SECONDARY PROFILE SCHEMA

SECONDARY_SCHEMA = """
-- Secondary profiles 
CREATE TABLE IF NOT EXISTS secondary_profiles (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    main_profile_id     INTEGER NOT NULL,
    commentor_id        INTEGER NOT NULL,
    profile_url         TEXT NOT NULL,
    name                TEXT,
    relationship_type   TEXT NOT NULL,
    score               REAL DEFAULT 0.0,
    comment_count       INTEGER DEFAULT 0,
    scraped_at          TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (main_profile_id) REFERENCES profiles(id),
    FOREIGN KEY (commentor_id)    REFERENCES commentors(id),
    UNIQUE(main_profile_id, commentor_id)
);

-- About fields for secondary profiles
CREATE TABLE IF NOT EXISTS secondary_profile_fields (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    secondary_profile_id    INTEGER NOT NULL,
    section                 TEXT,
    field_type              TEXT,
    label                   TEXT,
    value                   TEXT,
    sub_label               TEXT,
    FOREIGN KEY (secondary_profile_id) REFERENCES secondary_profiles(id)
);

-- Scores for ALL commentors (not just top/bottom)
CREATE TABLE IF NOT EXISTS commentor_scores (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    main_profile_id     INTEGER NOT NULL,
    commentor_id        INTEGER NOT NULL,
    comment_count       INTEGER DEFAULT 0,
    sentiment_score     REAL DEFAULT 0.0,
    emotion_score       REAL DEFAULT 0.0,
    stance_score        REAL DEFAULT 0.0,
    total_score         REAL DEFAULT 0.0,
    tier                TEXT,
    calculated_at       TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (main_profile_id) REFERENCES profiles(id),
    FOREIGN KEY (commentor_id)    REFERENCES commentors(id),
    UNIQUE(main_profile_id, commentor_id)
);

-- Country identification for ALL commentors
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

#  SCORING WEIGHTS

# Sentiment weights
SENTIMENT_WEIGHTS = {
    'positive':  1.0,
    'neutral':   0.0,
    'negative': -1.0
}

# Emotion weights — represent INTENSITY of engagement
# Direction is handled by stance context in calculate_score()
EMOTION_WEIGHTS = {
    'support':     0.3,   # mild positive engagement
    'anger':       0.8,   # high intensity — direction from stance
    'sarcasm':     0.4,   # moderate intensity
    'aggressive':  1.0    # highest intensity — direction from stance
}

# Stance weights (strongest signal)
STANCE_WEIGHTS = {
    'support_post':       1.5,
    'neutral_discussion': 0.0,
    'oppose_post':       -1.5
}

#  FETCH DATA FROM DB

def get_main_profile_id(con, profile_url):
    cur = con.cursor()
    cur.execute("SELECT id FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    return row[0] if row else None


def fetch_commentor_analysis(con, main_profile_id):
    """
    Fetch all commentors + their comment analysis for a given profile.
    Joins across photo_comments, reel_comments, text_comments.
    """
    cur = con.cursor()
    results = {}  # commentor_id -> {name, profile_url, comments: [{sentiment, emotion, stance}]}

    for table, id_col, source in [
        ('photo_comments', 'photo_post_id', 'photo'),
        ('reel_comments',  'reel_post_id',  'reel'),
        ('text_comments',  'text_post_id',  'text')
    ]:
        # Get post IDs for this profile
        post_table = table.replace('_comments', '_posts')
        try:
            cur.execute(f"SELECT id FROM {post_table} WHERE profile_id = ?", (main_profile_id,))
            post_ids = [r[0] for r in cur.fetchall()]
        except Exception:
            continue

        if not post_ids:
            continue

        # Get comments + analysis
        try:
            cur.execute(f"""
                SELECT
                    c.id        AS commentor_id,
                    c.name,
                    c.profile_url,
                    t.id        AS comment_id,
                    ca.sentiment,
                    ca.emotion,
                    ca.stance
                FROM {table} t
                JOIN commentors c ON c.id = t.commentor_id
                LEFT JOIN comment_analysis ca ON ca.comment_id = t.id
                    AND ca.db_source = '{source}'
                WHERE t.{id_col} IN ({','.join('?'*len(post_ids))})
            """, post_ids)

            for row in cur.fetchall():
                cid = row[0]
                if cid not in results:
                    results[cid] = {
                        'commentor_id': cid,
                        'name':         row[1] or '',
                        'profile_url':  row[2] or '',
                        'comments':     []
                    }
                results[cid]['comments'].append({
                    'sentiment': row[4],
                    'emotion':   row[5],
                    'stance':    row[6]
                })
        except Exception as e:
            print(f"    fetch error ({source}): {e}")

    return list(results.values())

#  SCORING ENGINE

def calculate_score(commentor_data):
    """
    Calculate total score for a commentor.
    Returns (total_score, sentiment_score, emotion_score, stance_score, comment_count)
    """
    comments    = commentor_data['comments']
    count       = len(comments)

    if count == 0:
        return 0.0, 0.0, 0.0, 0.0, 0

    sentiment_total = 0.0
    emotion_total   = 0.0
    stance_total    = 0.0

    for c in comments:
        sentiment = c.get('sentiment') or 'neutral'
        emotion   = c.get('emotion')   or 'support'
        stance    = c.get('stance')    or 'neutral_discussion'

        sentiment_total += SENTIMENT_WEIGHTS.get(sentiment, 0.0)

        # Emotion amplifies stance direction
        # aggressive/anger supporter → strong supporter (positive boost)
        # aggressive/anger opposer   → strong opposition (negative boost)
        emotion_intensity = abs(EMOTION_WEIGHTS.get(emotion, 0.0))
        if stance == 'support_post':
            emotion_total += emotion_intensity   # boost supporter score
        elif stance == 'oppose_post':
            emotion_total -= emotion_intensity   # boost opposition score
        else:
            emotion_total += EMOTION_WEIGHTS.get(emotion, 0.0)  # neutral context

        stance_total += STANCE_WEIGHTS.get(stance, 0.0)

    # Average per comment
    sentiment_score = sentiment_total / count
    emotion_score   = emotion_total   / count
    stance_score    = stance_total    / count

    # Frequency bonus — more comments = stronger signal
    frequency_score = min(count / 10.0, 1.0)  # caps at 10 comments

    # Total weighted score
    total_score = (
        stance_score    * 0.40 +   # strongest signal
        sentiment_score * 0.25 +
        emotion_score   * 0.20 +
        frequency_score * 0.25     # frequency bonus
    )

    return round(total_score, 4), round(sentiment_score, 4), \
           round(emotion_score, 4), round(stance_score, 4), count


def score_and_rank(commentor_list, top_n=TOP_N):
    """
    Score all commentors and assign tiers.
    Returns sorted list with scores and tiers.
    """
    scored = []
    for c in commentor_list:
        total, sent, emo, stance, count = calculate_score(c)
        scored.append({
            'commentor_id':   c['commentor_id'],
            'name':           c['name'],
            'profile_url':    c['profile_url'],
            'comment_count':  count,
            'total_score':    total,
            'sentiment_score': sent,
            'emotion_score':   emo,
            'stance_score':    stance,
        })

    # Sort by total_score descending, then comment_count, then name
    scored.sort(key=lambda x: (-x['total_score'], -x['comment_count'], x['name']))

    total = len(scored)
    effective_n = min(top_n, max(1, total // 3)) if total >= 3 else total

    # Assign tiers
    # Assign tiers based on actual score value — consistent with PDF report
    for c in scored:
        score = c['total_score']
        if score > 0.5:
            c['tier'] = 'Strong Supporter'
        elif score > 0.1:
            c['tier'] = 'Supporter'
        elif score > -0.1:
            c['tier'] = 'Neutral'
        elif score > -0.5:
            c['tier'] = 'Low Interaction'
        else:
            c['tier'] = 'Critical Voice'

    return scored


#  SAVE SCORES TO DB


def save_scores(con, main_profile_id, scored_list):
    cur = con.cursor()
    cur.executescript(SECONDARY_SCHEMA)
    con.commit()

    for c in scored_list:
        cur.execute("""
            INSERT OR REPLACE INTO commentor_scores
                (main_profile_id, commentor_id, comment_count,
                 sentiment_score, emotion_score, stance_score,
                 total_score, tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            main_profile_id,
            c['commentor_id'],
            c['comment_count'],
            c['sentiment_score'],
            c['emotion_score'],
            c['stance_score'],
            c['total_score'],
            c['tier']
        ))

    con.commit()
    print(f"   Saved scores for {len(scored_list)} commentors")


def save_secondary_profile(con, main_profile_id, commentor):
    cur = con.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO secondary_profiles
                (main_profile_id, commentor_id, profile_url, name,
                 relationship_type, score, comment_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            main_profile_id,
            commentor['commentor_id'],
            commentor['profile_url'],
            commentor['name'],
            commentor['tier'],
            commentor['total_score'],
            commentor['comment_count']
        ))
        con.commit()
        return cur.lastrowid
    except Exception as e:
        print(f"      secondary profile save error: {e}")
        return None


def save_secondary_profile_fields(con, secondary_profile_id, sections):
    """Save about fields for a secondary profile."""
    cur = con.cursor()
    count = 0
    for section, fields in sections.items():
        for f in fields:
            cur.execute("""
                INSERT INTO secondary_profile_fields
                    (secondary_profile_id, section, field_type, label, value, sub_label)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                secondary_profile_id,
                section,
                f.get('field_type'),
                f.get('label'),
                f.get('value'),
                f.get('sub_label')
            ))
            count += 1
    con.commit()
    return count

#  SCRAPE ABOUT FOR SECONDARY PROFILES (TOP 7 ONLY — FULL)

def scrape_about_for_profile(profile_url):
    """
    Scrape about sections for a secondary profile using fb_about_sb logic.
    Returns sections dict or None if failed.
    """
    import re

    DIRECTORY_SECTIONS = [
        "directory_personal_details",
        "directory_work",
        "directory_education",
        "directory_intro",
        "activities",
        "directory_names",
    ]

    FIELD_LABELS = {
        "current_city": "Current City", "hometown": "Hometown",
        "relationship": "Relationship", "family": "Family Member",
        "work": "Work", "employer": "Employer",
        "college": "College", "high_school": "High School",
        "education": "Education", "intro": "Introduction",
        "hobby": "Hobby", "hobbies": "Hobbies",
        "nickname": "Nickname", "other_name": "Other Name",
    }

    def get_directory_url(purl, section):
        purl = purl.rstrip('/')
        if 'profile.php' in purl:
            return purl + f"&sk={section}"
        return purl + f"/{section}"

    def parse_page_source(source, section):
        results = []
        seen_keys = set()
        main_pattern = re.compile(
            r'"field_type"\s*:\s*"([^"]+)"'
            r'.{0,300}?'
            r'"title"\s*:\s*\{'
            r'[^}]{0,300}?'
            r'"text"\s*:\s*"([^"]+)"',
            re.DOTALL
        )
        label_pattern = re.compile(
            r'"list_items"\s*:\s*\[\s*\{'
            r'[^}]{0,200}?'
            r'"text"\s*:\s*\{'
            r'[^}]{0,200}?'
            r'"text"\s*:\s*"([^"]+)"',
            re.DOTALL
        )
        for m in main_pattern.finditer(source):
            field_type = m.group(1)
            value      = m.group(2)
            if field_type in ('MEDIUM', 'HIGH', 'LOW') or len(field_type) > 50:
                continue
            key = f"{field_type}:{value}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            label = FIELD_LABELS.get(field_type, field_type.replace('_', ' ').title())
            results.append({'field_type': field_type, 'label': label, 'value': value, 'sub_label': None})
        sub_labels = label_pattern.findall(source)
        sub_idx = 0
        for r in results:
            if r["field_type"] in ("family", "relationship") and sub_idx < len(sub_labels):
                r["sub_label"] = sub_labels[sub_idx]
                sub_idx += 1
        return results

    def parse_directory_items(source, section):
        results = []
        seen    = set()
        pattern = re.compile(
            r'"group_key"\s*:\s*"([^"]+)"'
            r'.{0,500}?'
            r'"renderer"\s*:\s*\{'
            r'.{0,300}?'
            r'"title"\s*:\s*\{'
            r'.{0,200}?'
            r'"text"\s*:\s*"([^"]+)"',
            re.DOTALL
        )
        for m in pattern.finditer(source):
            group_key = m.group(1)
            value     = m.group(2)
            key = f"{group_key}:{value}"
            if key in seen:
                continue
            seen.add(key)
            label = group_key.replace('_', ' ').title()
            results.append({'field_type': group_key.lower(), 'label': label, 'value': value, 'sub_label': None})
        return results

    try:
        from seleniumbase import SB

        sections_data = {}

        with SB(uc=True, headless=False, xvfb=True, window_size="1280,900") as sb:
            # Login
            sb.open("https://www.facebook.com")
            time.sleep(3)
            for c in pickle.load(open(COOKIE_FILE, "rb")):
                try: sb.driver.add_cookie(c)
                except: pass
            sb.driver.refresh()
            time.sleep(5)

            for section in DIRECTORY_SECTIONS:
                url = get_directory_url(profile_url, section)
                try:
                    sb.open(url)
                    time.sleep(5)
                    source = sb.get_page_source()

                    if section == "activities":
                        fields = parse_directory_items(source, section)
                    else:
                        fields = parse_page_source(source, section)

                    if fields:
                        sections_data[section] = [
                            {'field_type': f['field_type'], 'label': f['label'],
                             'value': f['value'], 'sub_label': f.get('sub_label')}
                            for f in fields
                        ]
                except Exception as e:
                    print(f"        Section {section} error: {e}")

        return sections_data

    except Exception as e:
        print(f"      Scraping failed for {profile_url}: {e}")
        return None


#  MAIN SCORING FUNCTION

def run_scoring(profile_url, db_file=DB_FILE, scrape_about=True):
    print(f"\n{'═'*65}")
    print(f"Commentor Scoring & Ranking")
    print(f"Profile : {profile_url}")
    print(f"Top N   : {TOP_N}")
    print("═"*65)

    con = sqlite3.connect(db_file)

    # Ensure secondary schema exists
    con.executescript(SECONDARY_SCHEMA)
    con.commit()

    # Get main profile ID
    main_profile_id = get_main_profile_id(con, profile_url)
    if not main_profile_id:
        print(f"    Profile not found in DB: {profile_url}")
        con.close()
        return None

    # Fetch all commentor data
    print(f"\n  📊 Fetching commentor data...")
    commentor_list = fetch_commentor_analysis(con, main_profile_id)
    print(f"  👥 Total unique commentors: {len(commentor_list)}")

    if not commentor_list:
        print(f"    No commentors found")
        con.close()
        return None

    # Score and rank
    print(f"\n  Calculating scores...")
    scored = score_and_rank(commentor_list, TOP_N)

    # Save all scores
    save_scores(con, main_profile_id, scored)

    # Print results
    supporters  = [c for c in scored if c['tier'] in ('Strong Supporter', 'Supporter')]
    opposition  = [c for c in scored if c['tier'] == 'Critical Voice']
    neutral     = [c for c in scored if c['tier'] in ('Neutral', 'Low Interaction')]

    print(f"\n{'═'*65}")
    print(f"  SCORING RESULTS")
    print(f"{'═'*65}")
    print(f"  Total commentors : {len(scored)}")
    print(f"  Supporters       : {len(supporters)}")
    print(f"  Opposition       : {len(opposition)}")
    print(f"  Neutral          : {len(neutral)}")

    print(f"\n  🟢 TOP SUPPORTING VOICE:")
    for i, c in enumerate(supporters, 1):
        print(f"    {i}. {c['name']:30s} score={c['total_score']:+.3f}  comments={c['comment_count']}  {c['profile_url']}")

    print(f"\n  🔴 TOP CRITICAL VOICE:")
    for i, c in enumerate(opposition, 1):
        print(f"    {i}. {c['name']:30s} score={c['total_score']:+.3f}  comments={c['comment_count']}  {c['profile_url']}")

    print(f"\n  ⚪ NEUTRAL ({len(neutral)}):")
    for c in neutral[:5]:
        print(f"       {c['name']:30s} score={c['total_score']:+.3f}  comments={c['comment_count']}")
    if len(neutral) > 5:
        print(f"       ... and {len(neutral)-5} more")

    con.close()
    print(f"\n Scoring complete → {db_file}")
    return scored


def scrape_top14_about(profile_url, db_file=DB_FILE): #Do not confuse through function naming it scrapes 7 profile
    """
    Scrape full about for top 7 supporters only.
    Call AFTER country detection so data is already enriched.
    """
    print(f"\n{'═'*65}")
    print(f"  Full About Scrape — Top 7 Profiles Only")
    print("═"*65)

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.execute("SELECT id FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if not row:
        print(f"    Profile not found: {profile_url}")
        con.close()
        return

    main_profile_id = row[0]

    # Get top 7 by score only (supporters)
    cur.execute("""
        SELECT cs.commentor_id, cs.tier, cs.total_score, cs.comment_count,
               co.name, co.profile_url
        FROM commentor_scores cs
        JOIN commentors co ON co.id = cs.commentor_id
        WHERE cs.main_profile_id = ?
        ORDER BY cs.total_score DESC
        LIMIT 7
    """, (main_profile_id,))

    top_14 = []
    for row in cur.fetchall():
        top_14.append({
            'commentor_id': row[0],
            'tier':         row[1],
            'total_score':  row[2],
            'comment_count':row[3],
            'name':         row[4] or '',
            'profile_url':  row[5] or ''
        })

    if not top_14:
        print("    No scored profiles found — run scoring first")
        con.close()
        return

    print(f"   Scraping {len(top_14)} profiles...")

    for c in top_14:
        purl = c['profile_url']
        print(f"\n  [{c['tier'].upper()}] {c['name']} — {purl}")

        # Save secondary profile record
        sec_id = save_secondary_profile(con, main_profile_id, c)
        if not sec_id:
            continue

        print(f"     Scraping all about sections...")
        sections = scrape_about_for_profile(purl)

        if sections:
            field_count = save_secondary_profile_fields(con, sec_id, sections)
            print(f"     Saved {field_count} fields")
            for section, fields in sections.items():
                for f in fields:
                    sub = f" ({f['sub_label']})" if f.get('sub_label') else ""
                    print(f"       {f['label']:20s} → {f['value']}{sub}")
        else:
            print(f"      No about data found (locked or empty profile)")

        time.sleep(3)

    con.close()
    print(f"\n Top 7 about scrape complete → {db_file}")

def delete_secondary_data(profile_url, db_file=DB_FILE):
    """Delete all secondary profile data for a main profile."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.execute("SELECT id FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if not row:
        con.close()
        return

    profile_id = row[0]

    try:
        # Collect commentor IDs FIRST before deleting anything
        cur.execute("""
            SELECT DISTINCT commentor_id FROM commentor_scores
            WHERE main_profile_id = ?
        """, (profile_id,))
        commentor_ids = [r[0] for r in cur.fetchall()]

        # Get secondary profile IDs
        cur.execute("SELECT id FROM secondary_profiles WHERE main_profile_id = ?", (profile_id,))
        sec_ids = [r[0] for r in cur.fetchall()]

        if sec_ids:
            cur.execute(
                f"DELETE FROM secondary_profile_fields WHERE secondary_profile_id IN ({','.join('?'*len(sec_ids))})",
                sec_ids
            )
            print(f"   Deleted secondary profile fields: {cur.rowcount}")

        cur.execute("DELETE FROM secondary_profiles WHERE main_profile_id = ?", (profile_id,))
        print(f"   Deleted secondary profiles: {cur.rowcount}")

        cur.execute("DELETE FROM commentor_scores WHERE main_profile_id = ?", (profile_id,))
        print(f"   Deleted commentor scores: {cur.rowcount}")

        # Delete country identification using pre-collected IDs
        if commentor_ids:
            try:
                cur.execute(
                    f"DELETE FROM commentor_country WHERE commentor_id IN ({','.join('?'*len(commentor_ids))})",
                    commentor_ids
                )
                print(f"   Deleted country identifications: {cur.rowcount}")
            except Exception:
                pass

        con.commit()
    except Exception as e:
        print(f"    Delete secondary data error: {e}")
    finally:
        con.close()


if __name__ == "__main__":
    profile = input("Enter profile URL to score: ").strip()
    scrape  = input("Scrape about for top 14? (y/n): ").strip().lower() == 'y'
    run_scoring(profile, scrape_about=scrape)


#  BATCH SCORING — for manual DB (socmint_manual.db)


MANUAL_BATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS batch_commentor_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL,
    commentor_id    INTEGER NOT NULL,
    comment_count   INTEGER DEFAULT 0,
    sentiment_score REAL DEFAULT 0.0,
    emotion_score   REAL DEFAULT 0.0,
    stance_score    REAL DEFAULT 0.0,
    total_score     REAL DEFAULT 0.0,
    tier            TEXT,
    calculated_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
    UNIQUE(batch_id, commentor_id)
);
"""


def fetch_batch_commentor_analysis(con, batch_id):
    """Fetch all commentors + analysis for a manual batch."""
    cur = con.cursor()
    results = {}

    # Ensure comment_analysis table exists
    try:
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS comment_analysis (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                comment_id  INTEGER NOT NULL,
                db_source   TEXT NOT NULL,
                sentiment   TEXT,
                emotion     TEXT,
                stance      TEXT,
                language    TEXT,
                analyzed_at TEXT DEFAULT (datetime('now')),
                UNIQUE(comment_id, db_source)
            );
        """)
        con.commit()
    except Exception:
        pass

    try:
        cur.execute("""
            SELECT
                co.id       AS commentor_id,
                co.name,
                co.profile_url,
                c.id        AS comment_id,
                ca.sentiment,
                ca.emotion,
                ca.stance
            FROM comments c
            JOIN commentors co ON co.id = c.commentor_id
            JOIN manual_posts mp ON mp.id = c.post_id
            LEFT JOIN comment_analysis ca ON ca.comment_id = c.id
                AND ca.db_source = 'manual'
            WHERE mp.batch_id = ?
        """, (batch_id,))

        for row in cur.fetchall():
            cid = row[0]
            if cid not in results:
                results[cid] = {
                    'commentor_id': cid,
                    'name':         row[1] or '',
                    'profile_url':  row[2] or '',
                    'comments':     []
                }
            results[cid]['comments'].append({
                'sentiment': row[4],
                'emotion':   row[5],
                'stance':    row[6]
            })
    except Exception as e:
        print(f"    Batch fetch error: {e}")

    return list(results.values())


def run_batch_scoring(batch_id, db_file="socmint_manual.db", scrape_about=True):
    print(f"\n{'═'*65}")
    print(f"Batch Commentor Scoring")
    print(f"Batch ID : {batch_id}")
    print(f"Top N    : {TOP_N}")
    print("═"*65)

    con = sqlite3.connect(db_file)

    # Ensure schema
    con.executescript(MANUAL_BATCH_SCHEMA)
    con.commit()

    # Check batch exists
    cur = con.cursor()
    cur.execute("SELECT label FROM batches WHERE batch_id = ?", (batch_id,))
    row = cur.fetchone()
    if not row:
        print(f"    Batch not found: {batch_id}")
        con.close()
        return None
    print(f"   Label: {row[0]}")

    # Fetch commentor data
    print(f"\n   Fetching commentor data...")
    commentor_list = fetch_batch_commentor_analysis(con, batch_id)
    print(f"   Total unique commentors: {len(commentor_list)}")

    if not commentor_list:
        print(f"    No commentors found")
        con.close()
        return None

    # Score and rank
    print(f"\n   Calculating scores...")
    scored = score_and_rank(commentor_list, TOP_N)

    # Save scores
    for c in scored:
        cur.execute("""
            INSERT OR REPLACE INTO batch_commentor_scores
                (batch_id, commentor_id, comment_count,
                 sentiment_score, emotion_score, stance_score,
                 total_score, tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            batch_id,
            c['commentor_id'],
            c['comment_count'],
            c['sentiment_score'],
            c['emotion_score'],
            c['stance_score'],
            c['total_score'],
            c['tier']
        ))
    con.commit()
    print(f"   Saved scores for {len(scored)} commentors")

    # Print results
    supporters = [c for c in scored if c['tier'] in ('Strong Supporter', 'Supporter')]
    opposition = [c for c in scored if c['tier'] == 'Critical Voice']
    neutral    = [c for c in scored if c['tier'] in ('Neutral', 'Low Interaction')]

    print(f"\n{'═'*65}")
    print(f"  BATCH SCORING RESULTS — {batch_id}")
    print(f"{'═'*65}")
    print(f"  Total commentors : {len(scored)}")
    print(f"  Supporters       : {len(supporters)}")
    print(f"  Opposition       : {len(opposition)}")
    print(f"  Neutral          : {len(neutral)}")

    print(f"\n  🟢 TOP SUPPORTING VOICE:")
    for i, c in enumerate(supporters, 1):
        print(f"    {i}. {c['name']:30s} score={c['total_score']:+.3f}  comments={c['comment_count']}")

    print(f"\n  🔴 TOP CRITICAL VOICE:")
    for i, c in enumerate(opposition, 1):
        print(f"    {i}. {c['name']:30s} score={c['total_score']:+.3f}  comments={c['comment_count']}")

    print(f"\n  ⚪ NEUTRAL ({len(neutral)}):")
    for c in neutral[:5]:
        print(f"       {c['name']:30s} score={c['total_score']:+.3f}  comments={c['comment_count']}")
    if len(neutral) > 5:
        print(f"       ... and {len(neutral)-5} more")

    con.close()
    print(f"\n Batch scoring complete → {db_file}")
    return scored


def scrape_top14_about_batch(batch_id, db_file="socmint_manual.db"):
    """
    Scrape full about for top 7 profiles only for a manual batch.
    Call AFTER country detection.
    """
    print(f"\n{'═'*65}")
    print(f"  Full About Scrape — Top 7 Profiles Only (Batch)")
    print("═"*65)

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Get top 7 by score
    cur.execute("""
        SELECT bcs.commentor_id, bcs.tier, bcs.total_score, bcs.comment_count,
               co.name, co.profile_url
        FROM batch_commentor_scores bcs
        JOIN commentors co ON co.id = bcs.commentor_id
        WHERE bcs.batch_id = ?
        ORDER BY bcs.total_score DESC
        LIMIT 7
    """, (batch_id,))

    top_14 = []
    for row in cur.fetchall():
        top_14.append({
            'commentor_id': row[0],
            'tier':         row[1],
            'total_score':  row[2],
            'comment_count':row[3],
            'name':         row[4] or '',
            'profile_url':  row[5] or ''
        })

    if not top_14:
        print("    No scored profiles found — run scoring first")
        con.close()
        return

    print(f"   Scraping {len(top_14)} profiles...")

    for c in top_14:
        purl = c['profile_url']
        print(f"\n  [{c['tier'].upper()}] {c['name']} — {purl}")

        # Save secondary profile record
        try:
            cur.execute("""
                INSERT OR REPLACE INTO secondary_profiles
                    (batch_id, commentor_id, profile_url, name,
                     relationship_type, score, comment_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                batch_id, c['commentor_id'], c['profile_url'],
                c['name'], c['tier'], c['total_score'], c['comment_count']
            ))
            con.commit()
            sec_id = cur.lastrowid
        except Exception as e:
            print(f"      Save error: {e}")
            sec_id = None

        print(f"     Scraping all about sections...")
        sections = scrape_about_for_profile(purl)

        if sections and sec_id:
            field_count = 0
            for section, fields in sections.items():
                for f in fields:
                    try:
                        cur.execute("""
                            INSERT INTO secondary_profile_fields
                                (secondary_profile_id, section, field_type, label, value, sub_label)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (sec_id, section, f.get('field_type'),
                              f.get('label'), f.get('value'), f.get('sub_label')))
                        field_count += 1
                    except Exception:
                        pass
            con.commit()
            print(f"     Saved {field_count} fields")
            for section, fields in sections.items():
                for f in fields:
                    sub = f" ({f['sub_label']})" if f.get('sub_label') else ""
                    print(f"       {f['label']:20s} → {f['value']}{sub}")
        else:
            print(f"      No about data found")
        time.sleep(3)

    con.close()
    print(f"\n Top 7 batch about scrape complete → {db_file}")