import sqlite3
import json
import os
import re
from datetime import datetime

DB_FILE     = "socmint_manual.db"
MANUAL_JSON = "fb_manual_scrape.json"


#  DATABASE SCHEMA

SCHEMA = """
-- Batch identifiers for manual investigations
CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     TEXT UNIQUE NOT NULL,
    label        TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);

-- Manual entries (photo / post / reel mixed)
CREATE TABLE IF NOT EXISTS manual_posts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL,
    url             TEXT NOT NULL,
    type            TEXT,
    date_text       TEXT,
    image_src       TEXT,
    caption         TEXT,
    screenshot_path TEXT,
    profile_url     TEXT,
    scraped_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
    UNIQUE(batch_id, url)
);

-- Commentors
CREATE TABLE IF NOT EXISTS commentors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_url  TEXT UNIQUE NOT NULL,
    name         TEXT
);

-- Comments
CREATE TABLE IF NOT EXISTS comments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id      INTEGER NOT NULL,
    commentor_id INTEGER NOT NULL,
    comment_text TEXT,
    FOREIGN KEY (post_id)      REFERENCES manual_posts(id),
    FOREIGN KEY (commentor_id) REFERENCES commentors(id)
);

-- Comment analysis (AI results)
CREATE TABLE IF NOT EXISTS comment_analysis (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    comment_id   INTEGER NOT NULL,
    db_source    TEXT NOT NULL DEFAULT 'manual',
    sentiment    TEXT,
    emotion      TEXT,
    stance       TEXT,
    language     TEXT,
    analyzed_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(comment_id, db_source)
);

-- Commentor scores per batch
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
    FOREIGN KEY (batch_id)     REFERENCES batches(batch_id),
    FOREIGN KEY (commentor_id) REFERENCES commentors(id),
    UNIQUE(batch_id, commentor_id)
);

-- Commentor country identification
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

-- Secondary profiles (top supporters + opposition per batch)
CREATE TABLE IF NOT EXISTS secondary_profiles (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id            TEXT NOT NULL,
    commentor_id        INTEGER NOT NULL,
    profile_url         TEXT NOT NULL,
    name                TEXT,
    relationship_type   TEXT NOT NULL,
    score               REAL DEFAULT 0.0,
    comment_count       INTEGER DEFAULT 0,
    scraped_at          TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (batch_id)     REFERENCES batches(batch_id),
    FOREIGN KEY (commentor_id) REFERENCES commentors(id),
    UNIQUE(batch_id, commentor_id)
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
"""


#  HELPERS

def extract_profile_url(post_url):
    """Best-effort extract profile URL from a post/photo/reel URL."""
    m = re.match(r'(https://www\.facebook\.com/[^/?]+)', post_url)
    if m:
        candidate = m.group(1)
        skip = ['/photo', '/reel', '/posts', '/watch', '/video']
        if not any(x in candidate for x in skip):
            return candidate
    return None


def get_or_create_commentor(cur, profile_url, name):
    cur.execute("SELECT id FROM commentors WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if row:
        if name:
            cur.execute(
                "UPDATE commentors SET name = ? WHERE id = ? AND name IS NULL",
                (name, row[0])
            )
        return row[0]
    cur.execute(
        "INSERT INTO commentors (profile_url, name) VALUES (?, ?)",
        (profile_url, name)
    )
    return cur.lastrowid


#  IMPORTER

def generate_batch_id():
    return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def get_or_create_batch(cur, batch_id, label):
    cur.execute("SELECT id FROM batches WHERE batch_id = ?", (batch_id,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO batches (batch_id, label) VALUES (?, ?)",
            (batch_id, label)
        )


def init_db(db_file=DB_FILE):
    """Initialize the manual DB schema without importing data."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.executescript(SCHEMA)
    con.commit()
    con.close()


def import_manual(manual_json=MANUAL_JSON, db_file=DB_FILE, batch_id=None, label=None):
    print("\n" + "═"*65)
    print("SOCMINT Manual — JSON → SQLite Importer")
    print(f"Source : {manual_json}")
    print(f"DB     : {db_file}")
    print("═"*65)

    if not os.path.exists(manual_json):
        print(f" {manual_json} not found")
        return

    # Generate batch_id if not provided
    if not batch_id:
        batch_id = generate_batch_id()
    if not label:
        label = batch_id

    print(f"  Batch ID : {batch_id}")
    print(f"  Label    : {label}")

    with open(manual_json, encoding="utf-8") as f:
        items = json.load(f)

    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.executescript(SCHEMA)
    con.commit()

    # Migration — fix url UNIQUE constraint if old schema
    # SQLite doesn't support DROP CONSTRAINT, so recreate table if needed
    try:
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='manual_posts'")
        row = cur.fetchone()
        if row and 'url          TEXT UNIQUE' in row[0]:
            print("  Migrating manual_posts schema (removing url UNIQUE constraint)...")
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS manual_posts_new (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id     TEXT NOT NULL,
                    url          TEXT NOT NULL,
                    type         TEXT,
                    date_text    TEXT,
                    image_src    TEXT,
                    caption      TEXT,
                    profile_url  TEXT,
                    scraped_at   TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
                    UNIQUE(batch_id, url)
                );
                INSERT OR IGNORE INTO manual_posts_new
                    SELECT id, batch_id, url, type, date_text, image_src, caption, profile_url, scraped_at
                    FROM manual_posts;
                DROP TABLE manual_posts;
                ALTER TABLE manual_posts_new RENAME TO manual_posts;
            """)
            con.commit()
            print("  Migration complete")
    except Exception as e:
        print(f"  Migration skipped: {e}")

    # Register batch
    # Migration — add screenshot_path column if missing
    try:
        cur.execute("ALTER TABLE manual_posts ADD COLUMN screenshot_path TEXT")
        con.commit()
        print("  Migration: added screenshot_path column")
    except Exception:
        pass  # column already exists

    # Register batch
    get_or_create_batch(cur, batch_id, label)
    con.commit()

    photo_count   = 0
    post_count    = 0
    reel_count    = 0
    comment_count = 0

    for item in items:
        url      = item.get("url", "")
        itype    = item.get("type", "")
        comments = item.get("comments", [])

        if not url:
            continue

        profile_url = extract_profile_url(url)

        # Insert post with batch_id
        try:
            cur.execute("""
                INSERT OR IGNORE INTO manual_posts
                    (batch_id, url, type, date_text, image_src, caption, screenshot_path, profile_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                batch_id, url, itype,
                item.get("date"),
                item.get("image_src"),
                item.get("caption"),
                item.get("screenshot_path"),
                profile_url
            ))
        except Exception as e:
            print(f"  Insert error for {url}: {e}")
            continue

        # Migration — add screenshot_path column if missing
        cur.execute("SELECT id FROM manual_posts WHERE batch_id = ? AND url = ?", (batch_id, url,))
        
        row = cur.fetchone()
        if not row:
            continue
        post_id = row[0]

        if itype == "photo":
            photo_count += 1
        elif itype == "post":
            post_count += 1
        elif itype == "reel":
            reel_count += 1

        print(f"  [{itype:5s}] {url[:70]}")

        # Insert comments
        for c in comments:
            c_url  = c.get("profile_url", "")
            c_name = c.get("name", "")
            c_text = c.get("comment_text", "")

            if not c_url:
                continue

            cid = get_or_create_commentor(cur, c_url, c_name)
            cur.execute("""
                INSERT INTO comments (post_id, commentor_id, comment_text)
                VALUES (?, ?, ?)
            """, (post_id, cid, c_text))
            comment_count += 1

    con.commit()
    con.close()

    # Summary
    print(f"\n{'═'*65}")
    print("SUMMARY")
    print("═"*65)
    print(f"  photo entries : {photo_count}")
    print(f"  post entries  : {post_count}")
    print(f"  reel entries  : {reel_count}")
    print(f"  total comments: {comment_count}")

    con = sqlite3.connect(db_file)
    cur = con.cursor()
    for table in ['manual_posts', 'commentors', 'comments']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:15s} → {cur.fetchone()[0]} rows")
    con.close()

    print(f"\nImport complete → {db_file}")


#  DELETE

def delete_by_url(post_url, db_file=DB_FILE):
    """Delete a specific post and its comments from the manual DB."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.execute("SELECT id FROM manual_posts WHERE url = ?", (post_url,))
    row = cur.fetchone()
    if not row:
        print(f"  URL not found: {post_url}")
        con.close()
        return False

    post_id = row[0]

    # Get commentor IDs before deleting
    cur.execute("SELECT DISTINCT commentor_id FROM comments WHERE post_id = ?", (post_id,))
    commentor_ids = [r[0] for r in cur.fetchall()]

    # Delete comments
    cur.execute("DELETE FROM comments WHERE post_id = ?", (post_id,))
    print(f"  Deleted comments: {cur.rowcount}")

    # Delete post
    cur.execute("DELETE FROM manual_posts WHERE id = ?", (post_id,))
    print(f"  Deleted post: {post_url[:70]}")

    # Delete orphaned commentors
    orphans = []
    for cid in commentor_ids:
        cur.execute("SELECT COUNT(*) FROM comments WHERE commentor_id = ?", (cid,))
        if cur.fetchone()[0] == 0:
            orphans.append(cid)

    if orphans:
        cur.execute(
            f"DELETE FROM commentors WHERE id IN ({','.join('?'*len(orphans))})",
            orphans
        )
        print(f"  Deleted orphaned commentors: {cur.rowcount}")

    con.commit()
    con.close()
    print(f"\nDeleted successfully.")
    return True


def delete_by_profile(profile_url, db_file=DB_FILE):
    """Delete all posts belonging to a specific profile URL."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.execute("SELECT id FROM manual_posts WHERE profile_url = ?", (profile_url,))
    post_ids = [r[0] for r in cur.fetchall()]

    if not post_ids:
        print(f"  No posts found for profile: {profile_url}")
        con.close()
        return False

    # Collect commentor IDs
    cur.execute(
        f"SELECT DISTINCT commentor_id FROM comments WHERE post_id IN ({','.join('?'*len(post_ids))})",
        post_ids
    )
    commentor_ids = [r[0] for r in cur.fetchall()]

    # Delete comments
    cur.execute(
        f"DELETE FROM comments WHERE post_id IN ({','.join('?'*len(post_ids))})",
        post_ids
    )
    print(f"  Deleted comments: {cur.rowcount}")

    # Delete posts
    cur.execute("DELETE FROM manual_posts WHERE profile_url = ?", (profile_url,))
    print(f"  Deleted posts: {cur.rowcount}")

    # Delete orphaned commentors
    orphans = []
    for cid in commentor_ids:
        cur.execute("SELECT COUNT(*) FROM comments WHERE commentor_id = ?", (cid,))
        if cur.fetchone()[0] == 0:
            orphans.append(cid)

    if orphans:
        cur.execute(
            f"DELETE FROM commentors WHERE id IN ({','.join('?'*len(orphans))})",
            orphans
        )
        print(f"  Deleted orphaned commentors: {cur.rowcount}")

    con.commit()
    con.close()
    print(f"\nProfile posts deleted successfully.")
    return True


def delete_by_batch_id(batch_id, db_file=DB_FILE):
    """Delete all posts and comments belonging to a specific batch investigation."""
    print("\n" + "═"*65)
    print(f"Deleting batch: {batch_id}")
    print("═"*65)

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Check batch exists
    cur.execute("SELECT label FROM batches WHERE batch_id = ?", (batch_id,))
    row = cur.fetchone()
    if not row:
        print(f"  Batch not found: {batch_id}")
        con.close()
        return False

    label = row[0]
    print(f"  Label: {label}")

    # Get all post IDs for this batch
    cur.execute("SELECT id FROM manual_posts WHERE batch_id = ?", (batch_id,))
    post_ids = [r[0] for r in cur.fetchall()]

    if not post_ids:
        print(f"  No posts found for batch: {batch_id}")
        # Still delete the batch record
        cur.execute("DELETE FROM batches WHERE batch_id = ?", (batch_id,))
        con.commit()
        con.close()
        return True

    # Collect commentor IDs before deleting
    cur.execute(
        f"SELECT DISTINCT commentor_id FROM comments WHERE post_id IN ({','.join('?'*len(post_ids))})",
        post_ids
    )
    commentor_ids = [r[0] for r in cur.fetchall()]

    # Delete analysis results for these comments
    cur.execute(
        f"SELECT id FROM comments WHERE post_id IN ({','.join('?'*len(post_ids))})",
        post_ids
    )
    comment_ids = [r[0] for r in cur.fetchall()]
    if comment_ids:
        try:
            cur.execute(
                f"DELETE FROM comment_analysis WHERE comment_id IN ({','.join('?'*len(comment_ids))}) AND db_source = 'manual'",
                comment_ids
            )
            print(f"  Deleted analysis results: {cur.rowcount}")
        except Exception:
            pass

    # Delete batch commentor scores
    try:
        cur.execute("DELETE FROM batch_commentor_scores WHERE batch_id = ?", (batch_id,))
        print(f"  Deleted commentor scores: {cur.rowcount}")
    except Exception:
        pass

    # Delete secondary profile fields + secondary profiles
    try:
        cur.execute("SELECT id FROM secondary_profiles WHERE batch_id = ?", (batch_id,))
        sec_ids = [r[0] for r in cur.fetchall()]
        if sec_ids:
            cur.execute(
                f"DELETE FROM secondary_profile_fields WHERE secondary_profile_id IN ({','.join('?'*len(sec_ids))})",
                sec_ids
            )
            print(f"  Deleted secondary profile fields: {cur.rowcount}")
        cur.execute("DELETE FROM secondary_profiles WHERE batch_id = ?", (batch_id,))
        print(f"  Deleted secondary profiles: {cur.rowcount}")
    except Exception:
        pass

    # Delete country identification for commentors in this batch
    try:
        cur.execute("""
            DELETE FROM commentor_country
            WHERE commentor_id IN (
                SELECT DISTINCT commentor_id FROM comments c
                JOIN manual_posts mp ON mp.id = c.post_id
                WHERE mp.batch_id = ?
            )
        """, (batch_id,))
        print(f"  Deleted country identifications: {cur.rowcount}")
    except Exception:
        pass

    # Delete comments
    cur.execute(
        f"DELETE FROM comments WHERE post_id IN ({','.join('?'*len(post_ids))})",
        post_ids
    )
    print(f"  Deleted comments: {cur.rowcount}")

    # Delete posts
    cur.execute("DELETE FROM manual_posts WHERE batch_id = ?", (batch_id,))
    print(f"  Deleted posts: {cur.rowcount}")

    # Delete batch record
    cur.execute("DELETE FROM batches WHERE batch_id = ?", (batch_id,))
    print(f"  Deleted batch record")

    # Delete orphaned commentors
    orphans = []
    for cid in commentor_ids:
        cur.execute("SELECT COUNT(*) FROM comments WHERE commentor_id = ?", (cid,))
        if cur.fetchone()[0] == 0:
            orphans.append(cid)

    if orphans:
        cur.execute(
            f"DELETE FROM commentors WHERE id IN ({','.join('?'*len(orphans))})",
            orphans
        )
        print(f"  Deleted orphaned commentors: {cur.rowcount}")

    con.commit()
    con.close()
    print(f"\nBatch '{label}' deleted successfully.")
    return True


def list_batches(db_file=DB_FILE):
    """List all available batches in the manual DB."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    try:
        cur.execute("""
            SELECT b.batch_id, b.label, b.created_at,
                   COUNT(mp.id) AS post_count
            FROM batches b
            LEFT JOIN manual_posts mp ON mp.batch_id = b.batch_id
            GROUP BY b.batch_id
            ORDER BY b.created_at DESC
        """)
        rows = cur.fetchall()
        con.close()

        if not rows:
            print("  No batches found")
            return []

        print(f"\n{'═'*65}")
        print("📋  Available Batches")
        print("═"*65)
        for r in rows:
            print(f"      {r[0]}")
            print(f"      Label     : {r[1]}")
            print(f"      Created   : {r[2]}")
            print(f"      Posts     : {r[3]}")
            print()
        return rows
    except Exception as e:
        print(f"  Error listing batches: {e}")
        con.close()
        return []


#  MAIN

if __name__ == "__main__":
    import_manual()