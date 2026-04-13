import sqlite3
import json
import os
from datetime import datetime

DB_FILE       = "socmint.db"
PROFILE_JSON  = "fb_about.json"
PHOTOS_JSON   = "fb_photos.json"
REELS_JSON    = "fb_reels.json"
POSTS_JSON    = "fb_posts.json"


#  DATABASE SCHEMA

SCHEMA = """
-- Target profile being analyzed
CREATE TABLE IF NOT EXISTS profiles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_url  TEXT UNIQUE NOT NULL,
    owner_name   TEXT,
    is_locked    INTEGER DEFAULT 0,
    scraped_at   TEXT DEFAULT (datetime('now'))
);

-- Profile about fields (city, work, education etc.)
CREATE TABLE IF NOT EXISTS profile_fields (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id   INTEGER NOT NULL,
    section      TEXT,
    field_type   TEXT,
    label        TEXT,
    value        TEXT,
    sub_label    TEXT,
    FOREIGN KEY (profile_id) REFERENCES profiles(id)
);

-- Photo posts
CREATE TABLE IF NOT EXISTS photo_posts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id   INTEGER NOT NULL,
    photo_url    TEXT UNIQUE NOT NULL,
    date_text    TEXT,
    image_src    TEXT,
    caption      TEXT,
    scraped_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (profile_id) REFERENCES profiles(id)
);

-- Reel posts
CREATE TABLE IF NOT EXISTS reel_posts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id   INTEGER NOT NULL,
    reel_url     TEXT UNIQUE NOT NULL,
    scraped_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (profile_id) REFERENCES profiles(id)
);

-- Commentors (unique people who commented)
CREATE TABLE IF NOT EXISTS commentors (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_url  TEXT UNIQUE NOT NULL,
    name         TEXT
);

-- Comments on photo posts
CREATE TABLE IF NOT EXISTS photo_comments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_post_id   INTEGER NOT NULL,
    commentor_id    INTEGER NOT NULL,
    comment_text    TEXT,
    FOREIGN KEY (photo_post_id) REFERENCES photo_posts(id),
    FOREIGN KEY (commentor_id)  REFERENCES commentors(id),
    UNIQUE(photo_post_id, commentor_id)
);

-- Comments on reel posts
CREATE TABLE IF NOT EXISTS reel_comments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reel_post_id    INTEGER NOT NULL,
    commentor_id    INTEGER NOT NULL,
    comment_text    TEXT,
    FOREIGN KEY (reel_post_id) REFERENCES reel_posts(id),
    FOREIGN KEY (commentor_id) REFERENCES commentors(id),
    UNIQUE(reel_post_id, commentor_id)
);

-- Text posts (from /posts/ URLs — screenshot based)
CREATE TABLE IF NOT EXISTS text_posts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id        INTEGER NOT NULL,
    post_url          TEXT UNIQUE NOT NULL,
    date_text         TEXT,
    screenshot_path   TEXT,
    scraped_at        TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (profile_id) REFERENCES profiles(id)
);

-- Comments on text posts
CREATE TABLE IF NOT EXISTS text_comments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    text_post_id    INTEGER NOT NULL,
    commentor_id    INTEGER NOT NULL,
    comment_text    TEXT,
    FOREIGN KEY (text_post_id) REFERENCES text_posts(id),
    FOREIGN KEY (commentor_id) REFERENCES commentors(id),
    UNIQUE(text_post_id, commentor_id)
);

-- Commentor scores
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

-- Secondary profiles (top supporters + opposition)
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

-- Face clusters (unique persons detected across all photos)
CREATE TABLE IF NOT EXISTS face_clusters (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    person_label            TEXT NOT NULL,
    representative_face     TEXT,
    appearance_count        INTEGER DEFAULT 0,
    post_ids                TEXT,
    created_at              TEXT DEFAULT (datetime('now'))
);

-- Detected faces per photo post
CREATE TABLE IF NOT EXISTS detected_faces (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_post_id           INTEGER NOT NULL,
    face_index              INTEGER DEFAULT 0,
    face_image_path         TEXT,
    encoding                BLOB,
    person_id               INTEGER,
    detected_at             TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (photo_post_id) REFERENCES photo_posts(id),
    FOREIGN KEY (person_id)     REFERENCES face_clusters(id)
);
"""


#  HELPERS

def get_or_create_profile(cur, profile_url, owner_name=None, is_locked=False):
    cur.execute("SELECT id, owner_name FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if row:
        # Update owner_name if it was null (pre-created by web app before scraping)
        if owner_name and not row[1]:
            cur.execute("UPDATE profiles SET owner_name=?, is_locked=? WHERE profile_url=?",
                        (owner_name, int(is_locked), profile_url))
        return row[0]
    cur.execute(
        "INSERT INTO profiles (profile_url, owner_name, is_locked) VALUES (?, ?, ?)",
        (profile_url, owner_name, int(is_locked))
    )
    return cur.lastrowid


def get_or_create_commentor(cur, profile_url, name):
    cur.execute("SELECT id FROM commentors WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if row:
        # Update name if missing
        if name:
            cur.execute("UPDATE commentors SET name = ? WHERE id = ? AND name IS NULL",
                        (name, row[0]))
        return row[0]
    cur.execute(
        "INSERT INTO commentors (profile_url, name) VALUES (?, ?)",
        (profile_url, name)
    )
    return cur.lastrowid


#  IMPORTERS

def import_profile(cur, json_path):
    if not os.path.exists(json_path):
        print(f"    {json_path} not found — skipping")
        return None

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    profile_url = data.get("profile_url", "")
    owner_name  = data.get("owner_name")
    is_locked   = data.get("is_locked", False)

    profile_id = get_or_create_profile(cur, profile_url, owner_name, is_locked)
    print(f"   Profile: {owner_name} ({profile_url})")

    # Import all section fields
    field_count = 0
    for section, fields in data.get("sections", {}).items():
        for f in fields:
            cur.execute("""
                INSERT INTO profile_fields
                    (profile_id, section, field_type, label, value, sub_label)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                profile_id,
                section,
                f.get("field_type"),
                f.get("label"),
                f.get("value"),
                f.get("sub_label")
            ))
            field_count += 1

    print(f"   Imported {field_count} profile fields")
    return profile_id


def import_photos(cur, json_path, profile_id):
    if not os.path.exists(json_path):
        print(f"    {json_path} not found — skipping")
        return

    with open(json_path, encoding="utf-8") as f:
        posts = json.load(f)

    post_count    = 0
    comment_count = 0

    for post in posts:
        photo_url = post.get("photo_url", "")
        if not photo_url:
            continue

        # Insert photo post (ignore if already exists)
        try:
            cur.execute("""
                INSERT OR IGNORE INTO photo_posts
                    (profile_id, photo_url, date_text, image_src, caption)
                VALUES (?, ?, ?, ?, ?)
            """, (
                profile_id,
                photo_url,
                post.get("date"),
                post.get("image_src"),
                post.get("caption")
            ))
        except Exception as e:
            print(f"      photo insert error: {e}")
            continue

        cur.execute("SELECT id FROM photo_posts WHERE photo_url = ?", (photo_url,))
        photo_post_id = cur.fetchone()[0]
        post_count += 1

        # Insert comments
        for c in post.get("comments", []):
            commentor_id = get_or_create_commentor(
                cur, c.get("profile_url", ""), c.get("name")
            )
            cur.execute("""
                INSERT OR IGNORE INTO photo_comments (photo_post_id, commentor_id, comment_text)
                VALUES (?, ?, ?)
            """, (photo_post_id, commentor_id, c.get("comment_text")))
            comment_count += 1

    print(f"   Imported {post_count} photo posts, {comment_count} comments")


def import_reels(cur, json_path, profile_id):
    if not os.path.exists(json_path):
        print(f"    {json_path} not found — skipping")
        return

    with open(json_path, encoding="utf-8") as f:
        reels = json.load(f)

    reel_count    = 0
    comment_count = 0

    for reel in reels:
        reel_url = reel.get("reel_url", "")
        if not reel_url:
            continue

        try:
            cur.execute("""
                INSERT OR IGNORE INTO reel_posts (profile_id, reel_url)
                VALUES (?, ?)
            """, (profile_id, reel_url))
        except Exception as e:
            print(f"      reel insert error: {e}")
            continue

        cur.execute("SELECT id FROM reel_posts WHERE reel_url = ?", (reel_url,))
        reel_post_id = cur.fetchone()[0]
        reel_count += 1

        for c in reel.get("comments", []):
            commentor_id = get_or_create_commentor(
                cur, c.get("profile_url", ""), c.get("name")
            )
            cur.execute("""
                INSERT OR IGNORE INTO reel_comments (reel_post_id, commentor_id, comment_text)
                VALUES (?, ?, ?)
            """, (reel_post_id, commentor_id, c.get("comment_text")))
            comment_count += 1

    print(f"   Imported {reel_count} reels, {comment_count} comments")


def import_posts(cur, json_path, profile_id):
    if not os.path.exists(json_path):
        print(f"    {json_path} not found — skipping")
        return

    with open(json_path, encoding="utf-8") as f:
        posts = json.load(f)

    post_count    = 0
    comment_count = 0

    for post in posts:
        post_url = post.get("post_url", "")
        if not post_url:
            continue

        try:
            cur.execute("""
                INSERT OR IGNORE INTO text_posts
                    (profile_id, post_url, date_text, screenshot_path)
                VALUES (?, ?, ?, ?)
            """, (
                profile_id,
                post_url,
                post.get("date"),
                post.get("screenshot_path")
            ))
        except Exception as e:
            print(f"      post insert error: {e}")
            continue

        cur.execute("SELECT id FROM text_posts WHERE post_url = ?", (post_url,))
        row = cur.fetchone()
        if not row:
            continue
        text_post_id = row[0]
        post_count += 1

        for c in post.get("comments", []):
            commentor_id = get_or_create_commentor(
                cur, c.get("profile_url", ""), c.get("name")
            )
            cur.execute("""
                INSERT OR IGNORE INTO text_comments (text_post_id, commentor_id, comment_text)
                VALUES (?, ?, ?)
            """, (text_post_id, commentor_id, c.get("comment_text")))
            comment_count += 1

    print(f"   Imported {post_count} text posts, {comment_count} comments")

def init_db(db_file=DB_FILE):
    """
    Initialize the database schema without importing any data.
    Called by app.py when DB doesn't exist yet.
    """
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.executescript(SCHEMA)
    con.commit()
    con.close()


def import_all(
    profile_json = PROFILE_JSON,
    photos_json  = PHOTOS_JSON,
    reels_json   = REELS_JSON,
    posts_json   = POSTS_JSON,
    db_file      = DB_FILE
):
    print("\n" + "═"*65)
    print("SOCMINT — JSON → SQLite Importer")
    print("═"*65)

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Create schema
    cur.executescript(SCHEMA)
    con.commit()
    print(f"   Database: {db_file}")

    # Import profile first to get profile_id
    print("\n   Importing profile...")
    profile_id = import_profile(cur, profile_json)

    if profile_id is None:
        print("    No profile JSON — creating placeholder profile")
        if os.path.exists(photos_json):
            with open(photos_json) as f:
                posts = json.load(f)
            profile_url = "unknown"
        else:
            profile_url = "unknown"
        profile_id = get_or_create_profile(cur, profile_url)

    con.commit()

    # Import photos
    print("\n   Importing photo posts...")
    import_photos(cur, photos_json, profile_id)
    con.commit()

    # Import reels
    print("\n   Importing reel posts...")
    import_reels(cur, reels_json, profile_id)
    con.commit()

    # Import text posts
    print("\n   Importing text posts...")
    import_posts(cur, posts_json, profile_id)
    con.commit()

    con.close()

    # Summary stats
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    print(f"\n{'═'*65}")
    print(" DATABASE SUMMARY")
    print("═"*65)
    for table in ['profiles', 'profile_fields', 'photo_posts', 'reel_posts',
                  'text_posts', 'commentors', 'photo_comments', 'reel_comments',
                  'text_comments']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:20s} → {cur.fetchone()[0]} rows")
    con.close()

    print(f"\n Import complete → {db_file}")


def delete_profile(profile_url, db_file=DB_FILE):
    """Delete a profile and ALL related data — cascades through all tables."""
    print("\n" + "═"*65)
    print("SOCMINT — Delete Profile Investigation")
    print("═"*65)

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.execute("SELECT id, owner_name FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if not row:
        print(f"    Profile not found: {profile_url}")
        con.close()
        return False

    profile_id = row[0]
    owner_name = row[1]
    print(f"  Deleting: {owner_name} ({profile_url})")

    # Get all post IDs
    cur.execute("SELECT id FROM photo_posts WHERE profile_id = ?", (profile_id,))
    photo_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM reel_posts WHERE profile_id = ?", (profile_id,))
    reel_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM text_posts WHERE profile_id = ?", (profile_id,))
    text_ids = [r[0] for r in cur.fetchall()]

    # Collect commentor IDs
    commentor_ids = set()
    for ids, table in [(photo_ids, 'photo_comments'), (reel_ids, 'reel_comments'), (text_ids, 'text_comments')]:
        if ids:
            col = 'photo_post_id' if table == 'photo_comments' else 'reel_post_id' if table == 'reel_comments' else 'text_post_id'
            cur.execute(
                f"SELECT DISTINCT commentor_id FROM {table} WHERE {col} IN ({','.join('?'*len(ids))})",
                ids
            )
            commentor_ids.update(r[0] for r in cur.fetchall())

    # Delete secondary profile data (scores, about fields, secondary profiles)
    try:
        import commentor_scoring
        commentor_scoring.delete_secondary_data(profile_url, db_file)
    except Exception as e:
        print(f"    Secondary data delete error: {e}")

    # Delete AI analysis results first
    # comment_analysis
    if photo_ids:
        cur.execute(f"SELECT id FROM photo_comments WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids)
        pc_ids = [r[0] for r in cur.fetchall()]
        if pc_ids:
            cur.execute(f"DELETE FROM comment_analysis WHERE comment_id IN ({','.join('?'*len(pc_ids))}) AND db_source='photo'", pc_ids)
            print(f"   Deleted photo comment analysis: {cur.rowcount}")

    if reel_ids:
        cur.execute(f"SELECT id FROM reel_comments WHERE reel_post_id IN ({','.join('?'*len(reel_ids))})", reel_ids)
        rc_ids = [r[0] for r in cur.fetchall()]
        if rc_ids:
            cur.execute(f"DELETE FROM comment_analysis WHERE comment_id IN ({','.join('?'*len(rc_ids))}) AND db_source='reel'", rc_ids)
            print(f"   Deleted reel comment analysis:  {cur.rowcount}")

    if text_ids:
        cur.execute(f"SELECT id FROM text_comments WHERE text_post_id IN ({','.join('?'*len(text_ids))})", text_ids)
        tc_ids = [r[0] for r in cur.fetchall()]
        if tc_ids:
            cur.execute(f"DELETE FROM comment_analysis WHERE comment_id IN ({','.join('?'*len(tc_ids))}) AND db_source='text'", tc_ids)
            print(f"   Deleted text comment analysis:  {cur.rowcount}")

    # image_analysis and text_post_analysis (may not exist yet)
    if photo_ids:
        try:
            cur.execute(f"DELETE FROM image_analysis WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids)
            print(f"   Deleted image analysis:         {cur.rowcount}")
        except Exception:
            pass
        # Delete detected faces + clusters
        try:
            cur.execute(f"SELECT DISTINCT person_id FROM detected_faces WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids)
            cluster_ids = [r[0] for r in cur.fetchall() if r[0]]
            cur.execute(f"DELETE FROM detected_faces WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids)
            print(f"   Deleted detected faces:         {cur.rowcount}")
            if cluster_ids:
                cur.execute(f"DELETE FROM face_clusters WHERE id IN ({','.join('?'*len(cluster_ids))})", cluster_ids)
                print(f"   Deleted face clusters:          {cur.rowcount}")
        except Exception:
            pass
    if text_ids:
        try:
            cur.execute(f"DELETE FROM text_post_analysis WHERE text_post_id IN ({','.join('?'*len(text_ids))})", text_ids)
            print(f"   Deleted text post analysis:     {cur.rowcount}")
        except Exception:
            pass

    # Delete comments
    if photo_ids:
        cur.execute(f"DELETE FROM photo_comments WHERE photo_post_id IN ({','.join('?'*len(photo_ids))})", photo_ids)
        print(f"   Deleted photo comments: {cur.rowcount}")

    if reel_ids:
        cur.execute(f"DELETE FROM reel_comments WHERE reel_post_id IN ({','.join('?'*len(reel_ids))})", reel_ids)
        print(f"   Deleted reel comments:  {cur.rowcount}")

    if text_ids:
        cur.execute(f"DELETE FROM text_comments WHERE text_post_id IN ({','.join('?'*len(text_ids))})", text_ids)
        print(f"   Deleted text comments:  {cur.rowcount}")

    # Delete posts
    cur.execute("DELETE FROM photo_posts WHERE profile_id = ?", (profile_id,))
    print(f"   Deleted photo posts:    {cur.rowcount}")
    cur.execute("DELETE FROM reel_posts WHERE profile_id = ?", (profile_id,))
    print(f"   Deleted reel posts:     {cur.rowcount}")
    cur.execute("DELETE FROM text_posts WHERE profile_id = ?", (profile_id,))
    print(f"   Deleted text posts:     {cur.rowcount}")

    # Delete profile fields
    cur.execute("DELETE FROM profile_fields WHERE profile_id = ?", (profile_id,))
    print(f"   Deleted profile fields: {cur.rowcount}")

    # Delete profile
    cur.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
    print(f"   Deleted profile entry")

    # Delete orphaned commentors
    orphans = []
    for cid in commentor_ids:
        cur.execute("SELECT COUNT(*) FROM photo_comments WHERE commentor_id = ?", (cid,))
        pc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM reel_comments WHERE commentor_id = ?", (cid,))
        rc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM text_comments WHERE commentor_id = ?", (cid,))
        tc = cur.fetchone()[0]
        if pc == 0 and rc == 0 and tc == 0:
            orphans.append(cid)

    if orphans:
        cur.execute(f"DELETE FROM commentors WHERE id IN ({','.join('?'*len(orphans))})", orphans)
        print(f"   Deleted orphaned commentors: {cur.rowcount}")

    con.commit()
    con.close()
    print(f"\n Profile investigation deleted successfully.")
    return True


if __name__ == "__main__":
    import_all()