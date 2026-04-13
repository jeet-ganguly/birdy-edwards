import sqlite3
import os
import json
import pickle
import numpy as np
import urllib.request
import tempfile
import time
from datetime import datetime
import unicodedata

DB_FILE      = "socmint.db"
FACE_DIR     = "face_data"
TOLERANCE    = 0.42   # lower = stricter matching (0.4-0.6 recommended)
PADDING      = 0.30   # increased padding around face crop
MAX_IMG_SIZE = 1800   # increased — preserve more detail for group photos
UPSAMPLE     = 1      # upsample image before detection — finds small faces

#  SCHEMA


FACE_SCHEMA = """
CREATE TABLE IF NOT EXISTS face_clusters (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    person_label        TEXT NOT NULL,
    representative_face TEXT,
    appearance_count    INTEGER DEFAULT 0,
    post_ids            TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS detected_faces (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_post_id       INTEGER NOT NULL,
    source_type         TEXT DEFAULT 'photo',
    face_index          INTEGER DEFAULT 0,
    face_image_path     TEXT,
    encoding            BLOB,
    person_id           INTEGER,
    detected_at         TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (person_id) REFERENCES face_clusters(id)
);
"""

#  HELPERS

def check_dependencies():
    """Check if face_recognition and cv2 are available."""
    try:
        import face_recognition
        import cv2
        return True
    except ImportError as e:
        print(f"  ⚠️  Missing dependency: {e}")
        print("  Install with:")
        print("    sudo apt install cmake build-essential")
        print("    pip install dlib face_recognition opencv-python")
        return False


def load_image(source, is_local=False):
    """Load image from URL or local file path. Returns numpy RGB array."""
    try:
        import cv2

        if is_local:
            # Load from local file
            img = cv2.imread(source)
            if img is None:
                return None
        else:
            # Download from URL
            req = urllib.request.Request(source, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = resp.read()
            arr = np.frombuffer(data, np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img is None:
                return None

        # Resize if too large
        h, w = img.shape[:2]
        if max(h, w) > MAX_IMG_SIZE:
            scale = MAX_IMG_SIZE / max(h, w)
            img   = cv2.resize(img, (int(w*scale), int(h*scale)))

        # BGR → RGB
        return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

    except Exception as e:
        return None


def download_image(url):
    """Backward compat wrapper."""
    return load_image(url, is_local=False)


def crop_face(image, location, padding=PADDING):
    """Crop face from image with padding."""
    try:
        import cv2
        h, w = image.shape[:2]
        top, right, bottom, left = location

        # Add padding
        pad_h = int((bottom - top)  * padding)
        pad_w = int((right  - left) * padding)

        top    = max(0, top    - pad_h)
        bottom = min(h, bottom + pad_h)
        left   = max(0, left   - pad_w)
        right  = min(w, right  + pad_w)

        face_rgb = image[top:bottom, left:right]
        face_bgr = cv2.cvtColor(face_rgb, cv2.COLOR_RGB2BGR)
        return face_bgr
    except Exception:
        return None


def encoding_to_blob(encoding):
    """Convert numpy encoding array to bytes for DB storage."""
    return pickle.dumps(encoding)


def blob_to_encoding(blob):
    """Convert bytes from DB back to numpy array."""
    return pickle.loads(blob)

#  FETCH UNPROCESSED PHOTOS

def fetch_unprocessed_photos(con, profile_id):
    """Get photo posts + text post screenshots that haven't been face-processed yet."""
    cur = con.cursor()

    # Add source_type column if missing (migration)
    try:
        cur.execute("ALTER TABLE detected_faces ADD COLUMN source_type TEXT DEFAULT 'photo'")
        con.commit()
    except Exception:
        pass  # column already exists

    # Photo posts with CDN image URL
    cur.execute("""
        SELECT pp.id, pp.image_src, pp.date_text, pp.caption, 'photo' as source
        FROM photo_posts pp
        WHERE pp.profile_id = ?
        AND pp.image_src IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM detected_faces df
            WHERE df.photo_post_id = pp.id
            AND df.source_type = 'photo'
        )
    """, (profile_id,))
    photos = [{'id': r[0], 'image_src': r[1], 'date': r[2],
               'caption': r[3], 'source': r[4], 'is_local': False}
              for r in cur.fetchall()]

    # Text post screenshots (local file)
    cur.execute("""
        SELECT tp.id, tp.screenshot_path, tp.date_text, NULL, 'text' as source
        FROM text_posts tp
        WHERE tp.profile_id = ?
        AND tp.screenshot_path IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM detected_faces df
            WHERE df.photo_post_id = tp.id
            AND df.source_type = 'text'
        )
    """, (profile_id,))
    text_posts = [{'id': r[0], 'image_src': r[1], 'date': r[2],
                   'caption': r[3], 'source': r[4], 'is_local': True}
                  for r in cur.fetchall()
                  if r[1] and os.path.exists(r[1])]

    all_posts = photos + text_posts
    print(f"    Photo posts  : {len(photos)}")
    print(f"    Text posts   : {len(text_posts)} (with screenshots)")
    return all_posts

#  FACE DETECTION

def detect_faces_in_image(image):
    """
    Detect faces using HOG model (RAM safe for 8GB systems).
    Falls back with upsample to catch small faces.
    """
    try:
        import face_recognition

        locations = []

        # HOG model — safe on 8GB RAM
        # upsample=1 finds moderately small faces
        locations = face_recognition.face_locations(
            image, model='hog', number_of_times_to_upsample=UPSAMPLE
        )

        # If nothing found try upsample=2 for very small faces
        if not locations:
            locations = face_recognition.face_locations(
                image, model='hog', number_of_times_to_upsample=2
            )

        if not locations:
            return []

        # large model = 68-point landmarks (more accurate encoding)
        encodings = face_recognition.face_encodings(
            image, locations, num_jitters=1, model='large'
        )

        return list(zip(locations, encodings))

    except Exception as e:
        print(f"    ⚠️  Face detection error: {e}")
        return []


#  FACE CLUSTERING

def cluster_faces(all_faces):
    """
    Cluster faces by identity using face_recognition comparison.
    all_faces: list of {'post_id', 'face_idx', 'encoding', 'face_path'}

    Returns list of clusters:
    [{'person_id': 1, 'faces': [...], 'count': 3}]
    """
    try:
        import face_recognition

        clusters      = []  # list of {'encodings': [], 'faces': []}
        cluster_reprs = []  # representative encoding per cluster

        for face in all_faces:
            encoding = face['encoding']
            matched  = False

            if cluster_reprs:
                # Compare against all known cluster representatives
                distances = face_recognition.face_distance(
                    np.array(cluster_reprs), encoding
                )
                best_idx  = int(np.argmin(distances))
                best_dist = distances[best_idx]

                if best_dist <= TOLERANCE:
                    # Match found — add to existing cluster
                    clusters[best_idx]['faces'].append(face)
                    clusters[best_idx]['encodings'].append(encoding)
                    # Update representative to average encoding
                    cluster_reprs[best_idx] = np.mean(
                        clusters[best_idx]['encodings'], axis=0
                    )
                    matched = True

            if not matched:
                # New person
                clusters.append({
                    'encodings': [encoding],
                    'faces':     [face]
                })
                cluster_reprs.append(encoding)

        return clusters

    except Exception as e:
        print(f"  ⚠️  Clustering error: {e}")
        return []


#  MAIN ANALYSIS FUNCTION

def analyze_faces(profile_url, db_file=DB_FILE, rerun=False):
    print(f"\n{'═'*65}")
    print(f"Face Intelligence")
    print(f"Profile  : {profile_url}")
    print(f"Tolerance: {TOLERANCE}")
    print(f"Rerun    : {rerun}")
    print("═"*65)

    if not check_dependencies():
        return

    import cv2

    con = sqlite3.connect(db_file)
    con.executescript(FACE_SCHEMA)
    con.commit()
    cur = con.cursor()

    # Get profile ID
    cur.execute("SELECT id, owner_name FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if not row:
        print(f"  ⚠️  Profile not found: {profile_url}")
        con.close()
        return

    profile_id = row[0]
    name_safe = profile_url.rstrip('/').split('/')[-1]
    FACE_DIR = os.path.join('face_data', name_safe)
    
    name_safe = profile_url.rstrip('/').split('/')[-1]
    # Normalize unicode — replace non-breaking spaces with regular spaces
    name_safe = unicodedata.normalize('NFKC', name_safe)
    name_safe = name_safe.replace('\xa0', '_').replace(' ', '_')
    FACE_DIR = os.path.join('face_data', name_safe)

    # If rerun — clear previous results for this profile
    if rerun:
        print("   Rerun mode — clearing previous face data...")
        cur.execute("""
            SELECT pp.id FROM photo_posts pp WHERE pp.profile_id = ?
            UNION
            SELECT tp.id FROM text_posts tp WHERE tp.profile_id = ?
        """, (profile_id, profile_id))
        all_post_ids = [r[0] for r in cur.fetchall()]
        if all_post_ids:
            placeholders = ','.join('?'*len(all_post_ids))
            cur.execute(f"""
                SELECT DISTINCT person_id FROM detected_faces
                WHERE photo_post_id IN ({placeholders}) AND person_id IS NOT NULL
            """, all_post_ids)
            cluster_ids = [r[0] for r in cur.fetchall()]
            cur.execute(f"DELETE FROM detected_faces WHERE photo_post_id IN ({placeholders})", all_post_ids)
            if cluster_ids:
                cur.execute(f"DELETE FROM face_clusters WHERE id IN ({','.join('?'*len(cluster_ids))})", cluster_ids)
        con.commit()
        print(f"   Cleared previous data")

    # Create output directories
    raw_dir     = os.path.join(FACE_DIR, name_safe, 'raw')
    persons_dir = os.path.join(FACE_DIR, name_safe, 'persons')
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(persons_dir, exist_ok=True)

    # Fetch unprocessed photos
    photos = fetch_unprocessed_photos(con, profile_id)
    print(f"\n   {len(photos)} unprocessed photos found")

    if not photos:
        print("   All photos already processed")
        con.close()
        return

    # Step 1: Detect faces in all photos     
    all_faces    = []
    total_faces  = 0
    no_face_count = 0

    for i, photo in enumerate(photos, 1):
        src      = photo['image_src']
        is_local = photo.get('is_local', False)
        source   = photo.get('source', 'photo')

        print(f"\n  [{i}/{len(photos)}] Post ID: {photo['id']}  [{source.upper()}]")
        print(f"    {'Loading' if is_local else 'Downloading'} image...")

        image = load_image(src, is_local=is_local)
        if image is None:
            print(f"    ⚠️  Load failed — skipping")
            # Insert placeholder so we don't retry
            cur.execute("""
                INSERT OR IGNORE INTO detected_faces
                    (photo_post_id, source_type, face_index, face_image_path, encoding, person_id)
                VALUES (?, ?, -1, 'NO_DOWNLOAD', NULL, NULL)
            """, (photo['id'], source))
            con.commit()
            continue

        print(f"    Image: {image.shape[1]}x{image.shape[0]}px")
        print(f"    Detecting faces...")

        face_results = detect_faces_in_image(image)
        print(f"    Faces found: {len(face_results)}")

        if not face_results:
            no_face_count += 1
            # Insert placeholder so we don't retry
            cur.execute("""
                INSERT OR IGNORE INTO detected_faces
                    (photo_post_id, source_type, face_index, face_image_path, encoding, person_id)
                VALUES (?, ?, -1, 'NO_FACE', NULL, NULL)
            """, (photo['id'], source))
            con.commit()
            continue

        for j, (location, encoding) in enumerate(face_results):
            # Crop and save raw face
            face_img  = crop_face(image, location)
            face_path = None

            if face_img is not None:
                face_path = os.path.join(raw_dir, f"{source}_post_{photo['id']}_face_{j}.jpg")
                cv2.imwrite(face_path, face_img)

            # Save to DB (person_id assigned after clustering)
            cur.execute("""
                INSERT OR IGNORE INTO detected_faces
                    (photo_post_id, source_type, face_index, face_image_path, encoding, person_id)
                VALUES (?, ?, ?, ?, ?, NULL)
            """, (photo['id'], source, j, face_path, encoding_to_blob(encoding)))
            con.commit()

            all_faces.append({
                'post_id':   photo['id'],
                'face_idx':  j,
                'encoding':  encoding,
                'face_path': face_path,
                'date':      photo.get('date', ''),
            })
            total_faces += 1

        time.sleep(0.5)

    print(f"\n   Detection complete:")
    print(f"     Total faces detected : {total_faces}")
    print(f"     Photos with no faces : {no_face_count}")

    if not all_faces:
        print("  ⚠️  No faces found in any photo")
        con.close()
        return

    # Step 2: Cluster faces by identity 
    print(f"\n{'─'*65}")
    print(f"   Clustering {total_faces} faces by identity...")
    clusters = cluster_faces(all_faces)
    print(f"   {len(clusters)} unique person(s) identified")

    # Step 3: Save clusters to DB and disk 
    print(f"\n{'─'*65}")
    print(f"   Saving clusters...")

    for idx, cluster in enumerate(clusters, 1):
        person_label = f"person_{idx}"
        faces        = cluster['faces']
        count        = len(faces)
        post_ids     = list(set(f['post_id'] for f in faces))

        # Create person folder
        person_dir = os.path.join(persons_dir, person_label)
        os.makedirs(person_dir, exist_ok=True)

        # Copy best faces to person folder
        repr_path = None
        for fi, face in enumerate(faces):
            if face['face_path'] and os.path.exists(face['face_path']):
                import shutil
                dest = os.path.join(person_dir, f"face_{fi+1}.jpg")
                shutil.copy2(face['face_path'], dest)
                if repr_path is None:
                    repr_path = dest  # first face = representative

        # Save cluster to DB
        cur.execute("""
            INSERT INTO face_clusters
                (person_label, representative_face, appearance_count, post_ids)
            VALUES (?, ?, ?, ?)
        """, (person_label, repr_path, count, json.dumps(post_ids)))
        con.commit()
        cluster_id = cur.lastrowid

        # Update detected_faces with person_id
        for face in faces:
            cur.execute("""
                UPDATE detected_faces SET person_id = ?
                WHERE photo_post_id = ? AND face_index = ?
            """, (cluster_id, face['post_id'], face['face_idx']))
        con.commit()

        print(f"    {person_label:12s} → {count} face(s) in {len(post_ids)} post(s)"
              f"  → {person_dir}")

    # Summary 
    print(f"\n{'═'*65}")
    print(f" Face Intelligence Complete")
    print(f"   Photos processed    : {len(photos)}")
    print(f"   Total faces         : {total_faces}")
    print(f"   Unique persons      : {len(clusters)}")
    print(f"   Output directory    : {os.path.join(FACE_DIR, name_safe)}/")
    print("═"*65)

    con.close()
    return clusters


#  FETCH RESULTS (for report)

def fetch_face_results(profile_url, db_file=DB_FILE):
    """
    Fetch face clusters for report generation.
    Returns list of persons with their face images and post info.
    """
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    cur.execute("SELECT id FROM profiles WHERE profile_url = ?", (profile_url,))
    row = cur.fetchone()
    if not row:
        con.close()
        return []

    profile_id = row[0]

    cur.execute("""
        SELECT fc.id, fc.person_label, fc.representative_face,
               fc.appearance_count, fc.post_ids
        FROM face_clusters fc
        WHERE EXISTS (
            SELECT 1 FROM detected_faces df
            JOIN photo_posts pp ON pp.id = df.photo_post_id
            WHERE df.person_id = fc.id AND pp.profile_id = ?
        )
        ORDER BY fc.appearance_count DESC
    """, (profile_id,))

    results = []
    for r in cur.fetchall():
        post_ids = json.loads(r[4]) if r[4] else []

        # Get all face image paths for this person
        cur.execute("""
            SELECT df.face_image_path, pp.date_text
            FROM detected_faces df
            JOIN photo_posts pp ON pp.id = df.photo_post_id
            WHERE df.person_id = ? AND df.face_image_path NOT IN ('NO_FACE','NO_DOWNLOAD')
            ORDER BY pp.date_text
        """, (r[0],))
        face_paths = [{'path': fr[0], 'date': fr[1]} for fr in cur.fetchall()
                      if fr[0] and os.path.exists(fr[0])]

        results.append({
            'person_id':    r[0],
            'label':        r[1],
            'repr_face':    r[2],
            'count':        r[3],
            'post_ids':     post_ids,
            'face_paths':   face_paths,
        })

    con.close()
    return results


if __name__ == "__main__":
    profile = input("Enter profile URL: ").strip()
    rerun   = input("Rerun from scratch? (y/n): ").strip().lower() == 'y'
    analyze_faces(profile, rerun=rerun)