import sqlite3
import os

DB_FILES = {
    "socmint.db":        "Automated Profile Investigation DB",
    "socmint_manual.db": "Manual Investigation DB"
}

# Tables in correct delete order (respecting foreign keys)
SOCMINT_TABLES = [
    "secondary_profile_fields",
    "secondary_profiles",
    "commentor_scores",
    "commentor_country",
    "comment_analysis",
    "image_analysis",
    "text_post_analysis",
    "detected_faces",      # face_intelligence — must delete before face_clusters & photo_posts
    "face_clusters",       # face_intelligence
    "photo_comments",
    "reel_comments",
    "text_comments",
    "photo_posts",
    "reel_posts",
    "text_posts",
    "profile_fields",
    "profiles",
    "commentors",
]

MANUAL_TABLES = [
    "secondary_profile_fields",
    "secondary_profiles",
    "batch_commentor_scores",
    "commentor_country",
    "comment_analysis",
    "comments",
    "manual_posts",
    "batches",
    "commentors",
]


def clean_db(db_path, tables, label):
    print(f"\n{'═'*65}")
    print(f"  Cleaning: {label}")
    print(f"    File: {db_path}")
    print("═"*65)

    if not os.path.exists(db_path):
        print(f"    File not found — skipping")
        return

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Disable FK constraints temporarily for clean wipe
    cur.execute("PRAGMA foreign_keys = OFF")

    total_deleted = 0
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count > 0:
                cur.execute(f"DELETE FROM {table}")
                print(f"   {table:35s} → {count} rows deleted")
                total_deleted += count
            else:
                print(f"  ⚪ {table:35s} → already empty")
        except Exception as e:
            print(f"    {table:35s} → {e}")

    # Re-enable FK constraints
    cur.execute("PRAGMA foreign_keys = ON")

    # Reset auto-increment counters
    cur.execute("DELETE FROM sqlite_sequence")

    con.commit()

    # Vacuum to reclaim disk space
    con.execute("VACUUM")
    con.close()

    print(f"\n   Total deleted: {total_deleted} rows")
    print(f"   {db_path} cleaned and vacuumed")


def main():
    print("           SOCMINT — Database Cleaner                        ")
    print("\n  WARNING: This will DELETE ALL DATA from both databases!")
    print("    This action cannot be undone.\n")

    choice = input("Are you sure? Type 'YES' to confirm: ").strip()
    if choice != 'YES':
        print("\n Cancelled — no data deleted")
        return

    print("\nWhich DB to clean?")
    print("  1 → Both DBs")
    print("  2 → socmint.db only")
    print("  3 → socmint_manual.db only")
    target = input("Choice: ").strip()

    if target == "1":
        clean_db("socmint.db", SOCMINT_TABLES, DB_FILES["socmint.db"])
        clean_db("socmint_manual.db", MANUAL_TABLES, DB_FILES["socmint_manual.db"])
    elif target == "2":
        clean_db("socmint.db", SOCMINT_TABLES, DB_FILES["socmint.db"])
    elif target == "3":
        clean_db("socmint_manual.db", MANUAL_TABLES, DB_FILES["socmint_manual.db"])
    else:
        print(" Invalid choice — no data deleted")
        return

    print(f"\n{'═'*65}")
    print(" Cleanup complete!")
    print("═"*65)


if __name__ == "__main__":
    main()