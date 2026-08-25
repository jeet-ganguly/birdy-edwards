"""
queue_manager.py  —  Birdy-Edwards Investigation Queue
======================================================
Manages up to 5 queued investigations (profile or batch),
runs them sequentially with a 10-minute cooldown between each,
validates cookies before every run, fires Telegram notifications,
and supports a per-investigation stop flag.

Usage (from app.py):
    from queue_manager import queue

    queue.add_profile(profile_url, scan_level)
    queue.add_batch(batch_id, label, urls, scan_level)
    queue.stop_current()
    queue.remove(item_id)
    queue.clear_all()
    queue.get_state()          # → list of queue dicts for UI
    queue.is_stopped(item_id)  # → bool, checked inside pipeline
"""

import os
import json
import time
import sqlite3
import threading
from datetime import datetime


# ── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
QUEUE_DB  = os.path.join(BASE_DIR, 'queue.db')
STATUS_DIR = os.path.join(BASE_DIR, 'status')
#TG_FILE   = os.path.join(BASE_DIR, '.telegram_config')   # {"token":"..","chat_id":".."}
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')

os.makedirs(STATUS_DIR, exist_ok=True)

#from telegram_notify import telegram


# ── Queue item states ─────────────────────────────────────────────────────────

STATE_QUEUED  = 'queued'
STATE_RUNNING = 'running'
STATE_DONE    = 'done'
STATE_FAILED  = 'failed'
STATE_STOPPED = 'stopped'


# ─────────────────────────────────────────────────────────────────────────────
class InvestigationQueue:
    """
    Thread-safe investigation queue backed by SQLite.
    A single background worker thread picks items off the queue
    one at a time with a configurable cooldown.
    """

    MAX_SIZE        = 5
    COOLDOWN_SEC    = 600    # 10 minutes between investigations
    POLL_INTERVAL   = 5      # seconds between queue polls when idle

    def __init__(self):
        self._lock          = threading.Lock()
        self._stop_flags    = {}      # {item_id: threading.Event}
        self._worker_thread = None
        self._flask_app     = None    # injected by app.py after init
        self._init_db()
        self._start_worker()

    # ── DB ────────────────────────────────────────────────────────────────────

    def _get_conn(self):
        conn = sqlite3.connect(QUEUE_DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS investigation_queue (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                type         TEXT NOT NULL,          -- 'profile' | 'batch'
                label        TEXT NOT NULL,          -- display name
                config       TEXT NOT NULL,          -- JSON blob
                scan_level   TEXT NOT NULL DEFAULT 'medium',
                detect_all_countries  INTEGER DEFAULT 0,
                detect_top7_countries INTEGER DEFAULT 1,
                state        TEXT NOT NULL DEFAULT 'queued',
                position     INTEGER,               -- order in queue (1-based)
                queued_at    TEXT,
                started_at   TEXT,
                finished_at  TEXT,
                error_msg    TEXT
            )
        """)
        # Reset any items that were 'running' from a previous crash
        conn.execute(
            "UPDATE investigation_queue SET state=? WHERE state=?",
            (STATE_FAILED, STATE_RUNNING)
        )
        conn.commit()
        conn.close()

    def _all_rows(self, conn):
        """Return all queue rows ordered by position."""
        return [dict(r) for r in conn.execute(
            "SELECT * FROM investigation_queue ORDER BY position ASC, id ASC"
        ).fetchall()]

    def _next_position(self, conn):
        row = conn.execute(
            "SELECT MAX(position) FROM investigation_queue WHERE state=?",
            (STATE_QUEUED,)
        ).fetchone()
        return (row[0] or 0) + 1

    # ── Public API ────────────────────────────────────────────────────────────

    def add_profile(self, profile_url, scan_level='medium',
                    detect_all=False, detect_top7=True):
        """Add a profile investigation to the queue."""
        label  = profile_url.rstrip('/').split('/')[-1][:40]
        config = json.dumps({
            'profile_url': profile_url,
        })
        return self._enqueue('profile', label, config, scan_level,
                             detect_all, detect_top7)

    def add_batch(self, batch_id, label, urls, scan_level='medium',
                  detect_all=False, detect_top7=True):
        """Add a manual batch investigation to the queue."""
        config = json.dumps({
            'batch_id': batch_id,
            'label':    label,
            'urls':     urls,
        })
        return self._enqueue('batch', label[:40], config, scan_level,
                             detect_all, detect_top7)

    def _enqueue(self, inv_type, label, config, scan_level,
                 detect_all, detect_top7):
        with self._lock:
            conn = self._get_conn()
            try:
                # Count active items (queued or running)
                count = conn.execute(
                    "SELECT COUNT(*) FROM investigation_queue WHERE state IN (?,?)",
                    (STATE_QUEUED, STATE_RUNNING)
                ).fetchone()[0]

                if count >= self.MAX_SIZE:
                    return {'ok': False, 'error': f'Queue is full ({self.MAX_SIZE} max)'}

                pos = self._next_position(conn)
                conn.execute("""
                    INSERT INTO investigation_queue
                        (type, label, config, scan_level, detect_all_countries,
                         detect_top7_countries, state, position, queued_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (inv_type, label, config, scan_level,
                      int(detect_all), int(detect_top7),
                      STATE_QUEUED, pos,
                      datetime.now().isoformat()))
                conn.commit()
                item_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                return {'ok': True, 'item_id': item_id, 'position': pos}
            finally:
                conn.close()

    def remove(self, item_id):
        """Remove a queued item (cannot remove a running item)."""
        with self._lock:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT state FROM investigation_queue WHERE id=?", (item_id,)
                ).fetchone()
                if not row:
                    return {'ok': False, 'error': 'Item not found'}
                if row['state'] == STATE_RUNNING:
                    return {'ok': False, 'error': 'Cannot remove a running investigation — stop it first'}
                conn.execute("DELETE FROM investigation_queue WHERE id=?", (item_id,))
                conn.commit()
                self._reorder(conn)
                return {'ok': True}
            finally:
                conn.close()

    def clear_all(self):
        """Delete all queued (non-running) items."""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    "DELETE FROM investigation_queue WHERE state=?", (STATE_QUEUED,)
                )
                conn.commit()
                return {'ok': True}
            finally:
                conn.close()

    def stop_current(self):
        """Signal the currently running investigation to stop."""
        with self._lock:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT id FROM investigation_queue WHERE state=?",
                    (STATE_RUNNING,)
                ).fetchone()
                if not row:
                    return {'ok': False, 'error': 'Nothing is running'}
                item_id = row['id']
                if item_id not in self._stop_flags:
                    self._stop_flags[item_id] = threading.Event()
                self._stop_flags[item_id].set()
                return {'ok': True, 'stopped_id': item_id}
            finally:
                conn.close()

    def is_stopped(self, item_id):
        """Called from inside the pipeline to check for stop signal."""
        flag = self._stop_flags.get(item_id)
        return flag is not None and flag.is_set()

    def get_state(self):
        """Return full queue state for the UI."""
        conn = self._get_conn()
        try:
            rows = self._all_rows(conn)
            # Parse config JSON for display
            for r in rows:
                try:
                    r['config'] = json.loads(r.get('config') or '{}')
                except Exception:
                    r['config'] = {}
            return rows
        finally:
            conn.close()

    def get_running(self):
        """Return the currently running item, or None."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM investigation_queue WHERE state=?",
                (STATE_RUNNING,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_item(self, item_id):
        """Return a specific queue item."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM investigation_queue WHERE id=?", (item_id,)
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            try:
                d['config'] = json.loads(d.get('config') or '{}')
            except Exception:
                d['config'] = {}
            return d
        finally:
            conn.close()

    # ── Queue positions ───────────────────────────────────────────────────────

    def _reorder(self, conn):
        """Re-number positions after a delete."""
        rows = conn.execute(
            "SELECT id FROM investigation_queue WHERE state=? ORDER BY position ASC, id ASC",
            (STATE_QUEUED,)
        ).fetchall()
        for i, row in enumerate(rows, start=1):
            conn.execute(
                "UPDATE investigation_queue SET position=? WHERE id=?",
                (i, row[0])
            )
        conn.commit()

    # ── Worker thread ─────────────────────────────────────────────────────────

    def _start_worker(self):
        self._worker_thread = threading.Thread(
            target=self._worker_loop, daemon=True, name='QueueWorker'
        )
        self._worker_thread.start()

    def inject_app(self, flask_app):
        """Called from app.py after Flask app is created."""
        self._flask_app = flask_app

    def _worker_loop(self):
        """
        Main loop — picks the next queued item, validates cookies,
        runs the pipeline, sends Telegram notification, sleeps cooldown.
        """
        print('[Queue] Worker started')
        last_finished_at = None

        while True:
            try:
                # ── pick next queued item ──────────────────────────────────
                conn = self._get_conn()
                row  = conn.execute(
                    "SELECT * FROM investigation_queue WHERE state=? ORDER BY position ASC LIMIT 1",
                    (STATE_QUEUED,)
                ).fetchone()
                conn.close()

                if not row:
                    time.sleep(self.POLL_INTERVAL)
                    continue

                item = dict(row)
                item_id = item['id']

                # ── cooldown check ─────────────────────────────────────────
                if last_finished_at:
                    elapsed = time.time() - last_finished_at
                    remaining = self.COOLDOWN_SEC - elapsed
                    if remaining > 0:
                        print(f'[Queue] Cooldown — waiting {remaining:.0f}s')
                        self._write_cooldown_status(remaining)
                        time.sleep(min(remaining, self.POLL_INTERVAL))
                        continue

                # ── cookie validation ──────────────────────────────────────
                print(f'[Queue] Validating cookies before item #{item_id}')
                cookie_ok, cookie_reason = self._validate_cookies()
                if not cookie_ok:
                    print(f'[Queue] Cookie check FAILED: {cookie_reason}')
                    self._mark(item_id, STATE_FAILED,
                               error_msg=f'Cookie expired: {cookie_reason}')
                    self._write_queue_error('cookie_expired', cookie_reason)
                    last_finished_at = time.time()
                    continue

                # ── mark running ───────────────────────────────────────────
                self._mark(item_id, STATE_RUNNING)

                # create stop flag for this item
                with self._lock:
                    self._stop_flags[item_id] = threading.Event()

                # ── dispatch pipeline ──────────────────────────────────────
                try:
                    config = json.loads(item.get('config') or '{}')
                    print(f'[Queue] Running item #{item_id} ({item["type"]}) — {item["label"]}')

                    if item['type'] == 'profile':
                        self._run_profile(item_id, item, config)
                    else:
                        self._run_batch(item_id, item, config)

                    # check if stopped
                    if self.is_stopped(item_id):
                        self._mark(item_id, STATE_STOPPED)
                        #telegram.send(
                        #    f'⏹ Investigation stopped: *{item["label"]}*'
                        #)
                    else:
                        self._mark(item_id, STATE_DONE)
                        

                except Exception as e:
                    print(f'[Queue] Pipeline error item #{item_id}: {e}')
                    self._mark(item_id, STATE_FAILED, error_msg=str(e)[:300])
                    #telegram.send(
                    #    f'❌ Investigation failed: *{item["label"]}*\n`{str(e)[:200]}`'
                    #)

                finally:
                    last_finished_at = time.time()
                    # cleanup stop flag
                    with self._lock:
                        self._stop_flags.pop(item_id, None)

            except Exception as outer:
                print(f'[Queue] Worker outer error: {outer}')
                time.sleep(self.POLL_INTERVAL)

    # ── Pipeline dispatchers ──────────────────────────────────────────────────

    def _run_profile(self, item_id, item, config):
        """Dispatch to app.py's run_pipeline_auto — with stop-flag awareness."""
        profile_url = config['profile_url']
        scan_level  = item['scan_level']
        detect_all  = bool(item.get('detect_all_countries'))
        detect_top7 = bool(item.get('detect_top7_countries', 1))

        # We need app context for get_profile_stats etc.
        # Import lazily to avoid circular imports
        import app as flask_app_module

        # Create or look up profile in DB
        import socmint_db_import
        socmint_db_import.init_db(flask_app_module.DB_FILE)

        conn = sqlite3.connect(flask_app_module.DB_FILE)
        cur  = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO profiles (profile_url) VALUES (?)", (profile_url,))
        conn.commit()
        cur.execute("SELECT id FROM profiles WHERE profile_url=?", (profile_url,))
        profile_id = cur.fetchone()[0]
        conn.close()

        # Run the pipeline — passes stop_flag so pipeline can check
        flask_app_module.run_pipeline_auto(
            profile_url, profile_id, scan_level,
            stop_fn=lambda: self.is_stopped(item_id),
            detect_all_countries=detect_all,
            detect_top7_countries=detect_top7,
        )

    def _run_batch(self, item_id, item, config):
        """Dispatch to app.py's run_pipeline_manual — with stop-flag awareness."""
        import app as flask_app_module

        batch_id   = config['batch_id']
        label      = config['label']
        urls       = config['urls']
        scan_level = item['scan_level']
        detect_all = bool(item.get('detect_all_countries'))

        flask_app_module.run_pipeline_manual(
            batch_id, label, urls, scan_level,
            stop_fn=lambda: self.is_stopped(item_id),
            detect_all_countries=detect_all,
        )

    # ── DB state helpers ──────────────────────────────────────────────────────

    def _mark(self, item_id, state, error_msg=None):
        conn = self._get_conn()
        now  = datetime.now().isoformat()
        if state == STATE_RUNNING:
            conn.execute(
                "UPDATE investigation_queue SET state=?, started_at=? WHERE id=?",
                (state, now, item_id)
            )
        elif state in (STATE_DONE, STATE_FAILED, STATE_STOPPED):
            conn.execute(
                "UPDATE investigation_queue SET state=?, finished_at=?, error_msg=? WHERE id=?",
                (state, now, error_msg, item_id)
            )
        else:
            conn.execute(
                "UPDATE investigation_queue SET state=? WHERE id=?",
                (state, item_id)
            )
        conn.commit()
        conn.close()

    def _write_cooldown_status(self, remaining_seconds):
        """Write cooldown info to status dir so UI can display countdown."""
        path = os.path.join(STATUS_DIR, 'queue_cooldown.json')
        with open(path, 'w') as f:
            json.dump({
                'in_cooldown':  True,
                'remaining':    int(remaining_seconds),
                'until':        datetime.fromtimestamp(
                                    time.time() + remaining_seconds
                                ).strftime('%H:%M:%S'),
            }, f)

    def _write_queue_error(self, error_type, reason):
        """Write a queue-level error so UI can display it."""
        path = os.path.join(STATUS_DIR, 'queue_error.json')
        with open(path, 'w') as f:
            json.dump({
                'error_type': error_type,
                'reason':     reason,
                'at':         datetime.now().isoformat(),
            }, f)

    # ── Cookie validation ─────────────────────────────────────────────────────

    def _validate_cookies(self):
        """
        Lightweight cookie check — file exists + size threshold.
        Falls back to True so we don't block on SeleniumBase timing.
        The heavy SB check runs on the scan-new API call instead.
        """
        cookie_path = os.path.join(BASE_DIR, 'fb_cookies.pkl')
        if not os.path.exists(cookie_path):
            return False, 'Cookie file not found — run refresh_cookies.py'
        if os.path.getsize(cookie_path) < 700:
            return False, 'Cookie file too small — session likely expired'
        return True, 'Cookie file present'


    # ── On-demand country detection ───────────────────────────────────────────

    def detect_country_single(self, commentor_id, is_batch=False):
        """
        Trigger country detection for a single commentor on demand.
        Rate-limited by caller (4 per 2 min enforced in the API route).
        Returns dict with result.
        """
        try:
            import commentor_country

            # Get model from app config
            model = 'gemma3:4b'
            if self._flask_app:
                model = self._flask_app.config.get('OLLAMA_MODEL', 'gemma3:4b')
            commentor_country.OLLAMA_MODEL = model

            if is_batch:
                import socmint_manual_db
                db_file = os.path.join(BASE_DIR, 'socmint_manual.db')
            else:
                db_file = os.path.join(BASE_DIR, 'socmint.db')

            result = commentor_country.detect_single(commentor_id, db_file)
            return {'ok': True, 'result': result}
        except Exception as e:
            print(f'[Queue] Country detect error: {e}')
            return {'ok': False, 'error': str(e)}


# ── Singleton ─────────────────────────────────────────────────────────────────

queue = InvestigationQueue()