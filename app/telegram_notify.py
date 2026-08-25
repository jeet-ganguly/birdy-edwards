"""
telegram_notify.py  —  Birdy-Edwards Telegram Notification Module
==================================================================
Handles all Telegram Bot API communication:
  - Send text messages
  - Send PDF reports as documents
  - Save / load bot config
  - Test connection

Config is stored in .telegram_config (JSON) at the project root.
Format: { "token": "...", "chat_id": "..." }

Usage:
    from telegram_notify import telegram

    # Configure once
    telegram.save_config(token, chat_id)

    # Send messages
    telegram.send("Investigation complete!")
    telegram.send_report("/path/to/report.pdf", caption="John Doe — done")

    # Check status
    telegram.is_configured()   # → bool
    telegram.test()            # → (bool, str)
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime

# ── Config path ───────────────────────────────────────────────────────────────

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, '.telegram_config')

# ── Telegram API base ─────────────────────────────────────────────────────────

TG_BASE = 'https://api.telegram.org/bot{token}/{method}'

TIMEOUT_MSG = 10   # seconds for text messages
TIMEOUT_DOC = 60   # seconds for file uploads


# ─────────────────────────────────────────────────────────────────────────────
class TelegramNotifier:
    """
    Lightweight Telegram Bot API wrapper.
    No third-party dependencies — pure stdlib urllib.
    """

    # ── Config ────────────────────────────────────────────────────────────────

    def save_config(self, token: str, chat_id: str) -> dict:
        """
        Persist bot token and chat_id to .telegram_config.
        Called from /api/telegram/configure.
        """
        token   = token.strip()
        chat_id = chat_id.strip()

        if not token:
            return {'ok': False, 'error': 'Token is required'}
        if not chat_id:
            return {'ok': False, 'error': 'chat_id is required'}

        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump({
                    'token':      token,
                    'chat_id':    chat_id,
                    'saved_at':   datetime.now().isoformat(),
                }, f, indent=2)
            return {'ok': True}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def load_config(self) -> tuple[str, str]:
        """
        Return (token, chat_id) or ('', '') if not configured.
        """
        if not os.path.exists(CONFIG_FILE):
            return '', ''
        try:
            data    = json.loads(open(CONFIG_FILE).read())
            token   = data.get('token', '').strip()
            chat_id = data.get('chat_id', '').strip()
            return token, chat_id
        except Exception:
            return '', ''

    def is_configured(self) -> bool:
        """Return True if token + chat_id are both saved."""
        token, chat_id = self.load_config()
        return bool(token and chat_id)

    def status(self) -> dict:
        """Return configuration status for the UI."""
        token, chat_id = self.load_config()
        return {
            'configured': bool(token and chat_id),
            'chat_id':    chat_id or '',
        }

    # ── Core send helpers ─────────────────────────────────────────────────────

    def _api_url(self, token: str, method: str) -> str:
        return TG_BASE.format(token=token, method=method)

    def _post_json(self, token: str, method: str, payload: dict,
                   timeout: int = TIMEOUT_MSG) -> dict:
        """POST a JSON payload to the Telegram Bot API."""
        url  = self._api_url(token, method)
        data = json.dumps(payload).encode('utf-8')
        req  = urllib.request.Request(
            url, data=data,
            headers={'Content-Type': 'application/json'}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='replace')
            raise RuntimeError(f'Telegram HTTP {e.code}: {body}')

    def _post_multipart(self, token: str, method: str,
                        fields: dict, file_field: str,
                        file_path: str, file_mime: str,
                        timeout: int = TIMEOUT_DOC) -> dict:
        """POST multipart/form-data (for file uploads)."""
        url      = self._api_url(token, method)
        boundary = 'BirdyEdwards1337Boundary'

        def field_part(name: str, value: str) -> bytes:
            return (
                f'--{boundary}\r\n'
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f'{value}\r\n'
            ).encode('utf-8')

        body = b''
        for k, v in fields.items():
            body += field_part(k, str(v))

        filename = os.path.basename(file_path)
        with open(file_path, 'rb') as fh:
            file_data = fh.read()

        body += (
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{filename}"\r\n'
            f'Content-Type: {file_mime}\r\n\r\n'
        ).encode('utf-8') + file_data + b'\r\n'
        body += f'--{boundary}--\r\n'.encode('utf-8')

        req = urllib.request.Request(
            url, data=body,
            headers={'Content-Type': f'multipart/form-data; boundary={boundary}'}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            body_err = e.read().decode('utf-8', errors='replace')
            raise RuntimeError(f'Telegram HTTP {e.code}: {body_err}')

    # ── Public send API ───────────────────────────────────────────────────────

    def send(self, text: str, parse_mode: str = 'Markdown') -> bool:
        """
        Send a plain text message.
        Returns True on success, False if not configured or on error.
        """
        token, chat_id = self.load_config()
        if not token or not chat_id:
            print('[Telegram] Not configured — skipping message')
            return False
        try:
            result = self._post_json(token, 'sendMessage', {
                'chat_id':    chat_id,
                'text':       text,
                'parse_mode': parse_mode,
            })
            if result.get('ok'):
                print('[Telegram] Message sent ✓')
                return True
            else:
                print(f'[Telegram] API error: {result}')
                return False
        except Exception as e:
            print(f'[Telegram] Send error: {e}')
            return False

    def send_report(self, pdf_path: str, caption: str = '') -> bool:
        """
        Send a PDF file via sendDocument.
        Falls back to a text-only message if the file does not exist.
        Returns True on success.
        """
        token, chat_id = self.load_config()
        if not token or not chat_id:
            print('[Telegram] Not configured — skipping report')
            return False

        if not pdf_path or not os.path.exists(pdf_path):
            print(f'[Telegram] PDF not found: {pdf_path} — sending text only')
            return self.send(caption or 'Investigation complete (no PDF)')

        try:
            result = self._post_multipart(
                token        = token,
                method       = 'sendDocument',
                fields       = {
                    'chat_id':    chat_id,
                    'caption':    caption[:1024],   # Telegram caption limit
                    'parse_mode': 'Markdown',
                },
                file_field   = 'document',
                file_path    = pdf_path,
                file_mime    = 'application/pdf',
            )
            if result.get('ok'):
                print(f'[Telegram] PDF sent ✓ — {os.path.basename(pdf_path)}')
                return True
            else:
                print(f'[Telegram] sendDocument API error: {result}')
                # Fallback to text
                return self.send(caption or 'Investigation complete (PDF send failed)')
        except Exception as e:
            print(f'[Telegram] send_report error: {e}')
            # Fallback to text message
            return self.send(caption or f'Investigation complete (PDF error: {e})')

    def send_investigation_complete(self, label: str, inv_type: str,
                                    scan_level: str, pdf_path: str = None) -> bool:
        """
        High-level helper — sends completion notification after an investigation.
        Attaches PDF if available, text-only if not.
        Called from queue_manager.py after each pipeline run.
        """
        type_label  = '🔍 Profile' if inv_type == 'profile' else '📦 Batch'
        level_emoji = {'light': '⚡', 'medium': '🔥', 'deep': '🚀'}.get(scan_level, '🔥')

        caption = (
            f'✅ *Investigation complete*\n\n'
            f'{type_label}: `{label}`\n'
            f'Scan level: {level_emoji} {scan_level.capitalize()}\n'
            f'Completed at: {datetime.now().strftime("%d %b %Y · %H:%M")}'
        )

        if pdf_path and os.path.exists(pdf_path):
            return self.send_report(pdf_path, caption=caption)
        else:
            return self.send(caption + '\n_Report not generated_')

    def send_investigation_stopped(self, label: str, inv_type: str) -> bool:
        """Notify that an investigation was manually stopped."""
        type_label = '🔍 Profile' if inv_type == 'profile' else '📦 Batch'
        return self.send(
            f'⏹ *Investigation stopped*\n\n'
            f'{type_label}: `{label}`\n'
            f'Stopped at: {datetime.now().strftime("%d %b %Y · %H:%M")}'
        )

    def send_investigation_failed(self, label: str, inv_type: str,
                                   error: str = '') -> bool:
        """Notify that an investigation failed."""
        type_label = '🔍 Profile' if inv_type == 'profile' else '📦 Batch'
        msg = (
            f'❌ *Investigation failed*\n\n'
            f'{type_label}: `{label}`\n'
            f'Failed at: {datetime.now().strftime("%d %b %Y · %H:%M")}'
        )
        if error:
            msg += f'\n\nError:\n`{error[:300]}`'
        return self.send(msg)

    def send_cookie_expired(self, next_label: str = '') -> bool:
        """Notify that cookies expired before an investigation could start."""
        msg = (
            '🍪 *Session expired — Queue paused*\n\n'
            'Facebook cookies are invalid or missing.\n'
            'Please re-import your session cookies to resume the queue.'
        )
        if next_label:
            msg += f'\n\nNext in queue: `{next_label}`'
        return self.send(msg)

    # ── Test / verify ─────────────────────────────────────────────────────────

    def test(self) -> tuple[bool, str]:
        """
        Send a test message to verify the bot config works.
        Returns (success: bool, message: str).
        """
        token, chat_id = self.load_config()
        if not token or not chat_id:
            return False, 'Not configured — save token and chat_id first'

        # First verify the token is valid via getMe
        try:
            me = self._post_json(token, 'getMe', {})
            if not me.get('ok'):
                return False, f'Invalid token: {me}'
        except Exception as e:
            return False, f'Cannot reach Telegram API: {e}'

        # Send test message
        ok = self.send(
            '🔔 *Birdy-Edwards*\n\n'
            'Test notification — Telegram is configured correctly!\n'
            f'Bot: @{me["result"].get("username", "unknown")}'
        )
        if ok:
            return True, 'Test message sent successfully'
        else:
            return False, 'Token valid but message failed — check chat_id'


# ── Singleton ─────────────────────────────────────────────────────────────────

telegram = TelegramNotifier()