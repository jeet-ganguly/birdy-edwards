import time
import json
import pickle
import os
from seleniumbase import SB

COOKIES_PATH = 'fb_cookies.pkl'
TEST_URL     = 'REDACTED'
OUTPUT_FILE  = 'fb_video_test_output.json'


SCRAPE_COMMENTS_JS = """
var profiles = document.querySelectorAll('div.x1rg5ohu');
var seen = {};
profiles.forEach(function(div) {
    var parent = div.parentElement;
    var isReply = false;
    while (parent) {
        if (parent !== div && parent.classList && parent.classList.contains('x1rg5ohu')) {
            isReply = true; break;
        }
        parent = parent.parentElement;
    }
    if (isReply) return;

    var a = div.querySelector('a[href]');
    if (!a) return;
    var name = (a.innerText || '').trim();
    var raw  = a.href || '';
    var url  = raw.includes('profile.php') ? raw.split('&')[0] : raw.split('?')[0];

    if (!name || name.length < 2) return;
    if (url.includes('l.facebook.com') || url.includes('photo.php') ||
        url.includes('story.php')      || url.includes('permalink')  ||
        url.includes('share')          || url.includes('/posts/')    ||
        url.includes('/photos/')       || url.includes('/videos/')   ||
        url.includes('/hashtag/')) return;

    var key = url;
    if (seen[key]) return;

    var text = '';
    var spans = div.querySelectorAll('div[dir="auto"] span, span[dir="auto"]');
    for (var i = 0; i < spans.length; i++) {
        var t = (spans[i].innerText || '').trim();
        if (!t || t === name || t.length <= 1) continue;
        if (t.toLowerCase() === 'follow') continue;
        if (t.toLowerCase() === 'by author') continue;
        // Skip timestamps: '5d', '1w', '2h', '3m', '1y' etc
        if (/^\\d+[smhdwy]$/.test(t)) continue;
        // Skip short relative times like '5 days', '1 week'
        if (/^\\d+\\s+(second|minute|hour|day|week|month|year)s?$/.test(t)) continue;
        text = t; break;
    }

    if (!text) {
        var p = div.parentElement;
        for (var d = 0; d < 8 && p; d++, p = p.parentElement) {
            var ps = p.querySelectorAll('div[dir="auto"] span');
            for (var k = 0; k < ps.length; k++) {
               var t = (ps[k].innerText || '').trim();
                if (!t || t === name || t.length <= 1) continue;
                if (t.toLowerCase() === 'follow') continue;
                if (t.toLowerCase() === 'by author') continue;
                if (/^\\d+[smhdwy]$/.test(t)) continue;
                if (/^\\d+\\s+(second|minute|hour|day|week|month|year)s?$/.test(t)) continue;
                var parentA = ps[k].closest('a');
                if (parentA) continue;
                text = t; break;
            }
            if (text) break;
        }
    }

    if (!text) {
        var c = div.parentElement;
        for (var d2 = 0; d2 < 8 && c; d2++, c = c.parentElement) {
            var imgs = c.querySelectorAll('img[src]');
            for (var m = 0; m < imgs.length; m++) {
                var src = (imgs[m].src || '').toLowerCase();
                var alt = (imgs[m].alt || '').trim();
                if (src.includes('giphy') || src.includes('tenor') ||
                    src.includes('sticker') || src.includes('fbsbx'))
                    { text = alt ? '[Sticker: '+alt+']' : '[Sticker]'; break; }
                if (src.includes('.gif'))
                    { text = alt ? '[GIF: '+alt+']' : '[GIF]'; break; }
                if (src.includes('emoji') || src.includes('unicode'))
                    { text = alt ? '[Emoji: '+alt+']' : '[Emoji]'; break; }
            }
            if (text) break;
        }
    }

    seen[key] = { name: name, profile_url: url, comment_text: text || '[Non-text comment]' };
});
return Object.values(seen);
"""


SCROLL_PANEL_JS = """
var els = document.querySelectorAll('*');
for (var i = 0; i < els.length; i++) {
    var el = els[i];
    var style = window.getComputedStyle(el);
    var rect  = el.getBoundingClientRect();
    if ((style.overflowY === 'auto' || style.overflowY === 'scroll') &&
        rect.left > 800 && rect.height > 300) {
        el.scrollTop += 400;
        return el.scrollTop;
    }
}
window.scrollBy(0, 400);
return -1;
"""

PANEL_TO_BOTTOM_JS = """
var els = document.querySelectorAll('*');
for (var i = 0; i < els.length; i++) {
    var el = els[i];
    var style = window.getComputedStyle(el);
    var rect  = el.getBoundingClientRect();
    if ((style.overflowY === 'auto' || style.overflowY === 'scroll') &&
        rect.left > 800 && rect.height > 300) {
        el.scrollTop = el.scrollHeight;
        return true;
    }
}
window.scrollTo(0, document.body.scrollHeight);
return false;
"""

CLICK_COMMENT_ICON_JS = """
var btns = document.querySelectorAll('[aria-label="Comment"][role="button"]');
for (var i = 0; i < btns.length; i++) {
    if (btns[i].getAttribute('tabindex') === '0') {
        btns[i].click(); return true;
    }
}
if (btns.length > 0) { btns[0].click(); return true; }
return false;
"""

EXPAND_COMMENTS_JS = """
var clicked = 0;
var btns = document.querySelectorAll('div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').toLowerCase().trim();
    if (t.includes('view more comment') || t.includes('more comment')) {
        btns[i].click(); clicked++;
    }
}
return clicked;
"""

CLICK_ALL_COMMENTS_JS = """
var btns = document.querySelectorAll(
    'div[role="menuitem"], div[role="option"], div[role="button"], span[role="button"]'
);
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').trim().toLowerCase();
    if (t === 'all comments' || t.startsWith('all comments')) {
        btns[i].click(); return true;
    }
}
return false;
"""

SCRAPE_DATE_JS = """
var months = 'January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec';
var datePattern = new RegExp('^(\\\\d{1,2}\\\\s+(' + months + ')(\\\\s+\\\\d{4})?|(' + months + ')\\\\s+\\\\d{1,2},?(\\\\s+\\\\d{4})?|\\\\d{1,2}\\\\s+(' + months + ')\\\\s+at\\\\s+\\\\d{2}:\\\\d{2})$');

// Strategy 1 — _r_ / _R_ span
var dateEl =
    document.querySelector('.__fb-light-mode > span[id^="_r_"]') ||
    document.querySelector('.__fb-dark-mode > span[id^="_r_"]')  ||
    document.querySelector('.__fb-light-mode > span[id^="_R_"]') ||
    document.querySelector('.__fb-dark-mode > span[id^="_R_"]');
if (dateEl) {
    var t = dateEl.innerText.trim();
    if (t && t.length > 0) return t;
}

// Strategy 2 — scan all _r_ / _R_ prefix spans
var allSpans = document.querySelectorAll('span[id]');
for (var i = 0; i < allSpans.length; i++) {
    if (/^_[rR]_/.test(allSpans[i].id)) {
        var t2 = allSpans[i].innerText.trim();
        if (t2 && t2.length > 0) return t2;
    }
}

// Strategy 3 — scan all spans direct text
var candidates = document.querySelectorAll('span');
for (var j = 0; j < candidates.length; j++) {
    var directText = '';
    candidates[j].childNodes.forEach(function(node) {
        if (node.nodeType === 3) directText += node.textContent;
    });
    directText = directText.trim();
    if (datePattern.test(directText)) return directText;
}

return null;
"""



def scroll_panel(sb, max_scrolls=80):
    """Scroll the comment side-panel — same as reel scraper."""
    prev_pos  = -1
    no_change = 0

    for step in range(max_scrolls):
        # Expand any "View more comments" buttons
        clicked = sb.execute_script(
            f"(function(){{ {EXPAND_COMMENTS_JS} }})()"
        ) or 0
        if clicked:
            print(f"      [expand] clicked {clicked} buttons")
            time.sleep(2)

        pos = sb.execute_script(
            f"(function(){{ {SCROLL_PANEL_JS} }})()"
        ) or 0
        print(f"      [scroll-panel] step={step+1} pos={pos}")
        time.sleep(1.8)

        if pos <= prev_pos:
            no_change += 1
            if no_change >= 6:
                # Try scrolling to absolute bottom
                sb.execute_script(
                    f"(function(){{ {PANEL_TO_BOTTOM_JS} }})()"
                )
                time.sleep(3)
                print("      [scroll-panel] Panel bottom reached")
                break
        else:
            no_change = 0
        prev_pos = pos


def scrape_video_url(sb, url):
    """
    Scrape a Facebook /videos/ URL.
    Strategy: same as reel — open page, click comment icon,
    scroll side panel, extract comments.
    """
    print(f"\n  [video] {url}")
    result = {
        'url':      url,
        'type':     'video',
        'date_text': None,
        'caption':   None,
        'image_src': None,
        'comments':  []
    }

    try:
        sb.open(url)
        time.sleep(4)

        # Check if redirected to login
        current = sb.get_current_url()
        if 'login' in current or 'checkpoint' in current:
            print("      Redirected to login — cookies may be expired")
            return result

        # Try scraping date
        try:
            date = sb.execute_script(
                f"(function(){{ {SCRAPE_DATE_JS} }})()"
            )
            if date:
                result['date_text'] = date.strip()
                print(f"      date: {result['date_text']}")
        except Exception:
            pass

        # Try to click comment icon (opens side panel like reels)
        try:
            clicked = sb.execute_script(
                f"(function(){{ {CLICK_COMMENT_ICON_JS} }})()"
            )
            if clicked:
                print("      [click] Comment icon clicked")
                time.sleep(3)
            else:
                print("      [click] No comment icon found — trying direct scroll")
                time.sleep(2)
        except Exception:
            pass

        # Switch to All Comments if dropdown appears
        try:
            sb.execute_script(
                f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()"
            )
            time.sleep(2)
        except Exception:
            pass

        # Scroll panel to load all comments
        scroll_panel(sb, max_scrolls=60)

        # Scrape comments
        comments = sb.execute_script(
            f"(function(){{ {SCRAPE_COMMENTS_JS} }})()"
        ) or []

        result['comments'] = comments
        print(f"      {len(comments)} comments scraped")

    except Exception as e:
        print(f"      Error: {e}")

    return result


def main():
    print(f"\n  Test URL: {TEST_URL}\n")

    if not os.path.exists(COOKIES_PATH):
        print(f"Cookies not found at {COOKIES_PATH}")
        print("   Run: python3 refresh_cookies.py")
        return

    with SB(uc=True, headless=False, xvfb=False) as sb:
        # Load cookies
        print("  Loading cookies...")
        sb.open('https://www.facebook.com')
        time.sleep(3)
        cookies = pickle.load(open(COOKIES_PATH, 'rb'))
        for c in cookies:
            try:
                sb.driver.add_cookie(c)
            except Exception:
                pass
        sb.open('https://www.facebook.com')
        time.sleep(3)
        print("  Logged in")

        # Scrape the test video
        result = scrape_video_url(sb, TEST_URL)

        # Save output
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump([result], f, ensure_ascii=False, indent=2)

        print(f"\n  Output saved to: {OUTPUT_FILE}")
        print(f"  Comments found: {len(result['comments'])}")
        if result['comments']:
            print("\n  First 3 comments:")
            for c in result['comments'][:3]:
                print(f"    - {c['name']}: {c['comment_text'] or '(no text)'}")


if __name__ == '__main__':
    main()