from seleniumbase import SB
import pickle, time, os, subprocess, json

if os.name != 'nt':
    try:
        os.environ.setdefault('DISPLAY', ':99')
        subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1)
    except FileNotFoundError:
        pass

COOKIE_FILE  = "fb_cookies.pkl"
PROFILE_URL  = "REDACTED"
MAX_REELS    = 10
OUTPUT_FILE  = "fb_reels.json"


def login(sb):
    sb.open("https://www.facebook.com")
    time.sleep(3)
    for c in pickle.load(open(COOKIE_FILE, "rb")):
        try: sb.driver.add_cookie(c)
        except: pass
    sb.driver.refresh()
    time.sleep(5)
    print("Logged in")


def get_reels_url(profile_url):
    profile_url = profile_url.rstrip('/')
    if 'profile.php' in profile_url:
        return profile_url + "&sk=reels_tab"
    return profile_url + "/reels"


#  PHASE 1 — Collect reel URLs

COLLECT_REEL_LINKS_JS = r"""
var seen = new Set();
var result = [];
document.querySelectorAll('a').forEach(function(link) {
    var href = link.href || '';
    if (!href.includes('facebook.com')) return;
    if (!href.match(/\/reel\/[a-zA-Z0-9]+/) && !href.match(/\/reels\/[a-zA-Z0-9]+/)) return;
    if (seen.has(href)) return;
    seen.add(href);
    result.push(href.split('?')[0]);
});
return result;
"""

CLICK_COMMENT_ICON_JS = """
var btns = document.querySelectorAll('[aria-label="Comment"][role="button"]');
for (var i = 0; i < btns.length; i++) {
    if (btns[i].getAttribute('tabindex') === '0') {
        btns[i].click();
        return true;
    }
}
if (btns.length > 0) { btns[0].click(); return true; }
return false;
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
// fallback — scroll page
window.scrollBy(0, 400);
return -1;
"""

GET_PANEL_TOP_JS = """
var els = document.querySelectorAll('*');
for (var i = 0; i < els.length; i++) {
    var el = els[i];
    var style = window.getComputedStyle(el);
    var rect  = el.getBoundingClientRect();
    if ((style.overflowY === 'auto' || style.overflowY === 'scroll') &&
        rect.left > 800 && rect.height > 300) {
        return el.scrollTop;
    }
}
return window.scrollY;
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

CLICK_MOST_RELEVANT_JS = """
var btns = document.querySelectorAll('div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').trim().toLowerCase();
    if (t === 'most relevant' || t === 'newest' || t === 'all comments') {
        btns[i].click();
        return true;
    }
}
return false;
"""

CLICK_ALL_COMMENTS_JS = """
var btns = document.querySelectorAll('div[role="menuitem"], div[role="option"], div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').trim().toLowerCase();
    if (t === 'all comments' || t.startsWith('all comments')) {
        btns[i].click();
        return true;
    }
}
return false;
"""

EXPAND_COMMENTS_JS = """
var clicked = 0;
if (!window.__fb_clicked) window.__fb_clicked = new Set();
var btns = document.querySelectorAll('div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').toLowerCase().trim();
    var isMore = t.includes('view more comment') || t.includes('more comment');
    if (!isMore) continue;
    var rect = btns[i].getBoundingClientRect();
    var sig  = t.substring(0, 30) + '|' + Math.round(rect.top) + '|' + Math.round(rect.left);
    if (window.__fb_clicked.has(sig)) continue;
    window.__fb_clicked.add(sig);
    btns[i].click();
    clicked++;
}
return clicked;
"""

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
        url.includes('/reel/')         || url.includes('/hashtag/')) return;

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


def phase1_collect_reels(sb,PROFILE_URL=PROFILE_URL,MAX_REELS=MAX_REELS):
    print("\n" + "═"*65)
    print("PHASE 1 — Collecting reel URLs")
    print("═"*65)

    reels_url = get_reels_url(PROFILE_URL)
    print(f"Opening: {reels_url}")
    sb.open(reels_url)
    time.sleep(6)

    reel_links = []
    seen       = set()
    scroll_n   = 0
    no_change  = 0
    MAX_SCROLLS = 60

    while len(reel_links) < MAX_REELS and scroll_n < MAX_SCROLLS:

        found = sb.execute_script(
            f"(function(){{ {COLLECT_REEL_LINKS_JS} }})()"
        ) or []

        for href in found:
            if href in seen:
                continue
            seen.add(href)
            reel_links.append(href)
            print(f"  [{len(reel_links)}] {href}")
            if len(reel_links) >= MAX_REELS:
                break

        print(f"  scroll #{scroll_n}  total: {len(reel_links)}")

        if len(reel_links) >= MAX_REELS:
            break

        prev = len(reel_links)

        # Slow scroll — 200px steps
        current_y = sb.execute_script("(function(){ return window.scrollY; })()") or 0
        target_y  = current_y + 800
        step_y    = current_y + 200
        while step_y <= target_y:
            sb.execute_script(f"(function(){{ window.scrollTo(0, {step_y}); }})()")
            time.sleep(0.8)
            step_y += 200
        time.sleep(5)

        scroll_n += 1

        if len(reel_links) == prev:
            no_change += 1
        else:
            no_change = 0

        if no_change >= 8:
            print("  No new reels for 8 scrolls — stopping")
            break

    print(f"\n  Total reels found: {len(reel_links)}")
    return reel_links


def scroll_panel(sb):
    sb.execute_script("(function(){ window.__fb_clicked = new Set(); })()")
    step = 0
    no_new = 0
    prev_count = 0

    while step < 150:
        step += 1
        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()") or 0
        if clicked:
            print(f"      [expand] clicked {clicked} new buttons — waiting for load...")
            time.sleep(3)
            cur_count = sb.execute_script(
                "(function(){ return document.querySelectorAll('div.x1rg5ohu').length; })()"
            ) or 0
            if cur_count > prev_count:
                prev_count = cur_count
                no_new = 0
            else:
                no_new += 1
                if no_new >= 8:
                    print("      [expand] No new comments loading — stopping")
                    break
            continue

        new_top = sb.execute_script(f"(function(){{ {SCROLL_PANEL_JS} }})()") or 0
        time.sleep(2)
        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()") or 0
        if clicked:
            print(f"      [expand] clicked {clicked} new buttons after scroll...")
            time.sleep(3)
            no_new = 0
        else:
            no_new += 1
            print(f"      [panel scroll] step={step} scrollTop={new_top}px  no_new={no_new}")
            if no_new >= 8:
                print("      [panel scroll] No more comments — done")
                break

    sb.execute_script(f"(function(){{ {PANEL_TO_BOTTOM_JS} }})()")
    time.sleep(2)

def phase2_scrape_reel(sb, reel_url, idx, total):
    print(f"\n  🎬 [{idx}/{total}] {reel_url}")

    sb.open(reel_url)
    time.sleep(8)

    # Click comment icon
    print("    [comments] Clicking comment icon...")
    clicked = sb.execute_script(f"(function(){{ {CLICK_COMMENT_ICON_JS} }})()")
    if not clicked:
        print("    Comment icon not found")
    time.sleep(4)

    # Switch to All comments
    print("    [comments] Clicking sort dropdown...")
    sb.execute_script(f"(function(){{ {CLICK_MOST_RELEVANT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(3)

    # Scroll panel
    print("    [comments] Scrolling comment panel...")
    scroll_panel(sb)

    # Scrape
    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments] scraped {len(comments)} comments")

    return {
        'reel_url': reel_url,
        'comments': comments
    }


def main(PROFILE_URL=PROFILE_URL,MAX_REELS=MAX_REELS):
    results = []

    with SB(uc=True, headless=False, xvfb=True,
            window_size="1280,900") as sb:

        login(sb)

        # Phase 1
        reel_links = phase1_collect_reels(sb,PROFILE_URL,MAX_REELS)

        print(f"\n\n{'═'*65}")
        print(f"PHASE 2 — Scraping comments for {len(reel_links)} reels")
        print("═"*65)

        for i, reel_url in enumerate(reel_links, 1):
            try:
                result = phase2_scrape_reel(sb, reel_url, i, len(reel_links))
                results.append(result)
            except Exception as e:
                print(f"    Error on reel {i}: {e}")
                results.append({
                    'reel_url': reel_url,
                    'comments': [],
                    'error':    str(e)
                })
            time.sleep(3)

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n\n{'═'*65}")
    print("SUMMARY")
    print("═"*65)
    for r in results:
        print(f"\n  {r['reel_url']}")
        print(f"     comments: {len(r.get('comments', []))}")
        for c in r.get('comments', []):
            snippet = c['comment_text'][:60] + ('…' if len(c['comment_text']) > 60 else '')
            print(f"       {c['name']:25s}  {snippet}")

    print(f"\nSaved to {OUTPUT_FILE}")


