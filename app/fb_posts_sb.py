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

COOKIE_FILE   = "fb_cookies.pkl"
OUTPUT_FILE   = "fb_posts.json"


def login(sb):
    sb.open("https://www.facebook.com")
    time.sleep(3)
    for c in pickle.load(open(COOKIE_FILE, "rb")):
        try: sb.driver.add_cookie(c)
        except: pass
    sb.driver.refresh()
    time.sleep(5)
    print("Logged in")


def js(sb, script):
    return sb.execute_script(f"(function(){{ {script} }})()")



COLLECT_POSTS_JS = """
var seen = new Set();
var result = [];
var links = document.querySelectorAll('a[href*="/posts/"], a[href*="story_fbid"], a[href*="permalink.php"]');
links.forEach(function(a) {
    var href = a.href || '';
    if (!href.includes('facebook.com')) return;
    if (href.includes('/stories/')) return;

    // Clean URL
    var clean = href.includes('permalink.php') ? href : href.split('?')[0];
    if (seen.has(clean)) return;
    seen.add(clean);
    result.push(clean);
});
return result;
"""

POST_TEXT_JS = """
var text = null;

function extractNodeText(node) {
    var t = '';
    node.childNodes.forEach(function(child) {
        if (child.nodeType === 3) {
            t += child.textContent;
        } else if (child.nodeType === 1) {
            if (child.tagName === 'BR') {
                t += '\\n';
            } else {
                var img = child.tagName === 'IMG' ? child : child.querySelector('img[alt]');
                if (img) { t += img.getAttribute('alt') || ''; }
                else { t += extractNodeText(child); }
            }
        }
    });
    return t;
}

// Primary — exact class from DOM analysis
var container = document.querySelector(
    'div.xdj266r.x14z9mp.xat24cr.x1lziwak.x1vvkbs'
);
if (container) {
    text = extractNodeText(container).trim() || null;
}

// Fallback — data-ad-comet-preview message block
if (!text) {
    var msg = document.querySelector(
        '[data-ad-comet-preview="message"], [data-ad-preview="message"]'
    );
    if (msg) {
        text = extractNodeText(msg).trim() || null;
    }
}

return text;
"""


DATE_JS = """
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


SEE_MORE_JS = """
var btns = document.querySelectorAll('div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').trim();
    if (t === 'See more' || t === 'See More') { btns[i].click(); break; }
}
"""


CLICK_SORT_JS = """
var btns = document.querySelectorAll('div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').trim().toLowerCase();
    if (t === 'most relevant' || t === 'newest' || t === 'all comments') {
        btns[i].click(); return true;
    }
}
return false;
"""

CLICK_ALL_COMMENTS_JS = """
var btns = document.querySelectorAll('div[role="menuitem"], div[role="option"], div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').trim().toLowerCase();
    if (t === 'all comments' || t.startsWith('all comments')) {
        btns[i].click(); return true;
    }
}
return false;
"""


EXPAND_COMMENTS_JS = """
var clicked = 0;
var btns = document.querySelectorAll('div[role="button"], span[role="button"]');
for (var i = 0; i < btns.length; i++) {
    var t = (btns[i].innerText || '').toLowerCase().trim();
    if (t.includes('view more comment') ||
        t.includes('more comment') ||
        t.includes('see more comment') ||
        /^\\d+\\s+more comment/.test(t)) {
        btns[i].click();
        clicked++;
    }
}
return clicked;
"""

FIND_PANEL_JS = """
var els = document.querySelectorAll('*');
for (var i = 0; i < els.length; i++) {
    var el = els[i];
    var style = window.getComputedStyle(el);
    var rect = el.getBoundingClientRect();
    if ((style.overflowY === 'auto' || style.overflowY === 'scroll')
        && el.scrollHeight > el.clientHeight + 10
        && rect.height > 100
        && rect.left > 100) {
        el.setAttribute('data-comment-panel', 'true');
        return true;
    }
}
return false;
"""

SCROLL_PANEL_POST_JS = """
var panel = document.querySelector('[data-comment-panel="true"]');
if (panel) {
    panel.scrollTop += 600;
    return {scrollTop: panel.scrollTop, scrollHeight: panel.scrollHeight,
            atBottom: panel.scrollTop + panel.clientHeight >= panel.scrollHeight - 5};
}
window.scrollBy(0, 600);
return {scrollTop: window.scrollY, scrollHeight: document.body.scrollHeight, atBottom: false};
"""

PANEL_BOTTOM_POST_JS = """
var panel = document.querySelector('[data-comment-panel="true"]');
if (panel) { panel.scrollTop = panel.scrollHeight; return panel.scrollTop; }
window.scrollTo(0, document.body.scrollHeight);
return window.scrollY;
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


#  PHASE 1 — Collect /posts/ URLs

def phase1_collect_urls(sb, profile_url, max_posts):
    print("\n" + "═"*65)
    print("PHASE 1 — Collecting post URLs")
    print("═"*65)

    sb.open(profile_url)
    time.sleep(6)

    post_links = []
    seen       = set()
    scroll_n   = 0
    no_change  = 0
    MAX_SCROLLS = 60

    while len(post_links) < max_posts and scroll_n < MAX_SCROLLS:

        found = sb.execute_script(
            f"(function(){{ {COLLECT_POSTS_JS} }})()"
        ) or []

        for href in found:
            if href in seen:
                continue
            seen.add(href)
            post_links.append(href)
            print(f"  ✅ [{len(post_links)}] {href}")
            if len(post_links) >= max_posts:
                break

        print(f"  📋 scroll #{scroll_n}  total: {len(post_links)}")

        if len(post_links) >= max_posts:
            break

        prev = len(post_links)

        # Slow scroll
        current_y = sb.execute_script("(function(){ return window.scrollY; })()") or 0
        target_y  = current_y + 800
        step_y    = current_y + 200
        while step_y <= target_y:
            sb.execute_script(f"(function(){{ window.scrollTo(0, {step_y}); }})()")
            time.sleep(0.8)
            step_y += 200
        time.sleep(5)

        scroll_n += 1

        if len(post_links) == prev:
            no_change += 1
        else:
            no_change = 0

        if no_change >= 8:
            print("  ⚠️  No new posts for 8 scrolls — stopping")
            break

    print(f"\n  Total posts found: {len(post_links)}")
    return post_links


#  PHASE 2 — Scrape each post: text + date + comments


def scroll_to_bottom(sb):
    sb.execute_script("(function(){ window.__fb_clicked = new Set(); })()")

    # Find and mark scrollable comment panel
    found = sb.execute_script(f"(function(){{ {FIND_PANEL_JS} }})()")
    print(f"      [panel] found={found}")

    prev_count = 0
    no_change  = 0
    step       = 0

    while step < 150:
        step += 1

        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()") or 0
        if clicked:
            print(f"      [expand] clicked {clicked} buttons")
            time.sleep(3)
            no_change = 0

        r = sb.execute_script(f"(function(){{ {SCROLL_PANEL_POST_JS} }})()")
        time.sleep(2)
        sb.execute_script(f"(function(){{ {PANEL_BOTTOM_POST_JS} }})()")
        time.sleep(1)

        cur_count = sb.execute_script(
            "(function(){ return document.querySelectorAll('div.x1rg5ohu').length; })()"
        ) or 0

        at_bottom = r.get('atBottom', False) if isinstance(r, dict) else False
        print(f"      [scroll] step={step} dom={cur_count} scrollTop={r.get('scrollTop',0) if isinstance(r,dict) else r} atBottom={at_bottom}")

        if cur_count <= prev_count:
            no_change += 1
            if no_change >= 8:
                print("      [scroll] No new comments — done")
                break
        else:
            no_change = 0

        prev_count = cur_count

def take_post_screenshot(sb, post_url, idx):
    """Take screenshot of post area and save to screenshots folder."""
    import re
    os.makedirs("post_screenshots", exist_ok=True)

    # Generate filename from post URL
    fbid = re.search(r'pfbid(\w+)|/posts/(\w+)|fbid=(\w+)', post_url)
    if fbid:
        name = next(g for g in fbid.groups() if g)
    else:
        name = str(idx)

    filepath = os.path.join("post_screenshots", f"post_{name}.png")

    try:
        # Scroll to top first
        sb.execute_script("(function(){ window.scrollTo(0, 0); })()")
        time.sleep(1)
        sb.save_screenshot(filepath)
        print(f"    Screenshot saved: {filepath}")
        return filepath
    except Exception as e:
        print(f"    ⚠️Screenshot failed: {e}")
        return None


def phase2_scrape_post(sb, post_url, idx, total):
    print(f"\n  [{idx}/{total}] {post_url}")

    sb.open(post_url)
    time.sleep(8)

    # Date
    date = sb.execute_script(f"(function(){{ {DATE_JS} }})()")
    print(f"    date       : {date or 'None'}")

    # Expand See more before screenshot
    sb.execute_script(f"(function(){{ {SEE_MORE_JS} }})()")
    time.sleep(2)

    # Take screenshot of post
    screenshot_path = take_post_screenshot(sb, post_url, idx)

    # Switch to All Comments
    print("    [comments] Switching to All comments...")
    sb.execute_script(f"(function(){{ {CLICK_SORT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(3)

    # Scroll + expand
    print("    [comments] Scrolling...")
    scroll_to_bottom(sb)

    # Scrape comments
    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments] {len(comments)} scraped")

    return {
        'post_url':        post_url,
        'date':            date,
        'screenshot_path': screenshot_path,
        'comments':        comments
    }



def main(profile_url="https://www.facebook.com/fozia.s.qazi", max_posts=10):
    if not profile_url:
        profile_url = input("Enter profile URL: ").strip()

    results = []

    with SB(uc=True, headless=False, xvfb=True,
            window_size="1280,900",agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") as sb:

        login(sb)

        # Phase 1 — collect post URLs
        post_links = phase1_collect_urls(sb, profile_url, max_posts)

        print(f"\n\n{'═'*65}")
        print(f"PHASE 2 — Scraping {len(post_links)} posts")
        print("═"*65)

        for i, post_url in enumerate(post_links, 1):
            try:
                result = phase2_scrape_post(sb, post_url, i, len(post_links))
                results.append(result)
            except Exception as e:
                print(f"    ⚠️  Error on post {i}: {e}")
                results.append({
                    'post_url':        post_url,
                    'date':            None,
                    'screenshot_path': None,
                    'comments':        [],
                    'error':           str(e)
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
        print(f"\n {r['post_url']}")
        print(f"     date       : {r.get('date') or 'N/A'}")
        print(f"     screenshot : {r.get('screenshot_path') or 'N/A'}")
        print(f"     comments   : {len(r.get('comments', []))}")
        for c in r.get('comments', []):
            snippet = c['comment_text'][:60] + ('…' if len(c['comment_text']) > 60 else '')
            print(f"       {c['name']:25s}  {snippet}")

    print(f"\nSaved to {OUTPUT_FILE}")
    return results


if __name__ == "__main__":
    main()