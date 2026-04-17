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

COOKIE_FILE = "fb_cookies.pkl"
OUTPUT_FILE = "fb_manual_scrape.json"
MIN_URLS    = 1
MAX_URLS    = 15

def login(sb):
    sb.open("https://www.facebook.com")
    time.sleep(3)
    for c in pickle.load(open(COOKIE_FILE, "rb")):
        try: sb.driver.add_cookie(c)
        except: pass
    sb.driver.refresh()
    time.sleep(5)
    print(" Logged in")


def detect_url_type(url):
    """Detect whether URL is a photo, post, reel, or video."""
    if 'photo.php' in url or '/photo/' in url or '/photo?' in url:
        return 'photo'
    if '/reel/' in url or '/reels/' in url:
        return 'reel'
    if '/videos/' in url:
        return 'video'
    if '/posts/' in url or 'story_fbid' in url or 'permalink.php' in url:
        return 'post'
    return None


def collect_urls_from_user(MAX_URLS=MAX_URLS):
    print("\n" + "═"*65)
    print("Facebook Manual Scraper — Photo / Post / Reel / Video")
    print(f"Enter between {MIN_URLS} and {MAX_URLS} Facebook URLs.")
    print("Supports: photo.php, /posts/, /reel/, /videos/")
    print("Press ENTER on empty line when done.")
    print("═"*65)

    urls = []
    while len(urls) < MAX_URLS:
        try:
            raw = input(f"  URL [{len(urls)+1}/{MAX_URLS}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not raw:
            if len(urls) < MIN_URLS:
                print(f"  ⚠️  Please enter at least {MIN_URLS} URL(s).")
                continue
            break

        if 'facebook.com' not in raw:
            print(f"  ⚠️  Not a Facebook URL — skipped.")
            continue

        url_type = detect_url_type(raw)
        if not url_type:
            print(f"  ⚠️  Could not detect type (photo/post/reel) — skipped.")
            continue

        # Clean tracking params from post/video URLs
        if url_type in ('post', 'video') and '?' in raw:
            raw = raw.split('?')[0]
        elif url_type == 'photo' and 'permalink.php' not in raw and '?' in raw:
            # Keep fbid= for photo.php but strip __cft__ etc
            from urllib.parse import urlparse, urlencode, parse_qs
            p = urlparse(raw)
            qs = parse_qs(p.query)
            clean_qs = {k: v for k, v in qs.items() if k in ('fbid', 'set', 'id', 'story_fbid')}
            raw = p.scheme + '://' + p.netloc + p.path
            if clean_qs:
                raw += '?' + urlencode({k: v[0] for k, v in clean_qs.items()})

        if raw in [u['url'] for u in urls]:
            print(f"  ⚠️  Duplicate URL — skipped.")
            continue

        urls.append({'url': raw, 'type': url_type})
        print(f"   Added [{len(urls)}] [{url_type}] {raw}")

        if len(urls) == MAX_URLS:
            print(f"    Maximum {MAX_URLS} URLs reached.")
            break

    print(f"\n   Total URLs to scrape: {len(urls)}")
    for u in urls:
        print(f"     [{u['type']:5s}] {u['url']}")
    return urls


#  SHARED JS


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

#  PHOTO JS


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

IMAGE_SRC_JS = """
var imgs = document.querySelectorAll(
    'div.x6s0dn4.x78zum5.xdt5ytf.xl56j7k.x1n2onr6 img[src*="scontent"]'
);
var srcs = [];
imgs.forEach(function(img) { srcs.push(img.src); });
return srcs;
"""

CAPTION_JS = """
var caption = null;
var container = document.querySelector('div.xyinxu5.xyri2b');
if (container) {
    var span = container.querySelector('span[dir="auto"]');
    if (span) {
        var text = '';
        span.childNodes.forEach(function(node) {
            if (node.nodeType === 3) {
                text += node.textContent;
            } else if (node.nodeType === 1) {
                var el = node;
                var img = el.tagName === 'IMG' ? el : el.querySelector('img[alt]');
                if (img) {
                    text += img.getAttribute('alt') || '';
                } else {
                    text += el.innerText || '';
                }
            }
        });
        caption = text.trim() || null;
    }
}
if (!caption) {
    var msg = document.querySelector(
        '[data-ad-comet-preview="message"], [data-ad-preview="message"]'
    );
    if (msg) {
        var spans = msg.querySelectorAll('span, div');
        var text = '';
        for (var i = 0; i < spans.length; i++) {
            var el = spans[i];
            var style = el.getAttribute('style') || '';
            if (style.includes('position: absolute') || style.includes('top: 3em')) continue;
            if (el.children.length === 0) {
                var t = (el.innerText || '').trim();
                if (t) text += t + ' ';
            }
        }
        caption = text.trim() || null;
    }
}
return caption;
"""

#  POST JS


POST_TEXT_JS = """
var text = null;

function extractNodeText(node) {
    var t = '';
    node.childNodes.forEach(function(child) {
        if (child.nodeType === 3) {
            t += child.textContent;
        } else if (child.nodeType === 1) {
            var img = child.tagName === 'IMG' ? child : child.querySelector('img[alt]');
            if (img) {
                t += img.getAttribute('alt') || '';
            } else {
                t += extractNodeText(child);
            }
        }
    });
    return t;
}

// Primary — data-ad-comet-preview or data-ad-preview message block
var msg = document.querySelector(
    '[data-ad-comet-preview="message"], [data-ad-preview="message"]'
);
if (msg) {
    var paras = msg.querySelectorAll('div[dir="auto"]');
    var full = '';
    paras.forEach(function(para) {
        var line = extractNodeText(para).trim();
        if (line) full += line + '\\n';
    });
    text = full.trim() || null;
}

// Fallback — xdj266r container (post body wrapper from DOM)
if (!text) {
    var containers = document.querySelectorAll('div.xdj266r div[dir="auto"]');
    var full = '';
    var seen = new Set();
    for (var i = 0; i < containers.length; i++) {
        var line = extractNodeText(containers[i]).trim();
        if (!line || seen.has(line)) continue;
        // Skip UI labels
        if (['Like', 'Comment', 'Share', 'Follow', 'See more'].indexOf(line) !== -1) continue;
        seen.add(line);
        full += line + '\\n';
    }
    text = full.trim() || null;
}

return text;
"""

#  REEL JS
 
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


#  SCROLL 

def scroll_page(sb):
    """Scroll full page — for photo and post."""
    prev_y    = -1
    no_change = 0
    step      = 0

    while step < 150:
        step += 1
        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()") or 0
        if clicked:
            print(f"      [expand] clicked {clicked} buttons")
            time.sleep(3)
            # Reset no_change — new content may have loaded after expand
            no_change = 0

        sb.execute_script("(function(){ window.scrollBy(0, 400); })()")
        time.sleep(1.5)
        cur_y = sb.execute_script("(function(){ return window.scrollY; })()") or 0
        print(f"      [scroll] step={step} y={cur_y}px")

        if cur_y <= prev_y:
            no_change += 1
            if no_change >= 10:
                # One final big scroll before giving up
                sb.execute_script("(function(){ window.scrollTo(0, document.body.scrollHeight); })()")
                time.sleep(3)
                final_y = sb.execute_script("(function(){ return window.scrollY; })()") or 0
                if final_y > cur_y:
                    # Page grew — keep going
                    no_change = 0
                    prev_y = final_y
                    continue
                print("      [scroll] Bottom reached")
                break
        else:
            no_change = 0
        prev_y = cur_y

    sb.execute_script("(function(){ window.scrollTo(0, document.body.scrollHeight); })()")
    time.sleep(2)


def scroll_panel(sb):
    """Scroll side panel — for reels."""
    prev_top  = -1
    no_change = 0
    step      = 0

    while step < 150:
        step += 1
        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()") or 0
        if clicked:
            print(f"      [expand] clicked {clicked} buttons")
            time.sleep(2)

        new_top = sb.execute_script(f"(function(){{ {SCROLL_PANEL_JS} }})()") or 0
        time.sleep(1.2)
        print(f"      [panel scroll] step={step} scrollTop={new_top}px")

        if new_top <= prev_top:
            no_change += 1
            if no_change >= 5:
                print("      [panel scroll] Bottom reached")
                break
        else:
            no_change = 0
        prev_top = new_top

    sb.execute_script(f"(function(){{ {PANEL_TO_BOTTOM_JS} }})()")
    time.sleep(2)


#  TYPE-SPECIFIC SCRAPERS

def scrape_photo(sb, url, idx, total):
    print(f"\n   [{idx}/{total}] [photo] {url}")
    sb.open(url)
    time.sleep(8)

    date = None
    image_src = None
    caption = None
    
    for attempt in range(3):
        date      = sb.execute_script(f"(function(){{ {DATE_JS} }})()")
        srcs      = sb.execute_script(f"(function(){{ {IMAGE_SRC_JS} }})()") or []
        image_src = srcs[0] if srcs else None
        caption   = sb.execute_script(f"(function(){{ {CAPTION_JS} }})()")

        if date or image_src:
            break

        print(f"     Attempt {attempt+1}/3 — date/image not loaded yet, waiting...")
        time.sleep(4)

    print(f"    date:    {date or 'None'}")
    print(f"    image:   {image_src[:70] if image_src else 'NOT FOUND'}")
    print(f"    caption: {caption[:70] if caption else 'None'}")

    print("    [comments] Switching to All comments...")
    sb.execute_script(f"(function(){{ {CLICK_MOST_RELEVANT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(3)

    print("    [comments] Scrolling...")
    scroll_page(sb)

    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments]  {len(comments)} scraped")

    return {
        'url':      url,
        'type':     'photo',
        'date':     date,
        'image_src': image_src,
        'caption':  caption,
        'comments': comments
    }


def scrape_post(sb, url, idx, total):
    print(f"\n   [{idx}/{total}] [post] {url}")
    sb.open(url)
    time.sleep(10)

    date = sb.execute_script(f"(function(){{ {DATE_JS} }})()")
    print(f"    date:      {date or 'None'}")

    print("    [comments] Switching to All comments...")
    sb.execute_script(f"(function(){{ {CLICK_MOST_RELEVANT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(4)

    # Find and mark the scrollable comment panel
    found = sb.execute_script(f"(function(){{ {FIND_PANEL_JS} }})()")
    print(f"    [panel] found={found}")

    print("    [comments] Scrolling panel...")
    prev_count = 0
    seen_urls  = set()
    no_change  = 0
    step       = 0

    while step < 150:
        step += 1

        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()")
        if clicked:
            print(f"      [expand] clicked {clicked} buttons")
            time.sleep(3)
            no_change = 0

        r = sb.execute_script(f"(function(){{ {SCROLL_PANEL_POST_JS} }})()")
        time.sleep(2)
        sb.execute_script(f"(function(){{ {PANEL_BOTTOM_POST_JS} }})()")
        time.sleep(1)

        cur_count = sb.execute_script("""
            (function(){ return document.querySelectorAll('div.x1rg5ohu').length; })()
        """) or 0

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

    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments]  {len(comments)} scraped")

    # Take screenshot of post
    import os, re
    os.makedirs("post_screenshots", exist_ok=True)
    fbid = re.search(r'pfbid(\w+)|/posts/(\w+)', url)
    if fbid:
        name = next(g for g in fbid.groups() if g)
    else:
        name = str(idx)
    screenshot_path = os.path.join("post_screenshots", f"post_{name}.png")
    try:
        sb.execute_script("(function(){ window.scrollTo(0, 0); })()")
        time.sleep(1)
        sb.save_screenshot(screenshot_path)
        print(f"    📸 Screenshot saved: {screenshot_path}")
    except Exception as e:
        print(f"    ⚠️  Screenshot failed: {e}")
        screenshot_path = None

    return {
        'url':             url,
        'type':            'post',
        'date':            date,
        'screenshot_path': screenshot_path,
        'comments':        comments
    }

def scrape_reel(sb, url, idx, total):
    print(f"\n   [{idx}/{total}] [reel] {url}")
    sb.open(url)
    time.sleep(8)

    print("    [comments] Clicking comment icon...")
    clicked = sb.execute_script(f"(function(){{ {CLICK_COMMENT_ICON_JS} }})()")
    if not clicked:
        print("      Comment icon not found")
    time.sleep(4)

    print("    [comments] Switching to All comments...")
    sb.execute_script(f"(function(){{ {CLICK_MOST_RELEVANT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(3)

    print("    [comments] Scrolling panel...")
    scroll_panel(sb)

    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments]  {len(comments)} scraped")

    return {
        'url':      url,
        'type':     'reel',
        'comments': comments
    }


def scrape_video(sb, url, idx, total):
    """
    Scrape a Facebook /videos/ URL.
    Structure is identical to reels — video plays center, comments in side panel.
    Uses the same scroll_panel approach.
    """
    print(f"\n   [{idx}/{total}] [video] {url}")
    sb.open(url)
    time.sleep(8)

    # Try scraping date
    date = sb.execute_script(f"(function(){{ {DATE_JS} }})()")
    print(f"    date: {date or 'None'}")

    print("    [comments] Clicking comment icon...")
    clicked = sb.execute_script(f"(function(){{ {CLICK_COMMENT_ICON_JS} }})()")
    if not clicked:
        print("      Comment icon not found — trying direct scroll")
    time.sleep(4)

    print("    [comments] Switching to All comments...")
    sb.execute_script(f"(function(){{ {CLICK_MOST_RELEVANT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(3)

    print("    [comments] Scrolling panel...")
    scroll_panel(sb)

    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments]  {len(comments)} scraped")

    return {
        'url':      url,
        'type':     'video',
        'date':     date,
        'comments': comments
    }


#  MAIN

SCRAPERS = {
    'photo': scrape_photo,
    'post':  scrape_post,
    'reel':  scrape_reel,
    'video': scrape_video,
}

def main(MAX_URLS=MAX_URLS, urls=None):
    """
    urls: optional list of URL strings to bypass interactive input.
    If None, falls back to collect_urls_from_user() (CLI mode).
    """
    if urls is not None:
        # Web app mode — URLs passed directly, no input() needed
        url_items = []
        for raw in urls[:MAX_URLS]:
            raw = raw.strip()
            if not raw or 'facebook.com' not in raw:
                continue
            url_type = detect_url_type(raw)
            if not url_type:
                print(f"    Could not detect type for: {raw} — skipped")
                continue
            if url_type in ('post', 'video') and '?' in raw:
                raw = raw.split('?')[0]
            url_items.append({'url': raw, 'type': url_type})
        print(f"   {len(url_items)} URL(s) loaded from web app")
    else:
        url_items = collect_urls_from_user(MAX_URLS)

    if not url_items:
        print("   No URLs provided. Exiting.")
        return

    results = []

    with SB(uc=True, headless=False, xvfb=True,
            window_size="1280,900") as sb:

        login(sb)

        print(f"\n\n{'═'*65}")
        print(f"Scraping {len(url_items)} URL(s)")
        print("═"*65)

        for i, item in enumerate(url_items, 1):
            url      = item['url']
            url_type = item['type']
            scraper  = SCRAPERS[url_type]
            try:
                result = scraper(sb, url, i, len(url_items))
                results.append(result)
            except Exception as e:
                print(f"      Error: {e}")
                results.append({
                    'url':   url,
                    'type':  url_type,
                    'error': str(e)
                })
            time.sleep(3)

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n\n{'═'*65}")
    print("  SUMMARY")
    print("═"*65)
    for r in results:
        icon = {'photo': '📸', 'post': '📝', 'reel': '🎬', 'video': '🎥'}.get(r['type'], '🔗')
        print(f"\n  {icon} [{r['type']}] {r['url']}")
        if r.get('date'):
            print(f"     date:     {r['date']}")
        if r.get('caption'):
            print(f"     caption:  {r['caption'][:70]}")
        if r.get('post_text'):
            print(f"     text:     {r['post_text'][:70]}")
        print(f"     comments: {len(r.get('comments', []))}")
        for c in r.get('comments', []):
            snippet = c['comment_text'][:60] + ('…' if len(c['comment_text']) > 60 else '')
            print(f"       {c['name']:25s}  {snippet}")

    print(f"\n Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()