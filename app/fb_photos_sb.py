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
MAX_PHOTOS   = 12
OUTPUT_FILE  = "fb_photos.json"


def js(sb, script, *args):
    """Wrap all JS in IIFE — required for SeleniumBase UC/CDP mode."""
    if args:
        # Pass arguments via injected var
        arg_js = "var arguments = " + json.dumps(list(args)) + ";"
        return sb.execute_script(f"(function(){{ {arg_js} {script} }})()")
    return sb.execute_script(f"(function(){{ {script} }})()")


def login(sb):
    sb.open("https://www.facebook.com")
    time.sleep(3)
    for c in pickle.load(open(COOKIE_FILE, "rb")):
        try: sb.driver.add_cookie(c)
        except: pass
    sb.driver.refresh()
    time.sleep(5)
    print("Logged in")


def get_photos_url(profile_url):
    profile_url = profile_url.rstrip('/')
    if 'profile.php' in profile_url:
        return profile_url + "&sk=photos"
    return profile_url + "/photos"


#  PHASE 1 — Collect photo URLs from /photos page

COLLECT_PHOTO_LINKS_JS = """
var seen = new Set();
var result = [];
document.querySelectorAll('a').forEach(function(link) {
    var href = link.href || '';
    if (!href.includes('facebook.com')) return;
    if (!href.includes('fbid=')) return;
    if (seen.has(href)) return;

    var type = null;
    if (href.includes('photo.php')) {
        type = 'post_photo';
    } else if (href.includes('/photo/') && href.includes('__tn__=%3C')) {
        type = 'profile_picture';
    } else if (href.includes('/photo/') && href.includes('set=a.')) {
        type = 'cover_photo';
    }

    if (type) {
        seen.add(href);
        result.push({ url: href, type: type });
    }
});
return result;
"""

COLLECT_PHOTO_LINKS_JS_IIFE = """
var seen = new Set();
var result = [];
document.querySelectorAll('a').forEach(function(link) {
    var href = link.href || '';
    if (!href.includes('facebook.com')) return;
    if (!href.includes('fbid=')) return;
    if (seen.has(href)) return;

    var type = null;
    if (href.includes('photo.php')) {
        type = 'post_photo';
    } else if (href.includes('/photo/') && href.includes('__tn__=%3C')) {
        type = 'profile_picture';
    } else if (href.includes('/photo/') && href.includes('set=a.')) {
        type = 'cover_photo';
    }

    if (type) {
        seen.add(href);
        result.push({ url: href, type: type });
    }
});
return result;
"""


def phase1_collect_photos(sb,PROFILE_URL=PROFILE_URL,MAX_PHOTOS=MAX_PHOTOS):
    print("\n" + "═"*65)
    print("PHASE 1 — Collecting photo URLs")
    print("═"*65)

    photos_url = get_photos_url(PROFILE_URL)
    print(f"Opening: {photos_url}")
    sb.open(photos_url)
    time.sleep(6)

    photo_links = []
    seen        = set()
    scroll_n    = 0
    no_change   = 0
    MAX_SCROLLS = 60

    while len(photo_links) < MAX_PHOTOS and scroll_n < MAX_SCROLLS:

        found = sb.execute_script(
            f"(function(){{ {COLLECT_PHOTO_LINKS_JS_IIFE} }})()"
        ) or []

        for item in found:
            href  = item['url']
            ptype = item['type']
            if href in seen:
                continue
            seen.add(href)
            photo_links.append(item)
            print(f" [{len(photo_links)}] [{ptype}] {href}")
            if len(photo_links) >= MAX_PHOTOS:
                break

        print(f"  scroll #{scroll_n}  total: {len(photo_links)}")

        if len(photo_links) >= MAX_PHOTOS:
            break

        prev = len(photo_links)

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

        if len(photo_links) == prev:
            no_change += 1
        else:
            no_change = 0

        if no_change >= 8:
            print("  No new photos for 8 scrolls — stopping")
            break

    post    = [p for p in photo_links if p['type'] == 'post_photo']
    profile = [p for p in photo_links if p['type'] == 'profile_picture'] #Later write this link into a file
    cover   = [p for p in photo_links if p['type'] == 'cover_photo']     
    print(f"\n  post_photo={len(post)}  profile_picture={len(profile)}  cover_photo={len(cover)}")
    return photo_links


#  PHASE 2 — For each post_photo: grab image src + caption + scrape comments

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


def scroll_to_bottom(sb):
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

        sb.execute_script("(function(){ window.scrollBy(0, 300); })()")
        time.sleep(2)
        clicked = sb.execute_script(f"(function(){{ {EXPAND_COMMENTS_JS} }})()") or 0
        if clicked:
            print(f"      [expand] clicked {clicked} new buttons after scroll...")
            time.sleep(3)
            no_new = 0
        else:
            no_new += 1
            cur_y = sb.execute_script("(function(){ return window.scrollY; })()") or 0
            print(f"      [scroll] step={step} y={cur_y}px  no_new={no_new}")
            if no_new >= 8:
                print("      [scroll] No more comments — done")
                break

    sb.execute_script("(function(){ window.scrollTo(0, document.body.scrollHeight); })()")
    time.sleep(2)


def phase2_scrape_photo(sb, photo_url, idx, total):
    print(f"\n  [{idx}/{total}] {photo_url}")

    sb.open(photo_url)
    time.sleep(8)

    # Grab image src
    # Retry date/image/caption up to 3 times for slow-loading posts
    image_src = None
    date      = None
    caption   = None

    for attempt in range(3):
        srcs      = sb.execute_script(f"(function(){{ {IMAGE_SRC_JS} }})()") or []
        image_src = srcs[0] if srcs else None
        date      = sb.execute_script(f"(function(){{ {DATE_JS} }})()")
        caption   = sb.execute_script(f"(function(){{ {CAPTION_JS} }})()")

        if image_src or date:
            break

        print(f"    Attempt {attempt+1}/3 — page still loading, waiting 4s...")
        time.sleep(4)

    print(f"    image_src: {image_src[:80] if image_src else 'NOT FOUND'}")
    print(f"    caption: {caption[:80] if caption else 'None'}")
    print(f"    date: {date if date else 'NOT FOUND'}")

    # Switch to All Comments
    print("    [comments] Clicking sort dropdown...")
    sb.execute_script(f"(function(){{ {CLICK_MOST_RELEVANT_JS} }})()")
    time.sleep(3)
    sb.execute_script(f"(function(){{ {CLICK_ALL_COMMENTS_JS} }})()")
    time.sleep(3)

    # Scroll to load all comments
    print("    [comments] Scrolling to load all comments...")
    scroll_to_bottom(sb)

    # Scrape
    comments = sb.execute_script(f"(function(){{ {SCRAPE_COMMENTS_JS} }})()") or []
    print(f"    [comments] scraped {len(comments)} comments")

    return {
        'photo_url': photo_url,
        'date':      date,
        'image_src': image_src,
        'caption':   caption,
        'comments':  comments
    }


#  MAIN

def main(PROFILE_URL=PROFILE_URL,MAX_PHOTOS=MAX_PHOTOS):
    results = []

    with SB(uc=True, headless=False, xvfb=True,
            window_size="1280,900",agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") as sb:

        login(sb)

        # Phase 1
        all_photos  = phase1_collect_photos(sb,PROFILE_URL,MAX_PHOTOS)
        post_photos = [p for p in all_photos if p['type'] == 'post_photo']
        #profile_cover = [set(all_photos) - set(post_photos)]

        #print(f"Profile photo link -> {i for i in profile_cover if }")
        print(f"\n\n{'═'*65}")
        print(f"PHASE 2 — Scraping {len(post_photos)} post photos")
        print("═"*65)

        for i, photo in enumerate(post_photos, 1):
            try:
                result = phase2_scrape_photo(sb, photo['url'], i, len(post_photos))
                results.append(result)
            except Exception as e:
                print(f"    ⚠️  Error on photo {i}: {e}")
                results.append({
                    'photo_url': photo['url'],
                    'image_src': None,
                    'caption':   None,
                    'comments':  [],
                    'error':     str(e)
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
        print(f"\n   {r['photo_url']}")
        print(f"     date:     {r.get('date') or 'N/A'}")
        print(f"     image:    {r.get('image_src','')[:70] if r.get('image_src') else 'N/A'}")
        print(f"     caption:  {r.get('caption','')[:70] if r.get('caption') else 'N/A'}")
        print(f"     comments: {len(r.get('comments', []))}")
        for c in r.get('comments', []):
            snippet = c['comment_text'][:60] + ('…' if len(c['comment_text']) > 60 else '')
            print(f"       {c['name']:25s}  {snippet}")

    print(f"\n Saved to {OUTPUT_FILE}")
