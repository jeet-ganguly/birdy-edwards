from seleniumbase import SB
import pickle, time, os, subprocess, json, re

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
OUTPUT_FILE  = "fb_about.json"

DIRECTORY_SECTIONS = [
    "directory_personal_details",  # city, hometown, relationship, family
    "directory_work",              # work/employer
    "directory_education",         # college, high school
    "directory_intro",             # intro/bio
    "activities",                  # hobbies
    "directory_names",             # nicknames
]

FIELD_LABELS = {
    "current_city":     "Current City",
    "hometown":         "Hometown",
    "relationship":     "Relationship",
    "family":           "Family Member",
    "work":             "Work",
    "employer":         "Employer",
    "college":          "College",
    "high_school":      "High School",
    "education":        "Education",
    "intro":            "Introduction",
    "hobby":            "Hobby",
    "hobbies":          "Hobbies",
    "nickname":         "Nickname",
    "other_name":       "Other Name",
    "name":             "Name",
    "birth_date":       "Birth Date",
    "birthday":         "Birthday",
    "gender":           "Gender",
    "languages":        "Languages",
    "language":         "Language",
    "political_view":   "Political View",
    "religious_view":   "Religious View",
    "website":          "Website",
    "address":          "Address",
}

def decode_unicode(val):
    """Decode \\uXXXX escape sequences to proper unicode characters."""
    if not val or not isinstance(val, str):
        return val
    try:
        return val.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
    except Exception:
        try:
            return json.loads(f'"{val}"')
        except Exception:
            return val


def login(sb):
    sb.open("https://www.facebook.com")
    time.sleep(3)
    for c in pickle.load(open(COOKIE_FILE, "rb")):
        try: sb.driver.add_cookie(c)
        except: pass
    sb.driver.refresh()
    time.sleep(5)
    print("Logged in")


def get_directory_url(profile_url, section):
    profile_url = profile_url.rstrip('/')
    if 'profile.php' in profile_url:
        return profile_url + f"&sk={section}"
    return profile_url + f"/{section}"


# Locked profile check

IS_LOCKED_JS = """
var indicators = [
    'This account is private',
    'Add friend to see',
    'profile is locked',
    'Add as friend to see',
    'only visible to friends'
];
var bodyText = (document.body.innerText || '').toLowerCase();
for (var i = 0; i < indicators.length; i++) {
    if (bodyText.includes(indicators[i].toLowerCase())) return true;
}
// Check for lock icon in profile area
var lockImgs = document.querySelectorAll('image[href*="lock"], img[src*="lock"]');
if (lockImgs.length > 0) return true;
return false;
"""

# Owner name scraper

GET_OWNER_NAME_JS = """
// Try profile name from the cover photo area — most reliable
// Facebook puts the name in a span inside h1 on the profile page
var selectors = [
    'h1 span',                          // name inside h1 span
    '[data-overviewsection] h1',        // about section h1
    'div[data-pagelet="ProfileActions"] ~ div h1',  // near action buttons
];
for (var i = 0; i < selectors.length; i++) {
    var el = document.querySelector(selectors[i]);
    if (el) {
        var name = (el.innerText || '').trim();
        if (name.length > 1 && name !== 'Notifications') return name;
    }
}
// Fallback — all h1s, pick first that is not a UI label
var h1s = document.querySelectorAll('h1');
for (var j = 0; j < h1s.length; j++) {
    var name = (h1s[j].innerText || '').trim();
    var skip = ['Notifications', 'Facebook', 'Menu', 'Search', 'Home'];
    var bad = false;
    for (var k = 0; k < skip.length; k++) {
        if (name === skip[k]) { bad = true; break; }
    }
    if (!bad && name.length > 1) return name;
}
// Fallback — og:title meta tag
var meta = document.querySelector('meta[property="og:title"]');
if (meta) {
    var name = (meta.getAttribute('content') || '').trim();
    if (name.length > 1) return name;
}
return null;
"""


# Page source parsers 

def parse_page_source(source, section):
    results  = []
    seen_keys = set()

    main_pattern = re.compile(
        r'"field_type"\s*:\s*"([^"]+)"'
        r'.{0,300}?'
        r'"title"\s*:\s*\{'
        r'[^}]{0,300}?'
        r'"text"\s*:\s*"([^"]+)"',
        re.DOTALL
    )
    label_pattern = re.compile(
        r'"list_items"\s*:\s*\[\s*\{'
        r'[^}]{0,200}?'
        r'"text"\s*:\s*\{'
        r'[^}]{0,200}?'
        r'"text"\s*:\s*"([^"]+)"',
        re.DOTALL
    )

    for m in main_pattern.finditer(source):
        field_type = m.group(1)
        value      = decode_unicode(m.group(2))
        if field_type in ('MEDIUM', 'HIGH', 'LOW') or len(field_type) > 50:
            continue
        key = f"{field_type}:{value}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        label = FIELD_LABELS.get(field_type, field_type.replace('_', ' ').title())
        results.append({
            "section":    section,
            "field_type": field_type,
            "label":      label,
            "value":      value,
            "sub_label":  None
        })

    sub_labels = label_pattern.findall(source)
    sub_idx = 0
    for r in results:
        if r["field_type"] in ("family", "relationship") and sub_idx < len(sub_labels):
            r["sub_label"] = sub_labels[sub_idx]
            sub_idx += 1

    return results


def parse_directory_items(source, section):
    results = []
    seen    = set()

    pattern = re.compile(
        r'"group_key"\s*:\s*"([^"]+)"'
        r'.{0,500}?'
        r'"renderer"\s*:\s*\{'
        r'.{0,300}?'
        r'"title"\s*:\s*\{'
        r'.{0,200}?'
        r'"text"\s*:\s*"([^"]+)"',
        re.DOTALL
    )

    for m in pattern.finditer(source):
        group_key = m.group(1)
        value     = decode_unicode(m.group(2))
        key = f"{group_key}:{value}"
        if key in seen:
            continue
        seen.add(key)
        label = group_key.replace('_', ' ').title()
        results.append({
            "section":    section,
            "field_type": group_key.lower(),
            "label":      label,
            "value":      value,
            "sub_label":  None
        })

    return results


# Main scraper

def main(PROFILE_URL=PROFILE_URL):
    print("\n" + "═"*65)
    print("Facebook About Scraper")
    print(f"Profile: {PROFILE_URL}")
    print("═"*65)

    all_fields  = []
    owner_name  = None
    is_locked   = False

    with SB(uc=True, headless=False, xvfb=True,
            window_size="1280,900") as sb:

        login(sb)

        # Step 1: Open profile page — check locked + get owner name
        print(f"\n   Checking profile...")
        sb.open(PROFILE_URL)
        time.sleep(6)

        # Get owner name
        owner_name = sb.execute_script(f"(function(){{ {GET_OWNER_NAME_JS} }})()")
        print(f"   Owner name: {owner_name or 'NOT FOUND'}")

        # Check if locked
        is_locked = sb.execute_script(f"(function(){{ {IS_LOCKED_JS} }})()") or False
        if is_locked:
            print(f"   Profile is LOCKED — about sections may be restricted")
        else:
            print(f"   Profile is PUBLIC")

        # Step 2: Scrape directory sections 
        for section in DIRECTORY_SECTIONS:
            url = get_directory_url(PROFILE_URL, section)
            print(f"\n   {section}")
            print(f"     {url}")

            sb.open(url)
            time.sleep(5)

            source = sb.get_page_source()
            if section == "activities":
                fields = parse_directory_items(source, section)
            else:
                fields = parse_page_source(source, section)

            if fields:
                for f in fields:
                    sub = f" ({f['sub_label']})" if f['sub_label'] else ""
                    print(f" {f['label']:25s} → {f['value']}{sub}")
                all_fields.extend(fields)
            else:
                print(f"   No data found")

    # Organize output
    output = {
        "profile_url": PROFILE_URL,
        "owner_name":  owner_name,
        "is_locked":   is_locked,
        "sections":    {}
    }

    for f in all_fields:
        sec = f["section"]
        if sec not in output["sections"]:
            output["sections"][sec] = []
        entry = {
            "field_type": f["field_type"],
            "label":      f["label"],
            "value":      f["value"],
        }
        if f["sub_label"]:
            entry["sub_label"] = f["sub_label"]
        output["sections"][sec].append(entry)

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n\n{'═'*65}")
    print("  SUMMARY")
    print("═"*65)
    print(f"   Owner  : {owner_name or 'N/A'}")
    print(f"   Locked : {is_locked}")
    for sec, fields in output["sections"].items():
        print(f"\n  [{sec}]")
        for f in fields:
            sub = f" ({f['sub_label']})" if f.get('sub_label') else ""
            print(f"    {f['label']:25s} → {f['value']}{sub}")

    print(f"\n Saved to {OUTPUT_FILE}")
    return output

if __name__=="__main__":
    main()