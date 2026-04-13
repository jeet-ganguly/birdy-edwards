import sqlite3
import json
import os
import base64

DB_FILE        = "socmint.db"
MANUAL_DB_FILE = "socmint_manual.db"
REPORTS_DIR    = "reports"
THREAT_ICON_PATH = "icons/threat.png"
PERSON_ICON_PATH = "icons/user.png"


def load_icon(path, fallback_url):
    """
    Load icon as base64 data URI — works offline in pyvis HTML.
    Falls back to CDN URL if local file not found.
    """
    try:
        if os.path.exists(path):
            with open(path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("utf-8")
            ext = os.path.splitext(path)[1].lstrip('.')
            print(f"   Loaded icon: {path}")
            return f"data:image/{ext};base64,{b64}"
    except Exception as e:
        print(f"    Could not load icon {path}: {e}")
    return fallback_url


THREAT_ICON = load_icon(THREAT_ICON_PATH, "https://cdn-icons-png.flaticon.com/512/564/564619.png")
PERSON_ICON = load_icon(PERSON_ICON_PATH, "https://cdn-icons-png.flaticon.com/512/1077/1077012.png")

def get_node_color(score):
    if score > 0.5:   return "#2ecc71"   # 🟢 Strong Supporter
    if score > 0.1:   return "#3498db"   # 🔵 Supporter
    if score > -0.1:  return "#95a5a6"   # ⚪ Neutral
    if score > -0.5:  return "#f39c12"   # 🟡 Low Interaction
    return "#e74c3c"                     # 🔴 Critical Voice

def get_node_label(score):
    if score > 0.5:   return "Strong Supporter"
    if score > 0.1:   return "Supporter"
    if score > -0.1:  return "Neutral"
    if score > -0.5:  return "Low Interaction"
    return "Critical Voice"

def get_node_size(comment_count):
    if comment_count >= 5:  return 35
    if comment_count >= 3:  return 25
    if comment_count >= 2:  return 20
    return 15

def get_country_flag(country):
    """Return flag emoji for common countries."""
    if not country:
        return ""
    c = country.lower()
    flags = {
        # South Asia
        'india': '🇮🇳', 'pakistan': '🇵🇰', 'bangladesh': '🇧🇩',
        'nepal': '🇳🇵', 'sri lanka': '🇱🇰', 'afghanistan': '🇦🇫',
        'bhutan': '🇧🇹', 'maldives': '🇲🇻',

        # Middle East
        'saudi arabia': '🇸🇦', 'uae': '🇦🇪', 'united arab emirates': '🇦🇪',
        'iran': '🇮🇷', 'iraq': '🇮🇶', 'syria': '🇸🇾', 'jordan': '🇯🇴',
        'lebanon': '🇱🇧', 'israel': '🇮🇱', 'palestine': '🇵🇸',
        'yemen': '🇾🇪', 'oman': '🇴🇲', 'qatar': '🇶🇦', 'kuwait': '🇰🇼',
        'bahrain': '🇧🇭', 'turkey': '🇹🇷', 'turkiye': '🇹🇷',

        # East Asia
        'china': '🇨🇳', 'japan': '🇯🇵', 'south korea': '🇰🇷',
        'korea': '🇰🇷', 'north korea': '🇰🇵', 'taiwan': '🇹🇼',
        'hong kong': '🇭🇰', 'mongolia': '🇲🇳',

        # Southeast Asia
        'indonesia': '🇮🇩', 'malaysia': '🇲🇾', 'philippines': '🇵🇭',
        'thailand': '🇹🇭', 'vietnam': '🇻🇳', 'myanmar': '🇲🇲',
        'cambodia': '🇰🇭', 'laos': '🇱🇦', 'singapore': '🇸🇬',
        'brunei': '🇧🇳', 'timor-leste': '🇹🇱',

        # Central Asia
        'kazakhstan': '🇰🇿', 'uzbekistan': '🇺🇿', 'tajikistan': '🇹🇯',
        'kyrgyzstan': '🇰🇬', 'turkmenistan': '🇹🇲', 'azerbaijan': '🇦🇿',
        'armenia': '🇦🇲', 'georgia': '🇬🇪',

        # Europe — Western
        'united kingdom': '🇬🇧', 'uk': '🇬🇧', 'england': '🇬🇧',
        'scotland': '🏴󠁧󠁢󠁳󠁣󠁴󠁿', 'wales': '🏴󠁧󠁢󠁷󠁬󠁳󠁿',
        'germany': '🇩🇪', 'france': '🇫🇷', 'italy': '🇮🇹',
        'spain': '🇪🇸', 'portugal': '🇵🇹', 'netherlands': '🇳🇱',
        'belgium': '🇧🇪', 'switzerland': '🇨🇭', 'austria': '🇦🇹',
        'sweden': '🇸🇪', 'norway': '🇳🇴', 'denmark': '🇩🇰',
        'finland': '🇫🇮', 'ireland': '🇮🇪', 'luxembourg': '🇱🇺',
        'iceland': '🇮🇸', 'monaco': '🇲🇨', 'liechtenstein': '🇱🇮',

        # Europe — Eastern
        'russia': '🇷🇺', 'ukraine': '🇺🇦', 'poland': '🇵🇱',
        'czech republic': '🇨🇿', 'czechia': '🇨🇿', 'slovakia': '🇸🇰',
        'hungary': '🇭🇺', 'romania': '🇷🇴', 'bulgaria': '🇧🇬',
        'serbia': '🇷🇸', 'croatia': '🇭🇷', 'slovenia': '🇸🇮',
        'bosnia': '🇧🇦', 'albania': '🇦🇱', 'north macedonia': '🇲🇰',
        'kosovo': '🇽🇰', 'montenegro': '🇲🇪', 'moldova': '🇲🇩',
        'belarus': '🇧🇾', 'latvia': '🇱🇻', 'lithuania': '🇱🇹',
        'estonia': '🇪🇪',

        # North America
        'united states': '🇺🇸', 'usa': '🇺🇸', 'us': '🇺🇸',
        'america': '🇺🇸', 'canada': '🇨🇦', 'mexico': '🇲🇽',

        # Central America & Caribbean
        'guatemala': '🇬🇹', 'belize': '🇧🇿', 'honduras': '🇭🇳',
        'el salvador': '🇸🇻', 'nicaragua': '🇳🇮', 'costa rica': '🇨🇷',
        'panama': '🇵🇦', 'cuba': '🇨🇺', 'jamaica': '🇯🇲',
        'haiti': '🇭🇹', 'dominican republic': '🇩🇴', 'puerto rico': '🇵🇷',
        'trinidad': '🇹🇹', 'barbados': '🇧🇧', 'bahamas': '🇧🇸',

        # South America
        'brazil': '🇧🇷', 'argentina': '🇦🇷', 'colombia': '🇨🇴',
        'chile': '🇨🇱', 'peru': '🇵🇪', 'venezuela': '🇻🇪',
        'ecuador': '🇪🇨', 'bolivia': '🇧🇴', 'paraguay': '🇵🇾',
        'uruguay': '🇺🇾', 'guyana': '🇬🇾', 'suriname': '🇸🇷',

        # Africa — North
        'egypt': '🇪🇬', 'libya': '🇱🇾', 'tunisia': '🇹🇳',
        'algeria': '🇩🇿', 'morocco': '🇲🇦', 'sudan': '🇸🇩',

        # Africa — West
        'nigeria': '🇳🇬', 'ghana': '🇬🇭', 'senegal': '🇸🇳',
        'mali': '🇲🇱', 'niger': '🇳🇪', 'guinea': '🇬🇳',
        'ivory coast': '🇨🇮', 'burkina faso': '🇧🇫', 'togo': '🇹🇬',
        'benin': '🇧🇯', 'sierra leone': '🇸🇱', 'liberia': '🇱🇷',
        'gambia': '🇬🇲', 'cameroon': '🇨🇲',

        # Africa — East
        'ethiopia': '🇪🇹', 'kenya': '🇰🇪', 'tanzania': '🇹🇿',
        'uganda': '🇺🇬', 'rwanda': '🇷🇼', 'somalia': '🇸🇴',
        'djibouti': '🇩🇯', 'eritrea': '🇪🇷', 'south sudan': '🇸🇸',

        # Africa — South
        'south africa': '🇿🇦', 'zimbabwe': '🇿🇼', 'zambia': '🇿🇲',
        'mozambique': '🇲🇿', 'malawi': '🇲🇼', 'madagascar': '🇲🇬',
        'angola': '🇦🇴', 'namibia': '🇳🇦', 'botswana': '🇧🇼',
        'lesotho': '🇱🇸', 'eswatini': '🇸🇿',

        # Oceania
        'australia': '🇦🇺', 'new zealand': '🇳🇿', 'fiji': '🇫🇯',
        'papua new guinea': '🇵🇬', 'solomon islands': '🇸🇧',
        'vanuatu': '🇻🇺', 'samoa': '🇼🇸', 'tonga': '🇹🇴',
    }
    for key, flag in flags.items():
        if key in c:
            return flag
    return '???'


#  DATA FETCHERS

def fetch_profile_data(db_file, profile_url):
    """Fetch all data needed for star + co-comment graph for a profile."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Get profile info
    cur.execute("""
        SELECT id, owner_name, profile_url FROM profiles
        WHERE profile_url = ?
    """, (profile_url,))
    row = cur.fetchone()
    if not row:
        print(f"    Profile not found: {profile_url}")
        con.close()
        return None

    profile_id   = row[0]
    owner_name   = row[1] or "Target"
    profile_url  = row[2]

    # Get commentor scores
    cur.execute("""
        SELECT
            co.id, co.name, co.profile_url,
            cs.total_score, cs.comment_count, cs.tier,
            cs.sentiment_score, cs.emotion_score, cs.stance_score,
            cc.identified_country, cc.country_confidence
        FROM commentor_scores cs
        JOIN commentors co ON co.id = cs.commentor_id
        LEFT JOIN commentor_country cc ON cc.commentor_id = co.id
        WHERE cs.main_profile_id = ?
        ORDER BY cs.total_score DESC
    """, (profile_id,))

    commentors = []
    for r in cur.fetchall():
        commentors.append({
            'id':           r[0],
            'name':         r[1] or 'Unknown',
            'profile_url':  r[2] or '',
            'score':        r[3] or 0.0,
            'comment_count':r[4] or 0,
            'tier':         r[5] or 'neutral',
            'sentiment':    r[6] or 0.0,
            'emotion':      r[7] or 0.0,
            'stance':       r[8] or 0.0,
            'country':      r[9],
            'country_conf': r[10] or 0,
        })

    # Get co-commenting data — which commentors commented on same posts
    co_comments = {}
    for table, id_col in [
        ('photo_comments', 'photo_post_id'),
        ('reel_comments',  'reel_post_id'),
        ('text_comments',  'text_post_id'),
    ]:
        post_table = table.replace('_comments', '_posts')
        try:
            cur.execute(f"SELECT id FROM {post_table} WHERE profile_id = ?", (profile_id,))
            post_ids = [r[0] for r in cur.fetchall()]
            if not post_ids:
                continue

            cur.execute(f"""
                SELECT {id_col}, GROUP_CONCAT(commentor_id)
                FROM {table}
                WHERE {id_col} IN ({','.join('?'*len(post_ids))})
                GROUP BY {id_col}
            """, post_ids)

            for row in cur.fetchall():
                post_id = row[0]
                cids    = [int(x) for x in row[1].split(',') if x]
                if len(cids) > 1:
                    key = f"{table}_{post_id}"
                    co_comments[key] = cids
        except Exception as e:
            pass

    con.close()
    return {
        'profile_id':  profile_id,
        'owner_name':  owner_name,
        'profile_url': profile_url,
        'commentors':  commentors,
        'co_comments': co_comments
    }


def fetch_batch_data(db_file, batch_id):
    """Fetch all data needed for graphs for a manual batch."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Get batch info
    cur.execute("SELECT label FROM batches WHERE batch_id = ?", (batch_id,))
    row = cur.fetchone()
    if not row:
        print(f"    Batch not found: {batch_id}")
        con.close()
        return None

    label = row[0]

    # Get commentor scores
    cur.execute("""
        SELECT
            co.id, co.name, co.profile_url,
            bcs.total_score, bcs.comment_count, bcs.tier,
            bcs.sentiment_score, bcs.emotion_score, bcs.stance_score,
            cc.identified_country, cc.country_confidence
        FROM batch_commentor_scores bcs
        JOIN commentors co ON co.id = bcs.commentor_id
        LEFT JOIN commentor_country cc ON cc.commentor_id = co.id
        WHERE bcs.batch_id = ?
        ORDER BY bcs.total_score DESC
    """, (batch_id,))

    commentors = []
    for r in cur.fetchall():
        commentors.append({
            'id':           r[0],
            'name':         r[1] or 'Unknown',
            'profile_url':  r[2] or '',
            'score':        r[3] or 0.0,
            'comment_count':r[4] or 0,
            'tier':         r[5] or 'neutral',
            'sentiment':    r[6] or 0.0,
            'emotion':      r[7] or 0.0,
            'stance':       r[8] or 0.0,
            'country':      r[9],
            'country_conf': r[10] or 0,
        })

    # Co-commenting data
    cur.execute("""
        SELECT c.post_id, GROUP_CONCAT(c.commentor_id)
        FROM comments c
        JOIN manual_posts mp ON mp.id = c.post_id
        WHERE mp.batch_id = ?
        GROUP BY c.post_id
    """, (batch_id,))

    co_comments = {}
    for row in cur.fetchall():
        post_id = row[0]
        cids    = [int(x) for x in row[1].split(',') if x]
        if len(cids) > 1:
            co_comments[f"post_{post_id}"] = cids

    con.close()
    return {
        'profile_id':  batch_id,
        'owner_name':  label,
        'profile_url': batch_id,
        'commentors':  commentors,
        'co_comments': co_comments
    }


#  GRAPH BUILDERS

def build_star_graph(data, output_path):
    """
    Star network — target at center, all commentors around it.
    Edge weight = abs(score)
    """
    try:
        from pyvis.network import Network
        import networkx as nx
    except ImportError:
        print("    Run: pip install networkx pyvis")
        return

    G   = nx.Graph()
    net = Network(
        height="750px", width="100%",
        bgcolor="#1a1a2e", font_color="#ffffff",
        heading=f"SOCMINT — Star Network: {data['owner_name']}"
    )
    net.set_options("""
    {
        "physics": {
            "barnesHut": {
                "gravitationalConstant": -8000,
                "springLength": 200,
                "springConstant": 0.04
            }
        },
        "interaction": {
            "hover": true,
            "tooltipDelay": 100
        }
    }
    """)

    # Center node — threat actor custom icon
    net.add_node(
        "TARGET",
        label=data['owner_name'],
        color={"background": "#c0392b", "border": "#ff0000",
               "highlight": {"background": "#e74c3c", "border": "#ff6b6b"}},
        size=55,
        shape="image",
        image=THREAT_ICON,
        brokenImage="https://cdn-icons-png.flaticon.com/512/564/564619.png",
        title=f"<b> THREAT ACTOR</b><br>{data['owner_name']}<br>{data['profile_url']}",
        font={"size": 16, "color": "#ff4444"},
        borderWidth=4,
        shapeProperties={"useBorderWithImage": True, "interpolation": False}
    )

    commentor_map = {}

    for c in data['commentors']:
        node_id  = f"C_{c['id']}"
        color    = get_node_color(c['score'])
        size     = get_node_size(c['comment_count'])
        label    = get_node_label(c['score'])
        flag     = get_country_flag(c['country'])
        country  = c['country'] or 'Unknown'
        conf     = c['country_conf']

        # Short display name
        name_short = c['name'][:20] + ('…' if len(c['name']) > 20 else '')

        tooltip = (
            f"<b>{c['name']}</b><br>"
            f"Score: {c['score']:+.3f}<br>"
            f"Comments: {c['comment_count']}<br>"
            f"Category: {label}<br>"
            f"Country: {flag} {country} ({conf}%)<br>"
            f"Sentiment: {c['sentiment']:+.2f}<br>"
            f"Stance: {c['stance']:+.2f}<br>"
            f"<a href='{c['profile_url']}' target='_blank'>View Profile</a>"
        )

        net.add_node(
            node_id,
            label=f"{name_short}\n{flag}",
            color={"background": color, "border": color,
                   "highlight": {"background": color, "border": "#ffffff"}},
            size=size,
            shape="image",
            image=PERSON_ICON,
            title=tooltip,
            font={"size": 11, "color": "#ffffff"},
            borderWidth=3,
            shapeProperties={"useBorderWithImage": True, "interpolation": False}
        )

        # Edge to target
        edge_weight = max(abs(c['score']) * 5, 1)
        net.add_edge(
            "TARGET", node_id,
            width=edge_weight,
            color={"color": color, "opacity": 0.6},
            title=f"Score: {c['score']:+.3f} | Comments: {c['comment_count']}"
        )

        commentor_map[c['id']] = node_id

    # Legend HTML
    legend = f"""
    <div style='position:fixed;bottom:20px;left:20px;background:#1a1a2e;
                border:1px solid #444;padding:15px;border-radius:8px;
                color:#fff;font-family:Arial;font-size:12px;z-index:999'>
        <b>SOCMINT Network — {data['owner_name']}</b><br><br>
        <span style='color:#2ecc71'>●</span> Strong Supporter (score > +0.5)<br>
        <span style='color:#3498db'>●</span> Supporter (+0.1 to +0.5)<br>
        <span style='color:#95a5a6'>●</span> Neutral (-0.1 to +0.1)<br>
        <span style='color:#f39c12'>●</span> Low Interaction (-0.5 to -0.1)<br>
        <span style='color:#e74c3c'>●</span> Critical Voice (< -0.5)<br>
        <span style='color:#e91e8c'>★</span> Target Profile<br><br>
        Node size = comment frequency<br>
        Total commentors: {len(data['commentors'])}
    </div>
    """

    os.makedirs(REPORTS_DIR, exist_ok=True)
    net.save_graph(output_path)

    # Inject legend
    html = open(output_path).read()
    html = html.replace('</body>', legend + '</body>')
    open(output_path, 'w').write(html)

    print(f"  Star graph saved: {output_path}")


def build_cocomment_graph(data, output_path):
    """
    Co-commenting network — commentors connected if they commented on same post.
    Reveals coordinated behavior / clusters.
    """
    try:
        from pyvis.network import Network
        import networkx as nx
        from collections import defaultdict
    except ImportError:
        print("   Run: pip install networkx pyvis")
        return

    # Build co-comment pairs
    commentor_lookup = {c['id']: c for c in data['commentors']}
    edge_weights     = defaultdict(int)

    for post_id, cids in data['co_comments'].items():
        for i in range(len(cids)):
            for j in range(i+1, len(cids)):
                pair = tuple(sorted([cids[i], cids[j]]))
                edge_weights[pair] += 1

    if not edge_weights:
        print(f"    No co-commenting data found — skipping co-comment graph")
        return

    net = Network(
        height="750px", width="100%",
        bgcolor="#0d1117", font_color="#ffffff",
        heading=f"SOCMINT — Co-Comment Network: {data['owner_name']}"
    )
    net.set_options("""
    {
        "physics": {
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.01,
                "springLength": 150
            },
            "solver": "forceAtlas2Based"
        },
        "interaction": {
            "hover": true,
            "tooltipDelay": 100
        }
    }
    """)

    added_nodes = set()

    for (cid1, cid2), weight in edge_weights.items():
        for cid in [cid1, cid2]:
            if cid in added_nodes:
                continue
            if cid not in commentor_lookup:
                continue

            c       = commentor_lookup[cid]
            node_id = f"C_{cid}"
            color   = get_node_color(c['score'])
            size    = get_node_size(c['comment_count'])
            flag    = get_country_flag(c['country'])
            country = c['country'] or 'Unknown'
            label   = get_node_label(c['score'])
            name_short = c['name'][:20] + ('…' if len(c['name']) > 20 else '')

            tooltip = (
                f"<b>{c['name']}</b><br>"
                f"Score: {c['score']:+.3f}<br>"
                f"Comments: {c['comment_count']}<br>"
                f"Category: {label}<br>"
                f"Country: {flag} {country}<br>"
            )

            net.add_node(
                node_id,
                label=f"{name_short}\n{flag}",
                color={"background": color, "border": color,
                       "highlight": {"background": color, "border": "#ffffff"}},
                size=size,
                shape="image",
                image=PERSON_ICON,
                title=tooltip,
                font={"size": 11, "color": "#ffffff"},
                borderWidth=3,
                shapeProperties={"useBorderWithImage": True, "interpolation": False}
            )
            added_nodes.add(cid)

        # Add edge
        if cid1 in commentor_lookup and cid2 in commentor_lookup:
            net.add_edge(
                f"C_{cid1}", f"C_{cid2}",
                width=weight * 2,
                color={"color": "#ffffff", "opacity": 0.3},
                title=f"Co-commented on {weight} post(s)"
            )

    legend = f"""
    <div style='position:fixed;bottom:20px;left:20px;background:#0d1117;
                border:1px solid #444;padding:15px;border-radius:8px;
                color:#fff;font-family:Arial;font-size:12px;z-index:999'>
        <b>Co-Comment Network — {data['owner_name']}</b><br>
        <i>Connected = commented on same post</i><br><br>
        <span style='color:#2ecc71'>●</span> Strong Supporter<br>
        <span style='color:#3498db'>●</span> Supporter<br>
        <span style='color:#95a5a6'>●</span> Neutral<br>
        <span style='color:#f39c12'>●</span> Low Interaction<br>
        <span style='color:#e74c3c'>●</span> Critical Voice<br><br>
        Edge thickness = posts in common<br>
        Clusters = coordinated behavior
    </div>
    """

    os.makedirs(REPORTS_DIR, exist_ok=True)
    net.save_graph(output_path)

    html = open(output_path).read()
    html = html.replace('</body>', legend + '</body>')
    open(output_path, 'w').write(html)

    print(f"  Co-comment graph saved: {output_path}")


def build_focused_graph(data, commentors_subset, title, subtitle, output_path, center_color="#e91e8c"):
    """
    Focused star graph for a subset of commentors (top 7 or bottom 7).
    Bigger nodes, more detail, cleaner view.
    """
    try:
        from pyvis.network import Network
    except ImportError:
        print("  Run: pip install networkx pyvis")
        return

    if not commentors_subset:
        print(f"  No commentors for {title} — skipping")
        return

    net = Network(
        height="750px", width="100%",
        bgcolor="#1a1a2e", font_color="#ffffff",
        heading=f"SOCMINT — {title}: {data['owner_name']}"
    )
    net.set_options("""
    {
        "physics": {
            "barnesHut": {
                "gravitationalConstant": -5000,
                "springLength": 250,
                "springConstant": 0.03
            }
        },
        "interaction": {
            "hover": true,
            "tooltipDelay": 100
        }
    }
    """)

    # Center node — threat actor custom icon
    net.add_node(
        "TARGET",
        label=data['owner_name'],
        color={"background": "#c0392b", "border": "#ff0000",
               "highlight": {"background": "#e74c3c", "border": "#ff6b6b"}},
        size=60,
        shape="image",
        image=THREAT_ICON,
        brokenImage="https://cdn-icons-png.flaticon.com/512/564/564619.png",
        title=f"<b>THREAT ACTOR</b><br>{data['owner_name']}<br>{data['profile_url']}",
        font={"size": 18, "color": "#ff4444"},
        borderWidth=4,
        shapeProperties={"useBorderWithImage": True, "interpolation": False}
    )

    for rank, c in enumerate(commentors_subset, 1):
        node_id    = f"C_{c['id']}"
        color      = get_node_color(c['score'])
        size       = get_node_size(c['comment_count']) + 10  # bigger for focused view
        label      = get_node_label(c['score'])
        flag       = get_country_flag(c['country'])
        country    = c['country'] or 'Unknown'
        conf       = c['country_conf']
        name_short = c['name'][:22] + ('…' if len(c['name']) > 22 else '')

        tooltip = (
            f"<b>#{rank} {c['name']}</b><br>"
            f"Score: {c['score']:+.3f}<br>"
            f"Comments: {c['comment_count']}<br>"
            f"Category: {label}<br>"
            f"Country: {flag} {country} ({conf}%)<br>"
            f"Sentiment: {c['sentiment']:+.2f} | "
            f"Stance: {c['stance']:+.2f}<br>"
            f"<a href='{c['profile_url']}' target='_blank'>View Profile</a>"
        )

        net.add_node(
            node_id,
            label=f"#{rank} {name_short}\n{flag} {country}",
            color={"background": color, "border": color,
                   "highlight": {"background": color, "border": "#ffffff"}},
            size=size,
            shape="image",
            image=PERSON_ICON,
            title=tooltip,
            font={"size": 12, "color": "#ffffff"},
            borderWidth=3,
            shapeProperties={"useBorderWithImage": True, "interpolation": False}
        )

        edge_weight = max(abs(c['score']) * 6, 1.5)
        net.add_edge(
            "TARGET", node_id,
            width=edge_weight,
            color={"color": color, "opacity": 0.7},
            title=f"Score: {c['score']:+.3f} | Comments: {c['comment_count']}"
        )

    legend = f"""
    <div style='position:fixed;bottom:20px;left:20px;background:#1a1a2e;
                border:1px solid #444;padding:15px;border-radius:8px;
                color:#fff;font-family:Arial;font-size:12px;z-index:999'>
        <b>{title}</b><br>
        <i>{subtitle}</i><br><br>
        <span style='color:#2ecc71'>●</span> Strong Supporter (score > +0.5)<br>
        <span style='color:#3498db'>●</span> Supporter (+0.1 to +0.5)<br>
        <span style='color:#95a5a6'>●</span> Neutral (-0.1 to +0.1)<br>
        <span style='color:#f39c12'>●</span> Low Interaction (-0.5 to -0.1)<br>
        <span style='color:#e74c3c'>●</span> Critical Voice (< -0.5)<br><br>
        Node size = comment frequency<br>
        # = rank by score
    </div>
    """

    os.makedirs(REPORTS_DIR, exist_ok=True)
    net.save_graph(output_path)

    html = open(output_path).read()
    html = html.replace('</body>', legend + '</body>')
    open(output_path, 'w').write(html)

    print(f"  {title} graph saved: {output_path}")


#  MAIN RUNNERS

def run_for_profile(profile_url, db_file=DB_FILE):
    print(f"\n{'═'*65}")
    print(f"Network Graph — Profile")
    print(f"Profile : {profile_url}")
    print("═"*65)

    data = fetch_profile_data(db_file, profile_url)
    if not data:
        return

    if not data['commentors']:
        print("  No scored commentors found — run scoring first")
        return

    print(f"  {len(data['commentors'])} commentors loaded")

    name = data['owner_name'].replace(' ', '_').replace('/', '_')[:30]

    # Sort by score
    sorted_commentors = sorted(data['commentors'], key=lambda x: -x['score'])
    top7    = sorted_commentors[:7]
    bottom7 = sorted_commentors[-7:]

    star_path      = os.path.join(REPORTS_DIR, f"star_{name}.html")
    cocomment_path = os.path.join(REPORTS_DIR, f"cocomment_{name}.html")
    top7_path      = os.path.join(REPORTS_DIR, f"top7_{name}.html")
    bottom7_path   = os.path.join(REPORTS_DIR, f"bottom7_{name}.html")

    print(f"\n  Building star network (all commentors)...")
    build_star_graph(data, star_path)

    print(f"\n  🔗 Building co-comment network...")
    build_cocomment_graph(data, cocomment_path)

    print(f"\n  🟢 Building top 7 close relationship graph...")
    build_focused_graph(
        data, top7,
        title="Top 7 Close Relationship",
        subtitle="Highest engagement & positive interaction",
        output_path=top7_path,
        center_color="#e91e8c"
    )

    print(f"\n  🔴 Building bottom 7 less interaction/critical voice graph...")
    build_focused_graph(
        data, bottom7,
        title="Bottom 7 Less Interaction",
        subtitle="Lowest engagement or critical interaction",
        output_path=bottom7_path,
        center_color="#7f8c8d"
    )

    print(f"\n{'═'*65}")
    print(f"4 graphs saved to: {REPORTS_DIR}/")
    print(f"   → {star_path}")
    print(f"   → {cocomment_path}")
    print(f"   → {top7_path}")
    print(f"   → {bottom7_path}")
    print(f"   Open in browser:")
    print(f"   → {star_path}")
    print(f"   → {cocomment_path}")


def run_for_batch(batch_id, db_file=MANUAL_DB_FILE):
    print(f"\n{'═'*65}")
    print(f"Network Graph — Batch")
    print(f"Batch ID : {batch_id}")
    print("═"*65)

    data = fetch_batch_data(db_file, batch_id)
    if not data:
        return

    if not data['commentors']:
        print("    No scored commentors found — run scoring first")
        return

    print(f"   {len(data['commentors'])} commentors loaded")

    name = data['owner_name'].replace(' ', '_').replace('/', '_')[:30]

    # Sort by score
    sorted_commentors = sorted(data['commentors'], key=lambda x: -x['score'])
    top7    = sorted_commentors[:7]
    bottom7 = sorted_commentors[-7:]

    star_path      = os.path.join(REPORTS_DIR, f"star_{name}.html")
    cocomment_path = os.path.join(REPORTS_DIR, f"cocomment_{name}.html")
    top7_path      = os.path.join(REPORTS_DIR, f"top7_{name}.html")
    bottom7_path   = os.path.join(REPORTS_DIR, f"bottom7_{name}.html")

    print(f"\n  Building star network (all commentors)...")
    build_star_graph(data, star_path)

    print(f"\n  Building co-comment network...")
    build_cocomment_graph(data, cocomment_path)

    print(f"\n  🟢 Building top 7 close relationship graph...")
    build_focused_graph(
        data, top7,
        title="Top 7 Close Relationship",
        subtitle="Highest engagement & positive interaction",
        output_path=top7_path,
        center_color="#e91e8c"
    )

    print(f"\n  🔴 Building bottom 7 less interaction graph...")
    build_focused_graph(
        data, bottom7,
        title="Bottom 7 Less Interaction",
        subtitle="Lowest engagement or critical interaction",
        output_path=bottom7_path,
        center_color="#7f8c8d"
    )

    print(f"\n{'═'*65}")
    print(f"4 graphs saved to: {REPORTS_DIR}/")
    print(f"   → {star_path}")
    print(f"   → {cocomment_path}")
    print(f"   → {top7_path}")
    print(f"   → {bottom7_path}")


if __name__ == "__main__":
    print("SOCMINT — Network Graph Generator")
    print("1 → Automated profile")
    print("2 → Manual batch")
    choice = input("Choice: ").strip()

    if choice == "1":
        profile = input("Enter profile URL: ").strip()
        run_for_profile(profile)
    elif choice == "2":
        batch = input("Enter batch ID: ").strip()
        run_for_batch(batch)
    else:
        print("Invalid choice")