#!/usr/bin/env python3
"""
PS Daily Briefing — Google Chat Card v2
Autonomous version for GitHub Actions.

Env vars required:
  ROCKETLANE_API_KEY   — Rocketlane API key
  GCHAT_WEBHOOK_URL    — Google Chat Space webhook URL
"""

import json
import os
import sys
import re
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config from environment ──────────────────────────────────────────────────
API_KEY = os.environ.get("ROCKETLANE_API_KEY", "")
WEBHOOK_URL = os.environ.get("GCHAT_WEBHOOK_URL", "")
BASE_URL = "https://services.api.exterro.com/api/1.0"
RL_APP_BASE = "https://services.exterro.com/projects"

if not API_KEY:
    print("ERROR: ROCKETLANE_API_KEY not set"); sys.exit(1)
if not WEBHOOK_URL:
    print("ERROR: GCHAT_WEBHOOK_URL not set"); sys.exit(1)

DIRECTORS = {393610: "eDiscovery", 393604: "Data PSG", 393607: "Post Implementation"}
DIRECTOR_NAMES = {"eDiscovery": "Vanessa Graham", "Data PSG": "Maggie Ledbetter", "Post Implementation": "Oronde Ward"}
ACTIVE_STATUS_VALUES = {2, 4, 5, 6, 9, 12, 14, 15}
NOW = datetime.now()
ZOMBIE_THRESHOLD = 30

CONCERN_KEYWORDS = [
    "risk", "blocker", "blocked", "delayed", "escalat", "issue", "concern",
    "slipp", "behind", "overdue", "miss", "fail", "stop", "halt", "pause",
    "no response", "unresponsive", "at risk", "critical", "urgent",
]


# ── HTTP helpers (stdlib only, no curl dependency) ───────────────────────────
def api_get(path):
    url = f"{BASE_URL}/{path}"
    req = urllib.request.Request(url, headers={
        "api-key": API_KEY,
        "accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def post_webhook(payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(WEBHOOK_URL, data=data, headers={
        "Content-Type": "application/json; charset=UTF-8",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        status = resp.status
        body = resp.read().decode()
    return status, body


# ── Data fetchers ────────────────────────────────────────────────────────────
def fetch_all_projects():
    all_projects, page_token = [], None
    while True:
        url = "projects" + (f"?pageToken={page_token}" if page_token else "")
        resp = api_get(url)
        all_projects.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            page_token = pag["nextPageToken"]
        else:
            break
    return all_projects


def fetch_time_entries_for_project(pid):
    entries, token = [], None
    while True:
        url = f"time-entries?projectId.eq={pid}"
        if token:
            url += f"&pageToken={token}"
        resp = api_get(url)
        entries.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            token = pag["nextPageToken"]
        else:
            break
    return entries


def fetch_tasks_for_project(pid):
    tasks, token = [], None
    while True:
        url = f"tasks?projectId.eq={pid}"
        if token:
            url += f"&pageToken={token}"
        resp = api_get(url)
        tasks.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            token = pag["nextPageToken"]
        else:
            break
    return tasks


# ── Field helpers ────────────────────────────────────────────────────────────
def get_field(project, label):
    for f in project.get("fields", []):
        if f.get("fieldLabel") == label:
            return f.get("fieldValueLabel", f.get("fieldValue", ""))
    return None


def strip_html(text):
    return re.sub(r'<[^>]+>', '', str(text)).strip() if text else ""


def extract_date_from_text(text):
    if not text:
        return None
    patterns = [
        r'(\d{1,2}/\d{1,2}/\d{2,4})',
        r'(\d{4}-\d{2}-\d{2})',
        r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4})',
    ]
    dates = []
    for pat in patterns:
        for m in re.findall(pat, text, re.IGNORECASE):
            for fmt in ('%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%B %d, %Y', '%B %d %Y', '%b %d, %Y', '%b %d %Y'):
                try:
                    d = datetime.strptime(m.replace(',', ''), fmt)
                    if d.year > 2020 and d <= NOW:
                        dates.append(d)
                    break
                except ValueError:
                    continue
    return max(dates) if dates else None


def has_concern_keywords(text):
    if not text:
        return False
    for kw in CONCERN_KEYWORDS:
        if re.search(rf'\b{re.escape(kw)}', text, re.IGNORECASE):
            return True
    return False


def classify_edisc(sub_type):
    if not sub_type:
        return "non-z2e"
    st = sub_type.lower()
    if "z2e" not in st:
        return "non-z2e"
    if "phase 1" in st:
        return "z2e-phase1"
    if "not started" in st:
        return "z2e-not-started"
    return "z2e-phase2"


# ── Zombie scoring ───────────────────────────────────────────────────────────
def compute_zombie_score(p):
    pid = p["pid"]
    score = 0

    entries = fetch_time_entries_for_project(pid)
    if entries:
        dates = [e.get("date") for e in entries if e.get("date")]
        if dates:
            last = max(dates)
            try:
                stale_days = (NOW - datetime.strptime(last, "%Y-%m-%d")).days
                if stale_days > 120: score += 30
                elif stale_days > 90: score += 22
                elif stale_days > 60: score += 15
                elif stale_days > 30: score += 8
                p["last_time_entry"] = last
                p["time_stale_days"] = stale_days
            except ValueError:
                pass
    else:
        score += 5

    tasks = fetch_tasks_for_project(pid)
    total = len(tasks)
    overdue = 0
    for t in tasks:
        ts = t.get("status", {})
        tl = ts.get("label", "") if isinstance(ts, dict) else ""
        if tl in ("To do", "In progress"):
            due = t.get("dueDate")
            if due:
                try:
                    if datetime.strptime(due, "%Y-%m-%d") < NOW:
                        overdue += 1
                except ValueError:
                    pass
    if total > 0:
        ratio = overdue / total
        if ratio > 0.7: score += 15
        elif ratio > 0.4: score += 8
        elif ratio > 0.2: score += 3

    if p.get("notes_stale_days") and p["notes_stale_days"] > 90: score += 15
    elif p.get("notes_stale_days") and p["notes_stale_days"] > 45: score += 8

    if p.get("go_live_slip_days") and p["go_live_slip_days"] > 180: score += 10
    elif p.get("go_live_slip_days") and p["go_live_slip_days"] > 90: score += 5

    if p["sv"] == 4: score += 10
    elif p["sv"] == 9: score += 5
    if p["health"] == "red": score += 5

    p["zombie_score"] = score
    p["overdue_tasks"] = overdue
    p["total_tasks"] = total
    return p


# ── Project enrichment ───────────────────────────────────────────────────────
def enrich_project(p):
    sv = p.get("status", {}).get("value")
    sl = p.get("status", {}).get("label", "Unknown")
    owner = p.get("owner", {})
    owner_name = f'{owner.get("firstName","")} {owner.get("lastName","")}'.strip()
    customer = p.get("customer", {}).get("companyName", "N/A")
    health = (get_field(p, "Red/Yellow/Green Health") or "").strip().lower()
    health_notes = strip_html(get_field(p, "Internal Project Health Notes") or "")
    weekly_status = strip_html(get_field(p, "Internal Weekly Status") or "")
    sub_type = get_field(p, "eDisc: Project Sub-Type") or ""
    pid = p.get("projectId", "")
    name = p.get("projectName", "?")

    health_date = extract_date_from_text(health_notes)
    notes_stale_days = (NOW - health_date).days if health_date else None

    go_live_str = get_field(p, "eDisc: Go Live - Planned") or ""
    go_live_slip_days = None
    if go_live_str:
        try:
            gl = datetime.strptime(go_live_str, "%Y-%m-%d")
            if gl < NOW:
                go_live_slip_days = (NOW - gl).days
        except ValueError:
            pass

    return {
        "sv": sv, "status": sl, "health": health, "sub_type": sub_type,
        "pid": pid, "name": name, "customer": customer, "owner": owner_name,
        "health_notes": health_notes[:150], "weekly_status": weekly_status[:150],
        "notes_stale_days": notes_stale_days,
        "go_live_slip_days": go_live_slip_days,
        "has_concerns": has_concern_keywords(health_notes) or has_concern_keywords(weekly_status),
        "updated_at": p.get("updatedAt", 0),
    }


def plink(name, pid):
    return f'<a href="{RL_APP_BASE}/{pid}">{name}</a>'


# ── Card builder ─────────────────────────────────────────────────────────────
def build_card_v2(projects):
    today_str = NOW.strftime("%A, %B %d, %Y")

    team_raw = defaultdict(list)
    for p in projects:
        member_ids = {m.get("userId") for m in p.get("teamMembers", {}).get("members", [])}
        owner_id = p.get("owner", {}).get("userId")
        for did, team_name in DIRECTORS.items():
            if did in member_ids or did == owner_id:
                team_raw[team_name].append(p)
                break

    team_data = {}
    all_enriched = []
    for team, projs in team_raw.items():
        enriched = [enrich_project(p) for p in projs]
        team_data[team] = enriched
        all_enriched.extend(enriched)

    active = [p for p in all_enriched if p["sv"] in ACTIVE_STATUS_VALUES]
    blocked = [p for p in active if p["sv"] == 4]
    delayed = [p for p in active if p["sv"] == 12]
    red_all = [p for p in active if p["health"] == "red"]
    yellow_all = [p for p in active if p["health"] == "yellow"]
    no_health = [p for p in active if not p["health"]]

    concern_projects = [p for p in active if p["has_concerns"] and p["health"] in ("red", "yellow")]

    # Zombie scoring (parallel, limited candidates for speed)
    zombie_candidates = [p for p in active if p["health"] in ("red", "yellow", "") or p["sv"] in (4, 9)]
    zombie_candidates = sorted(zombie_candidates, key=lambda x: (0 if x["health"] == "red" else 1, -(x.get("notes_stale_days") or 0)))[:40]

    print(f"  Zombie scoring {len(zombie_candidates)} candidates...")
    scored = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(compute_zombie_score, p): p for p in zombie_candidates}
        for f in as_completed(futures):
            try:
                scored.append(f.result())
            except Exception:
                pass

    zombies = sorted([p for p in scored if p.get("zombie_score", 0) >= ZOMBIE_THRESHOLD],
                     key=lambda x: -x["zombie_score"])[:5]

    # ══════════════════════════════════════════════════════════════════════
    # BUILD THE CARD
    # ══════════════════════════════════════════════════════════════════════
    sections = []

    # Section 1: Executive KPIs
    kpi = (
        f"<b>{len(active)}</b> Active  •  "
        f"<font color=\"#dc2626\"><b>{len(blocked)}</b> Blocked</font>  •  "
        f"<font color=\"#dc2626\"><b>{len(delayed)}</b> Delayed</font>  •  "
        f"<font color=\"#dc2626\"><b>{len(red_all)}</b> Red</font>  •  "
        f"<font color=\"#ca8a04\"><b>{len(yellow_all)}</b> Yellow</font>"
    )
    if no_health:
        kpi += f"  •  <font color=\"#f97316\">⚠️ <b>{len(no_health)}</b> No Health</font>"
    sections.append({"widgets": [{"textParagraph": {"text": kpi}}]})

    # Section 2: VP Action Items
    action_lines = []
    if blocked:
        action_lines.append(f"🚫 <b>{len(blocked)} blocked projects</b> need unblocking")
    if no_health:
        action_lines.append(f"⚠️ <b>{len(no_health)} projects</b> have no health status set — director follow-up needed")
    if concern_projects:
        top_concerns = sorted(concern_projects, key=lambda x: x["name"])[:3]
        for p in top_concerns:
            snippet = p["health_notes"][:80] if p["health_notes"] else p["weekly_status"][:80]
            action_lines.append(f"🔴 {plink(p['name'], p['pid'])} — <i>{snippet}</i>")
        if len(concern_projects) > 3:
            action_lines.append(f"<i>...{len(concern_projects) - 3} more projects with escalation keywords</i>")
    if zombies:
        action_lines.append(f"🧟 <b>{len(zombies)} zombie projects</b> with staleness scores ≥{ZOMBIE_THRESHOLD}")

    if action_lines:
        sections.append({
            "header": "⚡ Needs Your Attention",
            "widgets": [{"textParagraph": {"text": "\n".join(action_lines)}}]
        })

    # Sections 3-5: Team sections
    for team_name in ["eDiscovery", "Data PSG", "Post Implementation"]:
        projs = team_data.get(team_name, [])
        t_active = [p for p in projs if p["sv"] in ACTIVE_STATUS_VALUES]
        t_red = [p for p in t_active if p["health"] == "red"]
        t_yellow = [p for p in t_active if p["health"] == "yellow"]
        t_green = [p for p in t_active if p["health"] == "green"]
        t_blocked = [p for p in t_active if p["sv"] == 4]
        t_no_health = [p for p in t_active if not p["health"]]

        summary_parts = [f"{len(t_active)} active"]
        if t_blocked:
            summary_parts.append(f"<font color=\"#dc2626\">{len(t_blocked)} blocked</font>")
        summary_parts.append(f"🔴{len(t_red)} 🟡{len(t_yellow)} 🟢{len(t_green)}")
        if t_no_health:
            summary_parts.append(f"<font color=\"#f97316\">⚠️{len(t_no_health)} unset</font>")
        summary = "  •  ".join(summary_parts)

        if team_name == "eDiscovery":
            z2e = [p for p in t_active if classify_edisc(p["sub_type"]) != "non-z2e"]
            non_z2e = [p for p in t_active if classify_edisc(p["sub_type"]) == "non-z2e"]
            z2e_p1 = sum(1 for p in t_active if classify_edisc(p["sub_type"]) == "z2e-phase1")
            z2e_p2 = sum(1 for p in t_active if classify_edisc(p["sub_type"]) == "z2e-phase2")
            z2e_ns = sum(1 for p in t_active if classify_edisc(p["sub_type"]) == "z2e-not-started")
            summary += f"\n<b>Z2E:</b> {len(z2e)} ({z2e_p1} P1 • {z2e_p2} P2 • {z2e_ns} NS)  |  <b>Non-Z2E:</b> {len(non_z2e)}"

        widgets = [{"decoratedText": {"topLabel": f"{team_name} — {DIRECTOR_NAMES[team_name]}", "text": summary}}]

        if t_red:
            by_pm = defaultdict(list)
            for p in t_red:
                by_pm[p["owner"] or "Unassigned"].append(p)
            red_lines = []
            for pm in sorted(by_pm.keys()):
                pm_projs = sorted(by_pm[pm], key=lambda x: x["name"])
                red_lines.append(f"\n<b>{pm}</b>")
                for p in pm_projs[:4]:
                    line = f"  🔴 {plink(p['name'], p['pid'])} — {p['customer']}"
                    if p["health_notes"]:
                        line += f"\n       <i>{p['health_notes'][:70]}</i>"
                    red_lines.append(line)
                if len(pm_projs) > 4:
                    red_lines.append(f"  <i>...+{len(pm_projs) - 4} more</i>")
            widgets.append({"textParagraph": {"text": "\n".join(red_lines)}})

        if t_yellow:
            yellow_lines = []
            for p in sorted(t_yellow, key=lambda x: x["name"])[:6]:
                yellow_lines.append(f"🟡 {plink(p['name'], p['pid'])} — {p['customer']}")
            if len(t_yellow) > 6:
                yellow_lines.append(f"<i>...+{len(t_yellow) - 6} more yellow</i>")
            widgets.append({"textParagraph": {"text": "\n".join(yellow_lines)}})

        sections.append({
            "header": team_name,
            "collapsible": True,
            "uncollapsibleWidgetsCount": 1,
            "widgets": widgets,
        })

    # Section 6: Zombie Watch
    if zombies:
        zombie_lines = ["<b>Top staleness scores — needs investigation:</b>\n"]
        for p in zombies:
            sc = p["zombie_score"]
            severity = "🔥" if sc >= 60 else "⚠️" if sc >= 45 else "🧟"
            te_info = ""
            if p.get("last_time_entry"):
                te_info = f" | Last time: {p['last_time_entry']}"
            elif p.get("time_stale_days") is None:
                te_info = " | <b>No time logged</b>"
            task_info = ""
            if p.get("total_tasks"):
                task_info = f" | Tasks: {p.get('overdue_tasks', 0)} overdue/{p['total_tasks']}"
            zombie_lines.append(
                f"{severity} <b>{sc}pts</b> — {plink(p['name'], p['pid'])} — {p['customer']}"
                f"\n     {p['status']} | {p['owner']}{te_info}{task_info}"
            )
        sections.append({
            "header": "🧟 Zombie Watch",
            "collapsible": True,
            "uncollapsibleWidgetsCount": 1,
            "widgets": [{"textParagraph": {"text": "\n".join(zombie_lines)}}]
        })

    # Footer
    sections.append({
        "widgets": [{"textParagraph": {"text": f"<i>Auto-generated from Rocketlane API • {NOW.strftime('%H:%M')} • {today_str}</i>"}}]
    })

    return {
        "cardsV2": [{
            "cardId": "ps-daily-briefing-v2",
            "card": {
                "header": {
                    "title": "PS Daily Briefing",
                    "subtitle": today_str,
                    "imageUrl": "https://fonts.gstatic.com/s/i/short-term/release/googlesymbols/monitoring/default/48px.svg",
                    "imageType": "CIRCLE"
                },
                "sections": sections
            }
        }]
    }


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("PS Daily Briefing — Google Chat Card v2")
    print("=" * 60)

    print("Fetching all Rocketlane projects...")
    projects = fetch_all_projects()
    print(f"Fetched {len(projects)} projects.")

    print("Building enriched Chat card v2...")
    card = build_card_v2(projects)
    card_json = json.dumps(card)
    print(f"Card built ({len(card_json)} bytes)")

    print("Posting to Google Chat webhook...")
    try:
        status, body = post_webhook(card)
        print(f"POST response: {status}")
        if status == 200:
            print("SUCCESS — briefing posted to Google Chat Space.")
        else:
            print(f"WARNING — unexpected status {status}: {body[:200]}")
            sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f"ERROR — HTTP {e.code}: {e.read().decode()[:200]}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR — {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
