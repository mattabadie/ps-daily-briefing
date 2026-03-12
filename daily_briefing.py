#!/usr/bin/env python3
"""
PS Daily Briefing — Unified Script
Shared data layer with two output modes: Google Chat card + HTML email report.

Usage:
  python daily_briefing.py --mode chat      # Post card to Google Chat
  python daily_briefing.py --mode email     # Email full HTML report
  python daily_briefing.py --mode both      # Do both

Env vars:
  ROCKETLANE_API_KEY   — Rocketlane API key (always required)
  GCHAT_WEBHOOK_URL    — Google Chat webhook (required for chat/both)
  GMAIL_ADDRESS        — Gmail address (required for email/both)
  GMAIL_APP_PASSWORD   — Gmail app password (required for email/both)
"""

import argparse
import json
import os
import re
import smtplib
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("ROCKETLANE_API_KEY", "")
WEBHOOK_URL = os.environ.get("GCHAT_WEBHOOK_URL", "")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
BASE_URL = "https://services.api.exterro.com/api/1.0"
RL_APP_BASE = "https://services.exterro.com/projects"

DIRECTORS = {393610: "eDiscovery", 393604: "Data PSG", 393607: "Post Implementation"}
DIRECTOR_NAMES = {"eDiscovery": "Vanessa Graham", "Data PSG": "Maggie Ledbetter", "Post Implementation": "Oronde Ward"}
ACTIVE_STATUS_VALUES = {2, 4, 5, 6, 9, 12, 14, 15}
NOW = datetime.now()
CUTOFF_MS = int((NOW - timedelta(hours=24)).timestamp() * 1000)
ZOMBIE_SCORE_THRESHOLD = 30

CONCERN_KEYWORDS = [
    "risk", "blocker", "blocked", "delayed", "escalat", "issue", "concern",
    "slipp", "behind", "overdue", "miss", "fail", "stop", "halt", "pause",
    "no response", "unresponsive", "at risk", "critical", "urgent",
]


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED: HTTP + DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════════════
def api_get(path):
    url = f"{BASE_URL}/{path}"
    req = urllib.request.Request(url, headers={"api-key": API_KEY, "accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


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


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED: FIELD HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
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


def days_since(dt):
    return (NOW - dt).days if dt else None


def has_concern_keywords(text):
    if not text:
        return False
    for kw in CONCERN_KEYWORDS:
        if re.search(rf'\b{re.escape(kw)}', text, re.IGNORECASE):
            return True
    return False


def highlight_concerns(text):
    if not text:
        return ""
    for kw in CONCERN_KEYWORDS:
        if " " in kw:
            pattern = re.compile(rf'\b({re.escape(kw)})\b', re.IGNORECASE)
        else:
            pattern = re.compile(rf'\b({re.escape(kw)}[a-zA-Z]*)\b', re.IGNORECASE)
        text = pattern.sub(r'<strong class="flag">\1</strong>', text)
    return text


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


def _is_zero_subtotal(val):
    if not val:
        return True
    try:
        return float(str(val).replace(",", "").replace("$", "")) == 0
    except (ValueError, TypeError):
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED: TEAM ASSIGNMENT + ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════════
def assign_to_teams(projects):
    team_projects = defaultdict(list)
    for p in projects:
        member_ids = {m.get("userId") for m in p.get("teamMembers", {}).get("members", [])}
        owner_id = p.get("owner", {}).get("userId")
        for did, team_name in DIRECTORS.items():
            if did in member_ids or did == owner_id:
                team_projects[team_name].append(p)
                break
    return team_projects


def enrich_project(p):
    sv = p.get("status", {}).get("value")
    sl = p.get("status", {}).get("label", "Unknown")
    owner = p.get("owner", {})
    owner_name = f'{owner.get("firstName","")} {owner.get("lastName","")}'.strip()
    customer = p.get("customer", {}).get("companyName", "N/A")
    health = get_field(p, "Red/Yellow/Green Health") or ""
    health_notes_raw = get_field(p, "Internal Project Health Notes") or ""
    weekly_status_raw = get_field(p, "Internal Weekly Status") or ""
    health_notes = strip_html(health_notes_raw)
    weekly_status = strip_html(weekly_status_raw)
    health_date = extract_date_from_text(health_notes)
    weekly_date = extract_date_from_text(weekly_status)
    health_stale_days = days_since(health_date)
    weekly_stale_days = days_since(weekly_date)
    project_type = get_field(p, "Project Type") or ""
    sub_type = get_field(p, "eDisc: Project Sub-Type") or ""
    service_subtotal = get_field(p, "Opp: Service Subtotal") or ""
    opp_type = get_field(p, "Opp: Opportunity Type") or ""
    project_id = p.get("projectId", "")
    go_live_str = get_field(p, "eDisc: Go Live - Planned") or ""
    go_live_date = None
    if go_live_str:
        try:
            go_live_date = datetime.strptime(go_live_str, "%Y-%m-%d")
        except ValueError:
            pass

    return {
        "id": project_id,
        "name": p.get("projectName", "?"),
        "status_val": sv,
        "status": sl,
        "owner": owner_name,
        "customer": customer,
        "health": health.strip().lower() if health else "",
        "health_notes": health_notes[:300] if health_notes else "",
        "weekly_status": weekly_status[:300] if weekly_status else "",
        "health_stale_days": health_stale_days,
        "weekly_stale_days": weekly_stale_days,
        "updated_at": p.get("updatedAt", 0),
        "project_type": project_type,
        "sub_type": sub_type,
        "service_subtotal": service_subtotal,
        "opp_type": opp_type,
        "go_live_date": go_live_date,
        "has_concerns": has_concern_keywords(health_notes) or has_concern_keywords(weekly_status),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED: ZOMBIE SCORING
# ═══════════════════════════════════════════════════════════════════════════════
def compute_zombie_score(enriched_p):
    pid = enriched_p["id"]
    sv = enriched_p["status_val"]
    health = enriched_p["health"]

    health_date = extract_date_from_text(enriched_p["health_notes"])
    weekly_date = extract_date_from_text(enriched_p["weekly_status"])
    latest_note_date = max(filter(None, [health_date, weekly_date]), default=None)
    notes_stale_days = (NOW - latest_note_date).days if latest_note_date else None

    go_live_slip_days = None
    if enriched_p.get("go_live_date") and enriched_p["go_live_date"] < NOW:
        go_live_slip_days = (NOW - enriched_p["go_live_date"]).days

    time_entries = fetch_time_entries_for_project(pid)
    last_time_entry = None
    time_entry_stale_days = None
    if time_entries:
        dates = [e.get("date") for e in time_entries if e.get("date")]
        if dates:
            last_time_entry = max(dates)
            try:
                time_entry_stale_days = (NOW - datetime.strptime(last_time_entry, "%Y-%m-%d")).days
            except ValueError:
                pass
    has_time_entries = last_time_entry is not None

    tasks = fetch_tasks_for_project(pid)
    total_tasks = len(tasks)
    completed_tasks = sum(1 for t in tasks if isinstance(t.get("status"), dict) and t["status"].get("label") == "Completed")
    task_pct = (completed_tasks / total_tasks * 100) if total_tasks > 0 else None
    overdue_tasks = 0
    for t in tasks:
        ts = t.get("status", {})
        tl = ts.get("label", "") if isinstance(ts, dict) else ""
        if tl in ("To do", "In progress"):
            due = t.get("dueDate")
            if due:
                try:
                    if datetime.strptime(due, "%Y-%m-%d") < NOW:
                        overdue_tasks += 1
                except ValueError:
                    pass

    score = 0
    if has_time_entries:
        if time_entry_stale_days > 120: score += 30
        elif time_entry_stale_days > 90: score += 22
        elif time_entry_stale_days > 60: score += 15
        elif time_entry_stale_days > 30: score += 8
    else:
        score += 5

    if notes_stale_days is not None:
        if notes_stale_days > 365: score += 25
        elif notes_stale_days > 180: score += 20
        elif notes_stale_days > 90: score += 15
        elif notes_stale_days > 45: score += 8
        elif notes_stale_days > 21: score += 3

    if total_tasks > 0:
        overdue_ratio = overdue_tasks / total_tasks
        if overdue_ratio > 0.7: score += 15
        elif overdue_ratio > 0.4: score += 8
        elif overdue_ratio > 0.2: score += 3

    if go_live_slip_days is not None:
        if go_live_slip_days > 365: score += 15
        elif go_live_slip_days > 180: score += 10
        elif go_live_slip_days > 90: score += 5

    if sv == 4: score += 10
    elif sv == 9: score += 5
    if health == "red": score += 5

    return {
        "score": score,
        "last_time_entry": last_time_entry or "Never",
        "time_entry_stale_days": time_entry_stale_days,
        "total_tasks": total_tasks,
        "completed_tasks": completed_tasks,
        "task_pct": round(task_pct, 0) if task_pct is not None else None,
        "overdue_tasks": overdue_tasks,
        "notes_stale_days": notes_stale_days,
        "go_live_slip_days": go_live_slip_days,
        "enriched": enriched_p,
    }


def run_zombie_scoring(enriched_by_team, max_candidates=None):
    """Score all active projects for zombie status. Returns list of scored results."""
    candidates = []
    for team, projs in enriched_by_team.items():
        for p in projs:
            if p["status_val"] in ACTIVE_STATUS_VALUES:
                candidates.append((team, p))

    print(f"  Zombie scoring {len(candidates)} candidates...")
    results = []

    def _score(team_proj):
        team, p = team_proj
        z = compute_zombie_score(p)
        z["team"] = team
        return z

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(_score, tp): tp for tp in candidates}
        done = 0
        for f in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"    {done}/{len(candidates)}...")
            try:
                results.append(f.result())
            except Exception:
                pass

    flagged = sorted([r for r in results if r["score"] >= ZOMBIE_SCORE_THRESHOLD], key=lambda x: -x["score"])
    return flagged


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED: CORE DATA PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
def load_briefing_data(projects):
    """Fetch, assign, enrich, and score. Returns a dict with everything both
    output modes need."""
    team_raw = assign_to_teams(projects)

    enriched_by_team = {}
    all_enriched = []
    for team, projs in team_raw.items():
        enriched = [enrich_project(p) for p in projs]
        enriched_by_team[team] = enriched
        all_enriched.extend(enriched)

    active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]
    zombies = run_zombie_scoring(enriched_by_team)

    return {
        "enriched_by_team": enriched_by_team,
        "all_enriched": all_enriched,
        "active": active,
        "zombies": zombies,
        "today_str": NOW.strftime("%A, %B %d, %Y"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT 1: GOOGLE CHAT CARD v2
# ═══════════════════════════════════════════════════════════════════════════════
def plink(name, pid):
    return f'<a href="{RL_APP_BASE}/{pid}">{name}</a>'


def build_chat_card(data):
    today_str = data["today_str"]
    enriched_by_team = data["enriched_by_team"]
    active = data["active"]
    zombies = data["zombies"]

    blocked = [p for p in active if p["status_val"] == 4]
    delayed = [p for p in active if p["status_val"] == 12]
    red_all = [p for p in active if p["health"] == "red"]
    yellow_all = [p for p in active if p["health"] == "yellow"]
    no_health = [p for p in active if not p["health"]]
    concern_projects = [p for p in active if p["has_concerns"] and p["health"] in ("red", "yellow")]

    sections = []

    # KPIs
    kpi = (
        f"<b>{len(active)}</b> Active  \u2022  "
        f"<font color=\"#dc2626\"><b>{len(blocked)}</b> Blocked</font>  \u2022  "
        f"<font color=\"#dc2626\"><b>{len(delayed)}</b> Delayed</font>  \u2022  "
        f"<font color=\"#dc2626\"><b>{len(red_all)}</b> Red</font>  \u2022  "
        f"<font color=\"#ca8a04\"><b>{len(yellow_all)}</b> Yellow</font>"
    )
    if no_health:
        kpi += f"  \u2022  <font color=\"#f97316\">\u26a0\ufe0f <b>{len(no_health)}</b> No Health</font>"
    sections.append({"widgets": [{"textParagraph": {"text": kpi}}]})

    # VP Action Items
    action_lines = []
    if blocked:
        action_lines.append(f"\ud83d\udeab <b>{len(blocked)} blocked projects</b> need unblocking")
    if no_health:
        action_lines.append(f"\u26a0\ufe0f <b>{len(no_health)} projects</b> have no health status set \u2014 director follow-up needed")
    if concern_projects:
        top_concerns = sorted(concern_projects, key=lambda x: x["name"])[:3]
        for p in top_concerns:
            snippet = p["health_notes"][:80] if p["health_notes"] else p["weekly_status"][:80]
            action_lines.append(f"\ud83d\udd34 {plink(p['name'], p['id'])} \u2014 <i>{snippet}</i>")
        if len(concern_projects) > 3:
            action_lines.append(f"<i>...{len(concern_projects) - 3} more projects with escalation keywords</i>")
    if zombies:
        action_lines.append(f"\ud83e\udddf <b>{len(zombies)} zombie projects</b> with staleness scores \u2265{ZOMBIE_SCORE_THRESHOLD}")
    if action_lines:
        sections.append({
            "header": "\u26a1 Needs Your Attention",
            "widgets": [{"textParagraph": {"text": "\n".join(action_lines)}}]
        })

    # Team sections
    for team_name in ["eDiscovery", "Data PSG", "Post Implementation"]:
        projs = enriched_by_team.get(team_name, [])
        t_active = [p for p in projs if p["status_val"] in ACTIVE_STATUS_VALUES]
        t_red = [p for p in t_active if p["health"] == "red"]
        t_yellow = [p for p in t_active if p["health"] == "yellow"]
        t_green = [p for p in t_active if p["health"] == "green"]
        t_blocked = [p for p in t_active if p["status_val"] == 4]
        t_no_health = [p for p in t_active if not p["health"]]

        summary_parts = [f"{len(t_active)} active"]
        if t_blocked:
            summary_parts.append(f"<font color=\"#dc2626\">{len(t_blocked)} blocked</font>")
        summary_parts.append(f"\ud83d\udd34{len(t_red)} \ud83d\udfe1{len(t_yellow)} \ud83d\udfe2{len(t_green)}")
        if t_no_health:
            summary_parts.append(f"<font color=\"#f97316\">\u26a0\ufe0f{len(t_no_health)} unset</font>")
        summary = "  \u2022  ".join(summary_parts)

        if team_name == "eDiscovery":
            z2e = [p for p in t_active if classify_edisc(p["sub_type"]) != "non-z2e"]
            non_z2e = [p for p in t_active if classify_edisc(p["sub_type"]) == "non-z2e"]
            z2e_p1 = sum(1 for p in t_active if classify_edisc(p["sub_type"]) == "z2e-phase1")
            z2e_p2 = sum(1 for p in t_active if classify_edisc(p["sub_type"]) == "z2e-phase2")
            z2e_ns = sum(1 for p in t_active if classify_edisc(p["sub_type"]) == "z2e-not-started")
            summary += f"\n<b>Z2E:</b> {len(z2e)} ({z2e_p1} P1 \u2022 {z2e_p2} P2 \u2022 {z2e_ns} NS)  |  <b>Non-Z2E:</b> {len(non_z2e)}"

        widgets = [{"decoratedText": {"topLabel": f"{team_name} \u2014 {DIRECTOR_NAMES[team_name]}", "text": summary}}]

        if t_red:
            by_pm = defaultdict(list)
            for p in t_red:
                by_pm[p["owner"] or "Unassigned"].append(p)
            red_lines = []
            for pm in sorted(by_pm.keys()):
                pm_projs = sorted(by_pm[pm], key=lambda x: x["name"])
                red_lines.append(f"\n<b>{pm}</b>")
                for p in pm_projs[:4]:
                    line = f"  \ud83d\udd34 {plink(p['name'], p['id'])} \u2014 {p['customer']}"
                    if p["health_notes"]:
                        line += f"\n       <i>{p['health_notes'][:70]}</i>"
                    red_lines.append(line)
                if len(pm_projs) > 4:
                    red_lines.append(f"  <i>...+{len(pm_projs) - 4} more</i>")
            widgets.append({"textParagraph": {"text": "\n".join(red_lines)}})

        if t_yellow:
            yellow_lines = []
            for p in sorted(t_yellow, key=lambda x: x["name"])[:6]:
                yellow_lines.append(f"\ud83d\udfe1 {plink(p['name'], p['id'])} \u2014 {p['customer']}")
            if len(t_yellow) > 6:
                yellow_lines.append(f"<i>...+{len(t_yellow) - 6} more yellow</i>")
            widgets.append({"textParagraph": {"text": "\n".join(yellow_lines)}})

        sections.append({
            "header": team_name,
            "collapsible": True,
            "uncollapsibleWidgetsCount": 1,
            "widgets": widgets,
        })

    # Zombie Watch
    if zombies:
        zombie_lines = ["<b>Top staleness scores \u2014 needs investigation:</b>\n"]
        for z in zombies[:5]:
            p = z["enriched"]
            sc = z["score"]
            severity = "\ud83d\udd25" if sc >= 60 else "\u26a0\ufe0f" if sc >= 45 else "\ud83e\udddf"
            te_info = ""
            if z.get("last_time_entry") and z["last_time_entry"] != "Never":
                te_info = f" | Last time: {z['last_time_entry']}"
            elif z.get("last_time_entry") == "Never":
                te_info = " | <b>No time logged</b>"
            task_info = ""
            if z.get("total_tasks"):
                task_info = f" | Tasks: {z.get('overdue_tasks', 0)} overdue/{z['total_tasks']}"
            zombie_lines.append(
                f"{severity} <b>{sc}pts</b> \u2014 {plink(p['name'], p['id'])} \u2014 {p['customer']}"
                f"\n     {p['status']} | {p['owner']}{te_info}{task_info}"
            )
        sections.append({
            "header": "\ud83e\udddf Zombie Watch",
            "collapsible": True,
            "uncollapsibleWidgetsCount": 1,
            "widgets": [{"textParagraph": {"text": "\n".join(zombie_lines)}}]
        })

    # Footer
    sections.append({
        "widgets": [{"textParagraph": {"text": f"<i>Auto-generated from Rocketlane API \u2022 {NOW.strftime('%H:%M')} \u2022 {today_str}</i>"}}]
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


def post_chat_card(card):
    data = json.dumps(card).encode("utf-8")
    req = urllib.request.Request(WEBHOOK_URL, data=data, headers={"Content-Type": "application/json; charset=UTF-8"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode()


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT 2: FULL HTML EMAIL REPORT
# ═══════════════════════════════════════════════════════════════════════════════
EMAIL_CSS = """
<style>
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       max-width: 780px; margin: 0 auto; color: #1a1a1a; font-size: 13px; line-height: 1.5; }
h2 { border-bottom: 2px solid #2563eb; padding-bottom: 8px; font-size: 18px; }
h3 { margin-top: 28px; font-size: 15px; border-left: 4px solid #2563eb; padding-left: 10px; }
h4 { margin: 16px 0 6px 0; font-size: 13px; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 14px 0; font-size: 11px; }
th, td { padding: 4px 8px; border: 1px solid #e2e8f0; text-align: left; vertical-align: top; }
th { background: #f1f5f9; font-weight: 600; }
td.num { text-align: right; }
a { color: #2563eb; text-decoration: none; }
.health-red { background: #fef2f2; border-left: 4px solid #dc2626; padding: 8px 10px; margin: 6px 0; border-radius: 4px; }
.health-yellow { background: #fefce8; border-left: 4px solid #ca8a04; padding: 8px 10px; margin: 6px 0; border-radius: 4px; }
.health-green { background: #f0fdf4; border-left: 4px solid #16a34a; padding: 8px 10px; margin: 6px 0; border-radius: 4px; }
.stale-badge { display: inline-block; background: #fbbf24; color: #78350f; font-size: 10px;
               padding: 1px 6px; border-radius: 10px; font-weight: 600; margin-left: 4px; }
.stale-badge.critical { background: #ef4444; color: #fff; }
.flag { font-weight: 700; color: #dc2626; }
.muted { color: #64748b; font-size: 11px; }
.section-box { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 10px 14px; margin: 8px 0; }
ul { margin: 4px 0; padding-left: 20px; }
li { margin-bottom: 4px; }
.kpi-row { display: flex; gap: 12px; flex-wrap: wrap; margin: 8px 0; }
.kpi { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 8px 14px; min-width: 90px; text-align: center; }
.kpi .num { font-size: 22px; font-weight: 700; }
.kpi .label { font-size: 10px; color: #64748b; text-transform: uppercase; }
.stoplight { display: inline-block; width: 14px; height: 14px; border-radius: 50%; vertical-align: middle; margin-right: 6px; }
.stoplight.red { background: #dc2626; }
.stoplight.yellow { background: #eab308; }
.stoplight.green { background: #16a34a; }
.z2e-tag { display: inline-block; background: #dbeafe; color: #1e40af; font-size: 10px;
           padding: 0 5px; border-radius: 3px; font-weight: 600; margin-left: 4px; }
.z2e-tag.ns { background: #fee2e2; color: #991b1b; }
.z2e-tag.p1 { background: #e0e7ff; color: #3730a3; }
.z2e-tag.p2 { background: #dbeafe; color: #1e40af; }
.seg-banner { padding: 5px 12px; border-radius: 4px; font-weight: 700; font-size: 13px; margin: 10px 0 4px 0; }
.seg-banner.z2e { background: #1e3a5f; color: #93c5fd; border-left: 4px solid #3b82f6; }
.seg-banner.non-z2e { background: #3f3f46; color: #d4d4d8; border-left: 4px solid #a1a1aa; }
.seg-banner .seg-count { font-size: 11px; font-weight: 400; opacity: 0.8; margin-left: 6px; }
.phase-group { margin: 8px 0 4px 8px; font-weight: 800; font-size: 14px; letter-spacing: 0.3px; }
.phase-group.p1 { color: #7e22ce; }
.phase-group.p2 { color: #0f766e; }
.phase-group.ns { color: #b91c1c; }
.pm-group { margin: 4px 0 2px 16px; font-weight: 700; font-size: 12px; color: #334155; text-transform: uppercase;
            letter-spacing: 0.5px; border-bottom: 1px solid #cbd5e1; padding-bottom: 2px; }
.health-red .pm-group { color: #991b1b; border-bottom-color: #fca5a5; }
.health-yellow .pm-group { color: #854d0e; border-bottom-color: #fde047; }
.pm-projects { padding-left: 8px; }
.no-health-warning { background: #fff7ed; border: 2px dashed #f97316; border-radius: 6px; padding: 8px 12px;
                     margin: 8px 0; color: #9a3412; font-weight: 700; font-size: 13px; }
.no-health-warning .count { font-size: 20px; font-weight: 800; color: #c2410c; }
.zombie-section { background: #1e1b2e; color: #e2e8f0; border-radius: 8px; padding: 14px 18px; margin: 14px 0; }
.zombie-section h3 { color: #a78bfa; border-left-color: #a78bfa; margin-top: 0; }
.zombie-section a { color: #93c5fd; }
.zombie-section table { color: #e2e8f0; }
.zombie-section th { background: #312e48; color: #c4b5fd; border-color: #4c4675; }
.zombie-section td { border-color: #4c4675; }
.score-critical { background: #7f1d1d; font-weight: 700; color: #fca5a5; }
.score-high { background: #78350f; font-weight: 700; color: #fed7aa; }
.score-medium { background: #422006; color: #fde68a; }
.zombie-section .muted { color: #94a3b8; }
</style>
"""


def staleness_badge(days):
    if days is None:
        return ""
    if days > 60:
        return f'<span class="stale-badge critical">{days}d stale</span>'
    elif days > 14:
        return f'<span class="stale-badge">{days}d stale</span>'
    return ""


def email_project_link(name, pid):
    return f'<a href="{RL_APP_BASE}/{pid}" target="_blank">{name}</a>'


def z2e_tag_html(classification):
    labels = {"z2e-phase1": ("P1", "p1"), "z2e-phase2": ("P2", "p2"), "z2e-not-started": ("NS", "ns")}
    if classification not in labels:
        return ""
    text, cls = labels[classification]
    return f'<span class="z2e-tag {cls}">{text}</span>'


def render_project_li(p, show_z2e=False):
    link = email_project_link(p["name"], p["id"])
    stale = staleness_badge(p["health_stale_days"])
    notes = highlight_concerns(p["health_notes"][:200])
    note_str = f' \u2014 <em>{notes}</em>' if notes else ""
    z2e = z2e_tag_html(classify_edisc(p["sub_type"])) if show_z2e else ""
    return f'<li><strong>{link}</strong>{z2e} \u2014 {p["customer"]} (PM: {p["owner"]}){stale}{note_str}</li>'


def render_projects_by_pm(projects, show_z2e=False):
    by_pm = defaultdict(list)
    for p in projects:
        by_pm[p["owner"] or "Unassigned"].append(p)
    html = ""
    for pm in sorted(by_pm.keys()):
        pm_projs = sorted(by_pm[pm], key=lambda x: x["name"])
        html += f'<div class="pm-group">{pm} ({len(pm_projs)})</div>'
        html += '<ul class="pm-projects">'
        for p in pm_projs:
            html += render_project_li(p, show_z2e=show_z2e)
        html += '</ul>'
    return html


def render_z2e_by_phase_and_pm(z2e_projects):
    phase_order = [("z2e-phase1", "Phase 1"), ("z2e-phase2", "Phase 2"), ("z2e-not-started", "Not Started")]
    html = ""
    for phase_key, phase_label in phase_order:
        phase_projs = [p for p in z2e_projects if classify_edisc(p["sub_type"]) == phase_key]
        if not phase_projs:
            continue
        tag_cls = {"z2e-phase1": "p1", "z2e-phase2": "p2", "z2e-not-started": "ns"}[phase_key]
        html += f'<div class="phase-group {tag_cls}">{phase_label} ({len(phase_projs)})</div>'
        html += render_projects_by_pm(phase_projs, show_z2e=False)
    return html


def score_class(score):
    if score >= 60: return "score-critical"
    elif score >= 45: return "score-high"
    elif score >= 30: return "score-medium"
    return ""


def health_dot(h):
    return f'<span class="stoplight {h}"></span>' if h in ("red", "yellow", "green") else ""


def build_email_edisc_health(enriched_projects):
    active = [p for p in enriched_projects if p["status_val"] in ACTIVE_STATUS_VALUES]
    red = [p for p in active if p["health"] == "red"]
    yellow = [p for p in active if p["health"] == "yellow"]
    green = [p for p in active if p["health"] == "green"]
    no_health = [p for p in active if not p["health"]]
    html = ""
    for color, projects, css_class, icon_class, label_color in [
        ("red", red, "health-red", "red", "#dc2626"),
        ("yellow", yellow, "health-yellow", "yellow", "#ca8a04"),
    ]:
        if not projects:
            continue
        z2e = [p for p in projects if classify_edisc(p["sub_type"]) != "non-z2e"]
        non_z2e = [p for p in projects if classify_edisc(p["sub_type"]) == "non-z2e"]
        html += f'<div class="{css_class}"><span class="stoplight {icon_class}"></span>'
        html += f'<strong style="color:{label_color};">{color.upper()} ({len(projects)})</strong>'
        if z2e:
            html += f'<div class="seg-banner z2e">Z2E MIGRATIONS<span class="seg-count">({len(z2e)} projects)</span></div>'
            html += render_z2e_by_phase_and_pm(z2e)
        if non_z2e:
            html += f'<div class="seg-banner non-z2e">NON-Z2E<span class="seg-count">({len(non_z2e)} projects)</span></div>'
            html += render_projects_by_pm(non_z2e)
        html += '</div>'
    if green:
        stale_greens = [p for p in green if p["health_stale_days"] and p["health_stale_days"] > 30]
        stale_note = f' ({len(stale_greens)} with stale notes &gt;30d)' if stale_greens else ""
        html += f'<div class="health-green"><span class="stoplight green"></span>'
        html += f'<strong style="color:#16a34a;">GREEN ({len(green)})</strong>{stale_note}</div>'
    if no_health:
        html += f'<div class="no-health-warning">\u26a0\ufe0f <span class="count">{len(no_health)}</span> active projects with NO HEALTH SET</div>'
    return html


def build_email_health_section(enriched_projects):
    active = [p for p in enriched_projects if p["status_val"] in ACTIVE_STATUS_VALUES]
    red = [p for p in active if p["health"] == "red"]
    yellow = [p for p in active if p["health"] == "yellow"]
    green = [p for p in active if p["health"] == "green"]
    no_health = [p for p in active if not p["health"]]
    html = ""
    for color, projects, css_class, icon_class, label_color in [
        ("red", red, "health-red", "red", "#dc2626"),
        ("yellow", yellow, "health-yellow", "yellow", "#ca8a04"),
    ]:
        if not projects:
            continue
        html += f'<div class="{css_class}"><span class="stoplight {icon_class}"></span>'
        html += f'<strong style="color:{label_color};">{color.upper()} ({len(projects)})</strong>'
        html += render_projects_by_pm(projects)
        html += '</div>'
    if green:
        stale_greens = [p for p in green if p["health_stale_days"] and p["health_stale_days"] > 30]
        stale_note = f' ({len(stale_greens)} with stale notes &gt;30d)' if stale_greens else ""
        html += f'<div class="health-green"><span class="stoplight green"></span>'
        html += f'<strong style="color:#16a34a;">GREEN ({len(green)})</strong> \u2014 {len(green)} projects healthy{stale_note}</div>'
    if no_health:
        html += f'<div class="no-health-warning">\u26a0\ufe0f <span class="count">{len(no_health)}</span> active projects with NO HEALTH SET</div>'
    return html


def build_email_staleness(active_projects):
    stale = []
    for p in active_projects:
        worst = None
        if p["health_stale_days"] and p["health_stale_days"] > 14:
            worst = p["health_stale_days"]
        if p["weekly_stale_days"] and p["weekly_stale_days"] > 14:
            if worst is None or p["weekly_stale_days"] > worst:
                worst = p["weekly_stale_days"]
        if worst:
            stale.append((p, worst))
    if not stale:
        return ""
    stale.sort(key=lambda x: -x[1])
    html = f'<h4>Stale Updates ({len(stale)} projects &gt;14 days)</h4><ul style="font-size:12px;">'
    for p, d in stale[:10]:
        badge = staleness_badge(d)
        link = email_project_link(p["name"], p["id"])
        html += f'<li>{link} \u2014 {p["customer"]} (PM: {p["owner"]}){badge}</li>'
    if len(stale) > 10:
        html += f'<li class="muted">...and {len(stale)-10} more</li>'
    html += '</ul>'
    return html


def build_email_recent(enriched_projects):
    recently_updated = [p for p in enriched_projects if p["updated_at"] >= CUTOFF_MS and p["status_val"] in ACTIVE_STATUS_VALUES]
    recently_completed = [p for p in enriched_projects if p["status_val"] == 3 and p["updated_at"] >= CUTOFF_MS]
    html = ""
    if recently_completed:
        html += f'<h4 style="color:#16a34a;">Completed (24h): {len(recently_completed)}</h4><ul style="font-size:12px;">'
        for p in sorted(recently_completed, key=lambda x: x["name"]):
            html += f'<li>{email_project_link(p["name"], p["id"])} \u2014 {p["customer"]}</li>'
        html += '</ul>'
    if recently_updated:
        html += f'<h4>Updated (24h): {len(recently_updated)}</h4><ul style="font-size:12px;">'
        for p in sorted(recently_updated, key=lambda x: x["updated_at"], reverse=True)[:8]:
            html += f'<li>{email_project_link(p["name"], p["id"])} \u2014 {p["status"]} (PM: {p["owner"]})</li>'
        if len(recently_updated) > 8:
            html += f'<li class="muted">...and {len(recently_updated)-8} more</li>'
        html += '</ul>'
    return html


def build_email_html(data):
    today_str = data["today_str"]
    enriched_by_team = data["enriched_by_team"]
    all_enriched = data["all_enriched"]
    zombies = data["zombies"]

    total = len(all_enriched)
    active = sum(1 for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES)
    blocked = sum(1 for p in all_enriched if p["status_val"] == 4)
    delayed = sum(1 for p in all_enriched if p["status_val"] == 12)
    red_health = sum(1 for p in all_enriched if p["health"] == "red" and p["status_val"] in ACTIVE_STATUS_VALUES)
    yellow_health = sum(1 for p in all_enriched if p["health"] == "yellow" and p["status_val"] in ACTIVE_STATUS_VALUES)

    html = f'''<html><head>{EMAIL_CSS}</head>
<body>
<h2>PS Daily Briefing \u2014 {today_str}</h2>
<p class="muted">eDiscovery | Data PSG | Post Implementation</p>
<div class="kpi-row">
<div class="kpi"><div class="num">{total}</div><div class="label">Total Projects</div></div>
<div class="kpi"><div class="num">{active}</div><div class="label">Active</div></div>
<div class="kpi"><div class="num" style="color:#dc2626;">{blocked}</div><div class="label">Blocked</div></div>
<div class="kpi"><div class="num" style="color:#dc2626;">{delayed}</div><div class="label">Delayed</div></div>
<div class="kpi"><div class="num" style="color:#dc2626;">{red_health}</div><div class="label">Red Health</div></div>
<div class="kpi"><div class="num" style="color:#ca8a04;">{yellow_health}</div><div class="label">Yellow Health</div></div>
</div>
'''

    # eDiscovery section
    edisc = enriched_by_team.get("eDiscovery", [])
    edisc_active = [p for p in edisc if p["status_val"] in ACTIVE_STATUS_VALUES]
    edisc_blocked = [p for p in edisc_active if p["status_val"] == 4]
    edisc_delayed = [p for p in edisc_active if p["status_val"] == 12]
    z2e_all = [p for p in edisc_active if classify_edisc(p["sub_type"]) != "non-z2e"]
    z2e_p1 = sum(1 for p in edisc_active if classify_edisc(p["sub_type"]) == "z2e-phase1")
    z2e_p2 = sum(1 for p in edisc_active if classify_edisc(p["sub_type"]) == "z2e-phase2")
    z2e_ns = sum(1 for p in edisc_active if classify_edisc(p["sub_type"]) == "z2e-not-started")
    non_z2e = [p for p in edisc_active if classify_edisc(p["sub_type"]) == "non-z2e"]

    html += f'<h3>eDiscovery \u2014 {DIRECTOR_NAMES["eDiscovery"]}</h3>'
    html += '<div class="kpi-row">'
    html += f'<div class="kpi"><div class="num">{len(edisc_active)}</div><div class="label">Active</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#dc2626;">{len(edisc_blocked)}</div><div class="label">Blocked</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#dc2626;">{len(edisc_delayed)}</div><div class="label">Delayed</div></div>'
    html += f'<div class="kpi"><div class="num">{len(z2e_all)}</div><div class="label">Z2E Total</div></div>'
    html += f'<div class="kpi"><div class="num">{len(non_z2e)}</div><div class="label">Non-Z2E</div></div>'
    html += '</div>'
    html += f'<div class="section-box"><strong>Z2E:</strong> <span class="z2e-tag p1">P1</span> {z2e_p1} | <span class="z2e-tag p2">P2</span> {z2e_p2} | <span class="z2e-tag ns">NS</span> {z2e_ns}</div>'
    html += build_email_edisc_health(edisc)
    html += build_email_staleness(edisc_active)
    html += build_email_recent(edisc)

    # Data PSG section
    dpsg = enriched_by_team.get("Data PSG", [])
    dpsg_active = [p for p in dpsg if p["status_val"] in ACTIVE_STATUS_VALUES]
    dpsg_blocked = [p for p in dpsg_active if p["status_val"] == 4]
    dpsg_delayed = [p for p in dpsg_active if p["status_val"] == 12]

    html += f'<h3>Data PSG \u2014 {DIRECTOR_NAMES["Data PSG"]}</h3>'
    html += '<div class="kpi-row">'
    html += f'<div class="kpi"><div class="num">{len(dpsg_active)}</div><div class="label">Active</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#dc2626;">{len(dpsg_blocked)}</div><div class="label">Blocked</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#dc2626;">{len(dpsg_delayed)}</div><div class="label">Delayed</div></div>'
    html += '</div>'
    html += build_email_health_section(dpsg)
    dpsg_attention = [p for p in dpsg_active if p["status_val"] in (4, 12)]
    if dpsg_attention:
        html += f'<h4>Blocked / Delayed ({len(dpsg_attention)})</h4><ul>'
        for p in sorted(dpsg_attention, key=lambda x: x["name"]):
            html += f'<li><strong>{email_project_link(p["name"], p["id"])}</strong> \u2014 {p["status"]} \u2014 {p["customer"]} (PM: {p["owner"]})</li>'
        html += '</ul>'
    html += build_email_staleness(dpsg_active)
    html += build_email_recent(dpsg)

    # Post Implementation section
    pimpl = enriched_by_team.get("Post Implementation", [])
    pimpl_active = [p for p in pimpl if p["status_val"] in ACTIVE_STATUS_VALUES]
    pimpl_blocked = [p for p in pimpl_active if p["status_val"] == 4]
    pimpl_delayed = [p for p in pimpl_active if p["status_val"] == 12]
    subs = [p for p in pimpl if p["project_type"].lower() in ("subscription", "consumption")]
    subs_active = [p for p in subs if p["status_val"] in ACTIVE_STATUS_VALUES]
    zero_subs = [p for p in subs if _is_zero_subtotal(p["service_subtotal"])]
    customer_zero_counts = Counter()
    for p in zero_subs:
        customer_zero_counts[p["customer"]] += 1
    repeat_zero = {c: n for c, n in customer_zero_counts.items() if n >= 2}

    html += f'<h3>Post Implementation \u2014 {DIRECTOR_NAMES["Post Implementation"]}</h3>'
    html += '<div class="kpi-row">'
    html += f'<div class="kpi"><div class="num">{len(pimpl_active)}</div><div class="label">Active</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#dc2626;">{len(pimpl_blocked)}</div><div class="label">Blocked</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#dc2626;">{len(pimpl_delayed)}</div><div class="label">Delayed</div></div>'
    html += f'<div class="kpi"><div class="num">{len(subs_active)}</div><div class="label">Subscriptions</div></div>'
    html += f'<div class="kpi"><div class="num" style="color:#b45309;">{len(zero_subs)}</div><div class="label">$0 Subs</div></div>'
    html += '</div>'
    html += build_email_health_section(pimpl)
    pimpl_attention = [p for p in pimpl_active if p["status_val"] in (4, 12)]
    if pimpl_attention:
        html += f'<h4>Blocked / Delayed ({len(pimpl_attention)})</h4><ul>'
        for p in sorted(pimpl_attention, key=lambda x: x["name"]):
            html += f'<li><strong>{email_project_link(p["name"], p["id"])}</strong> \u2014 {p["status"]} \u2014 {p["customer"]} (PM: {p["owner"]})</li>'
        html += '</ul>'
    if repeat_zero:
        html += '<h4 style="color:#b45309;">Repeat $0 Subscription Customers</h4>'
        html += '<div class="section-box" style="border-color:#fbbf24;">'
        html += '<p class="muted">Customers with multiple subscription projects at $0:</p><ul>'
        for cust, count in sorted(repeat_zero.items(), key=lambda x: -x[1]):
            html += f'<li><strong>{cust}</strong> \u2014 {count} subscription projects with $0/blank subtotal</li>'
        html += '</ul></div>'
    html += build_email_staleness(pimpl_active)
    html += build_email_recent(pimpl)

    # Zombie Watch
    if zombies:
        critical = sum(1 for r in zombies if r["score"] >= 60)
        high = sum(1 for r in zombies if 45 <= r["score"] < 60)
        medium = sum(1 for r in zombies if 30 <= r["score"] < 45)

        html += '<div class="zombie-section"><h3>Zombie Watch</h3>'
        html += '<p class="muted">Projects with corroborating staleness signals.</p>'
        html += '<div class="kpi-row">'
        html += f'<div class="kpi" style="background:#312e48;border-color:#4c4675;"><div class="num" style="color:#fca5a5;">{len(zombies)}</div><div class="label" style="color:#94a3b8;">Flagged</div></div>'
        if critical:
            html += f'<div class="kpi" style="background:#312e48;border-color:#4c4675;"><div class="num" style="color:#fca5a5;">{critical}</div><div class="label" style="color:#94a3b8;">Critical 60+</div></div>'
        if high:
            html += f'<div class="kpi" style="background:#312e48;border-color:#4c4675;"><div class="num" style="color:#fed7aa;">{high}</div><div class="label" style="color:#94a3b8;">High 45-59</div></div>'
        if medium:
            html += f'<div class="kpi" style="background:#312e48;border-color:#4c4675;"><div class="num" style="color:#fde68a;">{medium}</div><div class="label" style="color:#94a3b8;">Medium 30-44</div></div>'
        html += '</div>'

        for team_name in ["eDiscovery", "Data PSG", "Post Implementation"]:
            team_flagged = [r for r in zombies if r["team"] == team_name]
            if not team_flagged:
                continue
            html += f'<h4 style="color:#c4b5fd;margin-top:12px;">{team_name} ({len(team_flagged)})</h4>'
            html += '<table><tr><th>Score</th><th>Project</th><th>Customer</th><th>PM</th><th>Status</th><th>Health</th>'
            html += '<th>Last Time</th><th>Tasks</th><th>Overdue</th><th>Notes</th><th>Go-Live Slip</th></tr>'
            for r in team_flagged:
                p = r["enriched"]
                sc = score_class(r["score"])
                link = email_project_link(p["name"], p["id"])
                dot = health_dot(p["health"])
                te = r["last_time_entry"]
                if r["time_entry_stale_days"] and r["time_entry_stale_days"] < 9999:
                    te += f' ({r["time_entry_stale_days"]}d)'
                elif r["last_time_entry"] == "Never":
                    te = '<strong style="color:#fca5a5;">Never</strong>'
                task_str = f'{r["completed_tasks"]}/{r["total_tasks"]}' if r["total_tasks"] else "\u2014"
                if r["task_pct"] is not None:
                    task_str += f' ({int(r["task_pct"])}%)'
                overdue_str = str(r["overdue_tasks"]) if r["overdue_tasks"] else "\u2014"
                if r["overdue_tasks"] and r["total_tasks"] and r["overdue_tasks"] / r["total_tasks"] > 0.5:
                    overdue_str = f'<strong style="color:#fca5a5;">{r["overdue_tasks"]}</strong>'
                notes_str = f'{r["notes_stale_days"]}d' if r["notes_stale_days"] else "N/A"
                slip_str = f'{r["go_live_slip_days"]}d' if r["go_live_slip_days"] else "\u2014"
                if r["go_live_slip_days"] and r["go_live_slip_days"] > 180:
                    slip_str = f'<strong style="color:#fca5a5;">{r["go_live_slip_days"]}d</strong>'
                html += f'<tr><td class="num {sc}">{r["score"]}</td><td>{link}</td><td>{p["customer"]}</td>'
                html += f'<td>{p["owner"]}</td><td>{p["status"]}</td><td>{dot}{p["health"] or "\u2014"}</td>'
                html += f'<td>{te}</td><td class="num">{task_str}</td><td class="num">{overdue_str}</td>'
                html += f'<td class="num">{notes_str}</td><td class="num">{slip_str}</td></tr>'
            html += '</table>'
        html += '</div>'

    html += f'''<hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0;">
<p class="muted">Auto-generated from Rocketlane API \u00b7 {NOW.strftime("%H:%M")} \u00b7 services.api.exterro.com</p>
</body></html>'''

    return html


def send_email(subject, html_body):
    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = GMAIL_ADDRESS
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    print("Connecting to Gmail SMTP...")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)
    print("Email sent successfully.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="PS Daily Briefing")
    parser.add_argument("--mode", choices=["chat", "email", "both"], default="both",
                        help="Output mode: chat (Google Chat card), email (HTML report), or both")
    args = parser.parse_args()

    need_chat = args.mode in ("chat", "both")
    need_email = args.mode in ("email", "both")

    if not API_KEY:
        print("ERROR: ROCKETLANE_API_KEY not set"); sys.exit(1)
    if need_chat and not WEBHOOK_URL:
        print("ERROR: GCHAT_WEBHOOK_URL not set"); sys.exit(1)
    if need_email and (not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD):
        print("ERROR: GMAIL_ADDRESS and GMAIL_APP_PASSWORD required for email mode"); sys.exit(1)

    print("=" * 60)
    print(f"PS Daily Briefing — mode: {args.mode}")
    print("=" * 60)

    print("Fetching all Rocketlane projects...")
    projects = fetch_all_projects()
    print(f"Fetched {len(projects)} projects.")

    print("Enriching and scoring...")
    data = load_briefing_data(projects)

    if need_chat:
        print("\n--- Building Google Chat card ---")
        card = build_chat_card(data)
        card_json = json.dumps(card)
        print(f"Card built ({len(card_json)} bytes)")
        print("Posting to webhook...")
        try:
            status, body = post_chat_card(card)
            if status == 200:
                print("SUCCESS \u2014 card posted to Google Chat.")
            else:
                print(f"WARNING \u2014 HTTP {status}: {body[:200]}")
        except urllib.error.HTTPError as e:
            print(f"ERROR \u2014 HTTP {e.code}: {e.read().decode()[:200]}")
            if not need_email:
                sys.exit(1)
        except Exception as e:
            print(f"ERROR \u2014 {e}")
            if not need_email:
                sys.exit(1)

    if need_email:
        print("\n--- Building HTML email report ---")
        html = build_email_html(data)
        print(f"HTML built ({len(html)} bytes)")
        subject = f"PS Daily Briefing \u2014 {NOW.strftime('%b %d, %Y')}"
        send_email(subject, html)
        print("SUCCESS \u2014 email sent.")

    print("\nDone.")


if __name__ == "__main__":
    main()
