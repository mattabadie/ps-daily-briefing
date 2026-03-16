#!/usr/bin/env python3
"""
Forensics Daily Briefing — Standalone Script
Covers forensics implementation and post-implementation (subscription/GLAM) projects.

Projects are identified by:
  1. Opp: Service Hours Domain(s) containing "Forensics"
  2. Project name containing FTK, GLAM, or Forensics keywords

Usage:
  python forensics_briefing.py --mode chat      # Post card to Google Chat
  python forensics_briefing.py --mode email     # Email full HTML report
  python forensics_briefing.py --mode both      # Do both

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

DASH = "\u2014"
ACTIVE_STATUS_VALUES = {2, 4, 5, 6, 9, 12, 14, 15}
NOW = datetime.now()
CUTOFF_MS = int((NOW - timedelta(hours=24)).timestamp() * 1000)
ZOMBIE_SCORE_THRESHOLD = 30

# Forensics identification
FORENSICS_NAME_RE = re.compile(
    r'\b(glam|ftk|forensic|ad\s+enterprise|ad\s+lab)\b', re.IGNORECASE
)

CONCERN_KEYWORDS = [
    "risk", "blocker", "blocked", "delayed", "escalat", "issue", "concern",
    "slipp", "behind", "overdue", "miss", "fail", "stop", "halt", "pause",
    "no response", "unresponsive", "at risk", "critical", "urgent",
]

EXTRA_RECIPIENTS = ["matt.abadie@exterro.com"]


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP + DATA FETCHING
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
# FIELD HELPERS
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
        text = pattern.sub(r'<strong style="font-weight:700;color:#dc2626;">\1</strong>', text)
    return text


# ═══════════════════════════════════════════════════════════════════════════════
# FORENSICS PROJECT IDENTIFICATION
# ═══════════════════════════════════════════════════════════════════════════════
def is_forensics_project(p):
    """Identify forensics projects by domain field OR name keywords."""
    # Check domain field
    domain = get_field(p, "Opp: Service Hours Domain(s)") or ""
    if "forensic" in domain.lower():
        return True
    # Check name keywords
    name = p.get("projectName", "")
    if FORENSICS_NAME_RE.search(name):
        return True
    return False


def classify_forensics(p_enriched):
    """Classify a forensics project into a category."""
    pt = p_enriched["project_type"].lower()
    name = p_enriched["name"].lower()
    if pt == "subscription" or "glam" in name or "pss" in name:
        if "glam" in name:
            return "GLAM"
        return "Subscription"
    if pt == "implementation":
        if "upgrade" in name:
            return "Upgrade"
        if "installation" in name or "install" in name:
            return "New Install"
        if "migration" in name:
            return "Migration"
        return "Implementation"
    if pt == "presale":
        return "PreSale"
    return "Other"


# ═══════════════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════════
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
    project_type = get_field(p, "Project Type") or ""
    service_subtotal = get_field(p, "Opp: Service Subtotal") or ""
    domain = get_field(p, "Opp: Service Hours Domain(s)") or ""
    project_id = p.get("projectId", "")

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
        "health_stale_days": days_since(health_date),
        "weekly_stale_days": days_since(weekly_date),
        "updated_at": p.get("updatedAt", 0),
        "project_type": project_type,
        "service_subtotal": service_subtotal,
        "domain": domain,
        "has_concerns": has_concern_keywords(health_notes) or has_concern_keywords(weekly_status),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ZOMBIE SCORING
# ═══════════════════════════════════════════════════════════════════════════════
def compute_zombie_score(enriched_p):
    pid = enriched_p["id"]
    sv = enriched_p["status_val"]
    health = enriched_p["health"]

    health_date = extract_date_from_text(enriched_p["health_notes"])
    weekly_date = extract_date_from_text(enriched_p["weekly_status"])
    latest_note_date = max(filter(None, [health_date, weekly_date]), default=None)
    notes_stale_days = (NOW - latest_note_date).days if latest_note_date else None

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
        "enriched": enriched_p,
    }


def run_zombie_scoring(enriched_projects):
    candidates = [p for p in enriched_projects if p["status_val"] in ACTIVE_STATUS_VALUES]
    print(f"  Zombie scoring {len(candidates)} candidates...")
    results = []

    def _score(p):
        return compute_zombie_score(p)

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(_score, p): p for p in candidates}
        done = 0
        for f in as_completed(futures):
            done += 1
            if done % 20 == 0:
                print(f"    {done}/{len(candidates)}...")
            try:
                results.append(f.result())
            except Exception:
                pass

    return sorted([r for r in results if r["score"] >= ZOMBIE_SCORE_THRESHOLD], key=lambda x: -x["score"])


# ═══════════════════════════════════════════════════════════════════════════════
# CORE DATA PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
def load_briefing_data(all_projects):
    forensics_raw = [p for p in all_projects if is_forensics_project(p)]
    print(f"  {len(forensics_raw)} forensics projects identified")

    enriched = [enrich_project(p) for p in forensics_raw]

    # Classify into groups
    by_category = defaultdict(list)
    for p in enriched:
        cat = classify_forensics(p)
        by_category[cat].append(p)

    # Group into three main sections
    impl_cats = ("Implementation", "Upgrade", "New Install", "Migration", "Other")
    post_impl_cats = ("GLAM", "Subscription")

    impl_projects = []
    for cat in impl_cats:
        impl_projects.extend(by_category.get(cat, []))

    presale_projects = by_category.get("PreSale", [])

    post_impl_projects = []
    for cat in post_impl_cats:
        post_impl_projects.extend(by_category.get(cat, []))

    zombies = run_zombie_scoring(enriched)

    return {
        "all_enriched": enriched,
        "by_category": dict(by_category),
        "impl_projects": impl_projects,
        "presale_projects": presale_projects,
        "post_impl_projects": post_impl_projects,
        "zombies": zombies,
        "today_str": NOW.strftime("%A, %B %d, %Y"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE CHAT CARD
# ═══════════════════════════════════════════════════════════════════════════════
def plink(name, pid):
    return f'<a href="{RL_APP_BASE}/{pid}">{name}</a>'


def build_chat_card(data):
    today_str = data["today_str"]
    all_e = data["all_enriched"]
    impl = data["impl_projects"]
    presale = data["presale_projects"]
    post = data["post_impl_projects"]
    zombies = data["zombies"]

    active = [p for p in all_e if p["status_val"] in ACTIVE_STATUS_VALUES]
    blocked = [p for p in active if p["status_val"] == 4]
    delayed = [p for p in active if p["status_val"] == 12]
    red_all = [p for p in active if p["health"] == "red"]
    yellow_all = [p for p in active if p["health"] == "yellow"]
    no_health = [p for p in active if not p["health"]]
    concern_projects = [p for p in active if p["has_concerns"] and p["health"] in ("red", "yellow")]

    impl_active = [p for p in impl if p["status_val"] in ACTIVE_STATUS_VALUES]
    presale_active = [p for p in presale if p["status_val"] in ACTIVE_STATUS_VALUES]
    post_active = [p for p in post if p["status_val"] in ACTIVE_STATUS_VALUES]

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

    # Breakdown
    breakdown = (
        f"\ud83d\udee0\ufe0f <b>Implementation:</b> {len(impl_active)} active  \u2022  "
        f"\ud83d\udcdd <b>PreSale:</b> {len(presale_active)} active  \u2022  "
        f"\ud83d\udd04 <b>Post-Impl / GLAM:</b> {len(post_active)} active"
    )
    sections.append({"widgets": [{"textParagraph": {"text": breakdown}}]})

    # Action items
    action_lines = []
    if blocked:
        action_lines.append(f"\ud83d\udeab <b>{len(blocked)} blocked projects</b> need unblocking")
    if no_health:
        action_lines.append(f"\u26a0\ufe0f <b>{len(no_health)} projects</b> have no health status set")
    if concern_projects:
        top = sorted(concern_projects, key=lambda x: x["name"])[:3]
        for p in top:
            snippet = p["health_notes"][:80] if p["health_notes"] else p["weekly_status"][:80]
            action_lines.append(f"\ud83d\udd34 {plink(p['name'], p['id'])} \u2014 <i>{snippet}</i>")
    if zombies:
        action_lines.append(f"\ud83e\udddf <b>{len(zombies)} zombie projects</b> with staleness scores \u2265{ZOMBIE_SCORE_THRESHOLD}")
    if action_lines:
        sections.append({
            "header": "\u26a1 Needs Attention",
            "widgets": [{"textParagraph": {"text": "<br>".join(action_lines)}}],
        })

    # Red/Yellow detail
    for color, label, projects in [("red", "\ud83d\udd34 Red Health", red_all), ("yellow", "\ud83d\udfe1 Yellow Health", yellow_all)]:
        if not projects:
            continue
        lines = []
        for p in sorted(projects, key=lambda x: x["name"])[:8]:
            note = p["health_notes"][:100] if p["health_notes"] else ""
            cat = classify_forensics(p)
            lines.append(f"[{cat}] {plink(p['name'], p['id'])} \u2014 {p['customer']} (PM: {p['owner']})" + (f"<br><i>{note}</i>" if note else ""))
        sections.append({
            "header": f"{label} ({len(projects)})",
            "widgets": [{"textParagraph": {"text": "<br>".join(lines)}}],
        })

    card = {
        "cardsV2": [{
            "cardId": "forensics-briefing",
            "card": {
                "header": {
                    "title": f"Forensics Daily Briefing {DASH} {today_str}",
                    "subtitle": "FTK | GLAM | Forensics Subscriptions",
                },
                "sections": sections,
            },
        }]
    }
    return card


def post_chat_card(card):
    data = json.dumps(card).encode()
    req = urllib.request.Request(WEBHOOK_URL, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode()


# ═══════════════════════════════════════════════════════════════════════════════
# HTML EMAIL — ALL INLINE STYLES (Gmail-forward safe)
# ═══════════════════════════════════════════════════════════════════════════════
S_BODY = "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:780px;margin:0 auto;color:#1a1a1a;font-size:13px;line-height:1.5;"
S_H2 = "border-bottom:2px solid #7c3aed;padding-bottom:8px;font-size:18px;"
S_H3 = "margin-top:28px;font-size:15px;border-left:4px solid #7c3aed;padding-left:10px;"
S_H4 = "margin:16px 0 6px 0;font-size:13px;"
S_TABLE = "border-collapse:collapse;width:100%;margin:6px 0 14px 0;font-size:11px;"
S_TH = "padding:4px 8px;border:1px solid #e2e8f0;text-align:left;vertical-align:top;background:#f1f5f9;font-weight:600;"
S_TD = "padding:4px 8px;border:1px solid #e2e8f0;text-align:left;vertical-align:top;"
S_TD_NUM = "padding:4px 8px;border:1px solid #e2e8f0;text-align:right;vertical-align:top;"
S_LINK = "color:#7c3aed;text-decoration:none;"
S_MUTED = "color:#64748b;font-size:11px;"
S_KPI_ROW = "margin:8px 0;"
S_KPI = "display:inline-block;background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:8px 14px;min-width:90px;text-align:center;margin:4px 6px 4px 0;vertical-align:top;"
S_KPI_NUM = "font-size:22px;font-weight:700;"
S_KPI_LABEL = "font-size:10px;color:#64748b;text-transform:uppercase;"
S_SECTION_BOX = "background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:10px 14px;margin:8px 0;"
S_UL = "margin:4px 0;padding-left:20px;"
S_LI = "margin-bottom:4px;"
S_PM_GROUP = "margin:4px 0 2px 16px;font-weight:700;font-size:12px;color:#334155;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #cbd5e1;padding-bottom:2px;"
S_NO_HEALTH = "background:#fff7ed;border:2px dashed #f97316;border-radius:6px;padding:8px 12px;margin:8px 0;color:#9a3412;font-weight:700;font-size:13px;"
HEALTH_STYLES = {
    "red": "background:#fef2f2;border-left:4px solid #dc2626;padding:8px 10px;margin:6px 0;border-radius:4px;",
    "yellow": "background:#fefce8;border-left:4px solid #ca8a04;padding:8px 10px;margin:6px 0;border-radius:4px;",
    "green": "background:#f0fdf4;border-left:4px solid #16a34a;padding:8px 10px;margin:6px 0;border-radius:4px;",
}
STOPLIGHT_COLORS = {"red": "#dc2626", "yellow": "#eab308", "green": "#16a34a"}
S_STOPLIGHT_BASE = "display:inline-block;width:14px;height:14px;border-radius:50%;vertical-align:middle;margin-right:6px;"
CAT_TAG_STYLES = {
    "GLAM": "display:inline-block;background:#dbeafe;color:#1e40af;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
    "Subscription": "display:inline-block;background:#e0e7ff;color:#3730a3;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
    "Upgrade": "display:inline-block;background:#fef3c7;color:#92400e;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
    "New Install": "display:inline-block;background:#d1fae5;color:#065f46;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
    "Migration": "display:inline-block;background:#fce7f3;color:#9d174d;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
    "Implementation": "display:inline-block;background:#f3e8ff;color:#6b21a8;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
    "PreSale": "display:inline-block;background:#f1f5f9;color:#475569;font-size:10px;padding:0 5px;border-radius:3px;font-weight:600;margin-left:4px;",
}
S_ZOMBIE_SECTION = "background:#1e1b2e;color:#e2e8f0;border-radius:8px;padding:14px 18px;margin:14px 0;"
S_ZOMBIE_H3 = "margin-top:0;font-size:15px;border-left:4px solid #a78bfa;padding-left:10px;color:#a78bfa;"
S_ZOMBIE_TH = "padding:4px 8px;border:1px solid #4c4675;text-align:left;vertical-align:top;background:#312e48;font-weight:600;color:#c4b5fd;"
S_ZOMBIE_TD = "padding:4px 8px;border:1px solid #4c4675;text-align:left;vertical-align:top;color:#e2e8f0;"
S_ZOMBIE_TD_NUM = "padding:4px 8px;border:1px solid #4c4675;text-align:right;vertical-align:top;color:#e2e8f0;"
S_ZOMBIE_LINK = "color:#93c5fd;text-decoration:none;"
SCORE_STYLES = {
    "score-critical": "background:#7f1d1d;font-weight:700;color:#fca5a5;",
    "score-high": "background:#78350f;font-weight:700;color:#fed7aa;",
    "score-medium": "background:#422006;color:#fde68a;",
}


def staleness_badge(days):
    if days is None:
        return ""
    if days > 60:
        return f'<span style="display:inline-block;background:#ef4444;color:#fff;font-size:10px;padding:1px 6px;border-radius:10px;font-weight:600;margin-left:4px;">{days}d stale</span>'
    elif days > 14:
        return f'<span style="display:inline-block;background:#fbbf24;color:#78350f;font-size:10px;padding:1px 6px;border-radius:10px;font-weight:600;margin-left:4px;">{days}d stale</span>'
    return ""


def email_project_link(name, pid):
    return f'<a href="{RL_APP_BASE}/{pid}" target="_blank" style="{S_LINK}">{name}</a>'


def cat_tag(p_enriched):
    cat = classify_forensics(p_enriched)
    style = CAT_TAG_STYLES.get(cat, CAT_TAG_STYLES.get("Implementation", ""))
    return f'<span style="{style}">{cat}</span>'


def health_dot(h):
    if h in STOPLIGHT_COLORS:
        return f'<span style="{S_STOPLIGHT_BASE}background:{STOPLIGHT_COLORS[h]};"></span>'
    return ""


def _kpi(value, label, color=None):
    num_style = S_KPI_NUM + (f"color:{color};" if color else "")
    return f'<div style="{S_KPI}"><div style="{num_style}">{value}</div><div style="{S_KPI_LABEL}">{label}</div></div>'


def _zombie_kpi(value, label, num_color="#fca5a5"):
    s = "display:inline-block;background:#312e48;border:1px solid #4c4675;border-radius:6px;padding:8px 14px;min-width:90px;text-align:center;margin:4px 6px 4px 0;vertical-align:top;"
    return f'<div style="{s}"><div style="font-size:22px;font-weight:700;color:{num_color};">{value}</div><div style="font-size:10px;color:#94a3b8;text-transform:uppercase;">{label}</div></div>'


def score_class(score):
    if score >= 60: return "score-critical"
    elif score >= 45: return "score-high"
    elif score >= 30: return "score-medium"
    return ""


def render_project_li(p):
    link = email_project_link(p["name"], p["id"])
    stale = staleness_badge(p["health_stale_days"])
    notes = highlight_concerns(p["health_notes"][:200])
    note_str = f' {DASH} <em>{notes}</em>' if notes else ""
    tag = cat_tag(p)
    return f'<li style="{S_LI}"><strong>{link}</strong>{tag} {DASH} {p["customer"]} (PM: {p["owner"]}){stale}{note_str}</li>'


def render_projects_by_pm(projects):
    by_pm = defaultdict(list)
    for p in projects:
        by_pm[p["owner"] or "Unassigned"].append(p)
    html = ""
    for pm in sorted(by_pm.keys()):
        pm_projs = sorted(by_pm[pm], key=lambda x: x["name"])
        html += f'<div style="{S_PM_GROUP}">{pm} ({len(pm_projs)})</div>'
        html += f'<ul style="{S_UL}padding-left:8px;">'
        for p in pm_projs:
            html += render_project_li(p)
        html += '</ul>'
    return html


def build_health_section(enriched_projects):
    active = [p for p in enriched_projects if p["status_val"] in ACTIVE_STATUS_VALUES]
    red = [p for p in active if p["health"] == "red"]
    yellow = [p for p in active if p["health"] == "yellow"]
    green = [p for p in active if p["health"] == "green"]
    no_health = [p for p in active if not p["health"]]
    html = ""
    for color, projects, label_color in [
        ("red", red, "#dc2626"),
        ("yellow", yellow, "#ca8a04"),
    ]:
        if not projects:
            continue
        html += f'<div style="{HEALTH_STYLES[color]}">{health_dot(color)}'
        html += f'<strong style="color:{label_color};">{color.upper()} ({len(projects)})</strong>'
        html += render_projects_by_pm(projects)
        html += '</div>'
    if green:
        stale_greens = [p for p in green if p["health_stale_days"] and p["health_stale_days"] > 30]
        stale_note = f' ({len(stale_greens)} with stale notes &gt;30d)' if stale_greens else ""
        html += f'<div style="{HEALTH_STYLES["green"]}">{health_dot("green")}'
        html += f'<strong style="color:#16a34a;">GREEN ({len(green)})</strong> {DASH} {len(green)} projects healthy{stale_note}</div>'
    if no_health:
        html += f'<div style="{S_NO_HEALTH}">\u26a0\ufe0f <span style="font-size:20px;font-weight:800;color:#c2410c;">{len(no_health)}</span> active projects with NO HEALTH SET</div>'
    return html


def build_staleness(active_projects):
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
    html = f'<h4 style="{S_H4}">Stale Updates ({len(stale)} projects &gt;14 days)</h4><ul style="{S_UL}font-size:12px;">'
    for p, d in stale[:10]:
        badge = staleness_badge(d)
        link = email_project_link(p["name"], p["id"])
        html += f'<li style="{S_LI}">{link} {DASH} {p["customer"]} (PM: {p["owner"]}){badge}</li>'
    if len(stale) > 10:
        html += f'<li style="{S_LI}{S_MUTED}">...and {len(stale)-10} more</li>'
    html += '</ul>'
    return html


def build_recent(enriched_projects):
    recently_updated = [p for p in enriched_projects if p["updated_at"] >= CUTOFF_MS and p["status_val"] in ACTIVE_STATUS_VALUES]
    recently_completed = [p for p in enriched_projects if p["status_val"] == 3 and p["updated_at"] >= CUTOFF_MS]
    html = ""
    if recently_completed:
        html += f'<h4 style="{S_H4}color:#16a34a;">Completed (24h): {len(recently_completed)}</h4><ul style="{S_UL}font-size:12px;">'
        for p in sorted(recently_completed, key=lambda x: x["name"]):
            html += f'<li style="{S_LI}">{email_project_link(p["name"], p["id"])} {DASH} {p["customer"]}</li>'
        html += '</ul>'
    if recently_updated:
        html += f'<h4 style="{S_H4}">Updated (24h): {len(recently_updated)}</h4><ul style="{S_UL}font-size:12px;">'
        for p in sorted(recently_updated, key=lambda x: x["updated_at"], reverse=True)[:8]:
            html += f'<li style="{S_LI}">{email_project_link(p["name"], p["id"])} {DASH} {p["status"]} (PM: {p["owner"]})</li>'
        if len(recently_updated) > 8:
            html += f'<li style="{S_LI}{S_MUTED}">...and {len(recently_updated)-8} more</li>'
        html += '</ul>'
    return html


def build_email_html(data):
    today_str = data["today_str"]
    all_enriched = data["all_enriched"]
    impl = data["impl_projects"]
    presale = data["presale_projects"]
    post = data["post_impl_projects"]
    by_cat = data["by_category"]
    zombies = data["zombies"]

    active_all = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]
    blocked = sum(1 for p in active_all if p["status_val"] == 4)
    delayed = sum(1 for p in active_all if p["status_val"] == 12)
    red_health = sum(1 for p in active_all if p["health"] == "red")
    yellow_health = sum(1 for p in active_all if p["health"] == "yellow")

    impl_active = [p for p in impl if p["status_val"] in ACTIVE_STATUS_VALUES]
    presale_active = [p for p in presale if p["status_val"] in ACTIVE_STATUS_VALUES]
    post_active = [p for p in post if p["status_val"] in ACTIVE_STATUS_VALUES]

    html = f'''<html><head></head>
<body style="{S_BODY}">
<h2 style="{S_H2}">Forensics Daily Briefing {DASH} {today_str}</h2>
<p style="{S_MUTED}">FTK | GLAM | Forensics Subscriptions</p>
<div style="{S_KPI_ROW}">
{_kpi(len(active_all), "Active")}
{_kpi(len(impl_active), "Implementation")}
{_kpi(len(presale_active), "PreSale")}
{_kpi(len(post_active), "Post-Impl")}
{_kpi(blocked, "Blocked", "#dc2626")}
{_kpi(delayed, "Delayed", "#dc2626")}
{_kpi(red_health, "Red Health", "#dc2626")}
{_kpi(yellow_health, "Yellow Health", "#ca8a04")}
</div>
'''

    # Category breakdown box
    cat_counts = {cat: len([p for p in projs if p["status_val"] in ACTIVE_STATUS_VALUES]) for cat, projs in by_cat.items()}
    cat_parts = [f"<strong>{cat}:</strong> {cnt}" for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]) if cnt > 0]
    cat_line = " &nbsp;\u2022&nbsp; ".join(cat_parts)
    html += f'<div style="{S_SECTION_BOX}">{cat_line}</div>'

    # Implementation section
    html += f'<h3 style="{S_H3}">Implementation</h3>'
    impl_blocked = [p for p in impl_active if p["status_val"] == 4]
    impl_delayed = [p for p in impl_active if p["status_val"] == 12]

    html += f'<div style="{S_KPI_ROW}">'
    html += _kpi(len(impl_active), "Active")
    impl_upgrades = [p for p in impl_active if classify_forensics(p) == "Upgrade"]
    impl_installs = [p for p in impl_active if classify_forensics(p) == "New Install"]
    html += _kpi(len(impl_upgrades), "Upgrades")
    html += _kpi(len(impl_installs), "New Installs")
    html += _kpi(len(impl_blocked), "Blocked", "#dc2626")
    html += _kpi(len(impl_delayed), "Delayed", "#dc2626")
    html += '</div>'

    html += build_health_section(impl)

    if impl_blocked or impl_delayed:
        attention = impl_blocked + impl_delayed
        html += f'<h4 style="{S_H4}">Blocked / Delayed ({len(attention)})</h4><ul style="{S_UL}">'
        for p in sorted(attention, key=lambda x: x["name"]):
            html += f'<li style="{S_LI}"><strong>{email_project_link(p["name"], p["id"])}</strong>{cat_tag(p)} {DASH} {p["status"]} {DASH} {p["customer"]} (PM: {p["owner"]})</li>'
        html += '</ul>'

    html += build_staleness(impl_active)
    html += build_recent(impl)

    # PreSale section
    html += f'<h3 style="{S_H3}">PreSale</h3>'
    html += f'<div style="{S_KPI_ROW}">'
    html += _kpi(len(presale_active), "Active")
    html += '</div>'

    html += build_health_section(presale)
    html += build_staleness(presale_active)
    html += build_recent(presale)

    # Post-Implementation section (GLAM + Subscriptions)
    html += f'<h3 style="{S_H3}">Post-Implementation {DASH} GLAM &amp; Subscriptions</h3>'
    post_blocked = [p for p in post_active if p["status_val"] == 4]
    post_delayed = [p for p in post_active if p["status_val"] == 12]
    glam_active = [p for p in post_active if classify_forensics(p) == "GLAM"]
    sub_active = [p for p in post_active if classify_forensics(p) == "Subscription"]

    html += f'<div style="{S_KPI_ROW}">'
    html += _kpi(len(post_active), "Active")
    html += _kpi(len(glam_active), "GLAM")
    html += _kpi(len(sub_active), "Subscriptions")
    html += _kpi(len(post_blocked), "Blocked", "#dc2626")
    html += _kpi(len(post_delayed), "Delayed", "#dc2626")
    html += '</div>'

    html += build_health_section(post)

    if post_blocked or post_delayed:
        attention = post_blocked + post_delayed
        html += f'<h4 style="{S_H4}">Blocked / Delayed ({len(attention)})</h4><ul style="{S_UL}">'
        for p in sorted(attention, key=lambda x: x["name"]):
            html += f'<li style="{S_LI}"><strong>{email_project_link(p["name"], p["id"])}</strong>{cat_tag(p)} {DASH} {p["status"]} {DASH} {p["customer"]} (PM: {p["owner"]})</li>'
        html += '</ul>'

    html += build_staleness(post_active)
    html += build_recent(post)

    # Zombie Watch
    if zombies:
        critical = sum(1 for r in zombies if r["score"] >= 60)
        high = sum(1 for r in zombies if 45 <= r["score"] < 60)
        medium = sum(1 for r in zombies if 30 <= r["score"] < 45)

        html += f'<div style="{S_ZOMBIE_SECTION}"><h3 style="{S_ZOMBIE_H3}">Zombie Watch</h3>'
        html += f'<p style="color:#94a3b8;font-size:11px;">Projects with corroborating staleness signals.</p>'
        html += f'<div style="{S_KPI_ROW}">'
        html += _zombie_kpi(len(zombies), "Flagged", "#fca5a5")
        if critical:
            html += _zombie_kpi(critical, "Critical 60+", "#fca5a5")
        if high:
            html += _zombie_kpi(high, "High 45-59", "#fed7aa")
        if medium:
            html += _zombie_kpi(medium, "Medium 30-44", "#fde68a")
        html += '</div>'

        html += f'<table style="{S_TABLE}color:#e2e8f0;"><tr>'
        for hdr in ["Score", "Project", "Customer", "PM", "Category", "Status", "Health", "Last Time", "Tasks", "Overdue", "Notes"]:
            html += f'<th style="{S_ZOMBIE_TH}">{hdr}</th>'
        html += '</tr>'
        for r in zombies:
            p = r["enriched"]
            sc = score_class(r["score"])
            sc_style = SCORE_STYLES.get(sc, "")
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_ZOMBIE_LINK}">{p["name"]}</a>'
            dot = health_dot(p["health"])
            cat = classify_forensics(p)
            te = r["last_time_entry"]
            if r["time_entry_stale_days"] and r["time_entry_stale_days"] < 9999:
                te += f' ({r["time_entry_stale_days"]}d)'
            elif r["last_time_entry"] == "Never":
                te = '<strong style="color:#fca5a5;">Never</strong>'
            task_str = f'{r["completed_tasks"]}/{r["total_tasks"]}' if r["total_tasks"] else DASH
            if r["task_pct"] is not None:
                task_str += f' ({int(r["task_pct"])}%)'
            overdue_str = str(r["overdue_tasks"]) if r["overdue_tasks"] else DASH
            if r["overdue_tasks"] and r["total_tasks"] and r["overdue_tasks"] / r["total_tasks"] > 0.5:
                overdue_str = f'<strong style="color:#fca5a5;">{r["overdue_tasks"]}</strong>'
            notes_str = f'{r["notes_stale_days"]}d' if r["notes_stale_days"] else "N/A"
            html += f'<tr><td style="{S_ZOMBIE_TD_NUM}{sc_style}">{r["score"]}</td><td style="{S_ZOMBIE_TD}">{link}</td><td style="{S_ZOMBIE_TD}">{p["customer"]}</td>'
            html += f'<td style="{S_ZOMBIE_TD}">{p["owner"]}</td><td style="{S_ZOMBIE_TD}">{cat}</td><td style="{S_ZOMBIE_TD}">{p["status"]}</td><td style="{S_ZOMBIE_TD}">{dot}{p["health"] or DASH}</td>'
            html += f'<td style="{S_ZOMBIE_TD}">{te}</td><td style="{S_ZOMBIE_TD_NUM}">{task_str}</td><td style="{S_ZOMBIE_TD_NUM}">{overdue_str}</td>'
            html += f'<td style="{S_ZOMBIE_TD_NUM}">{notes_str}</td></tr>'
        html += '</table></div>'

    html += f'''<hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0;">
<p style="{S_MUTED}">Auto-generated from Rocketlane API \u00b7 {NOW.strftime("%H:%M")} \u00b7 Forensics Briefing</p>
</body></html>'''

    return html


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════════════════════════════
def send_email(subject, html_body):
    all_recipients = [GMAIL_ADDRESS] + EXTRA_RECIPIENTS
    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ", ".join(all_recipients)
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
    parser = argparse.ArgumentParser(description="Forensics Daily Briefing")
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
    print(f"Forensics Daily Briefing {DASH} mode: {args.mode}")
    print("=" * 60)

    print("Fetching all Rocketlane projects...")
    projects = fetch_all_projects()
    print(f"Fetched {len(projects)} projects.")

    print("Filtering forensics projects, enriching, and scoring...")
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
                print(f"SUCCESS {DASH} card posted to Google Chat.")
            else:
                print(f"WARNING {DASH} HTTP {status}: {body[:200]}")
        except urllib.error.HTTPError as e:
            print(f"ERROR {DASH} HTTP {e.code}: {e.read().decode()[:200]}")
            if not need_email:
                sys.exit(1)
        except Exception as e:
            print(f"ERROR {DASH} {e}")
            if not need_email:
                sys.exit(1)

    if need_email:
        print("\n--- Building HTML email report ---")
        html = build_email_html(data)
        print(f"HTML built ({len(html)} bytes)")
        subject = f"Forensics Daily Briefing {DASH} {NOW.strftime('%b %d, %Y')}"
        send_email(subject, html)
        print(f"SUCCESS {DASH} email sent.")

    print("\nDone.")


if __name__ == "__main__":
    main()
