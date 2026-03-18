#!/usr/bin/env python3
"""
PS Operations Daily Intelligence Digest
Comprehensive operational digest covering new projects, health changes,
PM-published updates, burn rate anomalies, and weekly narratives.

Usage:
  python daily_digest.py                              # Default: email mode
  python daily_digest.py --mode email                 # Email only
  python daily_digest.py --dry-run                    # Preview without sending
  python daily_digest.py --force-weekly               # Include weekly narrative

Env vars:
  ROCKETLANE_API_KEY    — Rocketlane API key (required)
  GMAIL_ADDRESS         — Gmail sender address (required)
  GMAIL_APP_PASSWORD    — Gmail app password (required)
"""

import argparse
import json
import os
import re
import smtplib
import sys
import time
import threading
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("ROCKETLANE_API_KEY", "")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
BASE_URL = "https://services.api.exterro.com/api/1.0"
RL_APP_BASE = "https://services.exterro.com/projects"

NOW = datetime.now()
DASH = "—"
ACCENT_COLOR = "#0f766e"  # teal
RED_HEALTH = "#ef4444"
YELLOW_HEALTH = "#f59e0b"
GREEN_HEALTH = "#22c55e"
MUTED_COLOR = "#64748b"

DIRECTORS = {
    393610: "eDiscovery",
    393604: "Data PSG",
    393607: "Post Implementation",
}
DIRECTOR_NAMES = {
    "eDiscovery": "Vanessa Graham",
    "Data PSG": "Maggie Ledbetter",
    "Post Implementation": "Oronde Ward",
}
ACTIVE_STATUS_VALUES = {2, 4, 5, 6, 9, 12, 14, 15}
EXTRA_RECIPIENTS = ["matt.abadie@exterro.com"]

SNAPSHOT_DIR = Path(__file__).parent / ".snapshots"

# Email styles (inline only)
S_BODY = "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:900px;margin:0 auto;color:#1a1a1a;font-size:13px;line-height:1.6;background:#ffffff;"
S_HEADER = f"background:linear-gradient(135deg,{ACCENT_COLOR},#0d9488);color:white;padding:24px;border-radius:8px 8px 0 0;margin-bottom:20px;"
S_HEADER_TITLE = "font-size:20px;font-weight:700;margin:0 0 4px 0;"
S_HEADER_SUBTITLE = "font-size:12px;color:rgba(255,255,255,0.85);margin:0;"
S_SECTION = f"background:#f8fafc;border-left:4px solid {ACCENT_COLOR};padding:16px;margin:16px 0;border-radius:0 4px 4px 0;"
S_SECTION_TITLE = "font-size:14px;font-weight:700;margin:0 0 12px 0;display:flex;align-items:center;gap:8px;"
S_BADGE = f"background:{ACCENT_COLOR};color:white;font-size:11px;font-weight:700;padding:2px 8px;border-radius:3px;margin-left:auto;"
S_ITEM = "padding:8px 0;border-bottom:1px solid #e2e8f0;"
S_ITEM_LAST = "padding:8px 0;"
S_LABEL = "font-weight:600;color:#334155;"
S_LINK = f"color:{ACCENT_COLOR};text-decoration:none;font-weight:600;"
S_MUTED = f"color:{MUTED_COLOR};font-size:11px;"
S_CHANGE = "background:#f0fdf4;padding:8px;border-radius:4px;margin:4px 0;border-left:3px solid #22c55e;font-size:12px;"
S_PROBLEM = "background:#fef2f2;padding:8px;border-radius:4px;margin:4px 0;border-left:3px solid #ef4444;font-size:12px;"
S_WARNING = "background:#fffbeb;padding:8px;border-radius:4px;margin:4px 0;border-left:3px solid #f59e0b;font-size:12px;"
S_FOOTER = f"text-align:center;color:{MUTED_COLOR};font-size:10px;padding-top:20px;border-top:1px solid #e2e8f0;margin-top:20px;"
S_TABLE = "width:100%;border-collapse:collapse;margin:8px 0;font-size:12px;"
S_TH = f"padding:8px;text-align:left;background:#f1f5f9;font-weight:700;border-bottom:2px solid {ACCENT_COLOR};color:#334155;"
S_TD = "padding:8px;border-bottom:1px solid #e2e8f0;"
S_TD_NUM = "padding:8px;border-bottom:1px solid #e2e8f0;text-align:right;"


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP + DATA FETCHING (rate limit: 60 GET requests/min)
# ═══════════════════════════════════════════════════════════════════════════════
_rate_lock = threading.Lock()
_request_times = []  # timestamps of recent requests
RATE_LIMIT = 55  # stay under 60/min with margin
RATE_WINDOW = 60  # seconds


def _rate_wait():
    """Block until we're under the rate limit."""
    with _rate_lock:
        now = time.time()
        # Purge old timestamps
        _request_times[:] = [t for t in _request_times if now - t < RATE_WINDOW]
        if len(_request_times) >= RATE_LIMIT:
            sleep_until = _request_times[0] + RATE_WINDOW
            wait = sleep_until - now + 0.1
            if wait > 0:
                time.sleep(wait)
            _request_times[:] = [t for t in _request_times if time.time() - t < RATE_WINDOW]
        _request_times.append(time.time())


def api_get(path, retries=3):
    """Fetch from Rocketlane API with rate limiting and exponential backoff on 429s."""
    url = f"{BASE_URL}/{path}"
    req = urllib.request.Request(url, headers={"api-key": API_KEY, "accept": "application/json"})
    for attempt in range(retries):
        _rate_wait()
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                # Use X-Retry-After header if available, else exponential backoff
                retry_after = e.headers.get("X-Retry-After")
                if retry_after:
                    try:
                        wait_until = int(retry_after) / 1000  # epoch millis → seconds
                        wait = max(0, wait_until - time.time()) + 0.5
                    except ValueError:
                        wait = 2 ** (attempt + 1)
                else:
                    wait = 2 ** (attempt + 1)
                print(f"    429 on {path[:60]}... retry in {wait:.1f}s")
                time.sleep(wait)
            else:
                raise


def fetch_all_projects():
    """Fetch all projects with pagination."""
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


def fetch_project_updates(created_after_ms):
    """Fetch PM-published project updates after given timestamp (epoch millis)."""
    updates, page_token = [], None
    while True:
        url = f"project-updates?createdAt.gt={created_after_ms}"
        if page_token:
            url += f"&pageToken={page_token}"
        resp = api_get(url)
        updates.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            page_token = pag["nextPageToken"]
        else:
            break
    return updates


def fetch_time_entries_for_project(pid, date_str=None):
    """Fetch time entries for a project, optionally filtered to a specific date."""
    entries, page_token = [], None
    while True:
        url = f"time-entries?projectId.eq={pid}"
        if date_str:
            url += f"&date.eq={date_str}"
        if page_token:
            url += f"&pageToken={page_token}"
        resp = api_get(url)
        entries.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            page_token = pag["nextPageToken"]
        else:
            break
    return entries


# ═══════════════════════════════════════════════════════════════════════════════
# FIELD HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_field(project, label):
    """Extract field value by label."""
    for f in project.get("fields", []):
        if f.get("fieldLabel") == label:
            return f.get("fieldValueLabel", f.get("fieldValue", ""))
    return None


def strip_html(text):
    """Remove HTML tags from text."""
    return re.sub(r'<[^>]+>', '', str(text)).strip() if text else ""


# ═══════════════════════════════════════════════════════════════════════════════
# TEAM ASSIGNMENT
# ═══════════════════════════════════════════════════════════════════════════════
def assign_to_teams(projects):
    """Assign projects to director teams."""
    team_projects = defaultdict(list)
    for p in projects:
        member_ids = {m.get("userId") for m in p.get("teamMembers", {}).get("members", [])}
        owner_id = p.get("owner", {}).get("userId")
        for did, team_name in DIRECTORS.items():
            if did in member_ids or did == owner_id:
                team_projects[team_name].append(p)
                break
    return team_projects


# ═══════════════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════════
def parse_latest_note_date(text):
    """Try to extract the most recent date stamp from PM notes text.
    PMs typically write dates like '3/12/26', '03/12/2026', '3/12/2026'."""
    if not text:
        return None
    # Match patterns like M/D/YY, M/D/YYYY, MM/DD/YY, MM/DD/YYYY
    dates = re.findall(r'(\d{1,2}/\d{1,2}/(?:\d{2}|\d{4}))', text)
    parsed = []
    for d in dates:
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                parsed.append(datetime.strptime(d, fmt))
                break
            except ValueError:
                continue
    return max(parsed) if parsed else None


# Escalation keyword patterns in PM notes
ESCALATION_KEYWORDS = re.compile(
    r'block|waiting on|no response|escalat|delayed|risk|slipp|behind|'
    r'concern|stuck|unresponsive|missed|overdue|hold.?up|stall',
    re.IGNORECASE,
)


def enrich_project(p):
    """Extract key fields from project data."""
    sv = p.get("status", {}).get("value")
    sl = p.get("status", {}).get("label", "Unknown")
    owner = p.get("owner", {})
    owner_name = f'{owner.get("firstName","")} {owner.get("lastName","")}'.strip()
    customer = p.get("customer", {}).get("companyName", "N/A")
    health = get_field(p, "Red/Yellow/Green Health") or ""
    health_notes = strip_html(get_field(p, "Internal Project Health Notes") or "")
    weekly_status = strip_html(get_field(p, "Internal Weekly Status") or "")
    project_type = get_field(p, "Project Type") or ""
    project_id = p.get("projectId", "")
    created_at = p.get("createdAt", 0)
    updated_at = p.get("updatedAt", 0)

    # New fields for deep analysis
    sub_type = get_field(p, "eDisc: Project Sub-Type") or ""
    client_segment = get_field(p, "Client Segmentation") or ""
    contract_value = 0.0
    try:
        contract_value = float(get_field(p, "Opp: Total Contract Value") or 0)
    except (ValueError, TypeError):
        pass
    ps_net_price = 0.0
    try:
        ps_net_price = float(get_field(p, "PSR: Total PS Net Price") or 0)
    except (ValueError, TypeError):
        pass

    # Parse latest date from notes for staleness detection
    combined_notes = f"{health_notes} {weekly_status}"
    latest_note_date = parse_latest_note_date(combined_notes)

    # Detect escalation keywords
    escalation_flags = []
    if ESCALATION_KEYWORDS.search(health_notes):
        escalation_flags.append("health_notes")
    if ESCALATION_KEYWORDS.search(weekly_status):
        escalation_flags.append("weekly_status")

    return {
        "id": project_id,
        "name": p.get("projectName", "?"),
        "status_val": sv,
        "status": sl,
        "owner": owner_name,
        "customer": customer,
        "health": health.strip().lower() if health else "",
        "health_notes": health_notes[:500],
        "weekly_status": weekly_status[:500],
        "project_type": project_type,
        "created_at": created_at,
        "updated_at": updated_at,
        "sub_type": sub_type,
        "client_segment": client_segment,
        "contract_value": contract_value,
        "ps_net_price": ps_net_price,
        "latest_note_date": latest_note_date,
        "escalation_flags": escalation_flags,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SNAPSHOT MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════
def load_snapshot():
    """Load previous snapshot if it exists."""
    snapshot_file = SNAPSHOT_DIR / "project_state.json"
    if not snapshot_file.exists():
        return {}
    try:
        with open(snapshot_file) as f:
            return json.load(f)
    except Exception:
        return {}


def save_snapshot(projects_by_id):
    """Save current project state for diffing on next run."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    snapshot = {}
    for pid, p in projects_by_id.items():
        snapshot[pid] = {
            "id": p["id"],
            "name": p["name"],
            "health": p["health"],
            "status": p["status"],
            "health_notes": p["health_notes"],
            "weekly_status": p["weekly_status"],
            "updated_at": p["updated_at"],
        }
    snapshot_file = SNAPSHOT_DIR / "project_state.json"
    with open(snapshot_file, "w") as f:
        json.dump(snapshot, f, indent=2)


def detect_changes(old_snapshot, new_projects_by_id):
    """Diff old snapshot against current state. Returns list of changes."""
    changes = []
    for pid, new_p in new_projects_by_id.items():
        if pid not in old_snapshot:
            continue  # skip new projects here
        old_p = old_snapshot[pid]

        # Health color change
        if old_p.get("health") != new_p["health"]:
            changes.append({
                "type": "health_change",
                "project": new_p["name"],
                "pid": pid,
                "from": old_p.get("health", "none"),
                "to": new_p["health"],
                "customer": new_p["customer"],
                "pm": new_p["owner"],
            })

        # Status change
        if old_p.get("status") != new_p["status"]:
            changes.append({
                "type": "status_change",
                "project": new_p["name"],
                "pid": pid,
                "from": old_p.get("status", ""),
                "to": new_p["status"],
                "customer": new_p["customer"],
                "pm": new_p["owner"],
            })

        # Health notes change
        if old_p.get("health_notes", "") != new_p["health_notes"] and new_p["health_notes"]:
            if not old_p.get("health_notes"):
                changes.append({
                    "type": "health_notes_new",
                    "project": new_p["name"],
                    "pid": pid,
                    "value": new_p["health_notes"],
                    "customer": new_p["customer"],
                    "pm": new_p["owner"],
                })
            else:
                changes.append({
                    "type": "health_notes_update",
                    "project": new_p["name"],
                    "pid": pid,
                    "from": old_p["health_notes"],
                    "to": new_p["health_notes"],
                    "customer": new_p["customer"],
                    "pm": new_p["owner"],
                })

        # Weekly status change
        if old_p.get("weekly_status", "") != new_p["weekly_status"] and new_p["weekly_status"]:
            if not old_p.get("weekly_status"):
                changes.append({
                    "type": "weekly_status_new",
                    "project": new_p["name"],
                    "pid": pid,
                    "value": new_p["weekly_status"],
                    "customer": new_p["customer"],
                    "pm": new_p["owner"],
                })
            else:
                changes.append({
                    "type": "weekly_status_update",
                    "project": new_p["name"],
                    "pid": pid,
                    "from": old_p["weekly_status"],
                    "to": new_p["weekly_status"],
                    "customer": new_p["customer"],
                    "pm": new_p["owner"],
                })

    return changes


# ═══════════════════════════════════════════════════════════════════════════════
# STALE PROJECT DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
# Statuses where zero time entries is expected (not truly "stale")
IDLE_STATUS_VALUES = {5, 9, 4}  # New, On Hold, Blocked


def find_stale_projects(active_projects):
    """Find in-flight Implementation projects with ZERO time entries in last 7 days.

    Filters:
    - Implementation projects only (excludes Subscription, Internal, PreSale)
    - Excludes New, On Hold, Blocked statuses (zero time is expected)
    Uses a single date-range query (date.ge + date.le) to fetch all recent
    time entries, then compares against the active project set.
    """
    today = datetime.now().date()
    seven_days_ago = today - timedelta(days=7)

    # Build set of active Implementation project IDs, excluding idle statuses
    projects_by_id = {
        p["id"]: p for p in active_projects
        if p["status_val"] in ACTIVE_STATUS_VALUES
        and p["status_val"] not in IDLE_STATUS_VALUES
        and p.get("project_type", "").lower() == "implementation"
    }
    if not projects_by_id:
        return []

    # Fetch all time entries for the last 7 days
    print(f"  Fetching 7-day time entries to detect stale projects ({len(projects_by_id)} active)...")
    entries, page_token = [], None
    while True:
        url = f"time-entries?date.ge={seven_days_ago.isoformat()}&date.le={today.isoformat()}"
        if page_token:
            url += f"&pageToken={page_token}"
        try:
            resp = api_get(url, retries=4)
        except Exception as e:
            print(f"    WARN: time-entries fetch error: {e}")
            return []
        entries.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            page_token = pag["nextPageToken"]
        else:
            break
    print(f"  Retrieved {len(entries)} time entries for last 7 days.")

    # Collect project IDs that have recent time entries
    active_pids = set()
    for e in entries:
        pid = e.get("project", {}).get("projectId", "")
        if pid:
            active_pids.add(pid)

    # Stale = active project with no time entries in last 7 days
    stale = []
    for pid, p in projects_by_id.items():
        if pid not in active_pids:
            stale.append(p)

    return sorted(stale, key=lambda x: x["customer"])


# ═══════════════════════════════════════════════════════════════════════════════
# BUILD EMAIL SECTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def build_section(title, count, html_content):
    """Build a styled section with title and count badge."""
    if not html_content:
        return ""
    return f'''<div style="{S_SECTION}">
<div style="{S_SECTION_TITLE}">{title}<span style="{S_BADGE}">{count}</span></div>
{html_content}
</div>'''


def build_new_projects_section(new_projects):
    """Build section for projects created in the last 24h."""
    if not new_projects:
        return ""

    html = ""
    for p in sorted(new_projects, key=lambda x: -x["created_at"]):
        link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
        type_str = f' • <span style="{S_MUTED}">{p["project_type"]}</span>' if p["project_type"] else ""
        status_str = f' • {p["status"]}' if p["status"] else ""
        html += f'''<div style="{S_ITEM}">
<div><strong>{link}</strong>{type_str}</div>
<div style="{S_MUTED}">{p["customer"]} • PM: {p["owner"]}{status_str}</div>
</div>'''

    return build_section("NEW PROJECTS (24h)", len(new_projects), html)


def build_health_changes_section(changes):
    """Build section for health and status changes."""
    if not changes:
        return ""

    # Group by type
    health_changes = [c for c in changes if c["type"] == "health_change"]
    status_changes = [c for c in changes if c["type"] == "status_change"]
    notes_new = [c for c in changes if c["type"] == "health_notes_new"]
    notes_update = [c for c in changes if c["type"] == "health_notes_update"]
    weekly_new = [c for c in changes if c["type"] == "weekly_status_new"]
    weekly_update = [c for c in changes if c["type"] == "weekly_status_update"]

    html = ""

    # Health changes
    if health_changes:
        html += '<h4 style="font-weight:700;margin:8px 0 4px 0;font-size:12px;">Health Color Changes</h4>'
        for c in health_changes:
            link = f'<a href="{RL_APP_BASE}/{c["pid"]}" style="{S_LINK}">{c["project"]}</a>'
            health_badge_from = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{"#ef4444" if c["from"]=="red" else "#f59e0b" if c["from"]=="yellow" else "#22c55e"};margin-right:4px;vertical-align:middle;"></span>'
            health_badge_to = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{"#ef4444" if c["to"]=="red" else "#f59e0b" if c["to"]=="yellow" else "#22c55e"};margin-right:4px;vertical-align:middle;"></span>'
            html += f'<div style="{S_CHANGE}">{link} {health_badge_from}{c["from"]} {DASH} {health_badge_to}{c["to"]}</div>'

    # Status changes
    if status_changes:
        html += '<h4 style="font-weight:700;margin:8px 0 4px 0;font-size:12px;">Status Changes</h4>'
        for c in status_changes:
            link = f'<a href="{RL_APP_BASE}/{c["pid"]}" style="{S_LINK}">{c["project"]}</a>'
            html += f'<div style="{S_CHANGE}">{link} {c["from"]} {DASH} <strong>{c["to"]}</strong></div>'

    # New health notes
    if notes_new:
        html += '<h4 style="font-weight:700;margin:8px 0 4px 0;font-size:12px;">New Health Notes</h4>'
        for c in notes_new:
            link = f'<a href="{RL_APP_BASE}/{c["pid"]}" style="{S_LINK}">{c["project"]}</a>'
            html += f'<div style="{S_CHANGE}"><strong>{link}</strong><div style="margin:4px 0 0 0;font-size:11px;">{c["value"]}</div></div>'

    # Updated health notes
    if notes_update:
        html += '<h4 style="font-weight:700;margin:8px 0 4px 0;font-size:12px;">Updated Health Notes</h4>'
        for c in notes_update:
            link = f'<a href="{RL_APP_BASE}/{c["pid"]}" style="{S_LINK}">{c["project"]}</a>'
            html += f'<div style="{S_CHANGE}"><strong>{link}</strong><div style="margin:4px 0 0 0;font-size:11px;">{c["to"]}</div></div>'

    # New weekly status
    if weekly_new:
        html += '<h4 style="font-weight:700;margin:8px 0 4px 0;font-size:12px;">New Weekly Status</h4>'
        for c in weekly_new:
            link = f'<a href="{RL_APP_BASE}/{c["pid"]}" style="{S_LINK}">{c["project"]}</a>'
            html += f'<div style="{S_CHANGE}"><strong>{link}</strong><div style="margin:4px 0 0 0;font-size:11px;">{c["value"]}</div></div>'

    # Updated weekly status
    if weekly_update:
        html += '<h4 style="font-weight:700;margin:8px 0 4px 0;font-size:12px;">Updated Weekly Status</h4>'
        for c in weekly_update:
            link = f'<a href="{RL_APP_BASE}/{c["pid"]}" style="{S_LINK}">{c["project"]}</a>'
            html += f'<div style="{S_CHANGE}"><strong>{link}</strong><div style="margin:4px 0 0 0;font-size:11px;">{c["to"]}</div></div>'

    return build_section("HEALTH & STATUS CHANGES", len(changes), html)


def build_pm_updates_section(updates, projects_by_id):
    """Build section for PM-published updates."""
    if not updates:
        return ""

    html = ""
    for u in sorted(updates, key=lambda x: -x.get("createdAt", 0)):
        project_id = u.get("project", {}).get("projectId", "")
        p = projects_by_id.get(project_id, {})

        title = u.get("title", "")
        pm_name = u.get("createdBy", {}).get("displayName", "Unknown")
        status_val = u.get("statusValue", "")
        created_at = u.get("createdAt", 0)

        # Convert epoch ms to readable date
        try:
            created_date = datetime.fromtimestamp(created_at / 1000).strftime("%b %d, %I:%M %p")
        except Exception:
            created_date = "N/A"

        link = f'<a href="{RL_APP_BASE}/{project_id}" style="{S_LINK}">{p.get("name", project_id)}</a>'
        status_label = f'<span style="{S_MUTED}"> • {status_val}</span>' if status_val else ""

        html += f'''<div style="{S_ITEM}">
<div><strong>{title}</strong></div>
<div style="{S_MUTED}">{link} • {p.get("customer", "N/A")} • PM: {pm_name}{status_label} • {created_date}</div>
</div>'''

    return build_section("PM-PUBLISHED UPDATES (24h)", len(updates), html)


# ═══════════════════════════════════════════════════════════════════════════════
# DEEP ANALYSIS SECTIONS
# ═══════════════════════════════════════════════════════════════════════════════
def build_attention_required_section(all_enriched):
    """Build ATTENTION REQUIRED section with 3 subsections:
    1. Escalation candidates — red/yellow + blocker language in notes
    2. High-value at risk — Pinnacle/Strategic clients with red/yellow health
    3. Stale commentary — active projects with no notes update in 14+ days
    """
    now = datetime.now()
    active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]

    # --- 1. Escalation candidates ---
    escalations = []
    for p in active:
        if p["health"] in ("red", "yellow") and p["escalation_flags"]:
            # Extract the matching keyword snippet for context
            snippet = ""
            text = p["health_notes"] if "health_notes" in p["escalation_flags"] else p["weekly_status"]
            match = ESCALATION_KEYWORDS.search(text)
            if match:
                start = max(0, match.start() - 30)
                end = min(len(text), match.end() + 80)
                snippet = ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")
            escalations.append((p, snippet))

    # Sort: red first, then by contract value desc
    escalations.sort(key=lambda x: (0 if x[0]["health"] == "red" else 1, -x[0]["contract_value"]))

    # --- 2. High-value at risk ---
    high_value = []
    for p in active:
        if p["health"] in ("red", "yellow") and p["client_segment"] in ("Pinnacle", "Strategic"):
            high_value.append(p)
        elif p["health"] in ("red", "yellow") and p["contract_value"] >= 100000:
            high_value.append(p)
    # Dedupe (some may also be in escalations)
    seen_ids = set()
    high_value_deduped = []
    for p in sorted(high_value, key=lambda x: -x["contract_value"]):
        if p["id"] not in seen_ids:
            high_value_deduped.append(p)
            seen_ids.add(p["id"])
    high_value = high_value_deduped[:10]

    # --- 3. Stale commentary ---
    stale_notes = []
    fourteen_days_ago = now - timedelta(days=14)
    for p in active:
        if p["status_val"] in IDLE_STATUS_VALUES:
            continue
        if p.get("project_type", "").lower() != "implementation":
            continue
        if p["latest_note_date"]:
            if p["latest_note_date"] < fourteen_days_ago:
                days_stale = (now - p["latest_note_date"]).days
                stale_notes.append((p, days_stale))
        elif p["health_notes"] or p["weekly_status"]:
            # Has notes but no parseable date — can't determine age
            pass
        else:
            # No notes at all on an active implementation project
            stale_notes.append((p, None))
    stale_notes.sort(key=lambda x: -(x[1] or 999))

    # --- Build HTML ---
    total_items = len(escalations) + len(high_value) + len(stale_notes)
    if total_items == 0:
        return ""

    html = ""

    # Escalation candidates
    if escalations:
        html += f'<div style="margin-bottom:16px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#dc2626;margin-bottom:6px;">Escalation Candidates ({len(escalations)})</div>'
        html += f'<div style="{S_MUTED};margin-bottom:6px;">Red/yellow health with blocker language in PM notes — may need VP intervention.</div>'
        for p, snippet in escalations[:10]:
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
            hc = p["health"]
            dot_color = "#ef4444" if hc == "red" else "#f59e0b"
            dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot_color};margin-right:4px;vertical-align:middle;"></span>'
            val_str = f' • ${p["contract_value"]:,.0f}' if p["contract_value"] else ""
            seg_str = f' • {p["client_segment"]}' if p["client_segment"] else ""
            style = S_PROBLEM if hc == "red" else S_WARNING
            html += f'<div style="{style}">'
            html += f'<div>{dot}<strong>{link}</strong> <span style="{S_MUTED}">— {p["customer"]} • {p["owner"]}{seg_str}{val_str}</span></div>'
            if snippet:
                html += f'<div style="margin:4px 0 0 12px;font-size:11px;color:#475569;font-style:italic;">"{snippet}"</div>'
            html += '</div>'
        html += '</div>'

    # High-value at risk
    if high_value:
        html += f'<div style="margin-bottom:16px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#dc2626;margin-bottom:6px;">High-Value Clients at Risk ({len(high_value)})</div>'
        html += f'<div style="{S_MUTED};margin-bottom:6px;">Pinnacle/Strategic clients or contracts $100K+ with red/yellow health.</div>'
        html += f'<table style="{S_TABLE}">'
        html += f'<tr><th style="{S_TH}">Project</th><th style="{S_TH}">Customer</th><th style="{S_TH}">Segment</th><th style="{S_TH}">Value</th><th style="{S_TH}">Health</th><th style="{S_TH}">PM</th></tr>'
        for p in high_value:
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
            hc = p["health"]
            health_badge = f'<span style="background:{"#ef4444" if hc=="red" else "#f59e0b"};color:white;padding:1px 6px;border-radius:3px;font-size:10px;">{hc.upper()}</span>'
            val_str = f'${p["contract_value"]:,.0f}' if p["contract_value"] else "—"
            html += f'<tr><td style="{S_TD}">{link}</td><td style="{S_TD}">{p["customer"]}</td>'
            html += f'<td style="{S_TD}">{p["client_segment"] or "—"}</td><td style="{S_TD_NUM}">{val_str}</td>'
            html += f'<td style="{S_TD}">{health_badge}</td><td style="{S_TD}">{p["owner"]}</td></tr>'
        html += '</table></div>'

    # Stale commentary
    if stale_notes:
        html += f'<div style="margin-bottom:8px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#b45309;margin-bottom:6px;">Stale Commentary ({len(stale_notes)})</div>'
        html += f'<div style="{S_MUTED};margin-bottom:6px;">Active implementation projects with no PM notes update in 14+ days.</div>'
        for p, days in stale_notes[:10]:
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
            age_str = f'{days} days' if days else "no notes"
            html += f'<div style="{S_WARNING}">{link} <span style="{S_MUTED}">— {p["customer"]} • {p["owner"]} • <strong>{age_str}</strong></span></div>'
        if len(stale_notes) > 10:
            html += f'<div style="{S_MUTED};font-style:italic;margin-top:4px;">+ {len(stale_notes) - 10} more</div>'
        html += '</div>'

    return build_section("ATTENTION REQUIRED", total_items, html)


def build_z2e_tracker_section(all_enriched):
    """Build Z2E Migration Tracker showing Phase 1 and Phase 2 progress.
    Phase 1 goal: complete ASAP. Phase 2 goal: complete by 12/31/26."""
    # Categorize by sub-type
    phase1 = []
    phase2 = []
    for p in all_enriched:
        st = p.get("sub_type", "").lower()
        if "z2e phase 1" in st:
            phase1.append(p)
        elif "z2e" in st and "z2e phase 1" not in st and "z2e - not started" not in st:
            phase2.append(p)

    if not phase1 and not phase2:
        return ""

    COMPLETED_STATUSES = {"Completed", "Closeout"}
    IN_PROGRESS_STATUSES = {"In progress", "Hypercare", "Partially Live"}
    BLOCKED_STATUSES = {"Blocked", "Delayed", "On Hold"}

    def status_summary(projects):
        completed = [p for p in projects if p["status"] in COMPLETED_STATUSES]
        in_progress = [p for p in projects if p["status"] in IN_PROGRESS_STATUSES]
        blocked = [p for p in projects if p["status"] in BLOCKED_STATUSES]
        other = [p for p in projects if p["status"] not in COMPLETED_STATUSES | IN_PROGRESS_STATUSES | BLOCKED_STATUSES]
        return completed, in_progress, blocked, other

    html = ""

    # Phase 1
    if phase1:
        comp, prog, blk, oth = status_summary(phase1)
        pct = int(len(comp) / len(phase1) * 100) if phase1 else 0
        bar_color = "#22c55e" if pct > 75 else "#f59e0b" if pct > 50 else "#ef4444"

        html += f'<div style="margin-bottom:16px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#334155;margin-bottom:4px;">Phase 1 (Z2E Phase 1) — Goal: Complete ASAP</div>'
        # Progress bar
        html += f'<div style="background:#e2e8f0;border-radius:4px;height:12px;margin:4px 0 8px 0;overflow:hidden;">'
        html += f'<div style="background:{bar_color};height:100%;width:{pct}%;border-radius:4px;"></div></div>'
        html += f'<div style="font-size:12px;margin-bottom:8px;">'
        html += f'<strong>{len(comp)}</strong>/{len(phase1)} complete ({pct}%) • '
        html += f'<span style="color:#0f766e;">{len(prog)} in progress</span> • '
        html += f'<span style="color:#dc2626;">{len(blk)} blocked/on hold</span>'
        if oth:
            html += f' • {len(oth)} other'
        html += '</div>'

        # Show blocked/red projects — these are the ones needing attention
        needs_attention = [p for p in blk + prog if p["health"] in ("red", "yellow")]
        if needs_attention:
            html += f'<div style="font-size:11px;font-weight:600;color:#b45309;margin:4px 0;">Needs attention:</div>'
            for p in needs_attention:
                link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
                hc = p["health"]
                dot_color = "#ef4444" if hc == "red" else "#f59e0b"
                dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot_color};margin-right:4px;vertical-align:middle;"></span>'
                html += f'<div style="font-size:11px;padding:2px 0 2px 12px;">{dot}{link} — {p["customer"]} • {p["owner"]} • {p["status"]}</div>'
        html += '</div>'

    # Phase 2
    if phase2:
        comp, prog, blk, oth = status_summary(phase2)
        pct = int(len(comp) / len(phase2) * 100) if phase2 else 0
        bar_color = "#22c55e" if pct > 50 else "#f59e0b" if pct > 25 else "#ef4444"

        # Days until deadline
        from datetime import date
        deadline = date(2026, 12, 31)
        days_left = (deadline - date.today()).days

        html += f'<div style="margin-bottom:8px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#334155;margin-bottom:4px;">Phase 2 (Z2E) — Deadline: 12/31/2026 ({days_left} days)</div>'
        # Progress bar
        html += f'<div style="background:#e2e8f0;border-radius:4px;height:12px;margin:4px 0 8px 0;overflow:hidden;">'
        html += f'<div style="background:{bar_color};height:100%;width:{pct}%;border-radius:4px;"></div></div>'
        html += f'<div style="font-size:12px;margin-bottom:8px;">'
        html += f'<strong>{len(comp)}</strong>/{len(phase2)} complete ({pct}%) • '
        html += f'<span style="color:#0f766e;">{len(prog)} in progress</span> • '
        html += f'<span style="color:#dc2626;">{len(blk)} blocked/on hold</span>'
        if oth:
            html += f' • {len(oth)} other'
        html += '</div>'

        # Show blocked/red
        needs_attention = [p for p in blk + prog if p["health"] in ("red", "yellow")]
        if needs_attention:
            html += f'<div style="font-size:11px;font-weight:600;color:#b45309;margin:4px 0;">Needs attention:</div>'
            for p in needs_attention:
                link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
                hc = p["health"]
                dot_color = "#ef4444" if hc == "red" else "#f59e0b"
                dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot_color};margin-right:4px;vertical-align:middle;"></span>'
                html += f'<div style="font-size:11px;padding:2px 0 2px 12px;">{dot}{link} — {p["customer"]} • {p["owner"]} • {p["status"]}</div>'
        html += '</div>'

    total = len(phase1) + len(phase2)
    return build_section("Z2E MIGRATION TRACKER", total, html)


def build_post_impl_watch_section(projects_by_team):
    """Build Post-Implementation Watch section for Oronde's team.
    Tracks subscription engagement and highlights projects needing attention."""
    post_impl = projects_by_team.get("Post Implementation", [])
    if not post_impl:
        return ""

    active = [p for p in post_impl if p["status_val"] in ACTIVE_STATUS_VALUES]
    subscriptions = [p for p in active if p.get("project_type", "").lower() == "subscription"]
    implementations = [p for p in active if p.get("project_type", "").lower() == "implementation"]

    html = f'<div style="font-size:12px;margin-bottom:12px;">'
    html += f'<strong>{len(active)}</strong> active projects: '
    html += f'<strong>{len(subscriptions)}</strong> subscriptions, '
    html += f'<strong>{len(implementations)}</strong> implementations'
    html += '</div>'

    # Red/yellow subscription projects
    at_risk_subs = [p for p in subscriptions if p["health"] in ("red", "yellow")]
    if at_risk_subs:
        html += f'<div style="font-weight:700;font-size:12px;color:#b45309;margin:8px 0 4px 0;">At-Risk Subscriptions ({len(at_risk_subs)})</div>'
        for p in sorted(at_risk_subs, key=lambda x: (0 if x["health"] == "red" else 1)):
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
            hc = p["health"]
            dot_color = "#ef4444" if hc == "red" else "#f59e0b"
            dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot_color};margin-right:4px;vertical-align:middle;"></span>'
            html += f'<div style="{S_WARNING}">{dot}{link} <span style="{S_MUTED}">— {p["customer"]} • {p["owner"]}</span></div>'

    # Subscriptions with no notes (low engagement signal)
    no_notes_subs = [p for p in subscriptions if not p["health_notes"] and not p["weekly_status"]]
    if no_notes_subs:
        html += f'<div style="font-weight:700;font-size:12px;color:{MUTED_COLOR};margin:12px 0 4px 0;">Subscriptions with No PM Commentary ({len(no_notes_subs)})</div>'
        for p in no_notes_subs:
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
            html += f'<div style="font-size:11px;padding:2px 0 2px 12px;color:{MUTED_COLOR};">{link} — {p["customer"]} • {p["status"]}</div>'

    return build_section("POST-IMPLEMENTATION WATCH (Oronde)", len(active), html)


def build_pm_notes_section(all_enriched):
    """Build section showing the most recently updated PM health notes and weekly status.

    Shows projects that have non-empty health notes or weekly status,
    sorted by updatedAt desc, capped to the most recent 15 projects.
    """
    # Filter to active projects with notes content
    with_notes = [
        p for p in all_enriched
        if p["status_val"] in ACTIVE_STATUS_VALUES
        and (p["health_notes"] or p["weekly_status"])
    ]
    if not with_notes:
        return ""

    # Sort by most recently updated, take top 15
    with_notes.sort(key=lambda x: -x["updated_at"])
    top = with_notes[:15]

    html = f'<div style="{S_MUTED};margin-bottom:8px;">Most recently updated projects with PM commentary.</div>'

    for p in top:
        link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'

        # Health color dot
        hc = p.get("health", "")
        dot_color = "#ef4444" if hc == "red" else "#f59e0b" if hc == "yellow" else "#22c55e" if hc == "green" else "#94a3b8"
        dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot_color};margin-right:4px;vertical-align:middle;"></span>'

        html += f'<div style="{S_ITEM}">'
        html += f'<div>{dot}<strong>{link}</strong> <span style="{S_MUTED}">— {p["customer"]} • {p["owner"]}</span></div>'

        if p["health_notes"]:
            html += f'<div style="margin:4px 0 2px 12px;font-size:11px;"><strong style="color:#334155;">Health:</strong> <span style="color:#475569;">{p["health_notes"]}</span></div>'
        if p["weekly_status"]:
            html += f'<div style="margin:2px 0 0 12px;font-size:11px;"><strong style="color:#334155;">Status:</strong> <span style="color:#475569;">{p["weekly_status"]}</span></div>'

        html += '</div>'

    if len(with_notes) > 15:
        html += f'<div style="{S_MUTED};margin-top:8px;font-style:italic;">{len(with_notes) - 15} more projects with notes not shown.</div>'

    return build_section("PM STATUS NOTES (Most Recent)", len(top), html)


def build_stale_projects_section(stale_projects):
    """Build section for active projects with no time entries in last 7 days.

    Shows a PM summary table (count per PM, sorted desc) followed by
    a compact grouped breakdown per PM with project details.
    """
    if not stale_projects:
        return ""

    # Group by PM
    by_pm = defaultdict(list)
    for p in stale_projects:
        pm = p.get("owner", "") or "Unassigned"
        by_pm[pm].append(p)

    # Sort PMs by count descending
    sorted_pms = sorted(by_pm.items(), key=lambda x: -len(x[1]))

    # PM summary table
    html = f'<div style="{S_MUTED};margin-bottom:8px;">In-flight projects (excl. New, On Hold, Blocked) with zero time logged in 7 days.</div>'
    html += f'''<table style="{S_TABLE}">
<tr><th style="{S_TH}">PM</th><th style="{S_TH}">Stale Projects</th></tr>'''
    for pm, projs in sorted_pms:
        html += f'<tr><td style="{S_TD}">{pm}</td><td style="{S_TD_NUM}"><strong>{len(projs)}</strong></td></tr>'
    html += '</table>'

    # Grouped detail per PM
    html += f'<div style="margin-top:12px;border-top:1px solid #e2e8f0;padding-top:8px;">'
    for pm, projs in sorted_pms:
        html += f'<div style="margin:8px 0;">'
        html += f'<div style="font-weight:600;font-size:12px;color:#334155;margin-bottom:4px;">{pm} ({len(projs)})</div>'
        for p in sorted(projs, key=lambda x: x["customer"]):
            link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
            html += f'<div style="font-size:11px;color:{MUTED_COLOR};padding:2px 0 2px 12px;">{link} — {p["customer"]} • {p["status"]}</div>'
        html += '</div>'
    html += '</div>'

    return build_section("STALE PROJECTS (No Time Logged — 7 Days)", len(stale_projects), html)


def build_weekly_narrative(projects_by_team, new_projects, changes, all_enriched, stale_projects):
    """Build a written narrative summary for Friday weekly reports."""
    # Count projects by team
    team_counts = {team: len(projs) for team, projs in projects_by_team.items()}

    # Health summary
    health_summary = {}
    for team, projs in projects_by_team.items():
        red = sum(1 for p in projs if p["health"] == "red" and p["status_val"] in ACTIVE_STATUS_VALUES)
        yellow = sum(1 for p in projs if p["health"] == "yellow" and p["status_val"] in ACTIVE_STATUS_VALUES)
        green = sum(1 for p in projs if p["health"] == "green" and p["status_val"] in ACTIVE_STATUS_VALUES)
        unknown = sum(1 for p in projs if not p["health"] and p["status_val"] in ACTIVE_STATUS_VALUES)
        health_summary[team] = {"red": red, "yellow": yellow, "green": green, "unknown": unknown}

    # Narrative text
    narrative = f'''<div style="{S_SECTION}">
<h3 style="margin:0 0 12px 0;font-size:14px;font-weight:700;">Weekly Executive Summary</h3>

<p style="margin:8px 0;line-height:1.6;">
This week across our PS operations, we onboarded <strong>{len(new_projects)} new projects</strong> across
the three service teams (eDiscovery, Data PSG, and Post Implementation).
The portfolio remains active with <strong>{sum(team_counts.values())} total projects</strong> under active management.
</p>

<p style="margin:8px 0;line-height:1.6;">
<strong>Health Snapshot:</strong>
'''

    for team in ["eDiscovery", "Data PSG", "Post Implementation"]:
        if team in health_summary:
            h = health_summary[team]
            narrative += f'''<br/>{DIRECTOR_NAMES[team]} ({team}):
<span style="background:#ef4444;color:white;padding:1px 6px;border-radius:3px;font-size:11px;margin-right:4px;">{h["red"]} Red</span>
<span style="background:#f59e0b;color:white;padding:1px 6px;border-radius:3px;font-size:11px;margin-right:4px;">{h["yellow"]} Yellow</span>
<span style="background:#22c55e;color:white;padding:1px 6px;border-radius:3px;font-size:11px;">{h["green"]} Green</span>'''

    narrative += '</p>'

    # Status changes
    if changes:
        change_count = len([c for c in changes if c["type"] in ("health_change", "status_change")])
        narrative += f'<p style="margin:8px 0;line-height:1.6;"><strong>Changes This Week:</strong> We detected <strong>{change_count} health or status transitions</strong> requiring attention or follow-up.</p>'

    # PM notes coverage
    with_notes = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES and (p["health_notes"] or p["weekly_status"])]
    if with_notes:
        total_active = sum(1 for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES)
        narrative += f'<p style="margin:8px 0;line-height:1.6;"><strong>PM Commentary:</strong> <strong>{len(with_notes)} of {total_active}</strong> active projects have health notes or weekly status updates populated.</p>'

    # Stale projects
    if stale_projects:
        narrative += f'<p style="margin:8px 0;line-height:1.6;"><strong>Stale Projects:</strong> <strong>{len(stale_projects)} active projects</strong> had zero time logged in the past 7 days — may need PM follow-up to confirm status or identify blockers.</p>'

    narrative += '''<p style="margin:8px 0;line-height:1.6;">
The digest below provides detailed breakdown by section. Escalations and red flags
are highlighted for immediate intervention where needed.
</p>
</div>'''

    return narrative


def build_email_html(digest_data, is_weekly=False):
    """Build complete HTML email."""
    today_str = digest_data["today_str"]
    new_projects = digest_data["new_projects"]
    changes = digest_data["changes"]
    stale_projects = digest_data["stale_projects"]
    all_enriched = digest_data["all_enriched"]
    projects_by_team = digest_data["projects_by_team"]
    projects_by_id = digest_data["projects_by_id"]
    has_prior_snapshot = digest_data["has_prior_snapshot"]

    # Build sections
    header = f'''<div style="{S_HEADER}">
<div style="{S_HEADER_TITLE}">PS Operations Daily Intelligence</div>
<div style="{S_HEADER_SUBTITLE}">{today_str} • eDiscovery | Data PSG | Post Implementation</div>
</div>'''

    # KPI row
    total_active = sum(1 for p in projects_by_id.values() if p["status_val"] in ACTIVE_STATUS_VALUES)
    red_health = sum(1 for p in projects_by_id.values() if p["health"] == "red" and p["status_val"] in ACTIVE_STATUS_VALUES)
    yellow_health = sum(1 for p in projects_by_id.values() if p["health"] == "yellow" and p["status_val"] in ACTIVE_STATUS_VALUES)

    kpi_html = f'''<div style="display:flex;gap:12px;margin:16px 0;flex-wrap:wrap;">
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:12px;flex:1;min-width:120px;">
<div style="font-size:20px;font-weight:700;color:{ACCENT_COLOR};">{total_active}</div>
<div style="font-size:11px;color:{MUTED_COLOR};text-transform:uppercase;">Active Projects</div>
</div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:12px;flex:1;min-width:120px;">
<div style="font-size:20px;font-weight:700;color:#ef4444;">{red_health}</div>
<div style="font-size:11px;color:{MUTED_COLOR};text-transform:uppercase;">Red Health</div>
</div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:12px;flex:1;min-width:120px;">
<div style="font-size:20px;font-weight:700;color:#f59e0b;">{yellow_health}</div>
<div style="font-size:11px;color:{MUTED_COLOR};text-transform:uppercase;">Yellow Health</div>
</div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:12px;flex:1;min-width:120px;">
<div style="font-size:20px;font-weight:700;color:{ACCENT_COLOR};">{len(new_projects)}</div>
<div style="font-size:11px;color:{MUTED_COLOR};text-transform:uppercase;">New (24h)</div>
</div>
</div>'''

    # Build sections — ordered by priority
    sections = []

    # Weekly narrative first on Fridays
    if is_weekly:
        sections.append(build_weekly_narrative(projects_by_team, new_projects, changes, all_enriched, stale_projects))

    # Attention required — escalations, high-value at risk, stale notes
    sections.append(build_attention_required_section(all_enriched))

    # Z2E migration progress
    sections.append(build_z2e_tracker_section(all_enriched))

    # Health changes (only if we have prior snapshot)
    if has_prior_snapshot:
        sections.append(build_health_changes_section(changes))
    else:
        sections.append(f'<div style="{S_SECTION}"><div style="{S_MUTED}"><em>Health change detection available from next run (snapshot baseline being established today).</em></div></div>')

    # Post-Implementation watch (Oronde)
    sections.append(build_post_impl_watch_section(projects_by_team))

    # New projects
    sections.append(build_new_projects_section(new_projects))

    # PM status notes
    sections.append(build_pm_notes_section(all_enriched))

    # Stale implementation projects
    sections.append(build_stale_projects_section(stale_projects))

    sections_html = "\n".join([s for s in sections if s])

    # Footer
    footer = f'''<div style="{S_FOOTER}">
Generated {NOW.strftime("%Y-%m-%d %H:%M:%S")} UTC<br/>
Rocketlane PS Operations Dashboard
</div>'''

    html = f'''<html><head><meta charset="utf-8"></head>
<body style="{S_BODY}">
{header}
{kpi_html}
{sections_html}
{footer}
</body></html>'''

    return html


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL SENDING
# ═══════════════════════════════════════════════════════════════════════════════
def send_email(subject, html_body, dry_run=False):
    """Send HTML email via Gmail."""
    all_recipients = [GMAIL_ADDRESS] + EXTRA_RECIPIENTS

    if dry_run:
        print(f"\n[DRY RUN] Email subject: {subject}")
        print(f"[DRY RUN] To: {', '.join(all_recipients)}")
        print(f"[DRY RUN] Body length: {len(html_body)} chars")
        return

    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ", ".join(all_recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    print("Connecting to Gmail SMTP...")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)
    print(f"Email sent to: {', '.join(all_recipients)}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="PS Operations Daily Intelligence Digest")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending email")
    parser.add_argument("--mode", choices=["email"], default="email", help="Output mode (email only)")
    parser.add_argument("--force-weekly", action="store_true", help="Include weekly narrative regardless of day")
    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: ROCKETLANE_API_KEY not set")
        sys.exit(1)
    if not args.dry_run and (not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD):
        print("ERROR: GMAIL_ADDRESS and GMAIL_APP_PASSWORD required for email mode")
        sys.exit(1)

    print("=" * 70)
    print("PS Operations Daily Intelligence Digest")
    print("=" * 70)

    # Fetch projects
    print("\nFetching all Rocketlane projects...")
    all_projects = fetch_all_projects()
    print(f"Fetched {len(all_projects)} projects.")

    # Assign to teams and enrich
    print("Assigning to teams and enriching...")
    raw_by_team = assign_to_teams(all_projects)
    projects_by_id = {}
    all_enriched = []
    projects_by_team = defaultdict(list)  # enriched version

    for team, projs in raw_by_team.items():
        for p in projs:
            enriched = enrich_project(p)
            projects_by_id[enriched["id"]] = enriched
            all_enriched.append(enriched)
            projects_by_team[team].append(enriched)

    print(f"Enriched {len(all_enriched)} projects across {len(projects_by_team)} teams.")

    # Find new projects (created in last 24h)
    print("Identifying new projects...")
    cutoff_ts = int((NOW - timedelta(hours=24)).timestamp() * 1000)
    new_projects = [p for p in all_enriched if p["created_at"] > cutoff_ts]
    print(f"Found {len(new_projects)} new projects in last 24h.")

    # Load snapshot and detect changes
    print("Loading previous snapshot and detecting changes...")
    old_snapshot = load_snapshot()
    changes = detect_changes(old_snapshot, projects_by_id) if old_snapshot else []
    has_prior_snapshot = bool(old_snapshot)
    print(f"Found {len(changes)} health/status changes (prior snapshot: {has_prior_snapshot}).")

    # Find stale projects (active but no time logged in 7 days)
    print("Detecting stale projects...")
    stale_projects = find_stale_projects(all_enriched)
    print(f"Found {len(stale_projects)} stale projects (no time entries in 7 days).")

    # Check if today is Friday or --force-weekly
    is_friday = NOW.weekday() == 4
    is_weekly = is_friday or args.force_weekly

    # Build digest data
    digest_data = {
        "today_str": NOW.strftime("%A, %B %d, %Y"),
        "new_projects": new_projects,
        "changes": changes,
        "stale_projects": stale_projects,
        "all_enriched": all_enriched,
        "projects_by_team": projects_by_team,
        "projects_by_id": projects_by_id,
        "has_prior_snapshot": has_prior_snapshot,
    }

    # Build email
    print("\nBuilding email...")
    html = build_email_html(digest_data, is_weekly=is_weekly)

    # Send email
    subject = f"PS Operations Daily Intelligence — {NOW.strftime('%b %d, %Y')}"
    if is_weekly:
        subject = f"[WEEKLY] {subject}"

    print(f"Subject: {subject}")
    send_email(subject, html, dry_run=args.dry_run)

    # Save snapshot
    print("Saving project state snapshot...")
    save_snapshot(projects_by_id)
    print("Done.")


if __name__ == "__main__":
    main()
