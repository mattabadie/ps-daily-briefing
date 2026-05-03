#!/usr/bin/env python3
"""
PS Operations Daily Intelligence Digest
Comprehensive operational digest covering new projects, health changes,
PM-published updates, burn rate anomalies, and weekly narratives.

Usage:
  python daily_digest.py                              # Default: email mode
  python daily_digest.py --mode email                 # Email only
  python daily_digest.py --dry-run                    # Preview without sending
  python daily_digest.py --scope forensics             # Forensics-only version

Env vars:
  ROCKETLANE_API_KEY    — Rocketlane API key (required)
  GMAIL_ADDRESS         — Gmail sender address (required)
  GMAIL_APP_PASSWORD    — Gmail app password (required)
"""

import argparse
import html as html_mod
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
from datetime import date, datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

try:
    from weasyprint import HTML as WeasyHTML
    HAS_WEASYPRINT = True
except ImportError:
    HAS_WEASYPRINT = False

from candidate_selection import build_candidate_lists

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("ROCKETLANE_API_KEY", "")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
try:
    from claude_utils import call_claude
except ImportError:
    call_claude = None
BASE_URL = "https://services.api.exterro.com/api/1.0"
RL_APP_BASE = "https://services.exterro.com/projects"

NOW = datetime.now()
DASH = "—"
ACCENT_COLOR = "#0f766e"  # teal
RED_HEALTH = "#ef4444"
YELLOW_HEALTH = "#f59e0b"
GREEN_HEALTH = "#22c55e"
MUTED_COLOR = "#64748b"

# ── Scope configurations ──
SCOPE_CONFIG = {
    "ps": {
        "label": "eDiscovery & Privacy",
        "vp": "Matt Abadie",
        "directors": {
            393610: "eDiscovery",
            393604: "Data PSG",
            393607: "Post Implementation",
        },
        "director_names": {
            "eDiscovery": "Vanessa Graham",
            "Data PSG": "Maggie Ledbetter",
            "Post Implementation": "Oronde Ward",
        },
        "extra_recipients": ["matt.abadie@exterro.com"],
        "email_subject_prefix": "eDiscovery & Privacy Daily Intelligence",
        "email_subtitle": "eDiscovery | Data PSG | Post Implementation",
        "snapshot_suffix": "",
    },
    "forensics": {
        "label": "Forensics",
        "vp": "Sarah Hargreaves",
        "directors": {
            393598: "Forensics Impl",
            650747: "Forensics Post-Impl",
            393608: "Forensics Leadership",  # Sarah — VP, included for project filtering
        },
        "director_names": {
            "Forensics Impl": "Ewelina Gramala",
            "Forensics Post-Impl": "Jon Cook",
            # "Forensics Leadership" intentionally excluded — Sarah is the VP/audience, not a director
        },
        "extra_recipients": ["matt.abadie@exterro.com"],  # TODO: add sarah.hargreaves@exterro.com when ready
        "email_subject_prefix": "Forensics Daily Intelligence",
        "email_subtitle": "FTK | GLAM | Forensics",
        "snapshot_suffix": "_forensics",
    },
}

# Active scope — set in main() based on --scope arg
SCOPE = "ps"

DIRECTORS = SCOPE_CONFIG["ps"]["directors"]
DIRECTOR_NAMES = SCOPE_CONFIG["ps"]["director_names"]
ACTIVE_STATUS_VALUES = {2, 4, 5, 6, 9, 12, 14, 15}
EXTRA_RECIPIENTS = SCOPE_CONFIG["ps"]["extra_recipients"]

SNAPSHOT_DIR = Path(__file__).parent / ".snapshots"

# Task status values (Rocketlane)
TASK_COMPLETED = 3
TASK_CANCELLED = 9

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
RATE_LIMIT = 55  # stay under 60/min; retry logic handles occasional 429s
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


def api_get(path, retries=4):
    """Fetch from Rocketlane API with rate limiting and retry on 429s and timeouts."""
    url = f"{BASE_URL}/{path}"
    for attempt in range(retries):
        req = urllib.request.Request(url, headers={"api-key": API_KEY, "accept": "application/json"})
        _rate_wait()
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                retry_after = e.headers.get("X-Retry-After")
                if retry_after:
                    try:
                        wait_until = int(retry_after) / 1000
                        wait = max(0, wait_until - time.time()) + 0.5
                    except ValueError:
                        wait = 2 ** (attempt + 1)
                else:
                    wait = 2 ** (attempt + 1)
                print(f"    429 on {path[:60]}... retry in {wait:.1f}s")
                time.sleep(wait)
            elif e.code == 400:
                # Bad request — don't retry
                raise
            elif attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"    HTTP {e.code} on {path[:60]}... retry in {wait:.1f}s")
                time.sleep(wait)
            else:
                raise
        except (TimeoutError, OSError) as e:
            if attempt < retries - 1:
                wait = 3 * (attempt + 1)
                print(f"    Timeout on {path[:60]}... retry in {wait:.1f}s")
                time.sleep(wait)
            else:
                raise


def fetch_all_projects():
    """Fetch all projects with pagination."""
    all_projects, page_token = [], None
    while True:
        url = "projects?pageSize=100"
        if page_token:
            url += f"&pageToken={page_token}"
        resp = api_get(url)
        all_projects.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            page_token = pag["nextPageToken"]
        else:
            break
    return all_projects



def fetch_task_progress(project_id):
    """Fetch task completion progress for a project.
    Returns (completed, total_active) excluding cancelled tasks.
    Only fetches first page (100 tasks) and uses totalRecordCount for estimate."""
    url = f"tasks?projectId.eq={project_id}&pageSize=100"
    resp = api_get(url)
    data = resp.get("data", [])
    pag = resp.get("pagination", {})
    total_record_count = pag.get("totalRecordCount", len(data))

    completed = 0
    cancelled = 0
    for t in data:
        sv = t.get("status", {}).get("value")
        if sv == TASK_CANCELLED:
            cancelled += 1
        elif sv == TASK_COMPLETED:
            completed += 1

    # If all tasks fit in one page, exact count
    if not pag.get("hasMore"):
        active_total = len(data) - cancelled
        return completed, active_total

    # Multi-page: extrapolate from first page ratio
    # completed_ratio on first page, apply to total
    page_active = len(data) - cancelled
    if page_active > 0:
        ratio = completed / page_active
        est_cancelled = int(cancelled / len(data) * total_record_count) if data else 0
        est_active = total_record_count - est_cancelled
        est_completed = int(ratio * est_active)
        return est_completed, est_active

    return completed, total_record_count - cancelled


Z2E_PROGRESS_CACHE = SNAPSHOT_DIR / "z2e_progress.json"


def _load_z2e_cache():
    """Load cached Z2E task progress from prior run."""
    if Z2E_PROGRESS_CACHE.exists():
        try:
            with open(Z2E_PROGRESS_CACHE) as f:
                raw = json.load(f)
            # Convert string keys back to int
            return {int(k): tuple(v) for k, v in raw.items()}
        except Exception:
            return {}
    return {}


def _save_z2e_cache(progress):
    """Persist Z2E task progress for next run."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    with open(Z2E_PROGRESS_CACHE, "w") as f:
        json.dump({str(k): list(v) for k, v in progress.items()}, f)


def fetch_z2e_progress(project_ids, project_statuses=None):
    """Fetch task progress for Z2E projects with caching.

    Uses cached values for projects already at 100%. Only re-fetches
    projects that are still in progress or had errors last time.
    Returns dict of {project_id: (completed, total)}.
    """
    cached = _load_z2e_cache()
    progress = {}
    to_fetch = []

    for pid in project_ids:
        if pid in cached:
            comp, total = cached[pid]
            # If 100% complete last time, trust the cache
            if total > 0 and comp >= total:
                progress[pid] = (comp, total)
                continue
        to_fetch.append(pid)

    print(f"  Z2E progress: {len(progress)} cached, {len(to_fetch)} to fetch...")
    for i, pid in enumerate(to_fetch):
        try:
            completed, total = fetch_task_progress(pid)
            progress[pid] = (completed, total)
        except Exception as e:
            print(f"    Error fetching tasks for {pid}: {e}")
            # Fall back to cached value if available
            if pid in cached:
                progress[pid] = cached[pid]
            else:
                progress[pid] = (0, 0)
        if (i + 1) % 25 == 0:
            print(f"    Progress: {i + 1}/{len(to_fetch)} projects...")

    print(f"  Task progress complete: {len(progress)} projects.")
    _save_z2e_cache(progress)
    return progress



# ═══════════════════════════════════════════════════════════════════════════════
# FIELD HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_field(project, label):
    """Extract field value by label."""
    for f in project.get("fields", []):
        if f.get("fieldLabel") == label:
            return f.get("fieldValueLabel", f.get("fieldValue", ""))
    return None


def build_field_index(project):
    """Build a dict index of field labels → values for O(1) lookups."""
    idx = {}
    for f in project.get("fields", []):
        label = f.get("fieldLabel", "")
        if label:
            idx[label] = f.get("fieldValueLabel", f.get("fieldValue", ""))
    return idx


def strip_html(text):
    """Remove HTML tags from text."""
    return re.sub(r'<[^>]+>', '', str(text)).strip() if text else ""


# ═══════════════════════════════════════════════════════════════════════════════
# TEAM ASSIGNMENT
# ═══════════════════════════════════════════════════════════════════════════════
FORENSICS_NAME_RE = re.compile(
    r'\b(glam|ftk|forensic|ad\s+enterprise|ad\s+lab)\b', re.IGNORECASE
)


def _is_forensics_project(p):
    """Check if a project is forensics via content signals (name keywords or service domain).
    Used as a fallback when director IDs don't match."""
    domain = get_field(p, "Opp: Service Hours Domain(s)") or ""
    if "forensic" in domain.lower():
        return True
    name = p.get("projectName", "")
    if FORENSICS_NAME_RE.search(name):
        return True
    return False


def assign_to_teams(projects):
    """Assign projects to director teams.

    For PS scope: matches by director user IDs on team members/owner.
    For forensics scope: matches by director IDs, Responsible Director field,
    OR content-based keywords (name/domain) as fallback.
    """
    team_projects = defaultdict(list)
    for p in projects:
        member_ids = {m.get("userId") for m in p.get("teamMembers", {}).get("members", [])}
        owner_id = p.get("owner", {}).get("userId")

        matched = False
        for did, team_name in DIRECTORS.items():
            if did in member_ids or did == owner_id:
                team_projects[team_name].append(p)
                matched = True
                break

        # Forensics scope: also check Responsible Director field and content-based matching
        if not matched and SCOPE == "forensics":
            resp_dir = get_field(p, "Responsible Director") or ""
            forensics_dir_emails = {"ewelina.gramala@exterro.com", "jon.cook@exterro.com",
                                    "sarah.hargreaves@exterro.com"}
            if resp_dir.lower().strip() in forensics_dir_emails:
                team_projects["Forensics Impl"].append(p)
            elif _is_forensics_project(p):
                team_projects["Forensics Impl"].append(p)

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

    # Build field index for O(1) lookups (replaces 10× linear scans)
    fi = build_field_index(p)

    health = fi.get("Red/Yellow/Green Health", "")
    health_notes = strip_html(fi.get("Internal Project Health Notes", "") or "")
    weekly_status = strip_html(fi.get("Internal Weekly Status", "") or "")
    project_type = fi.get("Project Type", "")
    project_id = p.get("projectId", "")
    created_at = p.get("createdAt", 0)
    updated_at = p.get("updatedAt", 0)

    # Fields for deep analysis
    sub_type = fi.get("eDisc: Project Sub-Type", "")
    client_segment = fi.get("Client Segmentation", "")
    responsible_director = (fi.get("Responsible Director", "") or "").strip()
    contract_value = 0.0
    try:
        contract_value = float(fi.get("Opp: Total Contract Value", 0) or 0)
    except (ValueError, TypeError):
        pass
    ps_net_price = 0.0
    try:
        ps_net_price = float(fi.get("PSR: Total PS Net Price", 0) or 0)
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

    # Project completion % straight off the bulk projects payload — same
    # number Rocketlane shows on the project screen. Replaces the previous
    # per-project task-count fetch (~108 sequential API calls).
    task_progress = p.get("progressPercentage")

    return {
        "id": project_id,
        "name": p.get("projectName", "?"),
        "status_val": sv,
        "status": sl,
        "owner": owner_name,
        "customer": customer,
        "health": health.strip().lower() if health else "",
        "health_notes": health_notes[:1500],
        "weekly_status": weekly_status[:1500],
        "project_type": project_type,
        "created_at": created_at,
        "updated_at": updated_at,
        "sub_type": sub_type,
        "client_segment": client_segment,
        "responsible_director": responsible_director,
        "contract_value": contract_value,
        "ps_net_price": ps_net_price,
        "latest_note_date": latest_note_date,
        "escalation_flags": escalation_flags,
        "task_progress": task_progress,
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
        snapshot[str(pid)] = {
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
        spid = str(pid)  # JSON keys are always strings
        if spid not in old_snapshot:
            continue  # skip new projects here
        old_p = old_snapshot[spid]

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

    Returns: (stale_projects, time_entries_7d) — entries are exposed so callers
    that emit JSON for downstream consumers don't refetch the data.
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
        return [], []

    # Fetch all time entries for the last 7 days
    print(f"  Fetching 7-day time entries to detect stale projects ({len(projects_by_id)} active)...")
    entries, page_token = [], None
    while True:
        url = f"time-entries?pageSize=100&date.ge={seven_days_ago.isoformat()}&date.le={today.isoformat()}"
        if page_token:
            url += f"&pageToken={page_token}"
        try:
            resp = api_get(url, retries=4)
        except Exception as e:
            print(f"    WARN: time-entries fetch error: {e}")
            return [], []
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

    return sorted(stale, key=lambda x: x["customer"]), entries


def aggregate_time_entries(entries):
    """Roll up raw 7-day time entries into per-project totals + a summary.

    Returns (by_pid, summary):
      by_pid:  {projectId: {hours, billable_hours, entry_count}}
      summary: {total_hours, billable_hours, project_hours, non_project_hours, entry_count}

    Some entries (admin work, internal categories) have no project nesting; their
    minutes flow into summary.non_project_hours but not into by_pid.
    """
    by_pid = defaultdict(lambda: {"hours": 0.0, "billable_hours": 0.0, "entry_count": 0})
    summary = {
        "total_hours": 0.0, "billable_hours": 0.0,
        "project_hours": 0.0, "non_project_hours": 0.0,
        "entry_count": len(entries),
    }
    for e in entries:
        minutes = e.get("minutes", 0) or 0
        hours = minutes / 60.0
        billable = bool(e.get("billable", False))
        summary["total_hours"] += hours
        if billable:
            summary["billable_hours"] += hours
        proj = e.get("project") or {}
        pid = proj.get("projectId") if isinstance(proj, dict) else None
        if pid:
            summary["project_hours"] += hours
            by_pid[pid]["hours"] += hours
            if billable:
                by_pid[pid]["billable_hours"] += hours
            by_pid[pid]["entry_count"] += 1
        else:
            summary["non_project_hours"] += hours
    for v in by_pid.values():
        v["hours"] = round(v["hours"], 2)
        v["billable_hours"] = round(v["billable_hours"], 2)
    for k in ("total_hours", "billable_hours", "project_hours", "non_project_hours"):
        summary[k] = round(summary[k], 2)
    return dict(by_pid), summary


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



# ═══════════════════════════════════════════════════════════════════════════════
# DEEP ANALYSIS SECTIONS
# ═══════════════════════════════════════════════════════════════════════════════
ATTENTION_SYSTEM_PROMPT = """You are an operational intelligence analyst for a Professional Services VP. Given PM notes from red/yellow health projects, produce a concise VP-level triage.

For each project that needs VP action, output ONE line in this exact format:
PROJECT_NAME | CUSTOMER | ACTION_NEEDED | URGENCY(high/medium)

Focus ONLY on projects where the VP should personally intervene:
- Customer executive not responding despite PM outreach
- Engineering blocker with no resolution timeline
- High-value deal at risk of churn
- Timeline slip on a strategic account
- Cross-functional escalation needed

Skip projects where the PM can handle it alone. Be selective — 5-8 items max.
Output ONLY the formatted lines, no headers or explanations."""


def _call_claude_attention(red_yellow_projects):
    """Call Claude to intelligently triage red/yellow projects for VP attention.
    Returns list of {project, customer, action, urgency} or None."""
    if not ANTHROPIC_API_KEY or not call_claude:
        return None

    lines = []
    for p in red_yellow_projects[:30]:
        seg = f" [{p['client_segment']}]" if p["client_segment"] else ""
        val = f" ${p['contract_value']:,.0f}" if p["contract_value"] else ""
        lines.append(f"--- {p['customer']}{seg}{val} | {p['name']} | {p['health'].upper()} | PM: {p['owner']} ---")
        if p["health_notes"]:
            lines.append(f"Health: {p['health_notes'][:400]}")
        if p["weekly_status"]:
            lines.append(f"Status: {p['weekly_status'][:400]}")
        lines.append("")

    print("  Calling Claude for attention triage...")
    text = call_claude(ATTENTION_SYSTEM_PROMPT, "\n".join(lines), max_tokens=1024)
    if not text:
        return None

    try:
        items = []
        for line in text.strip().split("\n"):
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 4:
                items.append({
                    "project": parts[0],
                    "customer": parts[1],
                    "action": parts[2],
                    "urgency": parts[3].lower(),
                })
        print(f"  Claude attention triage: {len(items)} action items.")
        return items if items else None
    except Exception as e:
        print(f"  Claude attention API error: {e}")
        return None


def build_attention_required_section(all_enriched):
    """Build ATTENTION REQUIRED section. Uses Claude for VP triage when available,
    plus structured data for high-value and stale subsections."""
    now = datetime.now()
    active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]

    # --- 0. LLM-powered VP action items ---
    red_yellow_with_notes = [p for p in active
                             if p["health"] in ("red", "yellow")
                             and (p["health_notes"] or p["weekly_status"])]
    red_yellow_with_notes.sort(key=lambda x: (0 if x["health"] == "red" else 1, -x["contract_value"]))
    llm_actions = _call_claude_attention(red_yellow_with_notes)

    # --- 1. Escalation candidates (regex fallback) ---
    escalations = []
    for p in active:
        if p["health"] in ("red", "yellow") and p["escalation_flags"]:
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
    total_items = len(llm_actions or []) + len(escalations) + len(high_value) + len(stale_notes)
    if total_items == 0:
        return ""

    html = ""

    # LLM-powered VP action items (top of section)
    if llm_actions:
        html += f'<div style="margin-bottom:16px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#7c3aed;margin-bottom:6px;">VP Action Items ({len(llm_actions)})</div>'
        html += f'<div style="{S_MUTED};margin-bottom:6px;">Analyzed PM notes across {len(red_yellow_with_notes)} red/yellow projects and identified these for your direct attention.</div>'
        for item in llm_actions:
            urgency_color = "#ef4444" if item["urgency"] == "high" else "#f59e0b"
            urgency_badge = f'<span style="background:{urgency_color};color:white;padding:1px 6px;border-radius:3px;font-size:10px;margin-left:6px;">{item["urgency"].upper()}</span>'
            html += f'<div style="background:#f5f3ff;border-left:3px solid #7c3aed;padding:8px 12px;margin:4px 0;font-size:12px;">'
            html += f'<div><strong>{item["project"]}</strong> — {item["customer"]}{urgency_badge}</div>'
            html += f'<div style="margin-top:3px;color:#475569;">{item["action"]}</div>'
            html += '</div>'
        html += '</div>'

    # Escalation candidates (regex-based, shown if LLM unavailable or as supplement)
    if escalations and not llm_actions:
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
    Phase 1 goal: complete ASAP. Phase 2 goal: complete by 12/31/26.
    Uses per-project task progress from the tasks API."""
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

    def avg_progress(projects):
        """Calculate average task progress across projects."""
        vals = [p.get("task_progress", 0) for p in projects if p.get("task_progress") is not None]
        return int(sum(vals) / len(vals)) if vals else 0

    def progress_badge(p):
        """Return HTML badge showing task progress %."""
        pct = p.get("task_progress")
        if pct is None:
            return '<span style="font-size:10px;color:#94a3b8;">—</span>'
        if pct >= 75:
            color = "#16a34a"
        elif pct >= 50:
            color = "#ca8a04"
        else:
            color = "#dc2626"
        return f'<span style="font-size:10px;font-weight:600;color:{color};">{pct}%</span>'

    html = ""

    # Phase 1
    if phase1:
        comp, prog, blk, oth = status_summary(phase1)
        # Overall phase progress = avg task progress across all projects
        phase_pct = avg_progress(phase1)
        projects_complete_pct = int(len(comp) / len(phase1) * 100) if phase1 else 0
        bar_color = "#22c55e" if phase_pct > 75 else "#f59e0b" if phase_pct > 50 else "#ef4444"

        html += f'<div style="margin-bottom:16px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#334155;margin-bottom:4px;">Phase 1 (Z2E Phase 1) — Goal: Complete ASAP</div>'
        # Progress bar based on avg task completion
        html += f'<div style="background:#e2e8f0;border-radius:4px;height:12px;margin:4px 0 8px 0;overflow:hidden;">'
        html += f'<div style="background:{bar_color};height:100%;width:{phase_pct}%;border-radius:4px;"></div></div>'
        html += f'<div style="font-size:12px;margin-bottom:8px;">'
        html += f'Avg task progress: <strong>{phase_pct}%</strong> • '
        html += f'<strong>{len(comp)}</strong>/{len(phase1)} projects complete ({projects_complete_pct}%) • '
        html += f'<span style="color:#0f766e;">{len(prog)} in progress</span> • '
        html += f'<span style="color:#dc2626;">{len(blk)} blocked/on hold</span>'
        if oth:
            html += f' • {len(oth)} other'
        html += '</div>'

        # Show in-progress + blocked projects with progress %
        active_projects = sorted(prog + blk + oth, key=lambda p: p.get("task_progress", 0))
        if active_projects:
            html += f'<div style="font-size:11px;font-weight:600;color:#475569;margin:8px 0 4px 0;">Active projects ({len(active_projects)}):</div>'
            html += '<table style="font-size:11px;border-collapse:collapse;width:100%;">'
            html += '<tr style="background:#f1f5f9;"><th style="padding:3px 6px;text-align:left;">Project</th><th style="padding:3px 6px;text-align:left;">Customer</th><th style="padding:3px 6px;text-align:left;">PM</th><th style="padding:3px 6px;text-align:left;">Status</th><th style="padding:3px 6px;text-align:right;">Progress</th></tr>'
            for p in active_projects:
                link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
                hc = p.get("health", "")
                dot = ""
                if hc in ("red", "yellow"):
                    dot_color = "#ef4444" if hc == "red" else "#f59e0b"
                    dot = f'<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:{dot_color};margin-right:3px;vertical-align:middle;"></span>'
                badge = progress_badge(p)
                html += f'<tr style="border-bottom:1px solid #e2e8f0;"><td style="padding:3px 6px;">{dot}{link}</td><td style="padding:3px 6px;">{p["customer"]}</td><td style="padding:3px 6px;">{p["owner"]}</td><td style="padding:3px 6px;">{p["status"]}</td><td style="padding:3px 6px;text-align:right;">{badge}</td></tr>'
            html += '</table>'
        html += '</div>'

    # Phase 2
    if phase2:
        comp, prog, blk, oth = status_summary(phase2)
        phase_pct = avg_progress(phase2)
        projects_complete_pct = int(len(comp) / len(phase2) * 100) if phase2 else 0
        bar_color = "#22c55e" if phase_pct > 50 else "#f59e0b" if phase_pct > 25 else "#ef4444"

        # Days until deadline
        deadline = date(2026, 12, 31)
        days_left = (deadline - date.today()).days

        html += f'<div style="margin-bottom:8px;">'
        html += f'<div style="font-weight:700;font-size:12px;color:#334155;margin-bottom:4px;">Phase 2 (Z2E) — Deadline: 12/31/2026 ({days_left} days)</div>'
        # Progress bar based on avg task completion
        html += f'<div style="background:#e2e8f0;border-radius:4px;height:12px;margin:4px 0 8px 0;overflow:hidden;">'
        html += f'<div style="background:{bar_color};height:100%;width:{phase_pct}%;border-radius:4px;"></div></div>'
        html += f'<div style="font-size:12px;margin-bottom:8px;">'
        html += f'Avg task progress: <strong>{phase_pct}%</strong> • '
        html += f'<strong>{len(comp)}</strong>/{len(phase2)} projects complete ({projects_complete_pct}%) • '
        html += f'<span style="color:#0f766e;">{len(prog)} in progress</span> • '
        html += f'<span style="color:#dc2626;">{len(blk)} blocked/on hold</span>'
        if oth:
            html += f' • {len(oth)} other'
        html += '</div>'

        # Show blocked/red projects with progress
        needs_attention = [p for p in blk + prog if p["health"] in ("red", "yellow")]
        if needs_attention:
            needs_attention.sort(key=lambda p: p.get("task_progress", 0))
            html += f'<div style="font-size:11px;font-weight:600;color:#b45309;margin:8px 0 4px 0;">Needs attention ({len(needs_attention)}):</div>'
            html += '<table style="font-size:11px;border-collapse:collapse;width:100%;">'
            html += '<tr style="background:#f1f5f9;"><th style="padding:3px 6px;text-align:left;">Project</th><th style="padding:3px 6px;text-align:left;">Customer</th><th style="padding:3px 6px;text-align:left;">PM</th><th style="padding:3px 6px;text-align:left;">Status</th><th style="padding:3px 6px;text-align:right;">Progress</th></tr>'
            for p in needs_attention:
                link = f'<a href="{RL_APP_BASE}/{p["id"]}" style="{S_LINK}">{p["name"]}</a>'
                hc = p["health"]
                dot_color = "#ef4444" if hc == "red" else "#f59e0b"
                dot = f'<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:{dot_color};margin-right:3px;vertical-align:middle;"></span>'
                badge = progress_badge(p)
                html += f'<tr style="border-bottom:1px solid #e2e8f0;"><td style="padding:3px 6px;">{dot}{link}</td><td style="padding:3px 6px;">{p["customer"]}</td><td style="padding:3px 6px;">{p["owner"]}</td><td style="padding:3px 6px;">{p["status"]}</td><td style="padding:3px 6px;text-align:right;">{badge}</td></tr>'
            html += '</table>'
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


# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE API — LLM-POWERED ANALYSIS ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
def _build_intelligence_system_prompt():
    """Build scope-aware system prompt for Claude intelligence analysis."""
    cfg = SCOPE_CONFIG[SCOPE]
    vp = cfg["vp"]
    director_list = ", ".join(f"{name} ({team})" for team, name in cfg["director_names"].items())
    director_blocks = "\n".join(
        f'<div style="background:#f5f3ff;padding:10px 12px;margin:6px 0;border-radius:4px;">\n<strong>{name} ({team}):</strong> [specific to their team]\n</div>'
        for team, name in cfg["director_names"].items()
    )
    director_bullet_points = "\n".join(
        f'- <strong>{name} ({team}):</strong> Their team\'s specific situation'
        for team, name in cfg["director_names"].items()
    )

    return f"""You are the operational intelligence engine for Exterro's {cfg['label']} VP, {vp}. You have 30+ years of enterprise PS experience in your analytical approach. You produce a DAILY executive briefing by deeply analyzing PM notes, project health data, and operational metrics.

{vp}'s directors: {director_list}.

ANALYTICAL APPROACH — READ EVERY PM NOTE CAREFULLY:
- Don't just summarize. Interpret. What does a PM's note *imply* about where this project is headed?
- A PM writing "waiting on customer" for 3 consecutive updates = the customer has disengaged. Say that.
- A PM writing "escalated to engineering" with no resolution date = an open-ended dependency. Flag the risk.
- Multiple PMs reporting similar blockers = a systemic issue. Name the pattern.
- A project at 90% task completion with red health = something broke late. That's worse than a project that's been red from the start.
- Look for what PMs are NOT saying: no update in 2+ weeks on an active project is a signal.
- $500K+ contracts at risk deserve specific attention with the dollar amount called out.

YOUR OUTPUT MUST HAVE THESE SECTIONS using the exact HTML shown. CRITICAL FORMATTING RULES:
- Wrap each critical item in a styled div for visual separation
- Use <p> tags between paragraphs, NOT raw text runs
- Each numbered item in Critical Items MUST be a separate visual block

<h4 style="color:#dc2626;margin:16px 0 8px 0;font-size:13px;border-bottom:1px solid #fecaca;padding-bottom:4px;">Critical Items Requiring VP Action</h4>
Only items where {vp} must personally intervene. Max 5-7 items.
FORMAT EACH ITEM AS:
<div style="background:#fef2f2;border-left:3px solid #ef4444;padding:10px 12px;margin:8px 0;border-radius:0 4px 4px 0;">
<div style="font-weight:700;font-size:13px;margin-bottom:4px;">[NUMBER]. [CUSTOMER] — $[VALUE] — [ONE-LINE HEADLINE]</div>
<p style="margin:4px 0;font-size:12px;">[2-3 sentences: what's wrong, how long, why it matters]</p>
<p style="margin:4px 0;font-size:12px;"><strong>Action:</strong> [What {vp} should do specifically]</p>
</div>

<h4 style="color:#b45309;margin:16px 0 8px 0;font-size:13px;border-bottom:1px solid #fde68a;padding-bottom:4px;">Cross-Portfolio Patterns</h4>
This is your highest-value analysis. FORMAT AS:
<div style="background:#fffbeb;border-left:3px solid #f59e0b;padding:10px 12px;margin:8px 0;border-radius:0 4px 4px 0;">
<p style="margin:4px 0;font-size:12px;"><strong>[Pattern name]:</strong> [Details with specific customer names and what connects them]</p>
</div>
Identify: engineering/product issues blocking multiple customers, customer responsiveness patterns, PM workload signals, common root causes, trends worsening vs. improving. Be specific — name customers, PMs, specific issues.

<h4 style="color:#0f766e;margin:16px 0 8px 0;font-size:13px;border-bottom:1px solid #a7f3d0;padding-bottom:4px;">Momentum &amp; Wins</h4>
<div style="background:#f0fdf4;border-left:3px solid #22c55e;padding:10px 12px;margin:8px 0;border-radius:0 4px 4px 0;">
Go-lives, UAT completions, hypercare exits. Name customers. Note PM capacity freeing up.
</div>

<h4 style="color:#7c3aed;margin:16px 0 8px 0;font-size:13px;border-bottom:1px solid #c4b5fd;padding-bottom:4px;">Director Priorities</h4>
Write ONE targeted paragraph for each director in separate styled blocks:
{director_blocks}

<h4 style="color:#334155;margin:16px 0 8px 0;font-size:13px;border-bottom:1px solid #e2e8f0;padding-bottom:4px;">Recommended Actions</h4>
5-7 items, each as:
<div style="padding:4px 0 4px 16px;font-size:12px;border-bottom:1px solid #f1f5f9;">
<strong>[#].</strong> [Action] → <em>[Assigned to] by [when]</em>
</div>

RULES:
- Use the exact HTML div/p structures shown above. Do NOT output raw unformatted text.
- Be direct and analytical. No corporate filler.
- Always name specific customers, PMs, dollar amounts, and percentages.
- If you see a risk not flagged by health color, call it out.
- Total response 1000-1500 words."""


def call_claude_intelligence(prompt_data):
    """Call Claude API to generate the daily intelligence brief."""
    if not ANTHROPIC_API_KEY or not call_claude:
        print("  Claude API unavailable, falling back to regex analysis.")
        return None
    print("  Calling Claude for daily intelligence analysis...")
    system_prompt = _build_intelligence_system_prompt()
    result = call_claude(system_prompt, prompt_data, max_tokens=4000, model="claude-opus-4-6")
    if result:
        print(f"  Claude intelligence received ({len(result)} chars).")
    return result


def _build_intelligence_prompt(projects_by_team, all_enriched, stale_projects, changes):
    """Build the structured data prompt for Claude to analyze."""
    active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]
    lines = []

    # Portfolio stats
    lines.append("=== PORTFOLIO OVERVIEW ===")
    for team in DIRECTOR_NAMES:
        projs = projects_by_team.get(team, [])
        team_active = [p for p in projs if p["status_val"] in ACTIVE_STATUS_VALUES]
        red = sum(1 for p in team_active if p["health"] == "red")
        yellow = sum(1 for p in team_active if p["health"] == "yellow")
        green = sum(1 for p in team_active if p["health"] == "green")
        lines.append(f"{DIRECTOR_NAMES[team]} ({team}): {len(team_active)} active — {red} red, {yellow} yellow, {green} green")

    # Health changes this period
    if changes:
        lines.append(f"\n=== HEALTH/STATUS CHANGES ({len(changes)}) ===")
        for c in changes[:20]:
            lines.append(f"- {c['project']} ({c['customer']}): {c['type']} {c.get('from','')} → {c.get('to','')}")

    # Z2E migration
    z2e_active = [p for p in all_enriched
                  if "z2e" in p.get("sub_type", "").lower()
                  and p.get("task_progress") is not None
                  and p["status"] not in ("Completed", "Closeout")]
    if z2e_active:
        z2e_avg = int(sum(p["task_progress"] for p in z2e_active) / len(z2e_active))
        lines.append(f"\n=== Z2E MIGRATION ({len(z2e_active)} active, avg {z2e_avg}% complete) ===")
        low = sorted([p for p in z2e_active if p["task_progress"] < 30], key=lambda x: x["task_progress"])
        if low:
            lines.append(f"Below 30% progress ({len(low)}):")
            for p in low[:8]:
                lines.append(f"  - {p['customer']}: {p['task_progress']}% [{p['status']}] PM: {p['owner']}")

    # High-value at risk
    hv = [p for p in active
          if p["health"] in ("red", "yellow")
          and (p["client_segment"] in ("Pinnacle", "Strategic") or p["contract_value"] >= 100000)]
    if hv:
        total_val = sum(p["contract_value"] for p in hv)
        lines.append(f"\n=== HIGH-VALUE AT RISK ({len(hv)} projects, ${total_val:,.0f} total) ===")
        for p in sorted(hv, key=lambda x: -x["contract_value"])[:10]:
            lines.append(f"  - {p['customer']} | {p['name']} | ${p['contract_value']:,.0f} | {p['health']} | {p['client_segment']} | PM: {p['owner']}")

    # Stale projects
    if stale_projects:
        lines.append(f"\n=== STALE PROJECTS ({len(stale_projects)} with no time logged in 7 days) ===")

    # PM notes — the gold mine. Include red/yellow health projects first, then recent updates.
    lines.append("\n=== PM NOTES (red/yellow health projects) ===")
    red_yellow = [p for p in active if p["health"] in ("red", "yellow") and (p["health_notes"] or p["weekly_status"])]
    for p in sorted(red_yellow, key=lambda x: (0 if x["health"] == "red" else 1, -x["contract_value"]))[:25]:
        seg = f" [{p['client_segment']}]" if p["client_segment"] else ""
        val = f" ${p['contract_value']:,.0f}" if p["contract_value"] else ""
        lines.append(f"\n--- {p['customer']}{seg}{val} | {p['name']} | {p['health'].upper()} | PM: {p['owner']} ---")
        if p["health_notes"]:
            lines.append(f"Health notes: {p['health_notes'][:800]}")
        if p["weekly_status"]:
            lines.append(f"Status: {p['weekly_status'][:800]}")

    # Also include some green projects with recent notes for momentum signals
    lines.append("\n=== PM NOTES (recent green/active — momentum signals) ===")
    green_with_notes = [p for p in active
                        if p["health"] == "green"
                        and (p["health_notes"] or p["weekly_status"])
                        and p.get("latest_note_date")]
    green_with_notes.sort(key=lambda x: x["latest_note_date"] or datetime.min, reverse=True)
    for p in green_with_notes[:15]:
        lines.append(f"\n--- {p['customer']} | {p['name']} | GREEN | PM: {p['owner']} ---")
        if p["health_notes"]:
            lines.append(f"Health notes: {p['health_notes'][:600]}")
        if p["weekly_status"]:
            lines.append(f"Status: {p['weekly_status'][:600]}")

    # PM coverage stats
    with_notes = [p for p in active if p["health_notes"] or p["weekly_status"]]
    lines.append(f"\n=== PM COMMENTARY COVERAGE ===")
    lines.append(f"{len(with_notes)} of {len(active)} active projects ({int(len(with_notes)/len(active)*100) if active else 0}%) have PM notes")

    # Per-team PM notes for director briefings
    for team in DIRECTOR_NAMES:
        team_projs = projects_by_team.get(team, [])
        team_active = [p for p in team_projs if p["status_val"] in ACTIVE_STATUS_VALUES]
        team_ry = [p for p in team_active if p["health"] in ("red", "yellow") and (p["health_notes"] or p["weekly_status"])]
        if team_ry:
            lines.append(f"\n=== {team.upper()} — {DIRECTOR_NAMES[team]} — RED/YELLOW NOTES ===")
            for p in sorted(team_ry, key=lambda x: (0 if x["health"] == "red" else 1))[:10]:
                seg = f" [{p['client_segment']}]" if p["client_segment"] else ""
                val = f" ${p['contract_value']:,.0f}" if p["contract_value"] else ""
                lines.append(f"\n{p['customer']}{seg}{val} | {p['name']} | {p['health'].upper()} | PM: {p['owner']}")
                if p["health_notes"]:
                    lines.append(f"  Notes: {p['health_notes'][:800]}")
                if p["weekly_status"]:
                    lines.append(f"  Status: {p['weekly_status'][:800]}")
        # Team stale count
        team_stale = [p for p in (stale_projects or [])
                      if any(tp["id"] == p["id"] for tp in team_projs)]
        if team_stale:
            lines.append(f"{team}: {len(team_stale)} stale projects (no time logged 7 days)")

    return "\n".join(lines)


# ── Theme detection patterns for narrative extraction (regex fallback) ──
_THEME_PATTERNS = {
    "customer_blocking": re.compile(
        r'waiting on (?:customer|client)|no response|customer.{0,20}(?:not respond|unresponsive|unavailable|no.?show)|'
        r'pending.{0,15}(?:customer|client)|customer.{0,15}(?:delay|hold)',
        re.IGNORECASE),
    "engineering_dependency": re.compile(
        r'escalat.{0,10}(?:to |)engineer|engineer.{0,15}(?:investigat|review|working|fix|resolv)|'
        r'product.{0,10}(?:team|issue|bug|defect)|awaiting.{0,10}(?:fix|patch|release)',
        re.IGNORECASE),
    "go_live_milestone": re.compile(
        r'go.?live|went live|now live|launched|go.?no.?go|cutover|production.?ready|'
        r'hypercare|UAT.{0,15}(?:complete|pass|success)',
        re.IGNORECASE),
    "on_hold": re.compile(
        r'on hold|paused|suspend|put on hold|project.{0,10}hold|no activity',
        re.IGNORECASE),
    "timeline_risk": re.compile(
        r'timeline.{0,15}(?:impact|slip|delay|at risk|push)|behind schedule|'
        r'missed.{0,10}(?:deadline|date|milestone)|overdue|延|pushed.{0,5}(?:back|out)',
        re.IGNORECASE),
    "resource_issue": re.compile(
        r'resource.{0,10}(?:gap|short|constrain|unavail)|no.{0,5}(?:PM|TL|resource).{0,5}assign|'
        r'staffing|capacity.{0,10}(?:issue|concern|limit)',
        re.IGNORECASE),
}


def _extract_themes(all_enriched):
    """Scan PM notes across portfolio and extract thematic patterns.
    Returns dict of theme → list of (project, snippet) tuples."""
    themes = {k: [] for k in _THEME_PATTERNS}
    active = [p for p in all_enriched
              if p["status_val"] in ACTIVE_STATUS_VALUES
              and (p["health_notes"] or p["weekly_status"])]

    for p in active:
        combined = f'{p["health_notes"]} {p["weekly_status"]}'
        for theme_name, pattern in _THEME_PATTERNS.items():
            match = pattern.search(combined)
            if match:
                start = max(0, match.start() - 20)
                end = min(len(combined), match.end() + 60)
                snippet = ("..." if start > 0 else "") + combined[start:end].strip() + ("..." if end < len(combined) else "")
                themes[theme_name].append((p, snippet))

    return themes


def build_daily_intelligence(projects_by_team, new_projects, changes, all_enriched, stale_projects):
    """Build the AI-powered daily intelligence brief. Runs every day.
    Falls back to regex-based analysis if Claude API unavailable."""

    # Try Claude-powered intelligence
    if ANTHROPIC_API_KEY and call_claude:
        prompt_data = _build_intelligence_prompt(projects_by_team, all_enriched, stale_projects, changes)
        claude_html = call_claude_intelligence(prompt_data)
        if claude_html:
            return f'''<div style="background:#faf5ff;border-left:4px solid #7c3aed;padding:20px;margin:16px 0;border-radius:0 6px 6px 0;">
<div style="display:flex;align-items:center;gap:8px;margin:0 0 14px 0;">
<div style="font-size:16px;font-weight:700;color:#7c3aed;">Daily Intelligence Brief</div>
</div>
<div style="font-size:13px;line-height:1.6;color:#1e293b;">{claude_html}</div>
</div>'''

    # Fallback to regex-based analysis
    return _build_regex_narrative(projects_by_team, new_projects, changes, all_enriched, stale_projects)


def _build_regex_narrative(projects_by_team, new_projects, changes, all_enriched, stale_projects):
    """Regex-based fallback narrative when Claude API is unavailable."""

    active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]
    total_active = len(active)

    # Health summary by team
    health_summary = {}
    for team, projs in projects_by_team.items():
        team_active = [p for p in projs if p["status_val"] in ACTIVE_STATUS_VALUES]
        red = sum(1 for p in team_active if p["health"] == "red")
        yellow = sum(1 for p in team_active if p["health"] == "yellow")
        green = sum(1 for p in team_active if p["health"] == "green")
        health_summary[team] = {"red": red, "yellow": yellow, "green": green, "total": len(team_active)}

    # Extract themes from PM notes
    themes = _extract_themes(all_enriched)

    # Z2E progress snapshot
    z2e_active = [p for p in all_enriched
                  if "z2e" in p.get("sub_type", "").lower()
                  and p.get("task_progress") is not None
                  and p["status"] not in ("Completed", "Closeout")]
    z2e_avg = int(sum(p["task_progress"] for p in z2e_active) / len(z2e_active)) if z2e_active else 0
    z2e_low = [p for p in z2e_active if p.get("task_progress", 0) < 30]

    # High-value at risk
    hv_at_risk = [p for p in active
                  if p["health"] in ("red", "yellow")
                  and (p["client_segment"] in ("Pinnacle", "Strategic") or p["contract_value"] >= 100000)]

    # PM notes coverage
    with_notes = [p for p in active if p["health_notes"] or p["weekly_status"]]
    notes_pct = int(len(with_notes) / total_active * 100) if total_active else 0

    # ── Build narrative HTML ──
    P = 'style="margin:8px 0;line-height:1.6;font-size:13px;"'
    BOLD = 'style="font-weight:700;"'
    CALLOUT = 'style="background:#fef2f2;border-left:3px solid #ef4444;padding:8px 12px;margin:8px 0;font-size:12px;line-height:1.5;"'
    INSIGHT = 'style="background:#eff6ff;border-left:3px solid #3b82f6;padding:8px 12px;margin:8px 0;font-size:12px;line-height:1.5;"'

    html = f'<div style="{S_SECTION}">'
    html += '<h3 style="margin:0 0 12px 0;font-size:14px;font-weight:700;">Weekly Executive Summary</h3>'

    # ── Portfolio overview ──
    html += f'<p {P}><span {BOLD}>Portfolio:</span> {total_active} active projects across 3 teams. '
    total_red = sum(h["red"] for h in health_summary.values())
    total_yellow = sum(h["yellow"] for h in health_summary.values())
    if total_red or total_yellow:
        html += f'<span style="color:#dc2626;">{total_red} red</span> and '
        html += f'<span style="color:#b45309;">{total_yellow} yellow</span> health flags. '
    html += f'{len(new_projects)} new projects onboarded this week.</p>'

    # ── Health by team ──
    html += f'<p {P}><span {BOLD}>Health by Team:</span></p>'
    for team in DIRECTOR_NAMES:
        if team in health_summary:
            h = health_summary[team]
            director = DIRECTOR_NAMES[team]
            concern = ""
            if h["red"] >= 3:
                concern = f' — <span style="color:#dc2626;">elevated red count, director review recommended</span>'
            elif h["red"] > 0 and h["red"] / max(h["total"], 1) > 0.15:
                concern = f' — <span style="color:#b45309;">{int(h["red"]/h["total"]*100)}% of portfolio at red</span>'
            html += f'''<div style="margin:2px 0 2px 12px;font-size:12px;">{director} ({team}):
<span style="background:#ef4444;color:white;padding:1px 6px;border-radius:3px;font-size:11px;margin-right:4px;">{h["red"]} Red</span>
<span style="background:#f59e0b;color:white;padding:1px 6px;border-radius:3px;font-size:11px;margin-right:4px;">{h["yellow"]} Yellow</span>
<span style="background:#22c55e;color:white;padding:1px 6px;border-radius:3px;font-size:11px;">{h["green"]} Green</span>{concern}</div>'''

    # ── Key risks extracted from PM notes ──
    risk_items = []

    # Customer blocking
    cust_blocking = themes["customer_blocking"]
    if cust_blocking:
        names = ", ".join(p["customer"] for p, _ in cust_blocking[:4])
        trail = f" (+{len(cust_blocking)-4} more)" if len(cust_blocking) > 4 else ""
        risk_items.append(
            f'<span {BOLD}>Customer responsiveness:</span> {len(cust_blocking)} projects blocked waiting on customer action '
            f'({names}{trail}). Consider exec-to-exec outreach on stalled accounts.')

    # Engineering dependencies
    eng_deps = themes["engineering_dependency"]
    if eng_deps:
        names = ", ".join(f'{p["customer"]}' for p, _ in eng_deps[:3])
        trail = f" (+{len(eng_deps)-3} more)" if len(eng_deps) > 3 else ""
        risk_items.append(
            f'<span {BOLD}>Engineering dependencies:</span> {len(eng_deps)} projects waiting on engineering resolution '
            f'({names}{trail}). Flag with product/engineering leadership if SLAs are slipping.')

    # Timeline risks
    timeline = themes["timeline_risk"]
    if timeline:
        names = ", ".join(p["customer"] for p, _ in timeline[:3])
        risk_items.append(
            f'<span {BOLD}>Timeline pressure:</span> {len(timeline)} projects reporting schedule impact '
            f'({names}). Review scoping accuracy and resource allocation.')

    # On hold
    on_hold = themes["on_hold"]
    if len(on_hold) >= 3:
        risk_items.append(
            f'<span {BOLD}>Stalled pipeline:</span> {len(on_hold)} projects currently on hold per PM notes. '
            f'Aging holds tie up PM capacity and should be triaged for reactivation or closure.')

    if risk_items:
        html += f'<p {P}><span {BOLD}>Key Risks (from PM notes analysis):</span></p>'
        for item in risk_items:
            html += f'<div {CALLOUT}>{item}</div>'

    # ── Positive momentum ──
    go_lives = themes["go_live_milestone"]
    if go_lives:
        names = ", ".join(p["customer"] for p, _ in go_lives[:5])
        trail = f" (+{len(go_lives)-5} more)" if len(go_lives) > 5 else ""
        html += f'<div {INSIGHT}><span {BOLD}>Momentum:</span> {len(go_lives)} projects at or near go-live '
        html += f'({names}{trail}). Strong execution pipeline this week.</div>'

    # ── Z2E migration status ──
    if z2e_active:
        html += f'<p {P}><span {BOLD}>Z2E Migration:</span> {len(z2e_active)} active projects averaging '
        html += f'<strong>{z2e_avg}%</strong> task completion. '
        if z2e_low:
            names = ", ".join(p["customer"] for p in z2e_low[:3])
            html += f'<span style="color:#dc2626;">{len(z2e_low)} projects below 30% progress ({names})</span> — '
            html += 'may need resource reallocation or scope review.'
        else:
            html += 'All projects tracking above 30% — on trajectory.'
        html += '</p>'

    # ── High-value at risk ──
    if hv_at_risk:
        total_val = sum(p["contract_value"] for p in hv_at_risk)
        html += f'<p {P}><span {BOLD}>Revenue exposure:</span> {len(hv_at_risk)} Pinnacle/Strategic or $100K+ projects '
        html += f'at red/yellow health, representing <strong>${total_val:,.0f}</strong> in contract value. '
        top = sorted(hv_at_risk, key=lambda x: -x["contract_value"])[:3]
        html += 'Top accounts: ' + ", ".join(f'{p["customer"]} (${p["contract_value"]:,.0f})' for p in top) + '.</p>'

    # ── PM coverage + stale ──
    html += f'<p {P}><span {BOLD}>PM Commentary:</span> {len(with_notes)} of {total_active} active projects ({notes_pct}%) '
    html += 'have health notes or status updates. '
    if notes_pct < 70:
        html += '<span style="color:#b45309;">Coverage below 70% — push directors to ensure PMs update notes weekly.</span>'
    if stale_projects:
        html += f' Additionally, <strong>{len(stale_projects)}</strong> implementation projects had zero time logged in 7 days.'
    html += '</p>'

    # ── Recommended actions ──
    actions = []
    if cust_blocking and any(p["client_segment"] in ("Pinnacle", "Strategic") for p, _ in cust_blocking):
        pinnacle_blocked = [(p, s) for p, s in cust_blocking if p["client_segment"] in ("Pinnacle", "Strategic")]
        actions.append(f'Schedule exec outreach for {len(pinnacle_blocked)} blocked Pinnacle/Strategic customers')
    if eng_deps and len(eng_deps) >= 3:
        actions.append(f'Escalate {len(eng_deps)} engineering-dependent projects to product leadership')
    if z2e_low and len(z2e_low) >= 2:
        actions.append(f'Review resourcing on {len(z2e_low)} Z2E projects below 30% completion')
    if stale_projects and len(stale_projects) >= 10:
        actions.append(f'Directors to audit {len(stale_projects)} stale projects for status accuracy')

    if actions:
        html += f'<p {P}><span {BOLD}>Recommended Actions:</span></p>'
        html += '<div style="margin:4px 0 0 12px;font-size:12px;">'
        for i, action in enumerate(actions, 1):
            html += f'<div style="margin:3px 0;">→ {action}</div>'
        html += '</div>'

    html += '</div>'
    return html


def build_email_html(digest_data):
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
    cfg = SCOPE_CONFIG[SCOPE]
    header = f'''<div style="{S_HEADER}">
<div style="{S_HEADER_TITLE}">{cfg["email_subject_prefix"]}</div>
<div style="{S_HEADER_SUBTITLE}">{today_str} • {cfg["email_subtitle"]}</div>
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

    # Daily Intelligence Brief — AI-powered, runs every day
    sections.append(build_daily_intelligence(projects_by_team, new_projects, changes, all_enriched, stale_projects))

    # Attention required — escalations, high-value at risk, stale notes
    sections.append(build_attention_required_section(all_enriched))

    # Z2E migration progress (PS scope only)
    if SCOPE == "ps":
        sections.append(build_z2e_tracker_section(all_enriched))

    # Health changes (only if we have prior snapshot)
    if has_prior_snapshot:
        sections.append(build_health_changes_section(changes))
    else:
        sections.append(f'<div style="{S_SECTION}"><div style="{S_MUTED}"><em>Health change detection available from next run (snapshot baseline being established today).</em></div></div>')

    # Post-Implementation watch (PS scope only — Oronde)
    if SCOPE == "ps":
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
# GOOGLE CHAT CARD
# ═══════════════════════════════════════════════════════════════════════════════
GCHAT_WEBHOOK_URL = os.environ.get("GCHAT_WEBHOOK_URL", "")


def _chat_link(name, pid):
    return f'<a href="{RL_APP_BASE}/{pid}">{name}</a>'


def build_chat_card(digest_data):
    """Build a condensed Google Chat card from digest data."""
    today_str = digest_data["today_str"]
    all_enriched = digest_data["all_enriched"]
    projects_by_team = digest_data["projects_by_team"]
    new_projects = digest_data["new_projects"]
    changes = digest_data["changes"]
    stale_projects = digest_data["stale_projects"]

    cfg = SCOPE_CONFIG[SCOPE]
    active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]

    # Counts
    blocked = [p for p in active if p["status_val"] == 4]
    delayed = [p for p in active if p["status_val"] == 12]
    red_all = [p for p in active if p["health"] == "red"]
    yellow_all = [p for p in active if p["health"] == "yellow"]
    no_health = [p for p in active if not p["health"]]
    escalation_projects = [p for p in active if p.get("escalation_flags") and p["health"] in ("red", "yellow")]

    sections = []

    # ── KPI row ──
    kpi = (
        f"<b>{len(active)}</b> Active  \u2022  "
        f"<font color=\"#dc2626\"><b>{len(blocked)}</b> Blocked</font>  \u2022  "
        f"<font color=\"#dc2626\"><b>{len(delayed)}</b> Delayed</font>  \u2022  "
        f"<font color=\"#dc2626\"><b>{len(red_all)}</b> Red</font>  \u2022  "
        f"<font color=\"#ca8a04\"><b>{len(yellow_all)}</b> Yellow</font>"
    )
    if no_health:
        kpi += f"  \u2022  <font color=\"#f97316\">\u26a0\ufe0f <b>{len(no_health)}</b> No Health</font>"
    if new_projects:
        kpi += f"  \u2022  <b>{len(new_projects)}</b> New (24h)"
    sections.append({"widgets": [{"textParagraph": {"text": kpi}}]})

    # ── Attention items ──
    action_lines = []
    if blocked:
        action_lines.append(f"\ud83d\udeab <b>{len(blocked)} blocked projects</b> need unblocking")
    if no_health:
        action_lines.append(f"\u26a0\ufe0f <b>{len(no_health)} projects</b> have no health status set")
    if escalation_projects:
        top = sorted(escalation_projects, key=lambda x: x["name"])[:3]
        for p in top:
            snippet = p["health_notes"][:80] if p["health_notes"] else p["weekly_status"][:80]
            action_lines.append(f"\ud83d\udd34 {_chat_link(p['name'], p['id'])} {DASH} <i>{snippet}</i>")
        if len(escalation_projects) > 3:
            action_lines.append(f"<i>...{len(escalation_projects) - 3} more with escalation keywords</i>")
    if stale_projects:
        action_lines.append(f"\ud83d\udc4b <b>{len(stale_projects)} stale projects</b> {DASH} no time logged in 7+ days")

    # Health changes from snapshot diff
    health_changes = [c for c in changes if c["type"] == "health_change"]
    downgrades = [c for c in health_changes if _health_rank(c["to"]) < _health_rank(c["from"])]
    if downgrades:
        for c in downgrades[:3]:
            action_lines.append(
                f"\u2b07\ufe0f <b>{c['project']}</b> {DASH} {c['from']} \u2192 {c['to']} ({c['pm']})"
            )
        if len(downgrades) > 3:
            action_lines.append(f"<i>...{len(downgrades) - 3} more health downgrades</i>")

    if action_lines:
        sections.append({
            "header": "\u26a1 Needs Your Attention",
            "widgets": [{"textParagraph": {"text": "\n".join(action_lines)}}]
        })

    # ── Team sections ──
    for team_name in DIRECTOR_NAMES.keys():
        projs = projects_by_team.get(team_name, [])
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

        # Z2E breakdown for eDiscovery (PS scope)
        if SCOPE == "ps" and team_name == "eDiscovery":
            z2e = [p for p in t_active if "z2e" in p.get("sub_type", "").lower() and "not started" not in p.get("sub_type", "").lower()]
            z2e_p1 = sum(1 for p in t_active if "z2e phase 1" in p.get("sub_type", "").lower())
            z2e_p2 = len(z2e) - z2e_p1
            non_z2e = len(t_active) - len(z2e)
            summary += f"\n<b>Z2E:</b> {len(z2e)} ({z2e_p1} P1 \u2022 {z2e_p2} P2)  |  <b>Non-Z2E:</b> {non_z2e}"

        widgets = [{"decoratedText": {"topLabel": f"{team_name} {DASH} {DIRECTOR_NAMES[team_name]}", "text": summary}}]

        # Red project details (collapsible)
        if t_red:
            by_pm = defaultdict(list)
            for p in t_red:
                by_pm[p["owner"] or "Unassigned"].append(p)
            red_lines = []
            for pm in sorted(by_pm.keys()):
                pm_projs = sorted(by_pm[pm], key=lambda x: x["name"])
                red_lines.append(f"\n<b>{pm}</b>")
                for p in pm_projs[:4]:
                    line = f"  \ud83d\udd34 {_chat_link(p['name'], p['id'])} {DASH} {p['customer']}"
                    if p["health_notes"]:
                        line += f"\n       <i>{p['health_notes'][:70]}</i>"
                    red_lines.append(line)
                if len(pm_projs) > 4:
                    red_lines.append(f"  <i>...+{len(pm_projs) - 4} more</i>")
            widgets.append({"textParagraph": {"text": "\n".join(red_lines)}})

        sections.append({
            "header": team_name,
            "collapsible": True,
            "uncollapsibleWidgetsCount": 1,
            "widgets": widgets,
        })

    # ── Footer ──
    sections.append({
        "widgets": [{"textParagraph": {"text": f"<i>Rocketlane PS Ops {DASH} {NOW.strftime('%H:%M')} {DASH} {today_str}</i>"}}]
    })

    return {
        "cardsV2": [{
            "cardId": "ps-daily-digest",
            "card": {
                "header": {
                    "title": cfg["label"] + " Daily Update",
                    "subtitle": today_str,
                    "imageUrl": "https://fonts.gstatic.com/s/i/short-term/release/googlesymbols/monitoring/default/48px.svg",
                    "imageType": "CIRCLE"
                },
                "sections": sections
            }
        }]
    }


def _health_rank(h):
    """Numeric rank for health values (higher = better)."""
    return {"green": 3, "yellow": 2, "red": 1}.get((h or "").lower(), 0)


def post_chat_card(card, dry_run=False):
    """Post a card to Google Chat via webhook."""
    if dry_run:
        print(f"\n[DRY RUN] Chat card: {len(json.dumps(card))} bytes")
        return

    data = json.dumps(card).encode("utf-8")
    req = urllib.request.Request(
        GCHAT_WEBHOOK_URL, data=data,
        headers={"Content-Type": "application/json; charset=UTF-8"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"Chat card posted: HTTP {resp.status}")


# ═══════════════════════════════════════════════════════════════════════════════
# PDF GENERATION
# ═══════════════════════════════════════════════════════════════════════════════
def generate_pdf(html_body):
    """Convert HTML email body to PDF bytes using WeasyPrint.
    Returns PDF bytes or None if WeasyPrint unavailable/fails."""
    if not HAS_WEASYPRINT:
        print("  WeasyPrint not installed — skipping PDF generation.")
        return None

    try:
        # Add print-friendly CSS overrides for PDF rendering
        pdf_css = """
        <style>
        @page {
            size: A4;
            margin: 1.5cm;
        }
        body {
            font-size: 12px !important;
            max-width: 100% !important;
            margin: 0 !important;
        }
        /* Prevent sections from breaking across pages */
        div[style*="background:#f8fafc"] {
            page-break-inside: avoid;
        }
        /* Ensure tables don't break mid-row */
        tr {
            page-break-inside: avoid;
        }
        </style>
        """
        # Inject print CSS into the HTML head
        pdf_html = html_body.replace("</head>", f"{pdf_css}</head>")

        print("  Generating PDF from HTML...")
        pdf_bytes = WeasyHTML(string=pdf_html).write_pdf()
        print(f"  PDF generated: {len(pdf_bytes):,} bytes ({len(pdf_bytes)/1024:.0f} KB)")
        return pdf_bytes
    except Exception as e:
        print(f"  PDF generation failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL SENDING
# ═══════════════════════════════════════════════════════════════════════════════
def send_email(subject, html_body, dry_run=False, pdf_bytes=None, pdf_filename=None):
    """Send HTML email via Gmail, optionally with a PDF attachment."""
    all_recipients = list(dict.fromkeys(EXTRA_RECIPIENTS + [GMAIL_ADDRESS]))

    if dry_run:
        print(f"\n[DRY RUN] Email subject: {subject}")
        print(f"[DRY RUN] To: {', '.join(all_recipients)}")
        print(f"[DRY RUN] Body length: {len(html_body)} chars")
        if pdf_bytes:
            print(f"[DRY RUN] PDF attachment: {pdf_filename} ({len(pdf_bytes):,} bytes)")
        return

    # Use mixed multipart so we can have HTML body + attachment
    msg = MIMEMultipart("mixed")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ", ".join(all_recipients)
    msg["Subject"] = subject

    # HTML body as an alternative part
    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(html_body, "html"))
    msg.attach(body_part)

    # Attach PDF if provided
    if pdf_bytes and pdf_filename:
        pdf_part = MIMEBase("application", "pdf")
        pdf_part.set_payload(pdf_bytes)
        encoders.encode_base64(pdf_part)
        pdf_part.add_header("Content-Disposition", f'attachment; filename="{pdf_filename}"')
        msg.attach(pdf_part)
        print(f"  PDF attached: {pdf_filename}")

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
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    parser.add_argument("--mode", choices=["email", "chat", "both"], default="email",
                        help="Output mode: email, chat (Google Chat card), or both")
    parser.add_argument("--scope", choices=list(SCOPE_CONFIG.keys()), default="ps",
                        help="Scope: ps (eDiscovery/Data PSG/Post Impl) or forensics")
    parser.add_argument("--output", metavar="PATH",
                        help="Emit digest_data JSON to PATH and exit; skip HTML/email/chat. "
                             "When set, only ROCKETLANE_API_KEY is required.")
    args = parser.parse_args()

    # Apply scope configuration to module-level globals
    global SCOPE, DIRECTORS, DIRECTOR_NAMES, EXTRA_RECIPIENTS, SNAPSHOT_DIR
    SCOPE = args.scope
    cfg = SCOPE_CONFIG[SCOPE]
    DIRECTORS = cfg["directors"]
    DIRECTOR_NAMES = cfg["director_names"]
    EXTRA_RECIPIENTS = cfg["extra_recipients"]
    if cfg["snapshot_suffix"]:
        SNAPSHOT_DIR = Path(__file__).parent / f".snapshots{cfg['snapshot_suffix']}"

    if not API_KEY:
        print("ERROR: ROCKETLANE_API_KEY not set")
        sys.exit(1)
    if not args.output and not args.dry_run:
        if args.mode in ("email", "both") and (not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD):
            print("ERROR: GMAIL_ADDRESS and GMAIL_APP_PASSWORD required for email mode")
            sys.exit(1)
        if args.mode in ("chat", "both") and not GCHAT_WEBHOOK_URL:
            print("ERROR: GCHAT_WEBHOOK_URL required for chat mode")
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

    # Project progress is already on the bulk project payload as
    # `progressPercentage` (set on each enriched dict as `task_progress` in
    # enrich_project). No per-project task fetch needed — saves ~108
    # sequential API calls and 15-20 minutes of runtime.
    #
    # Safety override: force Completed/Closeout to 100, in case the API
    # value lags (closeout doesn't always immediately roll all tasks to done).
    for p in all_enriched:
        if p["status"] in ("Completed", "Closeout") and (p.get("task_progress") or 0) < 100:
            p["task_progress"] = 100
    n_with_progress = sum(1 for p in all_enriched if p.get("task_progress") is not None)
    print(f"Project progress: {n_with_progress}/{len(all_enriched)} projects have progressPercentage.")

    # Find stale projects (active but no time logged in 7 days)
    print("Detecting stale projects...")
    stale_projects, time_entries_7d = find_stale_projects(all_enriched)
    print(f"Found {len(stale_projects)} stale projects (no time entries in 7 days).")

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

    # JSON-only path — emit digest_data.json for downstream consumers (Routine
    # session, etc.) and skip HTML/email/chat rendering entirely. Aggressively
    # slim the payload: roll up time entries to per-project totals, truncate
    # PM notes on non-attention projects (only red/yellow/escalation-flagged
    # need full text — those are what action/hotspot rules fire on).
    if args.output:
        team_by_pid = {p["id"]: team for team, projs in projects_by_team.items() for p in projs}
        hours_by_pid, time_summary = aggregate_time_entries(time_entries_7d)
        zero_rollup = {"hours": 0.0, "billable_hours": 0.0, "entry_count": 0}

        for p in all_enriched:
            p["team"] = team_by_pid.get(p["id"], "")
            r = hours_by_pid.get(p["id"], zero_rollup)
            p["hours_logged_7d"] = r["hours"]
            p["billable_hours_7d"] = r["billable_hours"]
            p["entry_count_7d"] = r["entry_count"]
            needs_full_notes = (p["health"] in ("red", "yellow")) or bool(p["escalation_flags"])
            if not needs_full_notes:
                for k in ("health_notes", "weekly_status"):
                    v = p.get(k, "") or ""
                    if len(v) > 200:
                        p[k] = v[:200].rstrip() + "…"

        active = [p for p in all_enriched if p["status_val"] in ACTIVE_STATUS_VALUES]
        kpis = {
            "total_active": len(active),
            "red_health": sum(1 for p in active if p["health"] == "red"),
            "yellow_health": sum(1 for p in active if p["health"] == "yellow"),
            "green_health": sum(1 for p in active if p["health"] == "green"),
            "no_health": sum(1 for p in active if not p["health"]),
            "new_24h": len(new_projects),
            "stale_count": len(stale_projects),
            "snapshot_diff_count": len(changes),
        }

        # Pre-curate action and hotspot candidates deterministically. Routine
        # session reads these and only does the writing/judgment work.
        # Rule definitions mirror routine_prompt.md — change both in lockstep.
        candidates = build_candidate_lists(all_enriched, NOW)
        print(
            f"Candidate actions: {len(candidates['candidate_actions'])} "
            f"({sum(1 for a in candidates['candidate_actions'] if 'red_escalation' in a['rule_reasons'])} red, "
            f"{sum(1 for a in candidates['candidate_actions'] if 'z2e_phase2_laggard' in a['rule_reasons'])} z2e, "
            f"{sum(1 for a in candidates['candidate_actions'] if 'review_module_blocker' in a['rule_reasons'])} review). "
            f"Hotspots: {len(candidates['candidate_hotspots'])}."
        )

        output_data = {
            "scope": SCOPE,
            "generated_at": NOW.isoformat(),
            "today_str": digest_data["today_str"],
            "has_prior_snapshot": has_prior_snapshot,
            "kpis": kpis,
            "projects": all_enriched,
            "new_projects": new_projects,
            "stale_projects": stale_projects,
            "time_summary_7d": time_summary,
            "snapshot_diff": changes,
            "candidate_actions": candidates["candidate_actions"],
            "candidate_hotspots": candidates["candidate_hotspots"],
        }

        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(output_data, f, default=str, indent=2)
        print(f"\nWrote digest data to {out_path} ({out_path.stat().st_size:,} bytes).")

        print("Saving project state snapshot...")
        save_snapshot(projects_by_id)
        print("Done.")
        return

    # Build and send outputs
    mode = args.mode

    if mode in ("email", "both"):
        print("\nBuilding email...")
        html = build_email_html(digest_data)
        subject = f"{cfg['email_subject_prefix']} {DASH} {NOW.strftime('%b %d, %Y')}"
        print(f"Subject: {subject}")

        # Generate PDF attachment
        pdf_bytes = generate_pdf(html)
        pdf_filename = f"{cfg['email_subject_prefix'].replace(' ', '_')}_{NOW.strftime('%Y-%m-%d')}.pdf"

        send_email(subject, html, dry_run=args.dry_run,
                   pdf_bytes=pdf_bytes, pdf_filename=pdf_filename)

    if mode in ("chat", "both"):
        print("\nBuilding Google Chat card...")
        card = build_chat_card(digest_data)
        post_chat_card(card, dry_run=args.dry_run)

    # Save snapshot
    print("Saving project state snapshot...")
    save_snapshot(projects_by_id)
    print("Done.")


if __name__ == "__main__":
    main()
