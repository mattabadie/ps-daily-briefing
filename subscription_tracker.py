#!/usr/bin/env python3
"""
Subscription Consumption Tracker
Monitors post-implementation "bucket of hours" subscription projects.
When consumption hits threshold (default 75%), emails a renewal prep package
to the AE and CSM with usage data, history, and account context.

Usage:
  python subscription_tracker.py                      # Default 75% threshold
  python subscription_tracker.py --threshold 50       # Custom threshold
  python subscription_tracker.py --dry-run             # Preview without sending
  python subscription_tracker.py --force-all           # Email for ALL projects (testing)

Env vars:
  ROCKETLANE_API_KEY   — Rocketlane API key (required)
  GMAIL_ADDRESS        — Gmail sender address (required)
  GMAIL_APP_PASSWORD   — Gmail app password (required)
"""

import argparse
import json
import os
import re
import smtplib
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("ROCKETLANE_API_KEY", "")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
BASE_URL = "https://services.api.exterro.com/api/1.0"
RL_APP_BASE = "https://services.exterro.com/projects"

ORONDE_ID = 393607  # Post Implementation director
ACTIVE_STATUS_VALUES = {2, 4, 5, 6, 9, 12, 14, 15}
NOW = datetime.now()
DASH = "\u2014"
DEFAULT_THRESHOLD = 75  # percent


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


def fetch_project_detail(pid):
    """Fetch individual project detail (includes financials not in list endpoint)."""
    return api_get(f"projects/{pid}")


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


# ═══════════════════════════════════════════════════════════════════════════════
# SUBSCRIPTION DATA EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════
def is_post_impl_project(p):
    """Check if project belongs to Oronde's Post Implementation team."""
    member_ids = {m.get("userId") for m in p.get("teamMembers", {}).get("members", [])}
    owner_id = p.get("owner", {}).get("userId")
    return ORONDE_ID in member_ids or ORONDE_ID == owner_id


def is_active_subscription(p):
    """Check if project is active and type = Subscription."""
    sv = p.get("status", {}).get("value")
    if sv not in ACTIVE_STATUS_VALUES:
        return False
    project_type = get_field(p, "Project Type") or ""
    return project_type.lower() == "subscription"


def extract_subscription_data(p):
    """Pull subscription contract financials and opp fields from a project.
    Fetches individual project detail to get financials (not in list endpoint)."""
    pid = p.get("projectId", "")

    # Fetch detail for financials (list endpoint doesn't include them)
    try:
        detail = fetch_project_detail(pid)
    except Exception as e:
        print(f"    WARNING: Could not fetch detail for {pid}: {e}")
        detail = p

    # Merge: use detail for financials, but keep list-level fields as fallback
    financials = detail.get("financials", {}) or {}
    contract_type = financials.get("contractType", "UNKNOWN") or "UNKNOWN"
    sub_contract = financials.get("subscriptionContract", {}) or {}
    tm_contract = financials.get("timeAndMaterialContract", {}) or {}
    tm_budget_dollars = tm_contract.get("projectBudget", 0) or 0

    period_minutes = sub_contract.get("periodMinutes", 0) or 0
    no_of_periods = sub_contract.get("noOfPeriods", 0) or 0
    period_budget = sub_contract.get("periodBudget", 0) or 0
    frequency = sub_contract.get("subscriptionFrequency", "") or ""
    start_date_str = sub_contract.get("subscriptionStartDate", "") or ""

    total_budgeted_minutes = period_minutes * no_of_periods if period_minutes and no_of_periods else 0
    total_budgeted_hours = total_budgeted_minutes / 60 if total_budgeted_minutes else 0

    # Parse subscription start date
    start_date = None
    if start_date_str:
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
            try:
                start_date = datetime.strptime(start_date_str.split("T")[0], "%Y-%m-%d")
                break
            except ValueError:
                continue

    # Calculate subscription end date
    end_date = None
    if start_date and no_of_periods and frequency:
        freq_lower = frequency.lower()
        if "year" in freq_lower:
            end_date = start_date.replace(year=start_date.year + no_of_periods)
        elif "quarter" in freq_lower:
            end_date = start_date + timedelta(days=90 * no_of_periods)
        elif "month" in freq_lower:
            end_date = start_date + timedelta(days=30 * no_of_periods)

    # Opp fields for contacts
    owner = p.get("owner", {})
    owner_name = f'{owner.get("firstName", "")} {owner.get("lastName", "")}'.strip()
    customer = p.get("customer", {}).get("companyName", "N/A")

    return {
        "project_id": pid,
        "project_name": p.get("projectName", "?"),
        "customer": customer,
        "pm_name": owner_name,
        "status": p.get("status", {}).get("label", "Unknown"),
        "health": (get_field(p, "Red/Yellow/Green Health") or "").strip().lower(),
        "health_notes": strip_html(get_field(p, "Internal Project Health Notes") or ""),
        "weekly_status": strip_html(get_field(p, "Internal Weekly Status") or ""),
        "project_type": get_field(p, "Project Type") or "",
        "contract_type": contract_type,
        "needs_correction": contract_type not in ("SUBSCRIPTION", "UNKNOWN"),
        # Subscription contract
        "period_minutes": period_minutes,
        "no_of_periods": no_of_periods,
        "period_budget_dollars": period_budget,
        "tm_budget_dollars": tm_budget_dollars,
        "frequency": frequency,
        "start_date": start_date,
        "end_date": end_date,
        "total_budgeted_hours": total_budgeted_hours,
        # Domain & Opp fields
        "service_hours_domains": get_field(p, "Opp: Service Hours Domain(s)") or "",
        "service_subtotal": get_field(p, "Opp: Service Subtotal") or "",
        "account_owner": get_field(p, "Opp: Account Owner") or "",
        "opp_owner": get_field(p, "Opp: Opportunity Owner") or "",
        "opp_owner_email": get_field(p, "Opp: Opportunity Owner Email") or "",
        "primary_contact_name": get_field(p, "Opp: Primary Customer Contact Name") or "",
        "primary_contact_email": get_field(p, "Opp: Primary Customer Contact Email") or "",
        "client_segment": get_field(p, "Opp: Client Segmentation") or "",
        "opp_start_date": get_field(p, "Opp: Opportunity Start Date") or "",
        "opp_end_date": get_field(p, "Opp: Opportunity End Date") or "",
        "opp_url": get_field(p, "Opp: Opportunity URL") or "",
        "opp_type": get_field(p, "Opp: Opportunity Type") or "",
        # Raw project ref for finding sibling projects
        "_raw": p,
    }


def compute_consumption(sub_data, time_entries):
    """Calculate consumption metrics from time entries against budget."""
    total_minutes_used = sum(e.get("minutes", 0) or 0 for e in time_entries)
    total_hours_used = total_minutes_used / 60
    budgeted = sub_data["total_budgeted_hours"]

    pct = (total_hours_used / budgeted * 100) if budgeted > 0 else 0

    # Monthly burn analysis (last 6 months)
    monthly_hours = defaultdict(float)
    for e in time_entries:
        date_str = e.get("date", "")
        if date_str:
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d")
                key = d.strftime("%Y-%m")
                monthly_hours[key] += (e.get("minutes", 0) or 0) / 60
            except ValueError:
                pass

    # Recent 3 months burn rate
    recent_months = sorted(monthly_hours.keys())[-3:]
    recent_hours = sum(monthly_hours[m] for m in recent_months)
    avg_monthly_burn = recent_hours / len(recent_months) if recent_months else 0

    # Estimate months remaining at current burn rate
    remaining_hours = max(0, budgeted - total_hours_used)
    months_remaining = remaining_hours / avg_monthly_burn if avg_monthly_burn > 0 else float("inf")

    # Recent time entry details (last 30 days)
    cutoff = NOW - timedelta(days=30)
    recent_entries = []
    for e in time_entries:
        date_str = e.get("date", "")
        if date_str:
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d")
                if d >= cutoff:
                    user = e.get("user", {})
                    user_name = f'{user.get("firstName", "")} {user.get("lastName", "")}'.strip()
                    task_name = e.get("task", {}).get("taskName", "") if e.get("task") else ""
                    recent_entries.append({
                        "date": date_str,
                        "user": user_name,
                        "hours": round((e.get("minutes", 0) or 0) / 60, 1),
                        "task": task_name,
                        "notes": strip_html(e.get("description", "") or ""),
                    })
            except ValueError:
                pass

    recent_entries.sort(key=lambda x: x["date"], reverse=True)

    return {
        "total_hours_used": round(total_hours_used, 1),
        "total_budgeted_hours": round(budgeted, 1),
        "pct_consumed": round(pct, 1),
        "remaining_hours": round(remaining_hours, 1),
        "monthly_hours": dict(sorted(monthly_hours.items())),
        "avg_monthly_burn": round(avg_monthly_burn, 1),
        "months_remaining": round(months_remaining, 1) if months_remaining != float("inf") else None,
        "recent_entries": recent_entries[:20],  # cap at 20
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FIND SIBLING PROJECTS (same customer)
# ═══════════════════════════════════════════════════════════════════════════════
def find_sibling_projects(sub_data, all_projects):
    """Find other active projects for the same customer."""
    customer_name = sub_data["customer"]
    current_pid = sub_data["project_id"]
    siblings = []
    for p in all_projects:
        pid = p.get("projectId", "")
        if pid == current_pid:
            continue
        cust = p.get("customer", {}).get("companyName", "")
        if cust == customer_name:
            sv = p.get("status", {}).get("value")
            if sv in ACTIVE_STATUS_VALUES:
                owner = p.get("owner", {})
                owner_name = f'{owner.get("firstName", "")} {owner.get("lastName", "")}'.strip()
                siblings.append({
                    "id": pid,
                    "name": p.get("projectName", "?"),
                    "status": p.get("status", {}).get("label", "Unknown"),
                    "owner": owner_name,
                    "type": get_field(p, "Project Type") or "",
                })
    return siblings


# ═══════════════════════════════════════════════════════════════════════════════
# HTML EMAIL BUILDER — RENEWAL PREP PACKAGE
# ═══════════════════════════════════════════════════════════════════════════════
def build_renewal_email(sub_data, consumption, siblings):
    """Build a rich HTML renewal prep package email."""
    pct = consumption["pct_consumed"]
    used = consumption["total_hours_used"]
    budget = consumption["total_budgeted_hours"]
    remaining = consumption["remaining_hours"]
    burn = consumption["avg_monthly_burn"]
    months_left = consumption["months_remaining"]

    # Urgency color
    if pct >= 90:
        urgency_color = "#ef4444"
        urgency_label = "CRITICAL"
        urgency_bg = "#fef2f2"
    elif pct >= 75:
        urgency_color = "#f59e0b"
        urgency_label = "ACTION NEEDED"
        urgency_bg = "#fffbeb"
    else:
        urgency_color = "#3b82f6"
        urgency_label = "MONITORING"
        urgency_bg = "#eff6ff"

    # Bar width (cap at 100%)
    bar_pct = min(pct, 100)
    bar_color = urgency_color

    project_url = f"{RL_APP_BASE}/{sub_data['project_id']}"
    opp_link = ""
    if sub_data["opp_url"]:
        opp_link = f'<a href="{sub_data["opp_url"]}" style="color:#3b82f6;">View Opportunity</a>'

    html = f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color:#1e293b; max-width:720px; margin:0 auto; padding:20px; background:#f8fafc; }}
.header {{ background: linear-gradient(135deg, #1e293b 0%, #334155 100%); color:white; padding:24px 28px; border-radius:12px 12px 0 0; }}
.header h1 {{ margin:0 0 4px 0; font-size:20px; font-weight:600; }}
.header .sub {{ color:#94a3b8; font-size:13px; }}
.body {{ background:white; padding:24px 28px; border:1px solid #e2e8f0; border-top:none; border-radius:0 0 12px 12px; }}
.urgency {{ display:inline-block; padding:3px 10px; border-radius:4px; font-weight:700; font-size:12px; letter-spacing:0.5px; }}
.section {{ margin:20px 0; }}
.section h2 {{ font-size:15px; font-weight:600; color:#475569; margin:0 0 10px 0; border-bottom:2px solid #e2e8f0; padding-bottom:6px; }}
.metric-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap:12px; margin:12px 0; }}
.metric {{ background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:12px; text-align:center; }}
.metric .value {{ font-size:22px; font-weight:700; color:#1e293b; }}
.metric .label {{ font-size:11px; color:#64748b; margin-top:2px; text-transform:uppercase; letter-spacing:0.5px; }}
.bar-container {{ background:#e2e8f0; border-radius:6px; height:24px; margin:8px 0; overflow:hidden; position:relative; }}
.bar-fill {{ height:100%; border-radius:6px; transition: width 0.3s; }}
.bar-text {{ position:absolute; right:8px; top:3px; font-size:12px; font-weight:600; color:#1e293b; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ text-align:left; padding:6px 8px; background:#f1f5f9; color:#475569; font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px; }}
td {{ padding:6px 8px; border-bottom:1px solid #f1f5f9; }}
.info-row {{ display:flex; justify-content:space-between; padding:4px 0; font-size:13px; border-bottom:1px solid #f8fafc; }}
.info-label {{ color:#64748b; }}
.info-value {{ font-weight:500; }}
.footer {{ text-align:center; padding:16px; font-size:11px; color:#94a3b8; }}
a {{ color:#3b82f6; text-decoration:none; }}
</style></head><body>

<div class="header">
  <h1>Subscription Renewal Package</h1>
  <div class="sub">{sub_data["customer"]} {DASH} {sub_data["project_name"]}</div>
</div>

<div class="body">

<!-- URGENCY BANNER -->
<div style="background:{urgency_bg}; border:1px solid {urgency_color}30; border-radius:8px; padding:14px 16px; margin-bottom:20px;">
  <span class="urgency" style="background:{urgency_color}; color:white;">{urgency_label}</span>
  <span style="margin-left:10px; font-size:14px;">
    <strong>{pct}%</strong> of budgeted hours consumed ({used}h of {budget}h)
  </span>
</div>

<!-- CONSUMPTION DASHBOARD -->
<div class="section">
  <h2>Consumption Dashboard</h2>
  <div class="metric-grid">
    <div class="metric"><div class="value">{used}h</div><div class="label">Hours Used</div></div>
    <div class="metric"><div class="value">{budget}h</div><div class="label">Hours Budgeted</div></div>
    <div class="metric"><div class="value">{remaining}h</div><div class="label">Hours Remaining</div></div>
    <div class="metric"><div class="value">{burn}h</div><div class="label">Avg Monthly Burn</div></div>
  </div>
  <div class="bar-container">
    <div class="bar-fill" style="width:{bar_pct}%; background:{bar_color};"></div>
    <div class="bar-text">{pct}%</div>
  </div>'''

    if months_left is not None:
        if months_left < 1:
            runway_text = f'<strong style="color:{urgency_color};">Less than 1 month</strong> of hours remaining at current burn rate'
        else:
            runway_text = f'<strong>{months_left} months</strong> of hours remaining at current burn rate'
        html += f'\n  <p style="font-size:13px; color:#64748b; margin:8px 0 0 0;">{runway_text}</p>'

    html += '\n</div>'

    # MONTHLY BURN CHART (text-based sparkline table)
    monthly = consumption["monthly_hours"]
    if monthly:
        html += '''
<div class="section">
  <h2>Monthly Usage Trend</h2>
  <table>
    <tr><th>Month</th><th>Hours</th><th style="width:60%;">Usage</th></tr>'''
        max_monthly = max(monthly.values()) if monthly.values() else 1
        for month_key, hours in list(monthly.items())[-12:]:  # last 12 months
            bar_w = (hours / max_monthly * 100) if max_monthly > 0 else 0
            html += f'''
    <tr>
      <td>{month_key}</td>
      <td style="font-weight:500;">{round(hours, 1)}h</td>
      <td><div style="background:#3b82f6; height:14px; width:{bar_w}%; border-radius:3px; min-width:2px;"></div></td>
    </tr>'''
        html += '\n  </table>\n</div>'

    # RECENT TIME ENTRIES
    recent = consumption["recent_entries"]
    if recent:
        html += '''
<div class="section">
  <h2>Recent Activity (Last 30 Days)</h2>
  <table>
    <tr><th>Date</th><th>Consultant</th><th>Hours</th><th>Task</th></tr>'''
        for e in recent[:15]:
            task_short = (e["task"][:50] + "...") if len(e["task"]) > 50 else e["task"]
            html += f'''
    <tr>
      <td>{e["date"]}</td>
      <td>{e["user"]}</td>
      <td>{e["hours"]}h</td>
      <td>{task_short}</td>
    </tr>'''
        html += '\n  </table>\n</div>'

    # CONTRACT & ACCOUNT INFO
    html += f'''
<div class="section">
  <h2>Contract &amp; Account Details</h2>
  <div class="info-row"><span class="info-label">Customer</span><span class="info-value">{sub_data["customer"]}</span></div>
  <div class="info-row"><span class="info-label">Project</span><span class="info-value"><a href="{project_url}">{sub_data["project_name"]}</a></span></div>
  <div class="info-row"><span class="info-label">PM</span><span class="info-value">{sub_data["pm_name"]}</span></div>
  <div class="info-row"><span class="info-label">Status</span><span class="info-value">{sub_data["status"]}</span></div>
  <div class="info-row"><span class="info-label">Subscription Type</span><span class="info-value">{sub_data["frequency"]} &times; {sub_data["no_of_periods"]} periods</span></div>'''

    if sub_data["start_date"]:
        html += f'\n  <div class="info-row"><span class="info-label">Subscription Start</span><span class="info-value">{sub_data["start_date"].strftime("%b %d, %Y")}</span></div>'
    if sub_data["end_date"]:
        html += f'\n  <div class="info-row"><span class="info-label">Subscription End</span><span class="info-value">{sub_data["end_date"].strftime("%b %d, %Y")}</span></div>'
    if sub_data["period_budget_dollars"]:
        html += f'\n  <div class="info-row"><span class="info-label">Period Budget ($)</span><span class="info-value">${sub_data["period_budget_dollars"]:,.2f}</span></div>'
    if sub_data["service_subtotal"]:
        html += f'\n  <div class="info-row"><span class="info-label">Service Subtotal</span><span class="info-value">{sub_data["service_subtotal"]}</span></div>'
    if sub_data["client_segment"]:
        html += f'\n  <div class="info-row"><span class="info-label">Client Segmentation</span><span class="info-value">{sub_data["client_segment"]}</span></div>'
    if sub_data["opp_type"]:
        html += f'\n  <div class="info-row"><span class="info-label">Opportunity Type</span><span class="info-value">{sub_data["opp_type"]}</span></div>'
    if opp_link:
        html += f'\n  <div class="info-row"><span class="info-label">Opportunity</span><span class="info-value">{opp_link}</span></div>'

    html += '\n</div>'

    # KEY CONTACTS
    html += '''
<div class="section">
  <h2>Key Contacts</h2>'''
    contacts = []
    if sub_data["opp_owner"]:
        contacts.append(("Account Executive", sub_data["opp_owner"], sub_data["opp_owner_email"]))
    if sub_data["account_owner"]:
        contacts.append(("Account Owner", sub_data["account_owner"], ""))
    if sub_data["primary_contact_name"]:
        contacts.append(("Primary Customer Contact", sub_data["primary_contact_name"], sub_data["primary_contact_email"]))
    if sub_data["pm_name"]:
        contacts.append(("Project Manager", sub_data["pm_name"], ""))

    if contacts:
        for role, name, email in contacts:
            email_link = f' (<a href="mailto:{email}">{email}</a>)' if email else ""
            html += f'\n  <div class="info-row"><span class="info-label">{role}</span><span class="info-value">{name}{email_link}</span></div>'
    html += '\n</div>'

    # SIBLING PROJECTS (other active projects for same customer)
    if siblings:
        html += f'''
<div class="section">
  <h2>Other Active Projects for {sub_data["customer"]}</h2>
  <table>
    <tr><th>Project</th><th>Type</th><th>Status</th><th>Owner</th></tr>'''
        for s in siblings:
            sib_url = f"{RL_APP_BASE}/{s['id']}"
            html += f'''
    <tr>
      <td><a href="{sib_url}">{s["name"]}</a></td>
      <td>{s["type"]}</td>
      <td>{s["status"]}</td>
      <td>{s["owner"]}</td>
    </tr>'''
        html += '\n  </table>\n</div>'

    # Health notes if present
    if sub_data["health_notes"] or sub_data["weekly_status"]:
        html += '\n<div class="section">\n  <h2>Project Notes</h2>'
        if sub_data["health_notes"]:
            html += f'\n  <p style="font-size:13px; margin:4px 0;"><strong>Health Notes:</strong> {sub_data["health_notes"][:500]}</p>'
        if sub_data["weekly_status"]:
            html += f'\n  <p style="font-size:13px; margin:4px 0;"><strong>Weekly Status:</strong> {sub_data["weekly_status"][:500]}</p>'
        html += '\n</div>'

    html += f'''
</div>

<div class="footer">
  Auto-generated Subscription Tracker &middot; {NOW.strftime("%b %d, %Y %H:%M")} &middot; Rocketlane API
</div>

</body></html>'''

    return html


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ═══════════════════════════════════════════════════════════════════════════════
def send_renewal_email(sub_data, html_body, dry_run=False):
    """Send the renewal package to AE, CSM, and VP."""
    recipients = set()

    # AE (Opportunity Owner)
    ae_email = sub_data.get("opp_owner_email", "").strip()
    if ae_email and "@" in ae_email:
        recipients.add(ae_email)

    # Primary customer contact email (CSM proxy if available)
    # NOTE: We CC the VP (GMAIL_ADDRESS) always
    # For now, we send to AE + CC VP. CSM field may need to be added in RL.

    # Always include VP
    recipients.add(GMAIL_ADDRESS)

    to_list = ", ".join(sorted(recipients))
    subject = f"Subscription Renewal Alert: {sub_data['customer']} {DASH} {sub_data['pct_consumed']}% consumed"

    if dry_run:
        print(f"  [DRY RUN] Would send to: {to_list}")
        print(f"  [DRY RUN] Subject: {subject}")
        return

    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = to_list
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)
    print(f"  Email sent to: {to_list}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Subscription Consumption Tracker")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                        help=f"Consumption threshold %% to trigger alert (default: {DEFAULT_THRESHOLD})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview which projects would trigger, without sending emails")
    parser.add_argument("--force-all", action="store_true",
                        help="Process ALL subscription projects regardless of threshold (testing)")
    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: ROCKETLANE_API_KEY not set"); sys.exit(1)
    if not args.dry_run and (not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD):
        print("ERROR: GMAIL_ADDRESS and GMAIL_APP_PASSWORD required"); sys.exit(1)

    print("=" * 60)
    print(f"Subscription Consumption Tracker")
    print(f"Threshold: {args.threshold}% | Dry run: {args.dry_run}")
    print("=" * 60)

    # 1. Fetch all projects
    print("Fetching all projects...")
    all_projects = fetch_all_projects()
    print(f"  {len(all_projects)} total projects")

    # 2. Filter to active subscriptions under Post Implementation
    subs = [p for p in all_projects if is_post_impl_project(p) and is_active_subscription(p)]
    print(f"  {len(subs)} active subscription projects under Post Implementation")

    # 3. Extract subscription data (fetches per-project detail for financials)
    print(f"Extracting subscription data ({len(subs)} projects, fetching details)...")
    sub_data_list = []
    for i, p in enumerate(subs):
        if i > 0:
            time.sleep(3)  # Rate limit protection for detail fetches
        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(subs)}...")
        sub_data_list.append(extract_subscription_data(p))

    # Flag projects with wrong contract type (T&M, FIXED_FEE, NON_BILLABLE, etc.)
    needs_fix = [s for s in sub_data_list if s["needs_correction"]]
    if needs_fix:
        print(f"\n  WARNING: {len(needs_fix)} projects have incorrect contract type (should be SUBSCRIPTION):")
        for s in needs_fix:
            extra = ""
            if s["tm_budget_dollars"]:
                extra = f" (T&M budget: ${s['tm_budget_dollars']:,.0f})"
            print(f"    - [{s['contract_type']}] {s['customer']}: {s['project_name']}{extra}")
        print("    These need to be corrected in Rocketlane to SUBSCRIPTION with hours budget.\n")

    # Filter to those with budget data
    with_budget = [s for s in sub_data_list if s["total_budgeted_hours"] > 0]
    no_budget = [s for s in sub_data_list if s["total_budgeted_hours"] == 0 and not s["needs_correction"]]
    print(f"  {len(with_budget)} have subscription budget, {len(needs_fix)} need contract type fix, {len(no_budget)} missing budget")

    if no_budget:
        print("  Projects missing budget data:")
        for s in no_budget[:10]:
            print(f"    - [{s['contract_type']}] {s['customer']}: {s['project_name']}")
        if len(no_budget) > 10:
            print(f"    ... and {len(no_budget) - 10} more")

    # 4. Fetch time entries and compute consumption (parallel)
    print(f"Computing consumption for {len(with_budget)} projects...")
    results = []

    def _process(sub_d):
        entries = fetch_time_entries_for_project(sub_d["project_id"])
        consumption = compute_consumption(sub_d, entries)
        sub_d["pct_consumed"] = consumption["pct_consumed"]
        siblings = find_sibling_projects(sub_d, all_projects)
        return sub_d, consumption, siblings

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(_process, s): s for s in with_budget}
        done = 0
        for f in as_completed(futures):
            done += 1
            if done % 10 == 0:
                print(f"  {done}/{len(with_budget)}...")
            try:
                results.append(f.result())
            except Exception as e:
                print(f"  Error: {e}")

    # Sort by consumption % descending
    results.sort(key=lambda x: -x[1]["pct_consumed"])

    # 5. Filter by threshold
    if args.force_all:
        triggered = results
    else:
        triggered = [(s, c, sib) for s, c, sib in results if c["pct_consumed"] >= args.threshold]

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {len(triggered)} projects at or above {args.threshold}% consumption")
    print(f"{'=' * 60}")

    for sub_d, consumption, siblings in triggered:
        pct = consumption["pct_consumed"]
        used = consumption["total_hours_used"]
        budget = consumption["total_budgeted_hours"]
        print(f"\n  {sub_d['customer']}: {sub_d['project_name']}")
        print(f"    {pct}% consumed ({used}h / {budget}h)")
        print(f"    Burn: {consumption['avg_monthly_burn']}h/mo | Remaining: {consumption['remaining_hours']}h")
        if consumption["months_remaining"]:
            print(f"    Runway: ~{consumption['months_remaining']} months")
        print(f"    AE: {sub_d['opp_owner']} ({sub_d['opp_owner_email']})")
        print(f"    Siblings: {len(siblings)} other active projects")

    # 6. Send emails
    if triggered:
        print(f"\nSending {len(triggered)} renewal package emails...")
        for sub_d, consumption, siblings in triggered:
            html = build_renewal_email(sub_d, consumption, siblings)
            send_renewal_email(sub_d, html, dry_run=args.dry_run)
    else:
        print("\nNo projects above threshold. No emails to send.")

    # Summary
    print(f"\n{'=' * 60}")
    print("CONSUMPTION SUMMARY (all projects with budget):")
    print(f"{'=' * 60}")
    for sub_d, consumption, _ in results:
        pct = consumption["pct_consumed"]
        flag = " *** ALERT ***" if pct >= args.threshold else ""
        print(f"  {pct:6.1f}%  {sub_d['customer'][:30]:30s}  {sub_d['project_name'][:40]}{flag}")

    # Report projects needing contract type correction
    if needs_fix:
        print(f"\n{'=' * 60}")
        print(f"ACTION REQUIRED: {len(needs_fix)} projects need contract type correction")
        print(f"{'=' * 60}")
        print("These projects have Project Type = 'Subscription' but their")
        print("Rocketlane financials contract type is NOT set to SUBSCRIPTION.")
        print("They need to be updated in Rocketlane so hours budget can be tracked.\n")
        for s in needs_fix:
            extra = ""
            if s["tm_budget_dollars"]:
                extra = f"  |  T&M Budget: ${s['tm_budget_dollars']:,.0f}"
            print(f"  [{s['contract_type']:20s}] {s['customer'][:25]:25s}  {s['project_name'][:50]}{extra}")

    print(f"\nDone. Processed {len(results)} subscription projects.")


if __name__ == "__main__":
    main()
