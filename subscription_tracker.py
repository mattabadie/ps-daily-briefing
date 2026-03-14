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
    """Pull subscription contract financials and opp fields from a raw project."""
    pid = p.get("projectId", "")
    financials = p.get("financials", {}) or {}
    sub_contract = financials.get("subscriptionContract", {}) or {}

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
        # Subscription contract
        "period_minutes": period_minutes,
        "no_of_periods": no_of_periods,
        "period_budget_dollars": period_budget,
        "frequency": frequency,
        "start_date": start_date,
        "end_date": end_date,
        "total_budgeted_hours": total_budgeted_hours,
        # Opp fields
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


def build_summary_email(triggered_results, threshold):
    """Build a single summary HTML email listing all subscriptions that would trigger renewal alerts."""
    rows_html = ""
    detail_sections = ""

    for i, (sub_d, consumption, siblings) in enumerate(triggered_results, 1):
        pct = consumption["pct_consumed"]
        used = consumption["total_hours_used"]
        budget = consumption["total_budgeted_hours"]
        remaining = consumption["remaining_hours"]
        burn = consumption["avg_monthly_burn"]
        months_left = consumption["months_remaining"]

        # Urgency color
        if pct >= 90:
            color = "#ef4444"
            label = "CRITICAL"
        elif pct >= 75:
            color = "#f59e0b"
            label = "ACTION NEEDED"
        else:
            color = "#3b82f6"
            label = "MONITORING"

        bar_pct = min(pct, 100)

        # AE / CSM / recipients info
        ae = sub_d.get("opp_owner", "") or "N/A"
        ae_email = sub_d.get("opp_owner_email", "") or ""
        account_owner = sub_d.get("account_owner", "") or "N/A"
        primary_contact = sub_d.get("primary_contact_name", "") or "N/A"
        primary_contact_email = sub_d.get("primary_contact_email", "") or ""
        pm = sub_d.get("pm_name", "") or "N/A"

        # Build intended recipients list
        intended_recipients = []
        if ae_email and "@" in ae_email:
            intended_recipients.append(f"{ae} ({ae_email})")
        if GMAIL_ADDRESS:
            intended_recipients.append(GMAIL_ADDRESS)
        intended_str = ", ".join(intended_recipients) if intended_recipients else "N/A"

        project_url = f"{RL_APP_BASE}/{sub_d['project_id']}"

        # Summary table row
        rows_html += f'''
    <tr>
      <td style="font-weight:500;"><a href="{project_url}" style="color:#3b82f6;">{sub_d["customer"]}</a></td>
      <td>{sub_d["project_name"][:45]}</td>
      <td style="text-align:center;"><span style="background:{color}; color:white; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:700;">{pct}%</span></td>
      <td style="text-align:right;">{used}h / {budget}h</td>
      <td style="text-align:right;">{remaining}h</td>
      <td>{ae}</td>
    </tr>'''

        # Detailed section per project
        runway_str = f"{months_left} months" if months_left is not None else "N/A"
        if months_left is not None and months_left < 1:
            runway_str = '<span style="color:#ef4444;">Less than 1 month</span>'

        # Monthly trend mini-table
        monthly = consumption.get("monthly_hours", {})
        monthly_rows = ""
        if monthly:
            max_m = max(monthly.values()) if monthly.values() else 1
            for mk, mh in list(monthly.items())[-6:]:
                bw = (mh / max_m * 100) if max_m > 0 else 0
                monthly_rows += f'<tr><td>{mk}</td><td>{round(mh, 1)}h</td><td><div style="background:#3b82f6; height:12px; width:{bw}%; border-radius:2px; min-width:2px;"></div></td></tr>'

        monthly_html = ""
        if monthly_rows:
            monthly_html = f'''
      <div style="margin-top:10px;">
        <strong style="font-size:12px; color:#475569;">Monthly Usage (last 6 months)</strong>
        <table style="margin-top:4px;"><tr><th>Month</th><th>Hours</th><th style="width:50%;">Usage</th></tr>{monthly_rows}</table>
      </div>'''

        # Recent entries
        recent = consumption.get("recent_entries", [])
        recent_html = ""
        if recent:
            recent_rows = ""
            for e in recent[:10]:
                task_short = (e["task"][:40] + "...") if len(e["task"]) > 40 else e["task"]
                recent_rows += f'<tr><td>{e["date"]}</td><td>{e["user"]}</td><td>{e["hours"]}h</td><td>{task_short}</td></tr>'
            recent_html = f'''
      <div style="margin-top:10px;">
        <strong style="font-size:12px; color:#475569;">Recent Activity (Last 30 Days)</strong>
        <table style="margin-top:4px;"><tr><th>Date</th><th>Consultant</th><th>Hours</th><th>Task</th></tr>{recent_rows}</table>
      </div>'''

        # Siblings
        siblings_html = ""
        if siblings:
            sib_rows = ""
            for s in siblings:
                sib_url = f"{RL_APP_BASE}/{s['id']}"
                sib_rows += f'<tr><td><a href="{sib_url}" style="color:#3b82f6;">{s["name"]}</a></td><td>{s["type"]}</td><td>{s["status"]}</td><td>{s["owner"]}</td></tr>'
            siblings_html = f'''
      <div style="margin-top:10px;">
        <strong style="font-size:12px; color:#475569;">Other Active Projects for {sub_d["customer"]}</strong>
        <table style="margin-top:4px;"><tr><th>Project</th><th>Type</th><th>Status</th><th>Owner</th></tr>{sib_rows}</table>
      </div>'''

        # Health notes
        notes_html = ""
        if sub_d.get("health_notes") or sub_d.get("weekly_status"):
            notes_parts = []
            if sub_d["health_notes"]:
                notes_parts.append(f'<strong>Health Notes:</strong> {sub_d["health_notes"][:300]}')
            if sub_d["weekly_status"]:
                notes_parts.append(f'<strong>Weekly Status:</strong> {sub_d["weekly_status"][:300]}')
            notes_html = '<div style="margin-top:10px; font-size:12px; color:#64748b;">' + "<br>".join(notes_parts) + '</div>'

        opp_link = ""
        if sub_d.get("opp_url"):
            opp_link = f' | <a href="{sub_d["opp_url"]}" style="color:#3b82f6; font-size:12px;">View Opportunity</a>'

        detail_sections += f'''
<div style="background:white; border:1px solid #e2e8f0; border-radius:8px; padding:16px 20px; margin:16px 0;">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
    <div>
      <strong style="font-size:15px;">{i}. {sub_d["customer"]}</strong>
      <span style="color:#64748b; font-size:13px;"> {DASH} {sub_d["project_name"]}</span>
    </div>
    <span style="background:{color}; color:white; padding:3px 10px; border-radius:4px; font-size:12px; font-weight:700;">{label} {DASH} {pct}%</span>
  </div>

  <!-- Progress bar -->
  <div style="background:#e2e8f0; border-radius:6px; height:20px; overflow:hidden; position:relative; margin-bottom:12px;">
    <div style="height:100%; width:{bar_pct}%; background:{color}; border-radius:6px;"></div>
    <span style="position:absolute; right:8px; top:2px; font-size:11px; font-weight:600;">{used}h / {budget}h</span>
  </div>

  <!-- Metrics row -->
  <div style="display:flex; gap:16px; flex-wrap:wrap; margin-bottom:10px; font-size:13px;">
    <div><span style="color:#64748b;">Remaining:</span> <strong>{remaining}h</strong></div>
    <div><span style="color:#64748b;">Avg Burn:</span> <strong>{burn}h/mo</strong></div>
    <div><span style="color:#64748b;">Runway:</span> <strong>{runway_str}</strong></div>
    <div><span style="color:#64748b;">PM:</span> {pm}</div>
  </div>

  <!-- Intended recipients -->
  <div style="background:#eff6ff; border:1px solid #bfdbfe; border-radius:6px; padding:8px 12px; font-size:12px; margin-bottom:8px;">
    <strong style="color:#1e40af;">Intended Recipients:</strong> {intended_str}
  </div>

  <!-- Contacts -->
  <div style="font-size:12px; color:#475569; margin-bottom:4px;">
    <strong>AE:</strong> {ae}{f" ({ae_email})" if ae_email else ""} |
    <strong>Account Owner:</strong> {account_owner} |
    <strong>Customer Contact:</strong> {primary_contact}{f" ({primary_contact_email})" if primary_contact_email else ""}
    {opp_link}
  </div>

  {monthly_html}
  {recent_html}
  {siblings_html}
  {notes_html}
</div>'''

    # Assemble the full email
    html = f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color:#1e293b; max-width:800px; margin:0 auto; padding:20px; background:#f8fafc; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; }}
th {{ text-align:left; padding:6px 8px; background:#f1f5f9; color:#475569; font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px; }}
td {{ padding:6px 8px; border-bottom:1px solid #f1f5f9; }}
a {{ color:#3b82f6; text-decoration:none; }}
</style></head><body>

<div style="background:linear-gradient(135deg, #1e293b 0%, #334155 100%); color:white; padding:24px 28px; border-radius:12px;">
  <h1 style="margin:0 0 4px 0; font-size:22px; font-weight:600;">Subscription Renewal Summary</h1>
  <div style="color:#94a3b8; font-size:13px;">
    {len(triggered_results)} subscriptions at or above {threshold}% consumption {DASH} {NOW.strftime("%b %d, %Y")}
  </div>
</div>

<!-- OVERVIEW TABLE -->
<div style="background:white; border:1px solid #e2e8f0; border-radius:8px; padding:16px 20px; margin:16px 0;">
  <h2 style="font-size:15px; font-weight:600; color:#475569; margin:0 0 10px 0; border-bottom:2px solid #e2e8f0; padding-bottom:6px;">At-a-Glance</h2>
  <table>
    <tr><th>Customer</th><th>Project</th><th style="text-align:center;">Consumed</th><th style="text-align:right;">Used / Budget</th><th style="text-align:right;">Remaining</th><th>AE</th></tr>
    {rows_html}
  </table>
</div>

<!-- DETAILED SECTIONS -->
{detail_sections}

<div style="text-align:center; padding:16px; font-size:11px; color:#94a3b8;">
  Auto-generated Subscription Summary {DASH} {NOW.strftime("%b %d, %Y %H:%M")} {DASH} Rocketlane API<br>
  This is a preview of emails that would be sent to each AE/CSM listed above.
</div>

</body></html>'''

    return html


def send_summary_email(html_body, recipient, threshold, count, dry_run=False):
    """Send the consolidated summary email to a single recipient."""
    subject = f"Subscription Renewal Summary: {count} projects at {threshold}%+ consumption {DASH} {NOW.strftime('%b %d, %Y')}"

    if dry_run:
        print(f"  [DRY RUN] Would send summary to: {recipient}")
        print(f"  [DRY RUN] Subject: {subject}")
        return

    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)
    print(f"  Summary email sent to: {recipient}")


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
    parser.add_argument("--summary-to", type=str, default="",
                        help="Send a single summary email to this address instead of individual AE/CSM emails")
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

    # 3. Extract subscription data
    print("Extracting subscription data...")
    sub_data_list = [extract_subscription_data(p) for p in subs]

    # Filter to those with budget data
    with_budget = [s for s in sub_data_list if s["total_budgeted_hours"] > 0]
    no_budget = [s for s in sub_data_list if s["total_budgeted_hours"] == 0]
    print(f"  {len(with_budget)} have budget data, {len(no_budget)} missing budget")

    if no_budget:
        print("  Projects missing budget data:")
        for s in no_budget[:10]:
            print(f"    - {s['customer']}: {s['project_name']}")
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

    with ThreadPoolExecutor(max_workers=10) as pool:
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
        if args.summary_to:
            # Send a single consolidated summary email
            print(f"\nBuilding summary email for {len(triggered)} projects...")
            summary_html = build_summary_email(triggered, args.threshold)
            send_summary_email(summary_html, args.summary_to, args.threshold,
                               len(triggered), dry_run=args.dry_run)
        else:
            # Send individual renewal emails to AE/CSM
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

    print(f"\nDone. Processed {len(results)} subscription projects.")


if __name__ == "__main__":
    main()
