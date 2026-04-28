"""
rocketlane_client — shared infrastructure for Rocketlane API integration.

Used by:
  - subscription_expansion_audit.py
  - subscription_tracker.py
  - probe_*.py
  - (future scripts)

Owns:
  - HTTP client with retry/backoff (handles 429s and 5xx)
  - Server-side filtered project fetch (subscription + active)
  - Bulk + per-project time-entry fetch
  - Bulk company fetch (with per-company fallback when fields[] is stripped)
  - PSR linkedResources fetch + parser (v1 endpoint)
  - Defensive SF Account ID extractor (handles multiple field shapes)
  - Common field/status helpers

Secrets are loaded from
  ~/Library/.../iCloud Storage/Exterro/.secrets/rocketlane.env
when this module is imported.

Why this exists: the same patterns kept getting copy-pasted across scripts and
drifting. New optimizations (e.g., bulk /companies) only landed in one script
at a time. Centralizing means fixes flow to every consumer for free.
"""

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════════
# SECRETS LOADER
# ═══════════════════════════════════════════════════════════════════════════════
SECRETS_FILE = Path(
    "/Users/matthew.abadie/Library/Mobile Documents/com~apple~CloudDocs/"
    "iCloud Storage/Exterro/.secrets/rocketlane.env"
)
if SECRETS_FILE.exists():
    for _line in SECRETS_FILE.read_text().splitlines():
        if "=" in _line and not _line.startswith("#"):
            _k, _v = _line.split("=", 1)
            if _v.strip():
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
API_KEY = os.environ.get("ROCKETLANE_API_KEY", "")
BASE_URL = "https://services.api.exterro.com/api/1.0"
V1_BASE_URL = "https://services.api.exterro.com/api/v1"  # exposes linkedResources, company fields
RL_APP_BASE = "https://services.exterro.com/projects"
SFDC_LIGHTNING_BASE = "https://exterroad.lightning.force.com/lightning/r"

# Project-level constants
PROJECT_TYPE_FIELD_ID = 1902713
SUBSCRIPTION_VALUE = 3
ACTIVE_STATUS_VALUES = [2, 4, 5, 6, 9, 12, 14, 15]

# Known team owner IDs (used by tracker filters; expand as needed)
ORONDE_ID = 393607  # Post Implementation director


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP — retry/backoff on 429s and 5xx
# ═══════════════════════════════════════════════════════════════════════════════
def api_request(method, path, *, body=None, retries=5, timeout=120, base=None):
    """HTTP request with exponential backoff on 429/5xx. Returns parsed JSON.
    Use for GET/PUT/PATCH/POST/DELETE.

    `base` overrides BASE_URL — pass V1_BASE_URL for v1 endpoints.
    `body`, if dict, is JSON-encoded and sent as application/json.
    """
    base = base or BASE_URL
    url = f"{base}/{path}"
    headers = {"api-key": API_KEY, "accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            last_err = e
            if (e.code == 429 or e.code >= 500) and attempt < retries - 1:
                time.sleep((2 ** attempt) + 0.1 * attempt)
                continue
            body_text = ""
            try:
                body_text = e.read().decode()[:500]
            except Exception:
                pass
            raise RuntimeError(f"HTTP {e.code} on {method} {path[:80]}: {body_text}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1 + attempt)
                continue
            raise
    if last_err:
        raise last_err


def api_get(path, retries=5, timeout=120, base=None):
    """GET — convenience wrapper around api_request()."""
    return api_request("GET", path, retries=retries, timeout=timeout, base=base)


# ═══════════════════════════════════════════════════════════════════════════════
# FIELD + STATUS HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_field(project, label):
    """Get custom field value by label. Falls back to fieldValueLabel→fieldValue.
    Returns None if no field with that label exists."""
    for f in project.get("fields") or []:
        if f.get("fieldLabel") == label:
            return f.get("fieldValueLabel") or f.get("fieldValue") or ""
    return None


def is_active_status(p):
    return p.get("status", {}).get("value") in ACTIVE_STATUS_VALUES


def is_subscription_type(p):
    """Client-side check via 'Project Type' field. Server-side filter via
    project.field.<id>.value=3 is preferred when available."""
    return (get_field(p, "Project Type") or "").lower() == "subscription"


def is_owned_or_member(p, user_id):
    """True if user_id is the project owner or in teamMembers."""
    if p.get("owner", {}).get("userId") == user_id:
        return True
    for m in p.get("teamMembers", {}).get("members", []) or []:
        if m.get("userId") == user_id:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# PROJECT LISTING — server-side filtered + sorted
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_subscription_projects(
    *,
    since_date=None,
    sort_by="annualizedRecurringRevenue",
    sort_order="DESC",
    extra_filters=None,
    page_size=100,
):
    """One server-side filtered, sorted call. Returns active subscription projects.
    Replaces fetch_all_projects → client-side filter pattern.

    Parameters:
      since_date: YYYY-MM-DD; if set, filters updatedAt >= that date
      sort_by, sort_order: list endpoint sort
      extra_filters: dict of additional query params (e.g., to constrain by a
                     custom field, pass {"project.field.<id>.value": "<value>"})
      page_size: page size hint

    Returns: (projects: list[dict], api_calls: int)
    """
    params = {
        f"project.field.{PROJECT_TYPE_FIELD_ID}.value": str(SUBSCRIPTION_VALUE),
        "status.oneOf": ",".join(str(v) for v in ACTIVE_STATUS_VALUES),
        "sortBy": sort_by,
        "sortOrder": sort_order,
        "includeAllFields": "true",
        "pageSize": str(page_size),
    }
    if since_date:
        try:
            dt = datetime.strptime(since_date, "%Y-%m-%d")
            params["updatedAt.ge"] = str(int(dt.timestamp() * 1000))
        except ValueError:
            pass
    if extra_filters:
        params.update(extra_filters)

    qs = urllib.parse.urlencode(params)
    return _paginate_projects(qs)


def _paginate_projects(qs):
    all_projects, token, calls = [], None, 0
    while True:
        url = f"projects?{qs}"
        if token:
            url += f"&pageToken={urllib.parse.quote(token)}"
        resp = api_get(url)
        calls += 1
        all_projects.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            token = pag["nextPageToken"]
        else:
            break
    return all_projects, calls


def fetch_project_detail(pid):
    """Per-project detail call. Avoid when possible — list endpoint with
    includeAllFields=true returns financials inline."""
    return api_get(f"projects/{pid}")


# ═══════════════════════════════════════════════════════════════════════════════
# TIME ENTRIES
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_bulk_time_entries(since_date, *, project_ids=None, timeout=90):
    """One paginated call for all time entries since date. Optionally filter to
    a subset via projectId.in. Returns (entries: list, api_calls: int)."""
    params = {"date.ge": since_date, "pageSize": "100"}
    if project_ids:
        params["projectId.in"] = ",".join(str(p) for p in project_ids)
    qs = urllib.parse.urlencode(params)

    all_entries, token, calls = [], None, 0
    while True:
        url = f"time-entries?{qs}"
        if token:
            url += f"&pageToken={urllib.parse.quote(token)}"
        resp = api_get(url, timeout=timeout)
        calls += 1
        all_entries.extend(resp.get("data", []))
        pag = resp.get("pagination", {})
        if pag.get("hasMore") and pag.get("nextPageToken"):
            token = pag["nextPageToken"]
        else:
            break
    return all_entries, calls


def fetch_time_entries_per_project(project_ids, since_date=None, max_workers=8):
    """Parallel per-project fetch. Use as fallback when bulk times out, or when
    you need per-project pagination semantics."""
    by_project = defaultdict(list)
    api_calls = 0

    def fetch_one(pid):
        params = {"projectId.eq": str(pid), "pageSize": "100"}
        if since_date:
            params["date.ge"] = since_date
        qs = urllib.parse.urlencode(params)
        local_entries, token, calls = [], None, 0
        while True:
            url = f"time-entries?{qs}"
            if token:
                url += f"&pageToken={urllib.parse.quote(token)}"
            resp = api_get(url, timeout=60)
            calls += 1
            local_entries.extend(resp.get("data", []))
            pag = resp.get("pagination", {})
            if pag.get("hasMore") and pag.get("nextPageToken"):
                token = pag["nextPageToken"]
            else:
                break
        return pid, local_entries, calls

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_one, pid): pid for pid in project_ids}
        for f in as_completed(futures):
            try:
                pid, entries, calls = f.result()
                by_project[int(pid)].extend(entries)
                api_calls += calls
            except Exception as e:
                pid = futures[f]
                print(f"  WARN: time entries for {pid}: {str(e)[:80]}")
    return by_project, api_calls


def fetch_time_entries_for_project(pid, since_date=None):
    """Single-project time-entry fetch. Convenience wrapper around per-project
    fan-out (with workers=1). Used by legacy code; new code should batch via
    fetch_bulk_time_entries(project_ids=[...])."""
    by_project, _ = fetch_time_entries_per_project([pid], since_date=since_date, max_workers=1)
    return by_project.get(int(pid), [])


def group_entries_by_project(entries):
    """Bucket a flat list of entries by projectId."""
    by_project = defaultdict(list)
    for e in entries:
        proj = e.get("project")
        pid = None
        if isinstance(proj, dict):
            pid = proj.get("projectId") or proj.get("id")
        elif isinstance(proj, int):
            pid = proj
        pid = pid or e.get("projectId")
        if pid:
            by_project[int(pid)].append(e)
    return by_project


# ═══════════════════════════════════════════════════════════════════════════════
# PSR linkedResources (v1 — exposes Salesforce integration metadata)
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_linked_resources(pid):
    """v1 project endpoint exposes linkedResources (which the public API strips).
    Returns the linkedResources list (possibly empty)."""
    try:
        resp = api_get(f"projects/{pid}", timeout=60, base=V1_BASE_URL)
        return resp.get("linkedResources", []) or []
    except Exception:
        return []


def find_psr_link(linked_resources):
    """Extract PSR + SF FK refs from linkedResources, or empty dict."""
    for lr in linked_resources or []:
        if (lr.get("externalResourceType") == "Professional_Service_Request__c"
                and not lr.get("deleted")
                and not lr.get("externalResourceDeleted")
                and lr.get("enabled", True)):
            ext_id = lr.get("externalResourceId")
            if ext_id:
                ctx = lr.get("context") or {}
                acct_id = ctx.get("Account_Name__c")
                opp_id = ctx.get("Opportunity_Name__c")
                return {
                    "psr_external_id": ext_id,
                    "psr_url": f"{SFDC_LIGHTNING_BASE}/Professional_Service_Request__c/{ext_id}/view",
                    "sf_account_id": acct_id,
                    "sf_account_url": f"{SFDC_LIGHTNING_BASE}/Account/{acct_id}/view" if acct_id else None,
                    "context_opp_id": opp_id,
                }
    return {}


def enrich_psr_links_parallel(projects, max_workers=10, verbose=False):
    """Fetch linkedResources for each project in parallel. Stash psr/sf refs
    on each project dict as `_psr_*` and `_sf_account_*` keys."""
    api_calls = 0

    def work(p):
        pid = p.get("projectId")
        lr = fetch_linked_resources(pid)
        info = find_psr_link(lr)
        p["_psr_external_id"] = info.get("psr_external_id")
        p["_psr_url"] = info.get("psr_url")
        p["_psr_context_opp"] = info.get("context_opp_id")
        p["_sf_account_id"] = info.get("sf_account_id")
        p["_sf_account_url"] = info.get("sf_account_url")
        p["_linked_resources_count"] = len(lr)
        return 1

    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(work, p): p for p in projects}
        for f in as_completed(futures):
            try:
                api_calls += f.result()
            except Exception as e:
                p = futures[f]
                print(f"  WARN linkedResources for {p.get('projectId')}: {str(e)[:80]}")
            done += 1
            if verbose and (done % 25 == 0 or done == len(projects)):
                print(f"  {done}/{len(projects)} projects checked for PSR links")
    return api_calls


# ═══════════════════════════════════════════════════════════════════════════════
# COMPANIES + SF Account ID (the "SFDC Acct No" custom field on company records)
# ═══════════════════════════════════════════════════════════════════════════════
# Salesforce Account IDs are 15 or 18 chars, alphanumeric, starting with "001".
SF_ACCT_ID_RE = re.compile(r"\b(001[A-Za-z0-9]{12,15})\b")
COMPANY_SF_ACCOUNT_FIELD_ID = 2008819  # Note: per probe, this fieldId was the SF
                                        # Opportunity URL. Real SFDC Acct No has
                                        # no consistent fieldId — extractor uses
                                        # 001-prefix regex on values.
COMPANY_SF_ACCOUNT_FIELD_LABEL_HINT = "salesforce account id"


def _coerce_value(f):
    val = f.get("fieldValue")
    if val is None:
        val = f.get("value")
    if val is None:
        val = f.get("valueText")
    if isinstance(val, dict):
        val = val.get("value") or val.get("text") or ""
    return str(val).strip() if val is not None else ""


def extract_acct_id_from_fields(fields):
    """Pull SF Account ID out of a company's fields list. Tries 3 strategies:
      1. Match by COMPANY_SF_ACCOUNT_FIELD_ID (when known)
      2. Regex scan: any field value matching 001-prefixed SF Acct ID
      3. Label hint match
    Returns acct_id (15 or 18 char SF ID) or None."""
    if not fields:
        return None
    for f in fields:
        fid = f.get("fieldId") or f.get("id")
        if fid == COMPANY_SF_ACCOUNT_FIELD_ID:
            val = _coerce_value(f)
            m = SF_ACCT_ID_RE.search(val)
            if m:
                return m.group(1)
            if val:
                return val
    for f in fields:
        val = _coerce_value(f)
        m = SF_ACCT_ID_RE.search(val)
        if m:
            return m.group(1)
    hint = COMPANY_SF_ACCOUNT_FIELD_LABEL_HINT.lower()
    for f in fields:
        label = " ".join(
            str(f.get(k) or "")
            for k in ("fieldLabel", "label", "name", "fieldName")
        ).lower()
        if hint in label:
            val = _coerce_value(f)
            m = SF_ACCT_ID_RE.search(val)
            if m:
                return m.group(1)
            if val:
                return val
    return None


def build_account_url(acct_id):
    return f"{SFDC_LIGHTNING_BASE}/Account/{acct_id}/view" if acct_id else None


def fetch_all_companies():
    """ONE paginated bulk call to v1 /companies. v1 returns top-level list (or
    occasionally a wrapped dict — both shapes handled)."""
    all_co, token, calls = [], None, 0
    base_qs = "pageSize=500&includeAllFields=true"
    while True:
        url = f"companies?{base_qs}"
        if token:
            url += f"&pageToken={urllib.parse.quote(token)}"
        resp = api_get(url, base=V1_BASE_URL, timeout=90)
        calls += 1
        if isinstance(resp, list):
            items = resp
            pag = {}
        elif isinstance(resp, dict):
            items = resp.get("data") or resp.get("companies") or []
            pag = resp.get("pagination") or {}
        else:
            items, pag = [], {}
        all_co.extend(items)
        if pag.get("hasMore") and pag.get("nextPageToken"):
            token = pag["nextPageToken"]
        else:
            break
    return all_co, calls


def fetch_companies_with_fields_fallback(company_ids, max_workers=10):
    """Parallel per-company fetch (v1) for cases where bulk strips fields[].
    Returns (results: dict[id→company], api_calls: int)."""
    results, calls = {}, 0

    def work(cid):
        try:
            return cid, api_get(f"companies/{cid}", timeout=60, base=V1_BASE_URL), 1
        except Exception:
            return cid, None, 1

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(work, cid) for cid in company_ids]
        for f in as_completed(futures):
            cid, co, c = f.result()
            calls += c
            if co:
                results[cid] = co
    return results, calls


def enrich_company_account_links_bulk(projects, debug=False, verbose=True):
    """Bulk /companies → fallback for fields → build map → stash on each project
    as `_sf_account_id_company` / `_sf_account_url_company`. Independent of PSR
    linkage — works for projects with no PSR. Returns api_calls used."""
    needed_company_ids = {
        (p.get("customer") or {}).get("companyId")
        for p in projects
        if (p.get("customer") or {}).get("companyId")
    }
    if not needed_company_ids:
        return 0

    if verbose:
        print("  bulk-fetching companies (v1)...")
    companies, api_calls = fetch_all_companies()
    if verbose:
        plural = "s" if api_calls != 1 else ""
        print(f"  pulled {len(companies)} companies in {api_calls} call{plural}")

    bulk_has_fields = any(("fields" in c) for c in companies[:5])
    co_by_id = {}
    if bulk_has_fields:
        for co in companies:
            cid = co.get("companyId") or co.get("id")
            if cid is not None:
                co_by_id[cid] = co
    else:
        if verbose:
            print("  bulk response did NOT include fields — falling back to per-company fetch")
        co_by_id, fb_calls = fetch_companies_with_fields_fallback(needed_company_ids)
        api_calls += fb_calls
        if verbose:
            print(f"  per-company fallback: {fb_calls} calls for {len(co_by_id)} companies")

    co_map = {}
    no_field = 0
    sample_dumped = False
    for cid, co in co_by_id.items():
        fields = co.get("fields") or co.get("customFields") or []
        acct_id = extract_acct_id_from_fields(fields)
        if acct_id:
            co_map[cid] = (acct_id, build_account_url(acct_id))
        else:
            no_field += 1
            if debug and not sample_dumped and fields:
                sample_dumped = True
                print(f"  DEBUG sample company without SF Acct ID — companyId={cid}")
                for f in fields[:8]:
                    fid = f.get("fieldId") or f.get("id")
                    val = str(f.get("fieldValue") or f.get("value") or "")[:60]
                    print(f"  DEBUG  fieldId={fid}  value={val!r}")
    if verbose:
        print(f"  {len(co_map)} companies have SF Account ID; {no_field} do not")

    matched = 0
    for p in projects:
        cid = (p.get("customer") or {}).get("companyId")
        if cid and cid in co_map:
            acct_id, acct_url = co_map[cid]
            p["_sf_account_id_company"] = acct_id
            p["_sf_account_url_company"] = acct_url
            matched += 1
    if verbose:
        print(f"  {matched}/{len(projects)} projects matched to a company with SF Acct ID")
    return api_calls
