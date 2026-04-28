#!/usr/bin/env python3
"""
Diagnostic: dump ALL Rocketlane company custom fields so we can identify which
fieldId stores the Salesforce Account ID.

Salesforce Account IDs are 15 or 18 chars, alphanumeric, starting with "001".
Salesforce Opportunity IDs start with "006". The probe flags both.

Usage:  python3 probe_company_fields.py
"""
import json
import os
import re
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

SECRETS = Path(
    "/Users/matthew.abadie/Library/Mobile Documents/com~apple~CloudDocs/"
    "iCloud Storage/Exterro/.secrets/rocketlane.env"
)
for line in SECRETS.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

API_KEY = os.environ["ROCKETLANE_API_KEY"]
BASE = "https://services.api.exterro.com/api/1.0"
V1 = "https://services.api.exterro.com/api/v1"

SF_ID_RE = re.compile(r"\b(001[A-Za-z0-9]{12,15})\b")
SF_OPP_RE = re.compile(r"\b(006[A-Za-z0-9]{12,15})\b")


def api_get(url):
    req = urllib.request.Request(
        url, headers={"api-key": API_KEY, "accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


# ── 1) Pull ~25 sample companies' worth of project data ──
print("Fetching 25 sample subscription projects...")
proj_resp = api_get(
    f"{BASE}/projects"
    f"?project.field.1902713.value=3"
    f"&pageSize=25&includeAllFields=true"
)
projects = proj_resp.get("data", [])
print(f"Got {len(projects)} projects\n")

# ── 2) Per-company dump — ALL fields, flag any SF-shaped value ──
print("=" * 78)
print("Dumping ALL fields for each unique company; flagging SF Acct/Opp matches")
print("=" * 78)

# Track which fieldIds yield Account-shaped vs Opp-shaped values across companies
fieldid_to_acct_hits = Counter()
fieldid_to_opp_hits = Counter()
fieldid_to_sample_value = {}
fieldid_to_companies = defaultdict(list)

seen = set()
for p in projects:
    cid = (p.get("customer") or {}).get("companyId")
    cname = (p.get("customer") or {}).get("companyName")
    if not cid or cid in seen:
        continue
    seen.add(cid)
    try:
        co = api_get(f"{V1}/companies/{cid}")
    except Exception as e:
        print(f"  ERROR companyId={cid}: {e}")
        continue
    fields = co.get("fields") or []
    print(f"\n--- companyId={cid}  ({cname})  #fields={len(fields)} ---")
    for f in fields:
        fid = f.get("fieldId")
        val = f.get("fieldValue")
        sval = "" if val is None else str(val)
        # Tag value type
        tag = ""
        if SF_ID_RE.search(sval):
            tag = "  ⬅ SF ACCOUNT ID match (001…)"
            fieldid_to_acct_hits[fid] += 1
        elif SF_OPP_RE.search(sval):
            tag = "  ⬅ SF OPPORTUNITY match (006…)"
            fieldid_to_opp_hits[fid] += 1
        elif "salesforce.com" in sval:
            tag = "  ⬅ contains salesforce.com"
        # Cap value length for printing
        printable = sval if len(sval) < 90 else sval[:87] + "..."
        print(f"  fieldId={fid:>8}  value={printable!r}{tag}")
        fieldid_to_sample_value.setdefault(fid, sval)
        fieldid_to_companies[fid].append(cname)

# ── 3) Summary: which fieldId is the SF Account ID? ──
print()
print("=" * 78)
print("SUMMARY — fieldIds that produced SF Account-shaped (001…) values")
print("=" * 78)
if fieldid_to_acct_hits:
    for fid, count in fieldid_to_acct_hits.most_common():
        print(f"  fieldId={fid}  hits={count}/{len(seen)} companies")
        print(f"    sample value: {fieldid_to_sample_value.get(fid)!r}")
        print(f"    sample companies: {fieldid_to_companies[fid][:5]}")
    top_fid = fieldid_to_acct_hits.most_common(1)[0][0]
    print(f"\n👉 Use COMPANY_SF_ACCOUNT_FIELD_ID = {top_fid}")
else:
    print("  ❌ NO field on any sampled company contains a 001-prefixed Salesforce ID.")
    print("     Either the field is empty on these companies, or SF Acct IDs aren't")
    print("     stored on the company record. Check 5-10 more companies, or look at")
    print("     a known-good company manually in Rocketlane UI to find the field.")

print()
print("SUMMARY — fieldIds that produced SF Opportunity-shaped (006…) values")
print("=" * 78)
for fid, count in fieldid_to_opp_hits.most_common():
    print(f"  fieldId={fid}  hits={count}  (likely SF Opp URL — NOT what we want)")

# ── 4) Bulk endpoint shape probe ──
print()
print("=" * 78)
print("Bulk /companies probe — what shape does the response come back in?")
print("=" * 78)
try:
    bulk = api_get(f"{V1}/companies?pageSize=5")
    if isinstance(bulk, list):
        print(f"✅ response is a TOP-LEVEL LIST of {len(bulk)} items")
        if bulk:
            print(f"   first item keys: {list(bulk[0].keys())}")
            has_fields = "fields" in bulk[0]
            print(f"   fields included? {has_fields}")
    elif isinstance(bulk, dict):
        print(f"response is a DICT with keys: {list(bulk.keys())}")
        items = bulk.get("data") or bulk.get("companies") or []
        print(f"   {len(items)} items via data/companies key")
        if items:
            print(f"   first item keys: {list(items[0].keys())}")
            print(f"   fields included? {'fields' in items[0]}")
except Exception as e:
    print(f"bulk endpoint failed: {e}")
