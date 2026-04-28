#!/usr/bin/env python3
"""
Targeted probe — Albany Medical Center (we KNOW it has SFDC Acct No populated).
Finds the company, dumps all fields, identifies which fieldId carries the
SF Account ID so we can hardcode it.

Usage:  python3 probe_albany.py
"""
import json
import os
import re
import urllib.request
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
V1 = "https://services.api.exterro.com/api/v1"

SF_ACCT = re.compile(r"\b(001[A-Za-z0-9]{12,15})\b")


def api_get(url):
    req = urllib.request.Request(
        url, headers={"api-key": API_KEY, "accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


# ── 1) bulk list, find Albany Medical Center ──
print("Pulling bulk company list...")
bulk = api_get(f"{V1}/companies?pageSize=1000")
items = bulk if isinstance(bulk, list) else (bulk.get("data") or bulk.get("companies") or [])
print(f"Got {len(items)} companies")

albany_id = None
albany = None
for c in items:
    name = (c.get("companyName") or "").lower()
    if "albany medical" in name:
        albany_id = c.get("companyId")
        albany = c
        print(f"Found: {c.get('companyName')}  companyId={albany_id}")
        break

if albany_id is None:
    print("ERROR: Albany Medical Center not found in bulk list. Searching for any 'albany'...")
    for c in items:
        name = (c.get("companyName") or "").lower()
        if "albany" in name:
            print(f"  candidate: {c.get('companyName')}  companyId={c.get('companyId')}")
    raise SystemExit(1)

# ── 2) fetch full v1 company response ──
print(f"\nFetching full /v1/companies/{albany_id}...")
co = api_get(f"{V1}/companies/{albany_id}")
print(f"top-level keys: {list(co.keys())}")
print(f"#fields: {len(co.get('fields') or [])}\n")

# ── 3) dump every field, flag the SFDC Acct No match ──
print("=" * 72)
print("ALL fields on Albany Medical Center")
print("=" * 72)
target_fid = None
for f in co.get("fields") or []:
    fid = f.get("fieldId")
    val = f.get("fieldValue")
    sval = "" if val is None else str(val)
    tag = ""
    if SF_ACCT.search(sval):
        tag = "  ⬅ ⬅ ⬅ SFDC ACCT NO MATCH"
        target_fid = fid
    elif "salesforce" in sval.lower():
        tag = "  ⬅ contains 'salesforce'"
    printable = sval if len(sval) < 100 else sval[:97] + "..."
    print(f"  fieldId={fid:>8}  value={printable!r}{tag}")

print()
if target_fid is not None:
    print(f"✅ COMPANY_SF_ACCOUNT_FIELD_ID should be set to: {target_fid}")
    print(f"   (matched value containing a 001-prefixed Salesforce Account ID)")
else:
    print("❌ No 001-prefixed value found on Albany Medical Center either.")
    print("   The screenshot showed 'SFDC Acct No = 0014x00000iKTPCAA4' but the API")
    print("   isn't returning it. Possibilities:")
    print("   - field belongs to a different sub-object (account, fields-by-section)")
    print("   - includeAllFields/scope param needed")
    print("   - field is on a different visibility tier than the API token")
