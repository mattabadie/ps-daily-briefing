#!/usr/bin/env python3
"""
Deep probe — dump the FULL v1 company response and scan the entire JSON tree
for SF Account-shaped IDs (001…). The previous probe only inspected `fields[]`
and found nothing. The SF Account ID may live elsewhere — `account` sub-object,
`companyUrl`, etc.

Usage:  python3 probe_company_full.py
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
BASE = "https://services.api.exterro.com/api/1.0"
V1 = "https://services.api.exterro.com/api/v1"

SF_ACCT = re.compile(r"\b(001[A-Za-z0-9]{12,15})\b")
SF_ANY = re.compile(r"\b([0-9a-zA-Z]{3}[A-Za-z0-9]{12,15})\b")


def api_get(url):
    req = urllib.request.Request(
        url, headers={"api-key": API_KEY, "accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


# Pull a few subscription projects so we have known-good company IDs
proj = api_get(
    f"{BASE}/projects?project.field.1902713.value=3&pageSize=5&includeAllFields=true"
)
companies_to_probe = []
for p in proj.get("data", []):
    cid = (p.get("customer") or {}).get("companyId")
    cname = (p.get("customer") or {}).get("companyName")
    if cid:
        companies_to_probe.append((cid, cname))

print(f"Will probe {len(companies_to_probe)} companies\n")


def walk(obj, path=""):
    """Yield (path, value) for every leaf string/number in the JSON tree."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from walk(v, f"{path}.{k}" if path else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk(v, f"{path}[{i}]")
    else:
        yield path, obj


for cid, cname in companies_to_probe:
    print("=" * 78)
    print(f"FULL company response — {cname}  (companyId={cid})")
    print("=" * 78)
    co = api_get(f"{V1}/companies/{cid}")
    # Print top-level keys + key sub-objects
    print(f"top-level keys: {list(co.keys())}")
    if "account" in co:
        acct = co["account"]
        if isinstance(acct, dict):
            print(f"  account keys: {list(acct.keys())}")
            print(f"  account: {json.dumps(acct, indent=2, default=str)[:600]}")
        else:
            print(f"  account = {acct!r}")
    for k in ("companyUrl", "companyLogoUrl", "default"):
        if k in co:
            print(f"  {k} = {co.get(k)!r}")
    # Walk entire tree, flag anything that contains a SF Account-shaped ID
    print("\n  scanning entire tree for 001-prefixed Salesforce Account IDs...")
    hits = []
    for path, val in walk(co):
        if val is None:
            continue
        sval = str(val)
        m = SF_ACCT.search(sval)
        if m:
            hits.append((path, m.group(1), sval[:120]))
    if hits:
        for path, ident, raw in hits:
            print(f"    ✅ {path}  →  {ident}   (raw: {raw!r})")
    else:
        print("    ❌ no 001-prefixed Salesforce Account IDs anywhere in this company's record")
        # As a sanity check, dump every short alphanumeric string in case the
        # ID prefix differs in this org
        print("\n  any alphanumeric 15/18-char tokens (any prefix) — sample:")
        seen_tokens = set()
        for path, val in walk(co):
            if val is None:
                continue
            for m in SF_ANY.finditer(str(val)):
                tok = m.group(1)
                if tok in seen_tokens:
                    continue
                seen_tokens.add(tok)
                if len(seen_tokens) > 12:
                    break
                print(f"    {path}  →  {tok}")
            if len(seen_tokens) > 12:
                break
    print()
