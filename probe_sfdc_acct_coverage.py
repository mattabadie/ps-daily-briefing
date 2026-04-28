#!/usr/bin/env python3
"""
Coverage report — how many of the ~694 Rocketlane company records have SFDC
Acct No populated, and how many don't?

Bulk /v1/companies returns the envelope but strips fields[], so we fan out
per-company in parallel. ~30-60s for ~700 companies at 20 workers.

Usage:  python3 probe_sfdc_acct_coverage.py
        python3 probe_sfdc_acct_coverage.py --csv missing.csv
"""
import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def api_get(url, retries=5, timeout=60):
    """GET with retry/backoff. Rocketlane returns 429 under parallel load — back off
    and retry instead of failing the record."""
    req = urllib.request.Request(
        url, headers={"api-key": API_KEY, "accept": "application/json"}
    )
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429 or e.code >= 500:
                # Exponential backoff with jitter
                sleep_s = (2 ** attempt) + (0.1 * attempt)
                time.sleep(sleep_s)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(1 + attempt)
            continue
    raise last_err if last_err else RuntimeError("api_get exhausted retries")


# Test/internal record markers to skip (case-insensitive substring match)
INTERNAL_PATTERNS = [
    "test", "uat", "internal", "exterro", "demo", " - non ", "training",
    "vanessa", "eg ", " eg",
]
# Also skip any "company" whose name is itself a Salesforce ID (creation artifact)
SF_ID_AS_NAME_RE = re.compile(r"^001[A-Za-z0-9]{12,15}$")


def has_sfdc_acct(co):
    """Return SF Acct ID if found, else None."""
    for f in co.get("fields") or []:
        val = str(f.get("fieldValue") or "")
        m = SF_ACCT.search(val)
        if m:
            return m.group(1)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="Write missing-companies CSV to this path")
    ap.add_argument("--workers", type=int, default=5,
                    help="Concurrent fetches (default 5; raise cautiously — Rocketlane rate-limits)")
    args = ap.parse_args()

    print("Pulling bulk company list...")
    bulk = api_get(f"{V1}/companies?pageSize=1000")
    items = bulk if isinstance(bulk, list) else (bulk.get("data") or bulk.get("companies") or [])
    print(f"Got {len(items)} companies\n")

    # Skip obvious non-customers (test/internal/dummy records).
    def looks_internal(c):
        raw = (c.get("companyName") or "").strip()
        if not raw:
            return True
        if SF_ID_AS_NAME_RE.match(raw):
            return True  # company NAME is literally a SF Acct ID — creation artifact
        name = raw.lower()
        return any(p in name for p in INTERNAL_PATTERNS)

    real = [c for c in items if not looks_internal(c)]
    skipped = len(items) - len(real)
    print(f"Filtered out {skipped} test/internal rows; {len(real)} real-customer candidates\n")

    print(f"Fetching each company's fields (parallel, {args.workers} workers)...")
    with_id = []
    without_id = []
    errors = []

    def work(c):
        cid = c.get("companyId")
        try:
            full = api_get(f"{V1}/companies/{cid}")
            acct = has_sfdc_acct(full)
            return cid, c.get("companyName"), acct, None
        except Exception as e:
            return cid, c.get("companyName"), None, str(e)[:100]

    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(work, c) for c in real]
        for f in as_completed(futures):
            cid, name, acct, err = f.result()
            if err:
                errors.append((cid, name, err))
            elif acct:
                with_id.append((cid, name, acct))
            else:
                without_id.append((cid, name))
            done += 1
            if done % 100 == 0 or done == len(real):
                print(f"  {done}/{len(real)} ...")

    total = len(real)
    fetched = len(with_id) + len(without_id)
    print()
    print("=" * 60)
    print("SFDC Acct No coverage on Rocketlane company records")
    print("=" * 60)
    print(f"  Total real-customer candidates:  {total}")
    print(f"  Successfully fetched:            {fetched}")
    print(f"  Errors fetching (after retry):   {len(errors)}")
    print()
    if fetched > 0:
        print("  Of successfully-fetched records:")
        print(f"  ✅ has SFDC Acct No:    {len(with_id):>4}  ({len(with_id)/fetched:.1%})")
        print(f"  ❌ missing SFDC Acct No:{len(without_id):>4}  ({len(without_id)/fetched:.1%})")
    if errors:
        print(f"\n  First 3 errors (sanity check):")
        for cid, name, err in errors[:3]:
            print(f"    {cid}  {name}  ->  {err}")

    if args.csv:
        with open(args.csv, "w", newline="") as fp:
            w = csv.writer(fp)
            w.writerow(["companyId", "companyName"])
            for cid, name in sorted(without_id, key=lambda r: (r[1] or "").lower()):
                w.writerow([cid, name])
        print(f"\n  Missing-list CSV: {args.csv}  ({len(without_id)} rows)")
    elif without_id:
        print("\nFirst 25 missing companies (companyId, companyName):")
        for cid, name in sorted(without_id, key=lambda r: (r[1] or "").lower())[:25]:
            print(f"  {cid:>8}  {name}")


if __name__ == "__main__":
    main()
