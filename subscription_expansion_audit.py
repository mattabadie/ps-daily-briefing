#!/usr/bin/env python3
"""
Subscription Expansion Audit

All active subscription projects across all teams (eDiscovery, Data PSG, Forensics,
Post Implementation), enriched for customer-success / expansion-opportunity analysis:
  - Modules licensed (Opp: Product Names + Product Families)
  - ARR (annualizedRecurringRevenue + Opp: Opportunity ARR)
  - % of subscription hours consumed (from project-level rollup — no time-entry calls)
  - Expansion-opportunity flags (OVER budget / Renewal window / Underutilized / etc.)

Uses native Rocketlane API server-side filtering and project-level consumption rollup.
~2 API calls total, <10 seconds runtime for ~200 projects.

Key design points:
  - ONE server-side filtered call replaces fetch-all-then-filter:
      ?project.field.1902713.value=3 (Project Type = Subscription)
      &status.oneOf=2,4,5,6,9,12,14,15 (active statuses)
      &sortBy=annualizedRecurringRevenue&sortOrder=DESC
  - Consumption uses project-level rollup (trackedHours, percentageBudgetedHoursConsumed)
    already present in the list response. No per-project time-entry calls.
  - Per-project detail calls only when financials missing from list response.
  - Optional --since flag for incremental refresh (skill-friendly).
  - Optional --with-entries flag pulls time entries only when last_entry_date
    or detailed burn analysis is needed (rarely).

Usage:
  python3 subscription_expansion_audit.py
  python3 subscription_expansion_audit.py --since 2026-01-01     # incremental
  python3 subscription_expansion_audit.py --with-entries         # also fetch time entries
  python3 subscription_expansion_audit.py --bench                # legacy comparison
"""

import argparse
import os
import re
import sys
import time
import urllib.error
from datetime import datetime, timedelta

# Shared infrastructure — HTTP, fetchers, enrichment, helpers
from rocketlane_client import (
    API_KEY,
    SFDC_LIGHTNING_BASE,
    fetch_subscription_projects,
    fetch_bulk_time_entries,
    fetch_time_entries_per_project,
    group_entries_by_project,
    fetch_project_detail,
    enrich_psr_links_parallel,
    enrich_company_account_links_bulk,
    get_field,
)

# Audit-specific row builder + workbook builder
from subscription_audit import extract_project_row
from subscription_expansion_audit_legacy import build_expansion_workbook

NOW = datetime.now()




# ─── Enrichment from project-level rollup (no time-entry calls) ────────────────
def enrich_row_from_rollup(p, detail, base_row, entries_by_project=None):
    """Use project-level rollup fields (trackedHours, percentageBudgetedHoursConsumed)
    that the list endpoint already returns. No per-project time-entry calls needed.
    If entries_by_project is provided (--with-entries mode), also computes last_entry_date.
    """
    pid = p.get("projectId")

    # Modules + product family
    base_row["modules"] = get_field(p, "Opp: Product Names") or ""
    base_row["product_family"] = get_field(p, "Opp: Product Families") or ""

    # ARR
    arr_top = (detail or p).get("annualizedRecurringRevenue") or 0
    arr_opp = get_field(p, "Opp: Opportunity ARR") or 0
    try:
        arr_opp = float(arr_opp) if arr_opp else 0
    except (TypeError, ValueError):
        arr_opp = 0
    base_row["arr"] = arr_top or arr_opp or 0
    base_row["total_contract_value"] = get_field(p, "Opp: Total Contract Value") or ""

    # Customer + ownership
    base_row["account_owner"] = get_field(p, "Opp: Account Owner") or ""
    base_row["primary_contact"] = get_field(p, "Opp: Primary Customer Contact Name") or ""
    base_row["primary_contact_email"] = get_field(p, "Opp: Primary Customer Contact Email") or ""
    base_row["responsible_director"] = get_field(p, "Responsible Director") or ""
    base_row["subscription_end"] = get_field(p, "Opp: Opportunity End Date") or ""

    # PSR link from v1 linkedResources (set earlier by enrich_psr_links_parallel)
    base_row["psr_external_id"] = p.get("_psr_external_id") or ""
    base_row["psr_url"] = p.get("_psr_url") or ""

    # SF Account URL — prefer company-level (always available) over PSR-context (only when PSR exists)
    base_row["sf_account_id"] = (
        p.get("_sf_account_id_company")
        or p.get("_sf_account_id")
        or ""
    )
    base_row["sf_account_url"] = (
        p.get("_sf_account_url_company")
        or p.get("_sf_account_url")
        or ""
    )

    health_notes = get_field(p, "Internal Project Health Notes") or ""
    base_row["health_notes"] = re.sub(r"<[^>]+>", "", str(health_notes))[:500].strip()

    # Consumption — straight from project-level rollup
    tracked_hrs = p.get("trackedHours")
    if tracked_hrs is None:
        tracked_min = p.get("trackedMinutes", 0) or 0
        tracked_hrs = tracked_min / 60 if tracked_min else 0
    base_row["hours_used"] = round(float(tracked_hrs or 0), 1)

    # % consumed: prefer Rocketlane's pre-computed value if present, else compute
    rl_pct = p.get("percentageBudgetedHoursConsumed")
    budgeted = base_row.get("total_budget_hrs", 0) or 0
    if rl_pct is not None and rl_pct > 0:
        base_row["pct_consumed"] = round(float(rl_pct), 1)
    elif budgeted > 0:
        base_row["pct_consumed"] = round((base_row["hours_used"] / budgeted * 100), 1)
    else:
        base_row["pct_consumed"] = 0

    # Last entry date — only available if --with-entries was used
    base_row["last_entry_date"] = ""
    if entries_by_project:
        entries = entries_by_project.get(int(pid), [])
        if entries:
            dates = []
            for e in entries:
                ds = e.get("date", "")
                if ds:
                    try:
                        dates.append(datetime.strptime(ds, "%Y-%m-%d"))
                    except ValueError:
                        pass
            if dates:
                base_row["last_entry_date"] = max(dates).strftime("%Y-%m-%d")
    else:
        # Use project's updatedAt as a proxy for "last activity"
        ua = p.get("updatedAt")
        if ua:
            try:
                base_row["last_entry_date"] = datetime.fromtimestamp(ua / 1000).strftime("%Y-%m-%d")
            except (TypeError, ValueError, OSError):
                pass

    # Expansion flags
    flags = []
    pct = base_row["pct_consumed"]
    if pct >= 100:
        flags.append("OVER budget")
    elif pct >= 75:
        flags.append("Renewal window")
    elif pct < 25 and base_row.get("total_budget_hrs", 0) > 0:
        flags.append("Underutilized")

    if not base_row["modules"]:
        flags.append("Module data missing")

    health = (base_row.get("health") or "").lower()
    if "red" in health:
        flags.append("Red health")
    elif "yellow" in health:
        flags.append("Yellow health")

    if base_row.get("needs_fix"):
        flags.append("Contract type wrong")

    seg = (base_row.get("client_segment") or "").lower()
    if "pinnacle" in seg or "strategic" in seg:
        flags.append(f"Tier: {base_row['client_segment']}")

    base_row["expansion_flags"] = " | ".join(flags) if flags else ""
    return base_row


# ─── Main ──────────────────────────────────────────────────────────────────────
def run_audit(since_date=None, with_entries=False, time_entries_lookback_days=365):
    if not API_KEY:
        print("ERROR: ROCKETLANE_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    print("=" * 70)
    print("Subscription Expansion Audit")
    print(f"Date: {NOW.strftime('%Y-%m-%d %H:%M')}")
    if since_date:
        print(f"Incremental mode: only projects updated since {since_date}")
    print("=" * 70)

    total_calls = 0
    t_start = time.time()

    # 1. Server-side filtered project list
    print("Fetching active subscription projects (server-side filter + sort)...")
    t = time.time()
    subs, calls = fetch_subscription_projects(since_date=since_date)
    total_calls += calls
    print(f"  {len(subs)} projects in {time.time()-t:.1f}s ({calls} API call{'s' if calls != 1 else ''})")

    if not subs:
        print("No projects matched. Exiting.")
        return

    # 2. Consumption — use project-level rollup by default. Only pull time entries if requested.
    entries_by_project = None
    if with_entries:
        cutoff = (NOW - timedelta(days=time_entries_lookback_days)).strftime("%Y-%m-%d")
        print(f"Fetching time entries since {cutoff} (--with-entries)...")
        t = time.time()
        try:
            entries, calls = fetch_bulk_time_entries(cutoff, timeout=90)
            total_calls += calls
            entries_by_project = group_entries_by_project(entries)
            covered = sum(1 for s in subs if int(s.get("projectId", 0)) in entries_by_project)
            print(
                f"  Bulk: {len(entries)} entries in {time.time()-t:.1f}s ({calls} API calls); "
                f"{covered}/{len(subs)} projects have entries"
            )
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            print(f"  Bulk fetch failed ({str(e)[:80]}); falling back to parallel per-project...")
            t2 = time.time()
            project_ids = [p["projectId"] for p in subs if p.get("projectId")]
            entries_by_project, calls = fetch_time_entries_per_project(
                project_ids, cutoff, max_workers=8
            )
            total_calls += calls
            total_entries = sum(len(v) for v in entries_by_project.values())
            covered = len(entries_by_project)
            print(
                f"  Per-project: {total_entries} entries in {time.time()-t2:.1f}s "
                f"({calls} API calls); {covered}/{len(subs)} projects have entries"
            )
    else:
        print("Using project-level rollup (trackedHours, %consumed) — no time-entry calls needed")

    # 2b. Fetch PSR linked resources in parallel (one v1 call per project)
    print(f"Fetching PSR linkedResources for {len(subs)} projects (parallel)...")
    t = time.time()
    lr_calls = enrich_psr_links_parallel(subs, max_workers=10)
    total_calls += lr_calls
    psr_count = sum(1 for s in subs if s.get("_psr_external_id"))
    print(f"  {psr_count}/{len(subs)} projects have PSR link in {time.time()-t:.1f}s ({lr_calls} API calls)")

    # 2c. Fetch SF Account URLs from company custom fields (independent of PSR linkage)
    # Single bulk /companies call replaces N parallel per-company calls.
    print("Resolving SF Account links from bulk /companies call...")
    t = time.time()
    co_calls = enrich_company_account_links_bulk(subs, debug=True)
    total_calls += co_calls
    acct_count = sum(1 for s in subs if s.get("_sf_account_url_company"))
    print(f"  {acct_count}/{len(subs)} projects have SF Account link in {time.time()-t:.1f}s ({co_calls} API call{'s' if co_calls != 1 else ''})")

    # 3. Build rows. Fallback to per-project detail only if financials missing.
    print("Building rows...")
    t = time.time()
    rows = []
    detail_calls = 0
    for p in subs:
        pid = p.get("projectId")
        # If the list response already has financials, skip detail call
        has_fin = bool(p.get("financials"))
        if has_fin:
            detail = p
        else:
            try:
                detail = fetch_project_detail(pid)
                detail_calls += 1
            except Exception:
                detail = p

        base = extract_project_row(p, detail)
        enrich_row_from_rollup(p, detail, base, entries_by_project)
        rows.append(base)

    total_calls += detail_calls
    print(f"  {len(rows)} rows built in {time.time()-t:.1f}s; fallback detail calls: {detail_calls}")

    # 4. Stats
    elapsed = time.time() - t_start
    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    arr_total = sum((r.get("arr") or 0) for r in rows)
    print(f"Total active subscription projects: {len(rows)}")
    print(f"Total ARR: ${arr_total:,.0f}")
    if True:  # always show — pct_consumed always available now
        over = sum(1 for r in rows if (r.get("pct_consumed") or 0) >= 100)
        renew = sum(1 for r in rows if 75 <= (r.get("pct_consumed") or 0) < 100)
        under = sum(1 for r in rows if 0 < (r.get("pct_consumed") or 0) < 25 and r.get("total_budget_hrs", 0) > 0)
        print(f"  OVER budget: {over}  |  Renewal window: {renew}  |  Underutilized: {under}")
    needs_fix = sum(1 for r in rows if r.get("needs_fix"))
    no_modules = sum(1 for r in rows if not r.get("modules"))
    print(f"  Contract type wrong: {needs_fix}  |  Missing modules: {no_modules}")
    print(f"  Total API calls: {total_calls}  |  Total time: {elapsed:.1f}s")

    # 5. Excel — deliver to Exterro/outputs/
    print("\nBuilding spreadsheet...")
    wb = build_expansion_workbook(rows)
    outputs_dir = "/Users/matthew.abadie/Library/Mobile Documents/com~apple~CloudDocs/iCloud Storage/Exterro/outputs"
    os.makedirs(outputs_dir, exist_ok=True)
    fname = f"Subscription_Expansion_Audit_{NOW.strftime('%Y%m%d')}.xlsx"
    out_path = os.path.join(outputs_dir, fname)
    wb.save(out_path)
    print(f"  Saved: {out_path}")

    return {
        "projects": len(rows),
        "api_calls": total_calls,
        "elapsed_seconds": round(elapsed, 1),
        "arr_total": arr_total,
    }


def run_bench():
    """Compare legacy fetch-all-then-filter vs current native-filter approach."""
    print("\n" + "=" * 70)
    print("BENCHMARK — Legacy vs Current")
    print("=" * 70)

    print("\n[Legacy] fetch_all_projects + client-side filter...")
    legacy_start = time.time()
    from subscription_audit import fetch_all_projects, is_active_subscription
    t = time.time()
    all_projects = fetch_all_projects()
    print(f"  fetch_all: {len(all_projects)} projects in {time.time()-t:.1f}s")
    subs_legacy = [p for p in all_projects if is_active_subscription(p)]
    print(f"  client-side filter: {len(subs_legacy)} active subscriptions")
    print(f"  Legacy simulated total (filter only): {time.time()-legacy_start:.1f}s")
    print("  (legacy also makes ~110 detail + ~110 time-entry calls; not run here)")

    print("\n[Current] run_audit()...")
    current = run_audit()

    print("\n" + "=" * 70)
    print(f"Current: {current['projects']} projects | {current['api_calls']} API calls | {current['elapsed_seconds']}s")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-entries", action="store_true",
                        help="Also pull time entries for last_entry_date and detailed burn (slower)")
    parser.add_argument("--since", help="Only fetch projects updated since YYYY-MM-DD (incremental sync)")
    parser.add_argument("--bench", action="store_true", help="Compare legacy vs current approach for timing")
    parser.add_argument("--time-lookback-days", type=int, default=365,
                        help="How far back to fetch time entries when --with-entries used (default: 365)")
    args = parser.parse_args()

    if args.bench:
        run_bench()
    else:
        run_audit(since_date=args.since, with_entries=args.with_entries,
               time_entries_lookback_days=args.time_lookback_days)


if __name__ == "__main__":
    main()
