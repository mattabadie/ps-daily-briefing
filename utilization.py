#!/usr/bin/env python3
"""utilization — CLI entry point for the resource-time-summary skill.

Pulls Rocketlane time entries directly via the REST API (bypassing the MCP
wrapper's broken time-entry pagination + 404 categories), groups by project /
customer / category, computes utilization against a per-email capacity baseline,
and prints a markdown brief to stdout.

Why direct API: see memory/feedback_rocketlane_hybrid_pattern.md. The MCP
wrapper has too many gaps for this specific workload to be MCP-first.

Run locally on Matt's Mac (the Cowork sandbox can't reach
services.api.exterro.com — proxy 403). Eventually this becomes a scheduled
task too.

Examples:
    # single resource, last 4 weeks
    python3 utilization.py --resource hamid.ebadi@exterro.com \\
        --start 2026-04-01 --end 2026-04-28

    # name-only resolution (errors if ambiguous)
    python3 utilization.py --resource Jake --start 2026-01-01 --end 2026-04-28

    # team mode — multiple resources, with rollup table at the top
    python3 utilization.py \\
        --resources hamid.ebadi@exterro.com,jake.hill@exterro.com \\
        --start 2026-04-01 --end 2026-04-28

    # group keyword resolved via config/teams.json
    python3 utilization.py --group ediscovery \\
        --start 2026-04-01 --end 2026-04-28

    # override capacity for the run
    python3 utilization.py --resource jake.hill@exterro.com \\
        --start 2026-04-01 --end 2026-04-28 --weekly-capacity 32
"""

from __future__ import annotations

import argparse
import json
import sys
import calendar as _calendar
from datetime import date, datetime, timedelta
from pathlib import Path

import rocketlane_client as rl
import render_brief as rb

REPO_ROOT = Path(__file__).resolve().parent
CAPACITY_PATH = REPO_ROOT / "config" / "capacity.json"
TEAMS_PATH = REPO_ROOT / "config" / "teams.json"


# ---------------------------------------------------------------------------
# Config loaders
# ---------------------------------------------------------------------------


def load_capacity_overrides() -> dict[str, float]:
    if not CAPACITY_PATH.exists():
        return {}
    try:
        raw = json.loads(CAPACITY_PATH.read_text())
        # Skip _comment / _examples keys
        return {
            k.lower(): float(v)
            for k, v in raw.items()
            if not k.startswith("_") and isinstance(v, (int, float))
        }
    except (json.JSONDecodeError, ValueError):
        return {}


def load_team_aliases() -> dict[str, list[str]]:
    if not TEAMS_PATH.exists():
        return {}
    try:
        raw = json.loads(TEAMS_PATH.read_text())
        return {
            k.lower(): [e.lower() for e in v]
            for k, v in raw.items()
            if not k.startswith("_") and isinstance(v, list)
        }
    except (json.JSONDecodeError, ValueError):
        return {}




# ---------------------------------------------------------------------------
# Period resolver: weekly / monthly cadences
# ---------------------------------------------------------------------------

DEFAULT_BUFFER_DAYS = 3  # late-entry grace period after a period closes


def resolve_period(
    period: str,
    as_of: date | None = None,
    buffer_days: int = DEFAULT_BUFFER_DAYS,
) -> tuple[date, date, str]:
    """Compute (start, end, label) for a named cadence.

    period:
      - "weekly":  most recent fully-closed Mon-Sun week, ended >= buffer_days ago
      - "monthly": most recent fully-closed calendar month, ended >= buffer_days ago

    as_of: anchor date for resolution. None -> today.

    Returns inclusive (start, end) and a human label for the brief title.
    """
    today = as_of or date.today()

    if period == "weekly":
        # Find the most recent Sunday whose distance from today is >= buffer_days.
        # weekday(): Mon=0..Sun=6. Days since the most-recent past Sunday:
        days_since_sunday = (today.weekday() + 1) % 7  # Sun -> 0, Mon -> 1, ...
        most_recent_sunday = today - timedelta(days=days_since_sunday) if days_since_sunday else today - timedelta(days=7)
        # Walk back week-by-week until buffer is satisfied.
        while (today - most_recent_sunday).days < buffer_days:
            most_recent_sunday -= timedelta(days=7)
        end = most_recent_sunday
        start = end - timedelta(days=6)  # Mon-Sun inclusive
        label = f"Week of {start.strftime('%b %-d')}-{end.strftime('%-d, %Y')}"
        return start, end, label

    if period == "monthly":
        # Most recent fully-closed calendar month, ended >= buffer_days ago.
        # Start from prior month relative to today.
        first_of_this_month = today.replace(day=1)
        end = first_of_this_month - timedelta(days=1)  # last day of prior month
        # If buffer not met (e.g. running on the 1st or 2nd), step back another month.
        while (today - end).days < buffer_days:
            end = end.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)
        label = end.strftime("%B %Y")
        return start, end, label

    raise ValueError(f"Unknown period '{period}' — expected weekly or monthly")


# Outputs land in Matt's iCloud Exterro folder so they're visible across
# devices and outside the repo. Override with --write for one-off paths.
OUTPUTS_DIR = Path(
    "/Users/matthew.abadie/Library/Mobile Documents/com~apple~CloudDocs/"
    "iCloud Storage/Exterro/outputs/utilization"
)


def default_write_path(period: str, group: str | None, start: date, end: date) -> Path:
    """Sensible default file name when --write isn't passed but --period is."""
    g = (group or "team").lower().replace(" ", "_")
    base = f"{g}_{period}_{start.isoformat()}_to_{end.isoformat()}.md"
    return OUTPUTS_DIR / base


# ---------------------------------------------------------------------------
# Resource resolution
# ---------------------------------------------------------------------------


def resolve_resources(args, all_users) -> list[dict]:
    """Resolve --resource / --resources / --group inputs into a list of user dicts.

    Exits with non-zero on ambiguous matches so the caller can re-prompt.
    """
    queries: list[str] = []
    if args.group:
        team_aliases = load_team_aliases()
        emails = team_aliases.get(args.group.lower())
        if not emails:
            sys.stderr.write(
                f"ERROR: group '{args.group}' not in config/teams.json. "
                f"Available: {sorted(team_aliases) or '(none)'}\n"
            )
            sys.exit(2)
        queries = emails
    else:
        if args.resource:
            queries = [args.resource]
        if args.resources:
            queries.extend(q.strip() for q in args.resources.split(",") if q.strip())

    if not queries:
        sys.stderr.write("ERROR: provide --resource, --resources, or --group\n")
        sys.exit(2)

    resolved: list[dict] = []
    for q in queries:
        user, candidates = rl.resolve_user(q, all_users)
        if user:
            resolved.append(user)
        elif len(candidates) > 1:
            options = ", ".join(
                f"{u.get('firstName','').strip()} {u.get('lastName','').strip()} "
                f"<{u.get('email')}>"
                for u in candidates
            )
            sys.stderr.write(
                f"ERROR: '{q}' matches multiple users — pick one and re-run with the email:\n"
                f"  {options}\n"
            )
            sys.exit(3)
        else:
            sys.stderr.write(f"ERROR: no active TEAM_MEMBER matches '{q}'\n")
            sys.exit(4)
    return resolved


def capacity_for(email: str, overrides: dict[str, float],
                 cli_override: float | None) -> tuple[float, str]:
    """Return (weekly_capacity, source_label) for the brief Notes block."""
    if cli_override is not None:
        return cli_override, "cli override"
    e = email.lower()
    if e in overrides:
        return overrides[e], "config override"
    return rb.DEFAULT_WEEKLY_CAPACITY, f"default {rb.DEFAULT_WEEKLY_CAPACITY:.0f} hrs/wk"



def domain_for(user: dict) -> str:
    """Return Rocketlane Domain field value (multi-value joined with ', ')."""
    for f in user.get("fields") or []:
        if f.get("fieldLabel") == "Domain":
            return f.get("fieldValueLabel") or ""
    return ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Render a Rocketlane time-entry utilization brief.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Examples:", 1)[-1] if __doc__ else "",
    )
    src = p.add_mutually_exclusive_group(required=False)
    src.add_argument("--resource", help="Single name or email")
    src.add_argument("--resources", help="Comma-separated emails (team mode)")
    src.add_argument("--group", help="Group keyword from config/teams.json")

    p.add_argument(
        "--period", choices=["weekly", "monthly"], default=None,
        help=(
            "Auto-compute date range for a cadence. "
            "weekly = last fully-closed Mon-Sun week (respecting --buffer-days). "
            "monthly = last fully-closed calendar month. "
            "Mutually exclusive with --start/--end."
        ),
    )
    p.add_argument(
        "--as-of", default=None,
        help="Anchor date YYYY-MM-DD for --period resolution (default: today). Useful for backfill/testing.",
    )
    p.add_argument(
        "--buffer-days", type=int, default=DEFAULT_BUFFER_DAYS,
        help=f"Days to wait after period close before reporting (default: {DEFAULT_BUFFER_DAYS}).",
    )
    p.add_argument("--start", required=False, help="Start date YYYY-MM-DD (inclusive). Required unless --period is set.")
    p.add_argument("--end", required=False, help="End date YYYY-MM-DD (inclusive). Required unless --period is set.")
    p.add_argument("--top-projects", type=int, default=5)
    p.add_argument("--top-customers", type=int, default=5)
    p.add_argument(
        "--weekly-capacity", type=float, default=None,
        help="Override capacity (hrs/wk) for ALL resources in this run."
    )
    p.add_argument(
        "--mode", choices=["single", "team", "auto"], default="auto",
        help="auto: 1 resource → single, 2+ → team."
    )
    p.add_argument(
        "--no-capacity", action="store_true",
        help="Suppress capacity / utilization figures."
    )
    p.add_argument(
        "--write", default=None,
        help="Optional path to also write the brief markdown to."
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="Suppress the leading '[utilization] fetching...' status line."
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    period_label: str | None = None
    if args.period:
        if args.start or args.end:
            sys.stderr.write("ERROR: --period is mutually exclusive with --start/--end\n")
            return 2
        try:
            as_of = (datetime.strptime(args.as_of, "%Y-%m-%d").date()
                     if args.as_of else date.today())
        except ValueError:
            sys.stderr.write("ERROR: --as-of must be YYYY-MM-DD\n")
            return 2
        try:
            start, end, period_label = resolve_period(args.period, as_of, args.buffer_days)
        except ValueError as e:
            sys.stderr.write(f"ERROR: {e}\n")
            return 2
        if not args.quiet:
            sys.stderr.write(
                f"[utilization] period={args.period} as_of={as_of} "
                f"buffer={args.buffer_days}d -> {start} to {end} ({period_label})\n"
            )
    else:
        if not (args.start and args.end):
            sys.stderr.write("ERROR: provide --period OR --start/--end\n")
            return 2
        try:
            start = datetime.strptime(args.start, "%Y-%m-%d").date()
            end = datetime.strptime(args.end, "%Y-%m-%d").date()
        except ValueError:
            sys.stderr.write("ERROR: --start and --end must be YYYY-MM-DD\n")
            return 2
        if end < start:
            sys.stderr.write("ERROR: --end must be >= --start\n")
            return 2

    if not rl.API_KEY:
        sys.stderr.write(
            "ERROR: ROCKETLANE_API_KEY not set. Expected in "
            f"{rl.SECRETS_FILE} or environment.\n"
        )
        return 5

    if not args.quiet:
        sys.stderr.write("[utilization] fetching users...\n")
    users, _ = rl.fetch_users()
    resolved = resolve_resources(args, users)

    user_ids = [str(u["userId"]) for u in resolved]
    if not args.quiet:
        sys.stderr.write(
            f"[utilization] fetching time entries for {len(user_ids)} resource(s) "
            f"{start} to {end}...\n"
        )
    entries, calls = rl.fetch_bulk_time_entries(
        since_date=start.isoformat(),
        until_date=end.isoformat(),
        user_ids=user_ids,
    )
    if not args.quiet:
        sys.stderr.write(f"[utilization] fetched {len(entries)} entries in {calls} API call(s)\n")

    overrides = load_capacity_overrides()

    # Bucket entries by user_id
    by_user: dict[str, list] = {uid: [] for uid in user_ids}
    for e in entries:
        uid = str((e.get("user") or {}).get("userId") or "")
        if uid in by_user:
            by_user[uid].append(e)

    summaries: list[rb.ResourceSummary] = []
    for u in resolved:
        uid = str(u["userId"])
        first = (u.get("firstName") or "").strip()
        last = (u.get("lastName") or "").strip()
        name = f"{first} {last}".strip() or u.get("email") or uid
        cap, source = capacity_for(u.get("email", ""), overrides, args.weekly_capacity)
        rs = rb.ResourceSummary(
            user_id=uid,
            email=(u.get("email") or "").lower(),
            name=name,
            weekly_capacity=cap,
            capacity_source=source,
            entries=rb.coerce_entries(by_user.get(uid, []), u.get("email"), name),
            domain=domain_for(u),
        )
        summaries.append(rs)

    output = rb.render(
        summaries, start, end,
        mode=args.mode,
        top_projects=args.top_projects,
        top_customers=args.top_customers,
        show_capacity=not args.no_capacity,
    )

    write_path = args.write
    if not write_path and args.period:
        wp = default_write_path(args.period, args.group, start, end)
        wp.parent.mkdir(parents=True, exist_ok=True)
        write_path = str(wp)
        if not args.quiet:
            sys.stderr.write(f"[utilization] writing brief to {write_path}\n")
    if write_path:
        Path(write_path).write_text(output)

    sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
