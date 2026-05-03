"""render_brief — pure transform from time-entry list → markdown utilization brief.

No API calls, no I/O beyond stdout/file write. Used by:
  - utilization.py (CLI entry point for the resource-time-summary skill)
  - any future scheduled task that needs the same rollup format

Designed so the same renderer powers single-resource briefs and multi-resource
team rollups. Capacity overrides + anomaly thresholds are constants you can
tune at the top of the file.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Iterable

from rocketlane_client import (
    INTERNAL_LABEL,
    NON_PROJECT_LABEL,
    parse_customer,
)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

DEFAULT_WEEKLY_CAPACITY = 40.0
LONG_DAY_THRESHOLD_HRS = 12.0
LIGHT_COVERAGE_DAYS = 3
HEAVY_ADMIN_PCT = 30.0
SINGLE_PROJECT_PCT = 70.0
NON_PROJECT_CATEGORY = "Non-project activities"
PROJECT_NAME_MAX = 60  # truncate long Forensics project names in the brief


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Entry:
    user_id: str
    user_email: str
    user_name: str
    date: date
    minutes: int
    billable: bool
    category: str
    activity: str
    project_id: str | None
    project_name: str | None
    customer: str

    @property
    def hours(self) -> float:
        return self.minutes / 60.0


@dataclass
class ResourceSummary:
    user_id: str
    email: str
    name: str
    weekly_capacity: float
    capacity_source: str  # "default 40 hrs/wk", "config override", "cli override"
    entries: list[Entry] = field(default_factory=list)
    domain: str = ""  # Rocketlane Domain field — multi-value joined with ", "


# ---------------------------------------------------------------------------
# Coercion: raw API entry dict → Entry
# ---------------------------------------------------------------------------


def coerce_entry(raw: dict, fallback_email: str | None = None,
                 fallback_name: str | None = None) -> Entry | None:
    """Convert a raw Rocketlane time-entry dict into an Entry.

    Returns None when the entry is unparseable (missing date, etc.) — caller
    should drop those rows.
    """
    try:
        d = datetime.strptime(raw["date"], "%Y-%m-%d").date()
    except (KeyError, ValueError, TypeError):
        return None
    user = raw.get("user") or {}
    project = raw.get("project") or {}
    category = (raw.get("category") or {}).get("categoryName") or "(uncategorized)"
    activity = raw.get("activityName") or (raw.get("task") or {}).get("taskName") or ""
    project_name = project.get("projectName")
    return Entry(
        user_id=str(user.get("userId") or ""),
        user_email=(user.get("emailId") or fallback_email or "").lower(),
        user_name=(
            f"{user.get('firstName', '').strip()} {user.get('lastName', '').strip()}".strip()
            or fallback_name
            or fallback_email
            or "Unknown"
        ),
        date=d,
        minutes=int(raw.get("minutes") or 0),
        billable=bool(raw.get("billable")),
        category=category,
        activity=activity,
        project_id=str(project.get("projectId")) if project.get("projectId") else None,
        project_name=project_name,
        customer=parse_customer(project_name),
    )


def coerce_entries(raw_entries: Iterable[dict],
                   fallback_email: str | None = None,
                   fallback_name: str | None = None) -> list[Entry]:
    out: list[Entry] = []
    for r in raw_entries:
        e = coerce_entry(r, fallback_email, fallback_name)
        if e and e.minutes > 0:
            out.append(e)
    return out


# ---------------------------------------------------------------------------
# Aggregation + anomaly detection
# ---------------------------------------------------------------------------


def working_days(start: date, end: date) -> int:
    """Mon–Fri count between start and end inclusive. Holidays not handled."""
    days = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days += 1
        cur += timedelta(days=1)
    return days


def fractional_weeks(start: date, end: date) -> float:
    return ((end - start).days + 1) / 7.0


def expected_capacity_hours(weekly: float, start: date, end: date) -> float:
    """Capacity = weekly * (working_days / 5). Saturdays/Sundays don't add capacity."""
    return round(weekly * working_days(start, end) / 5.0, 1)


def hours(minutes: int) -> float:
    return round(minutes / 60.0, 1)


def percent(numer: float, denom: float) -> float:
    if denom <= 0:
        return 0.0
    return round(100.0 * numer / denom, 0)


def aggregate(entries: list[Entry]) -> dict:
    total_min = sum(e.minutes for e in entries)
    billable_min = sum(e.minutes for e in entries if e.billable)

    by_project: dict[str, int] = defaultdict(int)
    by_customer: dict[str, int] = defaultdict(int)
    by_category: dict[str, int] = defaultdict(int)
    by_day: dict[date, int] = defaultdict(int)
    for e in entries:
        key = e.project_name or NON_PROJECT_LABEL
        by_project[key] += e.minutes
        by_customer[e.customer] += e.minutes
        by_category[e.category] += e.minutes
        by_day[e.date] += e.minutes

    return {
        "total_min": total_min,
        "billable_min": billable_min,
        "non_billable_min": total_min - billable_min,
        "by_project": by_project,
        "by_customer": by_customer,
        "by_category": by_category,
        "by_day": by_day,
    }


def detect_flags(rs: ResourceSummary, agg: dict, start: date, end: date,
                 capacity_hrs: float) -> list[str]:
    flags: list[str] = []
    total_min = agg["total_min"]
    if total_min == 0:
        flags.append("No time entries found in this period.")
        return flags

    wd = working_days(start, end)
    days_with_log = sum(1 for d, m in agg["by_day"].items() if m > 0 and d.weekday() < 5)
    missing = wd - days_with_log
    if missing >= LIGHT_COVERAGE_DAYS:
        flags.append(
            f"Light coverage: {missing} working day(s) with no logged time — "
            f"verify PTO vs missed entries."
        )

    long_days = [d for d, m in agg["by_day"].items() if m / 60.0 > LONG_DAY_THRESHOLD_HRS]
    if long_days:
        sample = ", ".join(d.isoformat() for d in sorted(long_days)[:3])
        suffix = "..." if len(long_days) > 3 else ""
        flags.append(
            f">{LONG_DAY_THRESHOLD_HRS:.0f} hr day(s) on: {sample}{suffix} "
            f"(often catch-up entries — math is still right)."
        )

    if total_min > 0 and agg["billable_min"] == 0:
        flags.append("0% billable for the period — flag if this is a delivery role.")

    non_proj_min = agg["by_category"].get(NON_PROJECT_CATEGORY, 0)
    pct_admin = percent(non_proj_min, total_min)
    if pct_admin > HEAVY_ADMIN_PCT:
        flags.append(f"Heavy non-project time: {pct_admin:.0f}% in '{NON_PROJECT_CATEGORY}'.")

    if agg["by_project"]:
        top_proj, top_min = max(agg["by_project"].items(), key=lambda kv: kv[1])
        top_pct = percent(top_min, total_min)
        if top_pct > SINGLE_PROJECT_PCT and top_proj != NON_PROJECT_LABEL:
            flags.append(
                f"{top_pct:.0f}% of hours on one project ({top_proj}) — "
                f"focus or staffing-fragility signal."
            )

    flags.append(
        f"Capacity baseline: {rs.weekly_capacity:.0f} hrs/wk ({rs.capacity_source}); "
        f"expected {capacity_hrs:.1f} hrs over the period."
    )
    return flags


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def fmt_range(start: date, end: date) -> str:
    weeks = fractional_weeks(start, end)
    if weeks == int(weeks):
        wlabel = f"{int(weeks)}w"
    else:
        wlabel = f"{weeks:.1f}w"
    return f"{start.isoformat()} to {end.isoformat()} ({wlabel})"


def render_resource(rs: ResourceSummary, start: date, end: date, *,
                    top_projects: int = 5, top_customers: int = 5,
                    show_capacity: bool = True) -> str:
    out: list[str] = []
    agg = aggregate(rs.entries)
    total_hrs = hours(agg["total_min"])
    billable_hrs = hours(agg["billable_min"])
    non_billable_hrs = hours(agg["non_billable_min"])
    cap_hrs = expected_capacity_hours(rs.weekly_capacity, start, end)
    util = percent(total_hrs, cap_hrs) if cap_hrs > 0 else 0.0
    bill_pct = percent(billable_hrs, total_hrs)
    non_bill_pct = percent(non_billable_hrs, total_hrs)

    out.append(f"**Time summary — {rs.name} — {fmt_range(start, end)}**")
    out.append("")
    if show_capacity:
        out.append(f"Logged: {total_hrs} hrs · Capacity: {cap_hrs} hrs · Utilization: {util:.0f}%")
    else:
        out.append(f"Logged: {total_hrs} hrs")
    out.append(
        f"Billable: {billable_hrs} hrs ({bill_pct:.0f}%) · "
        f"Non-billable: {non_billable_hrs} hrs ({non_bill_pct:.0f}%)"
    )

    non_project_min = agg["by_project"].get(NON_PROJECT_LABEL, 0)
    internal_min = sum(
        m for n, m in agg["by_project"].items() if parse_customer(n) == INTERNAL_LABEL
    )
    if non_project_min or internal_min:
        out.append(
            f"Non-project: {hours(non_project_min)} hrs · Internal: {hours(internal_min)} hrs "
            f"(excluded from customer rankings below)"
        )
    out.append("")

    out.append("Top customer projects (by hrs):")
    proj_filtered = [
        (name, mins) for name, mins in agg["by_project"].items()
        if name != NON_PROJECT_LABEL and parse_customer(name) != INTERNAL_LABEL
    ]
    proj_sorted = sorted(proj_filtered, key=lambda kv: kv[1], reverse=True)
    if not proj_sorted:
        out.append("- (none — all time was non-project or internal)")
    else:
        for i, (name, mins) in enumerate(proj_sorted[:top_projects], start=1):
            hrs_ = hours(mins)
            pct = percent(hrs_, total_hrs)
            short = name if len(name) <= PROJECT_NAME_MAX else name[: PROJECT_NAME_MAX - 1].rstrip() + "…"
            out.append(f"{i}. {short} — {hrs_} hrs ({pct:.0f}%)")
    out.append("")

    out.append("Top customers:")
    cust_filtered = [
        (name, mins) for name, mins in agg["by_customer"].items()
        if name not in (NON_PROJECT_LABEL, INTERNAL_LABEL)
    ]
    cust_sorted = sorted(cust_filtered, key=lambda kv: kv[1], reverse=True)
    if not cust_sorted:
        out.append("- (none — all time was non-project or internal)")
    else:
        for i, (name, mins) in enumerate(cust_sorted[:top_customers], start=1):
            out.append(f"{i}. {name} — {hours(mins)} hrs")
    out.append("")

    out.append("By category:")
    cat_sorted = sorted(agg["by_category"].items(), key=lambda kv: kv[1], reverse=True)
    if not cat_sorted:
        out.append("- (none)")
    else:
        for name, mins in cat_sorted:
            hrs_ = hours(mins)
            pct = percent(hrs_, total_hrs)
            out.append(f"- {name}: {hrs_} hrs ({pct:.0f}%)")
    out.append("")

    flags = detect_flags(rs, agg, start, end, cap_hrs)
    if flags:
        out.append("Notes:")
        for f in flags:
            out.append(f"- {f}")
    return "\n".join(out)


def render_team_rollup(resources: list[ResourceSummary], start: date, end: date) -> str:
    """Team rollup table + per-domain summary (when domains are populated)."""
    rows = []
    util_values: list[float] = []
    # Aggregate per-domain stats: domain -> [total_hrs_sum, cap_hrs_sum, util_list, count, billable_hrs, billable_pcts]
    by_domain: dict[str, dict] = {}
    for rs in resources:
        agg = aggregate(rs.entries)
        total_hrs = hours(agg["total_min"])
        billable_hrs = hours(agg["billable_min"])
        cap_hrs = expected_capacity_hours(rs.weekly_capacity, start, end)
        util = percent(total_hrs, cap_hrs) if cap_hrs > 0 else 0.0
        util_values.append(util)
        domain_label = rs.domain or "(no domain)"
        rows.append((rs.name or rs.email, domain_label, total_hrs, cap_hrs, util))
        d = by_domain.setdefault(domain_label, {
            "logged": 0.0, "capacity": 0.0, "billable": 0.0, "count": 0
        })
        d["logged"] += total_hrs
        d["capacity"] += cap_hrs
        d["billable"] += billable_hrs
        d["count"] += 1

    if not rows:
        return ""
    avg = sum(util_values) / len(util_values) if util_values else 0.0
    rows.sort(key=lambda r: r[4], reverse=True)

    out = [
        f"**Team time summary — {fmt_range(start, end)}**",
        "",
    ]

    # Per-domain summary block (only render if >1 domain present)
    has_domain_data = len([d for d in by_domain if d != "(no domain)"]) > 0
    distinct_domains = len(by_domain)
    if has_domain_data and distinct_domains > 1:
        out.append("**By domain:**")
        out.append("")
        out.append("| Domain | Resources | Logged | Capacity | Util % | Billable % |")
        out.append("|--------|-----------|--------|----------|--------|------------|")
        # sort domains by logged hrs desc
        for dom, stats in sorted(by_domain.items(), key=lambda kv: -kv[1]["logged"]):
            d_util = percent(stats["logged"], stats["capacity"]) if stats["capacity"] > 0 else 0.0
            d_bill = percent(stats["billable"], stats["logged"]) if stats["logged"] > 0 else 0.0
            out.append(
                f"| {dom} | {stats['count']} | {stats['logged']:.1f} | "
                f"{stats['capacity']:.1f} | {d_util:.0f}% | {d_bill:.0f}% |"
            )
        out.append("")

    out.append("| Resource | Domain | Logged | Capacity | Util % | vs. Team Avg |")
    out.append("|----------|--------|--------|----------|--------|--------------|")
    for name, dom, total_hrs, cap_hrs, util in rows:
        delta = util - avg
        sign = "+" if delta >= 0 else ""
        out.append(f"| {name} | {dom} | {total_hrs} | {cap_hrs} | {util:.0f}% | {sign}{delta:.0f} pp |")
    out.append("")
    out.append(f"_Team average utilization: {avg:.0f}%_")
    return "\n".join(out)


def render(resources: list[ResourceSummary], start: date, end: date, *,
           mode: str = "auto", top_projects: int = 5, top_customers: int = 5,
           show_capacity: bool = True) -> str:
    """Render the full brief — team rollup (if applicable) + per-resource sections."""
    if mode == "auto":
        mode = "team" if len(resources) > 1 else "single"

    blocks: list[str] = []
    if mode == "team":
        blocks.append(render_team_rollup(resources, start, end))
        blocks.append("")
    for rs in resources:
        blocks.append(render_resource(
            rs, start, end,
            top_projects=top_projects,
            top_customers=top_customers,
            show_capacity=show_capacity,
        ))
        blocks.append("")
    return "\n".join(blocks).rstrip() + "\n"
