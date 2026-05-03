"""Pre-curate action and hotspot candidate lists for the daily digest.

This module mirrors the rule-based filters and director attribution described
in routine_prompt.md, but runs them deterministically in Python so the Routine
session only does the writing/judgment work (composing "why now" lines, picking
action verbs, copy-editing in Matt's voice).

When the rules here change, update routine_prompt.md in lockstep so the prompt
and the data shape stay in sync.

Inputs: a list of enriched project dicts (as produced by daily_digest.enrich_project,
plus the `team` field set during the --output flow).
Outputs: {"candidate_actions": [...], "candidate_hotspots": [...]}.

Each candidate row carries the raw fields a downstream renderer needs to write
the line — including health_notes / weekly_status text on attention-flagged
projects so the writer can derive a "why now" without re-fetching anything.
"""

from collections import defaultdict
from datetime import datetime
from typing import Iterable

# ═══════════════════════════════════════════════════════════════════════════════
# DIRECTOR ATTRIBUTION
# ═══════════════════════════════════════════════════════════════════════════════
# Email → display name. Haydee and Geoff roll up under Vanessa per Matt's org.
DIRECTOR_EMAIL_MAP = {
    "vanessa.graham@exterro.com": "Vanessa",
    "haydee.alonso@exterro.com": "Vanessa",
    "geoff.gaydos@exterro.com": "Vanessa",
    "maggie.ledbetter@exterro.com": "Maggie",
    "oronde.ward@exterro.com": "Oronde",
}
CODY_OWNER_NAME = "Cody Greenwaldt"

# ═══════════════════════════════════════════════════════════════════════════════
# RULE THRESHOLDS
# ═══════════════════════════════════════════════════════════════════════════════
ACTION_RED_AGE_DAYS = 14
ACTION_RED_VALUE_THRESHOLD = 100_000
Z2E_PHASE2_LAGGARD_THRESHOLD = 30  # task_progress %

ACTION_PER_DIRECTOR_CAP = 3
ACTION_TOTAL_CAP = 10
HOTSPOT_TOTAL_CAP = 10


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _age_days(p: dict, now_ms: int) -> int:
    created = p.get("created_at") or 0
    if not created:
        return 0
    return max(0, int((now_ms - created) / 86_400_000))


def _is_z2e_phase2(p: dict) -> bool:
    """Mirrors build_z2e_tracker_section's Phase 2 definition: any Z2E sub-type
    that is NOT Phase 1 and NOT 'Z2E - Not Started'."""
    st = (p.get("sub_type") or "").lower()
    if "z2e" not in st:
        return False
    if "z2e phase 1" in st:
        return False
    if "z2e - not started" in st:
        return False
    return True


def _assign_director(p: dict) -> str:
    """Return the swimlane director label: Vanessa, Maggie, Oronde, Cody, or Unattributed.

    Order of precedence:
      1. Cody override — subscriptions owned by Cody Greenwaldt land in his lane
      2. Responsible Director email map (Haydee/Geoff roll up under Vanessa)
      3. Fallback: existing `team` field assigned during enrichment
    """
    if (
        p.get("owner") == CODY_OWNER_NAME
        and "subscription" in (p.get("project_type") or "").lower()
    ):
        return "Cody"

    rd = (p.get("responsible_director") or "").strip().lower()
    if rd in DIRECTOR_EMAIL_MAP:
        return DIRECTOR_EMAIL_MAP[rd]

    team = (p.get("team") or "").lower()
    if "ediscovery" in team:
        return "Vanessa"
    if "post" in team:
        return "Oronde"
    if "data psg" in team or "privacy" in team or "data, security" in team:
        return "Maggie"
    return "Unattributed"


# ═══════════════════════════════════════════════════════════════════════════════
# RULES
# ═══════════════════════════════════════════════════════════════════════════════
def _is_red_escalation(p: dict, now_ms: int) -> bool:
    """Red health AND aged >14d AND $100K+ AND escalation keywords in PM notes."""
    if (p.get("health") or "").lower() != "red":
        return False
    if _age_days(p, now_ms) <= ACTION_RED_AGE_DAYS:
        return False
    if (p.get("contract_value") or 0) <= ACTION_RED_VALUE_THRESHOLD:
        return False
    if not p.get("escalation_flags"):
        return False
    return True


def _is_z2e_phase2_laggard(p: dict) -> bool:
    """Z2E Phase 2 project below the laggard threshold for task progress."""
    if not _is_z2e_phase2(p):
        return False
    progress = p.get("task_progress")
    if progress is None:
        return False
    return progress < Z2E_PHASE2_LAGGARD_THRESHOLD


def _is_review_module_blocker(p: dict) -> bool:
    """Cody's subscription with escalation keywords in PM notes."""
    if p.get("owner") != CODY_OWNER_NAME:
        return False
    if "subscription" not in (p.get("project_type") or "").lower():
        return False
    if not p.get("escalation_flags"):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# PAYLOAD SHAPE
# ═══════════════════════════════════════════════════════════════════════════════
def _candidate_payload(
    p: dict, director: str, age_days: int, rule_reasons: list, score: float
) -> dict:
    """The shape of a candidate row passed to the Routine session.

    Carries enough context for the writer to compose a one-line "why now" without
    re-fetching anything: full health_notes / weekly_status text, escalation_flags
    indicating which field tripped, and the rule_reasons that surfaced it.
    """
    return {
        "id": p.get("id"),
        "name": p.get("name"),
        "customer": p.get("customer"),
        "owner": p.get("owner"),
        "director": director,
        "health": p.get("health"),
        "status": p.get("status"),
        "project_type": p.get("project_type"),
        "sub_type": p.get("sub_type"),
        "client_segment": p.get("client_segment"),
        "contract_value": p.get("contract_value") or 0,
        "ps_net_price": p.get("ps_net_price") or 0,
        "age_days": age_days,
        "task_progress": p.get("task_progress"),
        "responsible_director": p.get("responsible_director"),
        "escalation_flags": p.get("escalation_flags") or [],
        "health_notes": p.get("health_notes") or "",
        "weekly_status": p.get("weekly_status") or "",
        "hours_logged_7d": p.get("hours_logged_7d", 0.0),
        "billable_hours_7d": p.get("billable_hours_7d", 0.0),
        "entry_count_7d": p.get("entry_count_7d", 0),
        "rule_reasons": rule_reasons,
        "score": score,
    }


def _score(p: dict, age_days: int) -> float:
    """Ranking signal: contract_value * max(age_days, 1).

    The max(age_days, 1) prevents a 0 score for brand-new projects so they can
    still show up in the hotspot list when health is bad — they just rank low.
    """
    return float(p.get("contract_value") or 0) * max(age_days, 1)


# ═══════════════════════════════════════════════════════════════════════════════
# SELECTION
# ═══════════════════════════════════════════════════════════════════════════════
def select_candidate_actions(projects: Iterable[dict], now_ms: int) -> list:
    """Apply the three action rules; rank by score; cap per director and total."""
    candidates = []
    for p in projects:
        reasons = []
        if _is_red_escalation(p, now_ms):
            reasons.append("red_escalation")
        if _is_z2e_phase2_laggard(p):
            reasons.append("z2e_phase2_laggard")
        if _is_review_module_blocker(p):
            reasons.append("review_module_blocker")
        if not reasons:
            continue
        director = _assign_director(p)
        age = _age_days(p, now_ms)
        score = _score(p, age)
        candidates.append(_candidate_payload(p, director, age, reasons, score))

    candidates.sort(key=lambda c: c["score"], reverse=True)

    by_director = defaultdict(int)
    capped = []
    for c in candidates:
        if by_director[c["director"]] >= ACTION_PER_DIRECTOR_CAP:
            continue
        by_director[c["director"]] += 1
        capped.append(c)
        if len(capped) >= ACTION_TOTAL_CAP:
            break
    return capped


def select_candidate_hotspots(
    projects: Iterable[dict], action_ids: set, now_ms: int
) -> list:
    """Red/yellow projects ranked by score, excluding anything already in actions."""
    candidates = []
    for p in projects:
        if p.get("id") in action_ids:
            continue
        health = (p.get("health") or "").lower()
        if health not in ("red", "yellow"):
            continue
        age = _age_days(p, now_ms)
        score = _score(p, age)
        if score == 0:
            continue
        candidates.append(
            _candidate_payload(p, _assign_director(p), age, [], score)
        )
    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates[:HOTSPOT_TOTAL_CAP]


def build_candidate_lists(all_enriched: Iterable[dict], now: datetime) -> dict:
    """Top-level entry point used by daily_digest.py's --output flow.

    Returns {"candidate_actions": [...], "candidate_hotspots": [...]}.
    """
    now_ms = int(now.timestamp() * 1000)
    actions = select_candidate_actions(all_enriched, now_ms)
    action_ids = {a["id"] for a in actions}
    hotspots = select_candidate_hotspots(all_enriched, action_ids, now_ms)
    return {"candidate_actions": actions, "candidate_hotspots": hotspots}


# ═══════════════════════════════════════════════════════════════════════════════
# SWIMLANE STATS
#
# Pre-computed per-director health rollups so the Routine session can render
# the 4-line health summary at the top of each swimlane without iterating the
# full project roster (which is intentionally NOT in digest_data.json — see
# daily_digest.py's --output flow comment for context).
# ═══════════════════════════════════════════════════════════════════════════════
def build_swimlane_stats(
    active_projects: Iterable[dict],
    stale_project_ids: set = None,
) -> dict:
    """Roll up active projects by director swimlane.

    Returns:
        {
            "Vanessa": {"active": 156, "red": 18, "yellow": 35, "green": 36,
                        "no_health": 67, "hours_logged_7d": 234.5, "stale_count": 14},
            "Maggie": {...},
            "Oronde": {...},
            "Cody": {...},
            "Unattributed": {...},   # only present if non-empty
        }
    """
    stale_set = set(stale_project_ids or [])
    stats = {}
    for p in active_projects:
        d = _assign_director(p)
        s = stats.setdefault(
            d,
            {
                "active": 0,
                "red": 0,
                "yellow": 0,
                "green": 0,
                "no_health": 0,
                "hours_logged_7d": 0.0,
                "stale_count": 0,
            },
        )
        s["active"] += 1
        h = (p.get("health") or "").lower()
        if h in ("red", "yellow", "green"):
            s[h] += 1
        else:
            s["no_health"] += 1
        s["hours_logged_7d"] += p.get("hours_logged_7d") or 0.0
        if p.get("id") in stale_set:
            s["stale_count"] += 1
    # Round hours for cleaner JSON
    for d in stats:
        stats[d]["hours_logged_7d"] = round(stats[d]["hours_logged_7d"], 1)
    return stats
