# PS Operations Daily Intelligence Digest

Comprehensive daily operational digest for the VP of PS Services, covering new projects, health & status changes, PM-published updates, resource utilization anomalies, and (on Fridays) a written weekly narrative.

## Quick Start

```bash
# Generate and send today's digest
python daily_digest.py

# Preview without sending
python daily_digest.py --dry-run

# Include weekly narrative regardless of day (for testing)
python daily_digest.py --force-weekly

# Dry run + weekly narrative
python daily_digest.py --dry-run --force-weekly
```

## Environment Variables

- `ROCKETLANE_API_KEY` — Rocketlane API key (required)
- `GMAIL_ADDRESS` — Gmail sender address (required)
- `GMAIL_APP_PASSWORD` — Gmail app password (required)

## Features

### 1. NEW PROJECTS (24h)
Shows projects created in the last 24 hours under the three service teams:
- Customer, project name, PM, type, status, and Rocketlane link
- Sorted by creation date (newest first)

### 2. HEALTH & STATUS CHANGES
Compares current project state against a snapshot from the prior run. Detects:
- **Health color changes** (red ↔ yellow ↔ green)
- **Status label changes** (e.g., "In Progress" → "On Hold")
- **New/updated Internal Project Health Notes**
- **New/updated Internal Weekly Status**

Shows what changed, from→to, with full project context (customer, PM, link).

**Note:** This section is unavailable on the first run while the baseline snapshot is being established. Subsequent runs will compare against the saved state.

### 3. PM-PUBLISHED UPDATES (24h)
Fetches from the `project-updates` endpoint showing:
- Title of the update
- PM who published it
- Project name and customer
- Status label (Running Late, On Track, etc.)
- Timestamp and Rocketlane link

### 4. BURN RATE ANOMALIES
Analyzes time entries for active projects:
- Compares last 7 days of hours vs. 30-day average weekly burn
- Flags projects where:
  - **Last week > 2x the average** (accelerating/over-utilization)
  - **Last week < 0.3x the average** (decelerating/stalled)
- Shows hours, average, ratio, and risk flag
- Helps identify capacity issues or scope changes

### 5. WEEKLY NARRATIVE (Fridays only)
On Fridays, an additional written summary section is included that reads like a chief of staff's memo:
- New projects onboarded this week
- Health trend snapshot (red/yellow/green counts by team)
- Count of status/health changes
- PM update activity
- Resource utilization patterns
- Context for escalations

Use `--force-weekly` to include this section on any day (useful for testing).

## Snapshot Management

The script maintains a state snapshot at `.snapshots/project_state.json`:
- **First run:** Creates the baseline snapshot. Health changes section will show a note about availability on next run.
- **Subsequent runs:** Loads prior snapshot, compares current state, detects changes, and updates the snapshot.

Snapshots are lightweight (just fields needed for diffing) and are automatically managed.

## Email Format

- **Dark header** with teal accent color, showing title and date
- **KPI badges** for quick metrics (active projects, red/yellow health, new projects)
- **Organized sections** each with count badges
- **Professional executive briefing** tone, designed for VP-level consumption
- **Inline styles** throughout (Gmail-compatible, no CSS blocks)
- **Rocketlane project links** for easy navigation
- **Timestamp footer** with generation time

## Team Filtering

Projects are automatically assigned to the three service teams by checking if the director is:
- A member of the project's `teamMembers.members[]` list, OR
- The owner of the project

Directors:
- **eDiscovery:** Vanessa Graham (ID: 393610)
- **Data PSG:** Maggie Ledbetter (ID: 393604)
- **Post Implementation:** Oronde Ward (ID: 393607)

## Performance

- Uses ThreadPoolExecutor with 4 workers for parallel time entry fetching (burn rate analysis)
- Exponential backoff (2s, 4s) on API rate-limit errors (429)
- ~30-60 seconds typical runtime depending on project count and activity

## Integration with Scheduler

Add to cron/task scheduler:

```bash
# Daily at 8 AM
0 8 * * * cd /path/to/ps-daily-briefing && ROCKETLANE_API_KEY=... GMAIL_ADDRESS=... GMAIL_APP_PASSWORD=... python daily_digest.py

# Weekly narrative on Friday at 8 AM
0 8 * * 5 cd /path/to/ps-daily-briefing && ROCKETLANE_API_KEY=... GMAIL_ADDRESS=... GMAIL_APP_PASSWORD=... python daily_digest.py
```

The script auto-detects Fridays and includes the narrative; no special flag needed for scheduled runs.
