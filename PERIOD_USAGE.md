# Period-Aware Utilization Briefs

`utilization.py` now supports two cadence modes that auto-compute date ranges
and respect a configurable late-entry grace period.

## Cadences

### Weekly
- **Window:** Monday–Sunday (ISO 8601)
- **Default trigger:** any day after the buffer has elapsed past the most recent Sunday
- **Resolution rule:** the most recent Sunday whose distance from today is `>= --buffer-days`. If not satisfied, walk back one full week.

### Monthly
- **Window:** previous full calendar month
- **Default trigger:** the 3rd of the following month (with default `--buffer-days=3`)
- **Resolution rule:** the most recent calendar month whose last day is `>= --buffer-days` ago.

## CLI

```bash
# Weekly — runs as-of today, last fully-closed week
python3 utilization.py --period weekly --group ediscovery

# Monthly — runs as-of today, last fully-closed month
python3 utilization.py --period monthly --group all-ps

# Adjust the late-entry buffer (default 3 days)
python3 utilization.py --period weekly --group ediscovery --buffer-days 4

# Backfill or test against a specific anchor date
python3 utilization.py --period monthly --as-of 2026-05-03 --group all-ps

# --start/--end still work for ad-hoc; they are mutually exclusive with --period
python3 utilization.py --resource jake.hill@exterro.com --start 2026-03-01 --end 2026-03-15
```

## Output

When `--period` is set and `--write` is not passed, the brief is written to
`~/Library/Mobile Documents/com~apple~CloudDocs/iCloud Storage/Exterro/outputs/utilization/<group>_<period>_<start>_to_<end>.md`
(your iCloud Exterro/outputs/utilization folder), e.g.

- `Exterro/outputs/utilization/ediscovery_weekly_2026-04-20_to_2026-04-26.md`
- `Exterro/outputs/utilization/all-ps_monthly_2026-04-01_to_2026-04-30.md`

Override with `--write /full/path.md` if you need a different location.

## Recommended cron schedule (Mac, `crontab -e`)

```cron
# Weekly eDiscovery brief — every Wed at 6:00 AM (3 days after Sun close)
0 6 * * 3   cd ~/repos/ps-daily-briefing && /usr/bin/python3 utilization.py --period weekly --group ediscovery >> ~/Library/Logs/util_weekly.log 2>&1

# Weekly all-PS brief — every Wed at 6:30 AM
30 6 * * 3  cd ~/repos/ps-daily-briefing && /usr/bin/python3 utilization.py --period weekly --group all-ps >> ~/Library/Logs/util_weekly.log 2>&1

# Monthly all-PS brief — 3rd of every month at 7:00 AM
0 7 3 * *   cd ~/repos/ps-daily-briefing && /usr/bin/python3 utilization.py --period monthly --group all-ps >> ~/Library/Logs/util_monthly.log 2>&1

# Refresh teams.json from Rocketlane Domain field — Sundays at 11:00 PM
0 23 * * 0  cd ~/repos/ps-daily-briefing && /usr/bin/python3 refresh_teams.py >> ~/Library/Logs/refresh_teams.log 2>&1
```

> If you'd rather use macOS launchd or a Cowork scheduled task, the same commands apply — point the runner at `python3 utilization.py --period {weekly|monthly} --group <cohort>`.

## Buffer-day rationale

| Cadence  | Buffer | Reasoning |
|----------|--------|-----------|
| Weekly   | 3 days | Sunday close → run Wednesday. Captures Mon/Tue catch-up entries from late-Friday work. |
| Monthly  | 3 days | Month close → run on the 3rd. Most month-end clean-up happens in the first 48 hours; day-3 is a safe ceiling. |

Override on a per-run basis with `--buffer-days N`.

## Backfill examples

```bash
# Generate the previous 4 weekly briefs in one go (POSIX shell)
for d in 2026-04-29 2026-04-22 2026-04-15 2026-04-08; do
  python3 utilization.py --period weekly --as-of $d --group ediscovery
done

# Generate Q1 monthly briefs
for d in 2026-02-03 2026-03-03 2026-04-03; do
  python3 utilization.py --period monthly --as-of $d --group all-ps
done
```

## Future: annual retrospective

A `--period yearly` mode would follow the same pattern (most recent fully
closed calendar year, buffer 3 days). Hold for now — Q4 work.
