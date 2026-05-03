# PS Daily Digest Routine — Setup

How to wire up the PS Daily Digest as an Anthropic Routine.

## What it does

Each run, the Routine fetches Rocketlane project state via `daily_digest.py --output digest_data.json`, then drafts a Gmail to PS director leadership with this morning's actions, hotspots, and four swimlanes (eDiscovery / Cody's subscriptions / Post-Impl / Privacy-Data-Gov). Drafts only — Matt reviews and sends manually for the first week, then the trigger flips to scheduled.

## Routine config

| Setting | Value |
|---|---|
| Name | `PS Daily Digest` |
| Repo | `mattabadie/ps-daily-briefing` |
| Branch | `main` |
| Setup | `pip install -r requirements.txt` (no-op — script is stdlib-only) |
| Trigger | **Manual only** for the first week. Then weekdays at 06:00 PT. |

## System prompt

Paste from [`routine_prompt.md`](routine_prompt.md) — the section between the `---` rules.

## Environment

Outbound network must be allowed to:
- `services.api.exterro.com` (Rocketlane API)

Required env var (set as a Routine secret):
- `ROCKETLANE_API_KEY` — current value lives in `~/Library/Mobile Documents/com~apple~CloudDocs/iCloud Storage/Exterro/.secrets/rocketlane.env`

Not needed (the `--output` path skips them):
- `GMAIL_ADDRESS`, `GMAIL_APP_PASSWORD`, `GCHAT_WEBHOOK_URL`, `ANTHROPIC_API_KEY`

## Connectors

**Gmail only** (least privilege). The Routine creates a draft in Matt's account; nothing else is needed.

## Disable the GitHub Actions path

The repo has `.github/workflows/daily-digest.yml` that runs the same script via personal-Gmail SMTP. Disable it once Routine goes live to avoid double-sends:

```bash
gh workflow disable daily-digest.yml --repo mattabadie/ps-daily-briefing
```

(or comment out the schedule trigger in the workflow file).

## First-week test loop

1. Open the Routine, click **Run now**
2. Wait ~30 minutes for the Rocketlane fetch to complete
3. Open Gmail drafts, find the new "PS Daily Intelligence — …" draft
4. Eyeball it. Edit if needed. Send manually.
5. Note anything wrong. Update the system prompt or the script. Re-run.

After ~5 clean drafts in a row, flip the trigger to scheduled (weekdays 06:00 PT).

## Known gaps to revisit

- **`responsible_director` populated on 92% of projects** — 39 projects have it blank. Those get bucketed under the existing team-by-director-ID logic from `SCOPE_CONFIG` instead. Not blocking; worth a backfill pass eventually.
- **Phase 2 — pre-curate actions/hotspots in Python.** Once the rule-based action selection is stable in the Routine prompt, port the rules into `daily_digest.py` so they ship as deterministic lists in the JSON. Routine then just renders.
- **Long-form HTML report.** The legacy 157KB HTML report still gets built when the script runs without `--output`. Decide whether to host it (S3, GitHub Pages) and link from the email, or kill it.
