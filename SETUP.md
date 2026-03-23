# PS Daily Briefing — GitHub Actions Setup

## What This Does
Runs every weekday, pulls live data from Rocketlane API, and delivers:
- **6 AM EST** — Full HTML intelligence digest via email
- **8 AM EST** — Condensed summary card to Google Chat

Covers both PS (eDiscovery, Data PSG, Post Implementation) and Forensics scopes.

## Prerequisites
- A GitHub account (free tier is fine — you get 2,000 Actions minutes/month)
- Your Rocketlane API key
- A Google Chat Space webhook URL
- A Gmail address + app password for email delivery
- (Optional) Anthropic API key for AI-powered intelligence brief

## Repo Structure
```
ps-daily-briefing/
├── .github/
│   └── workflows/
│       ├── daily-digest.yml          # PS scope (eDiscovery, Data PSG, Post Impl)
│       ├── forensics-digest.yml      # Forensics scope
│       ├── subscription-tracker.yml  # Subscription consumption monitor
│       └── subscription-audit.yml    # Monthly subscription audit
├── daily_digest.py                   # Main digest script (email + chat)
├── subscription_tracker.py           # Subscription hour tracking
├── subscription_audit.py             # Monthly audit
├── claude_utils.py                   # Shared AI helper
└── .snapshots/                       # State for change detection (auto-managed)
```

## Secrets Required
Go to: **Settings > Secrets and variables > Actions > New repository secret**

| Secret Name | Required For |
|---|---|
| `ROCKETLANE_API_KEY` | All workflows |
| `GCHAT_WEBHOOK_URL` | Chat card delivery |
| `GMAIL_ADDRESS` | Email delivery |
| `GMAIL_APP_PASSWORD` | Email delivery |
| `ANTHROPIC_API_KEY` | AI intelligence brief (optional) |
| `GCHAT_SUB_WEBHOOK_URL` | Subscription tracker chat |

## Usage

```bash
# Email only (default)
python daily_digest.py

# Chat card only
python daily_digest.py --mode chat

# Both email + chat
python daily_digest.py --mode both

# Forensics scope
python daily_digest.py --scope forensics --mode both

# Preview without sending
python daily_digest.py --dry-run
```

## Schedule
- **Email**: `0 11 * * 1-5` (11:00 UTC = 6:00 AM EST, weekdays)
- **Chat**: `0 13 * * 1-5` (13:00 UTC = 8:00 AM EST, weekdays)
- GitHub Actions cron can drift up to ~15 minutes

## Monitoring
- Check **Actions tab** for run history and logs
- Failed runs show red; click into them to see the error output
- GitHub sends email notifications on workflow failures by default

## Updating
Edit `daily_digest.py` directly in the repo and push. Changes take effect on the next scheduled run.
