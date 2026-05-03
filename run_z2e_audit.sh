#!/bin/zsh
# run_z2e_audit.sh — wrapper invoked by launchd weekly cron.
#
# Runs the Z2E migration audit with the Customers-tab xlsx as the ARR source,
# writes the workbook to Exterro/Outputs/, and snapshots today's metrics into
# Exterro/Outputs/z2e_snapshots/ for the trend tab.
#
# launchd cron: ~/Library/LaunchAgents/com.exterro.z2e-audit.plist (Monday 7am).

set -e
set -o pipefail

REPO="$HOME/repos/ps-daily-briefing"
ARR_FILE="$HOME/Library/Mobile Documents/com~apple~CloudDocs/iCloud Storage/Exterro/Strategic Planning/_(NEW) Zapproved Transition Status Tracking.xlsx"

cd "$REPO"

# Pin to a known python3. Override via Z2E_PYTHON env var if you need a venv.
PYTHON="${Z2E_PYTHON:-/usr/bin/python3}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] z2e audit run starting"
echo "  python: $PYTHON"
echo "  cwd:    $REPO"
echo "  arr:    $ARR_FILE"

# Full audit — every milestone-eligible project (snapshot in z2e_snapshots/)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] running FULL audit"
"$PYTHON" z2e_migration_audit.py --arr-file "$ARR_FILE"

# Remaining-work audit — only projects with work left to do
# (snapshot in z2e_remaining_snapshots/, separate workbook)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] running REMAINING-ONLY audit"
"$PYTHON" z2e_migration_audit.py --arr-file "$ARR_FILE" --remaining-only

echo "[$(date '+%Y-%m-%d %H:%M:%S')] z2e audit run completed"
