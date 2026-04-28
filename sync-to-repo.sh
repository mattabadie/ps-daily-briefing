#!/bin/bash
# sync-to-repo.sh
# One-way rsync: iCloud workspace → ~/repos/ps-daily-briefing
# Excludes .git, __pycache__, .snapshots, .xlsx artifacts
# Run manually or via launchd (see com.ps-briefing.sync.plist)
#
# 2026-04-28: SRC repointed to Tools/ps-daily-briefing-code (active workspace).
# The previous SRC at Rocketlane/ps-daily-briefing/ went stale on 2026-04-09.

SRC="$HOME/Library/Mobile Documents/com~apple~CloudDocs/iCloud Storage/Exterro/Tools/ps-daily-briefing-code/"
DEST="$HOME/repos/ps-daily-briefing/"

# Bail if source doesn't exist
if [ ! -d "$SRC" ]; then
    echo "ERROR: Source not found: $SRC"
    exit 1
fi

# Create dest if needed
mkdir -p "$DEST"

# NOTE: --delete intentionally REMOVED on 2026-04-28. With it on, rsync was
# silently wiping repo-only files (GitHub Actions workflows, daily_briefing.py,
# forensics_briefing.py) every time the iCloud workspace got out of sync with
# the repo. Sync is now purely additive — to remove a file from the repo,
# delete it via `git rm` in ~/repos/ps-daily-briefing, not by deleting from
# iCloud and letting rsync propagate.
rsync -av --update \
    --exclude='.git/' \
    --exclude='.claude/' \
    --exclude='__pycache__/' \
    --exclude='.snapshots/' \
    --exclude='*.xlsx' \
    --exclude='.DS_Store' \
    "$SRC" "$DEST"

echo "$(date '+%Y-%m-%d %H:%M:%S') — synced to $DEST"
