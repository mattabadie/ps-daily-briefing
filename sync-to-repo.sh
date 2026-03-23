#!/bin/bash
# sync-to-repo.sh
# One-way rsync: iCloud workspace → ~/repos/ps-daily-briefing
# Excludes .git, __pycache__, .snapshots, .xlsx artifacts
# Run manually or via launchd (see com.ps-briefing.sync.plist)

SRC="$HOME/Library/Mobile Documents/com~apple~CloudDocs/iCloud Storage/Exterro/Rocketlane/ps-daily-briefing/"
DEST="$HOME/repos/ps-daily-briefing/"

# Bail if source doesn't exist
if [ ! -d "$SRC" ]; then
    echo "ERROR: Source not found: $SRC"
    exit 1
fi

# Create dest if needed
mkdir -p "$DEST"

rsync -av --update --delete \
    --exclude='.git/' \
    --exclude='__pycache__/' \
    --exclude='.snapshots/' \
    --exclude='*.xlsx' \
    --exclude='.DS_Store' \
    "$SRC" "$DEST"

echo "$(date '+%Y-%m-%d %H:%M:%S') — synced to $DEST"
