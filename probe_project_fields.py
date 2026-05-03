"""One-off probe: dump every key on a single project with includeAllFields=true.

Goal: confirm whether `inferredProgress` (or any task-completion field) is on
the project payload. If yes, we can drop the per-project task fetch entirely.

Usage: ROCKETLANE_API_KEY=... python3 _probe_progress.py [project_id]
"""
import json
import os
import sys
import urllib.request

API_KEY = os.environ["ROCKETLANE_API_KEY"]
BASE = "https://services.api.exterro.com/api/1.0"

# Default to T-Mobile Z2E (a known Phase 2 project) if no arg given
PID = sys.argv[1] if len(sys.argv) > 1 else "875284"

url = f"{BASE}/projects/{PID}?includeAllFields=true"
req = urllib.request.Request(url, headers={"api-key": API_KEY, "accept": "application/json"})
with urllib.request.urlopen(req, timeout=30) as r:
    proj = json.loads(r.read())

print(f"Project: {proj.get('projectName', '?')}")
print(f"Status:  {proj.get('status', {}).get('label', '?')}")
print()
print("=" * 70)
print("ALL TOP-LEVEL KEYS")
print("=" * 70)
for k in sorted(proj.keys()):
    v = proj[k]
    if isinstance(v, (dict, list)):
        print(f"  {k}: <{type(v).__name__}>")
    else:
        s = repr(v)
        if len(s) > 60:
            s = s[:60] + "…"
        print(f"  {k}: {s}")

print()
print("=" * 70)
print("PROGRESS-ADJACENT FIELDS")
print("=" * 70)
hits = sorted(k for k in proj if any(t in k.lower() for t in
    ["progress", "completion", "complete", "percent", "tasks"]))
for k in hits:
    print(f"  {k}: {proj[k]!r}")
if not hits:
    print("  (none found at top level — check nested objects)")

print()
print("=" * 70)
print("STATUS OBJECT (if present)")
print("=" * 70)
print(json.dumps(proj.get("status", {}), indent=2))

# Compact JSON dump for grep
print()
print("=" * 70)
print("FULL JSON (compact)")
print("=" * 70)
print(json.dumps(proj, default=str)[:3000])
