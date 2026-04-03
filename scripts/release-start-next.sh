#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/release-start-next.sh <next-snapshot-version>" >&2
  exit 1
fi

NEXT_VERSION="$1"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DEVELOPMENT_GROUP_ID="com.momao"

if [[ "$NEXT_VERSION" != *-SNAPSHOT ]]; then
  echo "Next development version must end with -SNAPSHOT." >&2
  exit 1
fi

python3 "$ROOT_DIR/scripts/set-project-version.py" "$NEXT_VERSION"
python3 "$ROOT_DIR/scripts/set-project-group-id.py" "$DEVELOPMENT_GROUP_ID"

echo
echo "[release] switched project to next development version: $NEXT_VERSION"
echo "[release] restored development groupId: $DEVELOPMENT_GROUP_ID"
echo "[release] recommended follow-up:"
echo "  git add pom.xml */pom.xml"
echo "  git commit -m \"chore: start $NEXT_VERSION\""
