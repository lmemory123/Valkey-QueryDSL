#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: scripts/release-tag.sh <release-version> [--push]" >&2
  exit 1
fi

RELEASE_VERSION="$1"
PUSH_TAG="${2:-}"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [[ "$RELEASE_VERSION" == *-SNAPSHOT ]]; then
  echo "Release tag version must not end with -SNAPSHOT." >&2
  exit 1
fi

cd "$ROOT_DIR"

CURRENT_VERSION="$(
python3 - <<'PY'
from pathlib import Path
import re
text = Path('pom.xml').read_text()
match = re.search(r'<version>(.*?)</version>', text, re.S)
print(match.group(1).strip() if match else '')
PY
)"
if [[ "$CURRENT_VERSION" != "$RELEASE_VERSION" ]]; then
  echo "Current pom version ($CURRENT_VERSION) does not match release version ($RELEASE_VERSION)." >&2
  exit 1
fi

TAG_NAME="v$RELEASE_VERSION"
git rev-parse "$TAG_NAME" >/dev/null 2>&1 && {
  echo "Tag already exists: $TAG_NAME" >&2
  exit 1
}

git tag -a "$TAG_NAME" -m "Release $RELEASE_VERSION"
echo "[release] created tag $TAG_NAME"

if [[ "$PUSH_TAG" == "--push" ]]; then
  git push origin "$TAG_NAME"
  echo "[release] pushed tag $TAG_NAME"
fi
