#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: scripts/release-publish.sh <release-version> <gpg-key-id> [maven-bin]" >&2
  exit 1
fi

RELEASE_VERSION="$1"
GPG_KEY_ID="$2"
MVN_BIN="${3:-/tmp/apache-maven-3.9.11/bin/mvn}"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RELEASE_GROUP_ID="io.github.lmemory123"

if [[ "$RELEASE_VERSION" == *-SNAPSHOT ]]; then
  echo "Release version must not end with -SNAPSHOT." >&2
  exit 1
fi

if [[ -z "${MAVEN_GPG_PASSPHRASE:-}" ]]; then
  echo "MAVEN_GPG_PASSPHRASE is not set." >&2
  exit 1
fi

if [[ ! -x "$MVN_BIN" ]]; then
  echo "Maven binary not found or not executable: $MVN_BIN" >&2
  exit 1
fi

echo "[release] set project version -> $RELEASE_VERSION"
python3 "$ROOT_DIR/scripts/set-project-version.py" "$RELEASE_VERSION"
echo "[release] set project groupId -> $RELEASE_GROUP_ID"
python3 "$ROOT_DIR/scripts/set-project-group-id.py" "$RELEASE_GROUP_ID"

echo "[release] run real Valkey regression"
MAVEN_BIN="$MVN_BIN" "$ROOT_DIR/scripts/run-real-tests.sh" all

echo "[release] publish to Maven Central"
"$MVN_BIN" -B -ntp -Prelease -Dgpg.keyname="$GPG_KEY_ID" clean deploy

echo
echo "[release] published version $RELEASE_VERSION"
echo "[release] next steps:"
echo "  1. git tag v$RELEASE_VERSION"
echo "  2. git push origin v$RELEASE_VERSION"
echo "  3. scripts/release-start-next.sh <next-snapshot-version>"
