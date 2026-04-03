#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT_DIR"

/tmp/apache-maven-3.9.11/bin/mvn -B -ntp \
  -pl valkey-query-core,valkey-query-glide-adapter,valkey-query-spring-boot-starter,valkey-query-test-example \
  -am test
