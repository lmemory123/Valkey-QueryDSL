#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-all}"
DEFAULT_MAVEN_BIN="/tmp/apache-maven-3.9.11/bin/mvn"
if [[ -n "${MAVEN_BIN:-}" ]]; then
  RESOLVED_MAVEN_BIN="$MAVEN_BIN"
elif [[ -x "$DEFAULT_MAVEN_BIN" ]]; then
  RESOLVED_MAVEN_BIN="$DEFAULT_MAVEN_BIN"
else
  RESOLVED_MAVEN_BIN="$(command -v mvn)"
fi

if [[ -z "${RESOLVED_MAVEN_BIN:-}" ]]; then
  echo "mvn not found; install Maven or export MAVEN_BIN" >&2
  exit 1
fi

run_standalone() {
  cd "$ROOT_DIR"
  env VALKEY_INTEGRATION=true "$RESOLVED_MAVEN_BIN" -B -ntp -f valkey-query-test-example/pom.xml \
    -Dtest=EnableValkeyQueryIntegrationTests,GlideValkeyRepositoryIntegrationTests,BulkWriteIntegrationTests,IndexManagementIntegrationTests,SkuAdvancedIntegrationTest,SkuDeserializationTest,ValkeyComparisonTests,ProjectionIntegrationTests,AggregateIntegrationTests,VectorIntegrationTests \
    -Dsurefire.failIfNoSpecifiedTests=false test
}

run_rw() {
  cd "$ROOT_DIR"
  env VALKEY_RW_INTEGRATION=true "$RESOLVED_MAVEN_BIN" -B -ntp -f valkey-query-test-example/pom.xml \
    -Dtest=ReadWriteSplitIntegrationTests,ReadWriteSplitIndexManagementIntegrationTests,ReadWriteSplitVectorIntegrationTests \
    -Dsurefire.failIfNoSpecifiedTests=false test
}

run_cluster() {
  cd "$ROOT_DIR"
  env VALKEY_CLUSTER_INTEGRATION=true "$RESOLVED_MAVEN_BIN" -B -ntp -f valkey-query-test-example/pom.xml \
    -Dtest=ClusterBulkWriteIntegrationTests,ClusterAggregateIntegrationTests,ClusterIndexManagementIntegrationTests,ClusterVectorIntegrationTests \
    -Dsurefire.failIfNoSpecifiedTests=false test
}

bootstrap_modules() {
  cd "$ROOT_DIR"
  "$RESOLVED_MAVEN_BIN" -B -ntp -pl valkey-query-test-example,valkey-query-processor -am -Dmaven.test.skip=true clean install
}

case "$MODE" in
  standalone)
    bootstrap_modules
    run_standalone
    ;;
  rw|read-write-split)
    bootstrap_modules
    run_rw
    ;;
  cluster)
    bootstrap_modules
    run_cluster
    ;;
  all)
    bootstrap_modules
    run_standalone
    run_rw
    run_cluster
    ;;
  *)
    echo "Usage: $0 [standalone|rw|cluster|all]" >&2
    exit 1
    ;;
esac
