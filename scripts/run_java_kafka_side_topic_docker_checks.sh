#!/usr/bin/env bash
set -euo pipefail

# Full Docker validation for the Java side-topic checker.
#
# Starts Kafka 3.x in Docker, creates the side topics, then runs the existing
# Java side-topic fixture matrix inside an apache/spark:3.5.x container.
#
# Build the jar first:
#   GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

DOCKER_BIN="${DOCKER_BIN:-docker}"
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.6}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.7.0}"
NETWORK_NAME="${NETWORK_NAME:-recon-side-topic-net}"
KAFKA_CONTAINER_NAME="${KAFKA_CONTAINER_NAME:-recon-kafka-side-topic-docker}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-$KAFKA_CONTAINER_NAME:9092}"
SOURCE_TOPIC="${SOURCE_TOPIC:-orders}"
CANARY_TOPIC="${CANARY_TOPIC:-orders-canary}"
DEAD_LETTER_TOPIC="${DEAD_LETTER_TOPIC:-orders-dlq}"
DEAD_LETTER_ONLY_TOPIC="${DEAD_LETTER_ONLY_TOPIC:-orders-dlq-only}"
EMPTY_CANARY_TOPIC="${EMPTY_CANARY_TOPIC:-orders-empty-canary}"
EMPTY_DEAD_LETTER_TOPIC="${EMPTY_DEAD_LETTER_TOPIC:-orders-empty-dlq}"
BAD_CANARY_TOPIC="${BAD_CANARY_TOPIC:-orders-bad-canary}"
RUN_DATE="${RUN_DATE:-2026-07-02}"
HOST_FIXTURE_ROOT="${HOST_FIXTURE_ROOT:-$PWD/.recon-local-side/fixtures}"
HOST_EVIDENCE_ROOT="${HOST_EVIDENCE_ROOT:-$PWD/.recon-local-side/evidence}"
HOST_RUN_EVIDENCE_ROOT="${HOST_RUN_EVIDENCE_ROOT:-$HOST_EVIDENCE_ROOT/run}"
CONTAINER_FIXTURE_ROOT="${CONTAINER_FIXTURE_ROOT:-/fixtures/run}"
CONTAINER_EVIDENCE_ROOT="${CONTAINER_EVIDENCE_ROOT:-/evidence/run}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHECKER_JAR="${CHECKER_JAR:-$WORKSPACE_ROOT/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar}"
CLEANUP="${CLEANUP:-true}"

is_true() {
  case "$1" in
    true|TRUE|yes|YES|1|y|Y) return 0 ;;
    *) return 1 ;;
  esac
}

cleanup() {
  if is_true "$CLEANUP"; then
    "$DOCKER_BIN" rm -f "$KAFKA_CONTAINER_NAME" >/dev/null 2>&1 || true
    "$DOCKER_BIN" network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
  fi
}

wait_for_kafka() {
  local attempt
  for attempt in $(seq 1 60); do
    if "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

create_topic() {
  local topic="$1"
  "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
}

capture_kafka_version() {
  local version_dir="$HOST_RUN_EVIDENCE_ROOT/kafka_version"
  mkdir -p "$version_dir"
  printf '%q ' "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh --version > "$version_dir/command.txt"
  printf '\n' >> "$version_dir/command.txt"
  "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh --version \
    > "$version_dir/stdout.log" \
    2> "$version_dir/stderr.log"
  local version_code=$?
  printf '%s\n' "$version_code" > "$version_dir/exit_code.txt"
  if [[ "$version_code" -eq 0 ]] && grep -Eiq '(^|[^0-9])3\.[0-9]' "$version_dir/stdout.log" "$version_dir/stderr.log"; then
    printf 'pass\n' > "$version_dir/verdict.txt"
    return 0
  fi
  printf 'fail\n' > "$version_dir/verdict.txt"
  echo "[recon-side-docker] ERROR: Kafka version did not report 3.x evidence=$version_dir" >&2
  return 1
}

case "$SPARK_IMAGE" in
  *:3.5.*) ;;
  *)
    echo "[recon-side-docker] ERROR: refusing non-Spark-3.5 image: $SPARK_IMAGE" >&2
    exit 2
    ;;
esac

case "$KAFKA_IMAGE" in
  *:3.*) ;;
  *)
    echo "[recon-side-docker] ERROR: refusing non-Kafka-3.x image: $KAFKA_IMAGE" >&2
    exit 2
    ;;
esac

if [[ ! -f "$CHECKER_JAR" ]]; then
  echo "[recon-side-docker] ERROR: checker jar not found: $CHECKER_JAR" >&2
  echo "[recon-side-docker] Build it first with: GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar" >&2
  exit 2
fi

mkdir -p "$HOST_FIXTURE_ROOT" "$HOST_EVIDENCE_ROOT"
chmod 0777 "$HOST_FIXTURE_ROOT" "$HOST_EVIDENCE_ROOT"

trap cleanup EXIT

"$DOCKER_BIN" rm -f "$KAFKA_CONTAINER_NAME" >/dev/null 2>&1 || true
"$DOCKER_BIN" network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
"$DOCKER_BIN" network create "$NETWORK_NAME" >/dev/null

echo "[recon-side-docker] starting Kafka image=$KAFKA_IMAGE container=$KAFKA_CONTAINER_NAME"
"$DOCKER_BIN" run --rm -d \
  --name "$KAFKA_CONTAINER_NAME" \
  --network "$NETWORK_NAME" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$KAFKA_CONTAINER_NAME:9092" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  "$KAFKA_IMAGE" >/dev/null

if ! wait_for_kafka; then
  "$DOCKER_BIN" logs "$KAFKA_CONTAINER_NAME" >&2 || true
  echo "[recon-side-docker] ERROR: Kafka did not become ready" >&2
  exit 1
fi

echo "[recon-side-docker] creating Kafka topics"
create_topic "$CANARY_TOPIC" >/dev/null
create_topic "$DEAD_LETTER_TOPIC" >/dev/null
create_topic "$DEAD_LETTER_ONLY_TOPIC" >/dev/null
create_topic "$EMPTY_CANARY_TOPIC" >/dev/null
create_topic "$EMPTY_DEAD_LETTER_TOPIC" >/dev/null
create_topic "$BAD_CANARY_TOPIC" >/dev/null

echo "[recon-side-docker] running Spark image=$SPARK_IMAGE"
"$DOCKER_BIN" run --rm \
  --network "$NETWORK_NAME" \
  --entrypoint /bin/bash \
  -e SPARK_SHELL_BIN=/opt/spark/bin/spark-shell \
  -e SPARK_SUBMIT_BIN=/opt/spark/bin/spark-submit \
  -e CHECKER_JAR=/workspace/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
  -e FIXTURE_ROOT="$CONTAINER_FIXTURE_ROOT" \
  -e EVIDENCE_ROOT="$CONTAINER_EVIDENCE_ROOT" \
  -e RUN_DATE="$RUN_DATE" \
  -e START_KAFKA=false \
  -e KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e SOURCE_TOPIC="$SOURCE_TOPIC" \
  -e CANARY_TOPIC="$CANARY_TOPIC" \
  -e DEAD_LETTER_TOPIC="$DEAD_LETTER_TOPIC" \
  -e DEAD_LETTER_ONLY_TOPIC="$DEAD_LETTER_ONLY_TOPIC" \
  -e EMPTY_CANARY_TOPIC="$EMPTY_CANARY_TOPIC" \
  -e EMPTY_DEAD_LETTER_TOPIC="$EMPTY_DEAD_LETTER_TOPIC" \
  -e BAD_CANARY_TOPIC="$BAD_CANARY_TOPIC" \
  -e SPARK_JARS_IVY=/tmp/recon-ivy \
  -v "$WORKSPACE_ROOT":/workspace:ro \
  -v "$HOST_FIXTURE_ROOT":/fixtures \
  -v "$HOST_EVIDENCE_ROOT":/evidence \
  -w /workspace \
  "$SPARK_IMAGE" \
  -lc 'scripts/run_java_kafka_side_topic_fixture_checks.sh'

mkdir -p "$HOST_RUN_EVIDENCE_ROOT/kafka"
printf '%s\n' "$KAFKA_IMAGE" > "$HOST_RUN_EVIDENCE_ROOT/kafka/kafka_image.txt"
capture_kafka_version

echo "[recon-side-docker] evidence_root=$HOST_RUN_EVIDENCE_ROOT"
