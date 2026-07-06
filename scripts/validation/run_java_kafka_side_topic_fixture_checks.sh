#!/usr/bin/env bash
set -u

# Local Kafka 3.x side-topic fixture validation runner for the Java
# spark-submit checker. Spark shell is used only to create parquet/Kafka fixture
# data; every checker scenario runs through spark-submit.

SPARK_SHELL_BIN="${SPARK_SHELL_BIN:-spark-shell}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4}"
SPARK_JARS_IVY="${SPARK_JARS_IVY:-}"
FIXTURE_ROOT="${FIXTURE_ROOT:-/tmp/recon-kafka-offset-side-topic-fixtures-java}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/recon-kafka-offset-side-topic-evidence-java}"
RUN_DATE="${RUN_DATE:-2026-07-02}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CHECKER_JAR="${CHECKER_JAR:-$WORKSPACE_ROOT/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar}"
CHECKER_CLASS="${CHECKER_CLASS:-com.reconciliation.kafka.KafkaOffsetGapChecker}"
PARQUET_GENERATOR="$WORKSPACE_ROOT/scripts/fixtures/generate_kafka_offset_gap_fixtures.scala"
SIDE_TOPIC_GENERATOR="$WORKSPACE_ROOT/scripts/fixtures/generate_kafka_side_topic_records.scala"
PROD_WRAPPER="$WORKSPACE_ROOT/scripts/check/run_java_kafka_offset_gap_check_prod.sh"
SCENARIO_ROOT="$EVIDENCE_ROOT/scenarios"
KAFKA_EVIDENCE_ROOT="$EVIDENCE_ROOT/kafka"
RESULTS_TSV="$EVIDENCE_ROOT/scenario_results.tsv"
ASSERTIONS_TSV="$EVIDENCE_ROOT/assertion_results.tsv"

START_KAFKA="${START_KAFKA:-true}"
CLEANUP_KAFKA="${CLEANUP_KAFKA:-true}"
DOCKER_BIN="${DOCKER_BIN:-docker}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.7.0}"
KAFKA_CONTAINER_NAME="${KAFKA_CONTAINER_NAME:-recon-kafka-side-topic-fixture}"
KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-19092}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:$KAFKA_HOST_PORT}"
SOURCE_TOPIC="${SOURCE_TOPIC:-orders}"
CANARY_TOPIC="${CANARY_TOPIC:-orders-canary}"
DEAD_LETTER_TOPIC="${DEAD_LETTER_TOPIC:-orders-dlq}"
DEAD_LETTER_ONLY_TOPIC="${DEAD_LETTER_ONLY_TOPIC:-orders-dlq-only}"
EMPTY_CANARY_TOPIC="${EMPTY_CANARY_TOPIC:-orders-empty-canary}"
EMPTY_DEAD_LETTER_TOPIC="${EMPTY_DEAD_LETTER_TOPIC:-orders-empty-dlq}"
BAD_CANARY_TOPIC="${BAD_CANARY_TOPIC:-orders-bad-canary}"

print_command() {
  local output_file="$1"
  shift
  printf '%q ' "$@" > "$output_file"
  printf '\n' >> "$output_file"
}

write_listing() {
  local path="$1"
  local output_file="$2"

  if [[ -d "$path" ]]; then
    find "$path" -maxdepth 7 -print | sort > "$output_file"
  else
    printf 'missing: %s\n' "$path" > "$output_file"
  fi
}

write_common_env_file() {
  local output_file="$1"
  local roots="${2:-}"
  local with_packages="${3:-}"

  {
    printf 'SPARK_SUBMIT_BIN=%q\n' "$SPARK_SUBMIT_BIN"
    printf 'SPARK_SHELL_BIN=%q\n' "$SPARK_SHELL_BIN"
    printf 'SPARK_MASTER=%q\n' "$SPARK_MASTER"
    printf 'SPARK_PACKAGES=%q\n' "$SPARK_PACKAGES"
    printf 'SPARK_JARS_IVY=%q\n' "$SPARK_JARS_IVY"
    printf 'CHECKER_JAR=%q\n' "$CHECKER_JAR"
    printf 'CHECKER_CLASS=%q\n' "$CHECKER_CLASS"
    printf 'RUN_DATE=%q\n' "$RUN_DATE"
    printf 'INPUT_ROOTS_CSV=%q\n' "$roots"
    printf 'WITH_PACKAGES=%q\n' "$with_packages"
    printf 'KAFKA_BOOTSTRAP_SERVERS=%q\n' "$KAFKA_BOOTSTRAP_SERVERS"
    printf 'SOURCE_TOPIC=%q\n' "$SOURCE_TOPIC"
    printf 'CANARY_TOPIC=%q\n' "$CANARY_TOPIC"
    printf 'DEAD_LETTER_TOPIC=%q\n' "$DEAD_LETTER_TOPIC"
    printf 'DEAD_LETTER_ONLY_TOPIC=%q\n' "$DEAD_LETTER_ONLY_TOPIC"
    printf 'EMPTY_CANARY_TOPIC=%q\n' "$EMPTY_CANARY_TOPIC"
    printf 'EMPTY_DEAD_LETTER_TOPIC=%q\n' "$EMPTY_DEAD_LETTER_TOPIC"
    printf 'BAD_CANARY_TOPIC=%q\n' "$BAD_CANARY_TOPIC"
  } > "$output_file"
}

write_yaml_side_config() {
  local output_file="$1"
  local roots="$2"
  local source_topic="$3"
  local bootstrap_servers="$4"
  local canary_topic="$5"
  local dead_letter_topic="$6"
  local missing_offsets_limit="${7:-1000}"
  local root
  local root_items=()
  local broker_file

  broker_file="$(dirname "$output_file")/kafka-brokers.yml"

  IFS=',' read -r -a root_items <<< "$roots"
  {
    printf 'spring:\n'
    printf '  config:\n'
    printf '    import: "file:%s"\n' "$broker_file"
    printf '\n'
    printf 'recon:\n'
    printf '  input-roots:\n'
    for root in "${root_items[@]}"; do
      printf '    - "%s"\n' "$root"
    done
    printf '  metadata-column: cactus__metadata\n'
    printf '  date-partition-column: timestampcolumn\n'
    printf '  run-date: "%s"\n' "$RUN_DATE"
    printf '  normalized-offsets-overwrite: true\n'
    printf '  fail-on-invalid-rows: true\n'
    printf '  fail-on-gaps: true\n'
    printf '  missing-offsets-limit: %s\n' "$missing_offsets_limit"
    printf '  exit-on-completion: true\n'
    printf '  source-topic: "%s"\n' "$source_topic"
    printf '  kafka-alias: main-kafka\n'
    if [[ -n "$canary_topic" ]]; then
      printf '  canary-topic: "%s"\n' "$canary_topic"
    fi
    if [[ -n "$dead_letter_topic" ]]; then
      printf '  dead-letter-topic: "%s"\n' "$dead_letter_topic"
    fi
    printf '  side-topic-starting-offsets: earliest\n'
  } > "$output_file"

  {
    printf 'kafka-configs:\n'
    printf '  broker:\n'
    printf '    main-kafka:\n'
    printf '      conf:\n'
    printf '        "[bootstrap.servers]": "%s"\n' "$bootstrap_servers"
    printf '        "[security.protocol]": PLAINTEXT\n'
    printf '        "[max.poll.records]": 500\n'
  } > "$broker_file"
}

is_true() {
  case "$1" in
    true|TRUE|yes|YES|1|y|Y) return 0 ;;
    *) return 1 ;;
  esac
}

cleanup_kafka() {
  if is_true "$START_KAFKA" && is_true "$CLEANUP_KAFKA"; then
    "$DOCKER_BIN" rm -f "$KAFKA_CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}

start_kafka_if_requested() {
  if ! is_true "$START_KAFKA"; then
    echo "[recon-side-test] using existing Kafka bootstrap=$KAFKA_BOOTSTRAP_SERVERS"
    return 0
  fi

  case "$KAFKA_IMAGE" in
    *:3.*) ;;
    *)
      echo "[recon-side-test] refusing non-Kafka-3.x image: $KAFKA_IMAGE"
      return 2
      ;;
  esac

  mkdir -p "$KAFKA_EVIDENCE_ROOT"
  printf '%s\n' "$KAFKA_IMAGE" > "$KAFKA_EVIDENCE_ROOT/kafka_image.txt"
  print_command "$KAFKA_EVIDENCE_ROOT/start_kafka_command.txt" \
    "$DOCKER_BIN" run --rm -d --name "$KAFKA_CONTAINER_NAME" \
    -p "$KAFKA_HOST_PORT:9092" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$KAFKA_HOST_PORT" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$KAFKA_IMAGE"

  "$DOCKER_BIN" rm -f "$KAFKA_CONTAINER_NAME" >/dev/null 2>&1 || true
  "$DOCKER_BIN" run --rm -d --name "$KAFKA_CONTAINER_NAME" \
    -p "$KAFKA_HOST_PORT:9092" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$KAFKA_HOST_PORT" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$KAFKA_IMAGE" > "$KAFKA_EVIDENCE_ROOT/container_id.txt"
  local start_code=$?
  if [[ "$start_code" -ne 0 ]]; then
    echo "[recon-side-test] failed to start Kafka container image=$KAFKA_IMAGE"
    return "$start_code"
  fi

  local ready="false"
  local attempt
  for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60; do
    "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list > "$KAFKA_EVIDENCE_ROOT/wait_topics.stdout" 2> "$KAFKA_EVIDENCE_ROOT/wait_topics.stderr"
    if [[ "$?" -eq 0 ]]; then
      ready="true"
      break
    fi
    sleep 1
  done

  if [[ "$ready" != "true" ]]; then
    "$DOCKER_BIN" logs "$KAFKA_CONTAINER_NAME" > "$KAFKA_EVIDENCE_ROOT/kafka_container.log" 2>&1 || true
    echo "[recon-side-test] Kafka container did not become ready evidence=$KAFKA_EVIDENCE_ROOT"
    return 1
  fi

  echo "[recon-side-test] Kafka container ready bootstrap=$KAFKA_BOOTSTRAP_SERVERS"
}

capture_kafka_version() {
  local version_dir="$EVIDENCE_ROOT/kafka_version"
  mkdir -p "$version_dir"

  if is_true "$START_KAFKA"; then
    print_command "$version_dir/command.txt" "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh --version
    "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh --version > "$version_dir/stdout.log" 2> "$version_dir/stderr.log"
    local version_code=$?
    printf '%s\n' "$version_code" > "$version_dir/exit_code.txt"
    if [[ "$version_code" -ne 0 ]]; then
      printf 'fail\n' > "$version_dir/verdict.txt"
      return 1
    fi
    if grep -Eiq '(^|[^0-9])3\.[0-9]' "$version_dir/stdout.log" "$version_dir/stderr.log"; then
      printf 'pass\n' > "$version_dir/verdict.txt"
      return 0
    fi
    printf 'fail\n' > "$version_dir/verdict.txt"
    echo "[recon-side-test] Kafka version did not report 3.x evidence=$version_dir"
    return 1
  fi

  printf 'external Kafka configured; START_KAFKA=false\n' > "$version_dir/stdout.log"
  printf '0\n' > "$version_dir/exit_code.txt"
  printf 'not-run\n' > "$version_dir/verdict.txt"
  echo "[recon-side-test] Kafka version proof skipped for external Kafka evidence=$version_dir"
  return 0
}

create_kafka_topics() {
  if ! is_true "$START_KAFKA"; then
    return 0
  fi

  local topic
  for topic in "$CANARY_TOPIC" "$DEAD_LETTER_TOPIC" "$DEAD_LETTER_ONLY_TOPIC" "$EMPTY_CANARY_TOPIC" "$EMPTY_DEAD_LETTER_TOPIC" "$BAD_CANARY_TOPIC"; do
    print_command "$KAFKA_EVIDENCE_ROOT/create_topic_${topic}.command.txt" \
      "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "$topic" --partitions 1 --replication-factor 1
    "$DOCKER_BIN" exec "$KAFKA_CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "$topic" --partitions 1 --replication-factor 1 \
      > "$KAFKA_EVIDENCE_ROOT/create_topic_${topic}.stdout" \
      2> "$KAFKA_EVIDENCE_ROOT/create_topic_${topic}.stderr"
    local create_code=$?
    if [[ "$create_code" -ne 0 ]]; then
      echo "[recon-side-test] failed to create Kafka topic=$topic evidence=$KAFKA_EVIDENCE_ROOT"
      return "$create_code"
    fi
  done
}

run_spark_shell_plain_command() {
  local script_path="$1"
  shift

  "$SPARK_SHELL_BIN" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    -i "$script_path"
}

run_spark_shell_side_command() {
  local script_path="$1"
  shift

  local package_args=()
  if [[ -n "$SPARK_PACKAGES" ]]; then
    package_args+=(--packages "$SPARK_PACKAGES")
  fi
  if [[ -n "$SPARK_JARS_IVY" ]]; then
    package_args+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
  fi

  "$SPARK_SHELL_BIN" \
    --master "$SPARK_MASTER" \
    "${package_args[@]}" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    -i "$script_path"
}

run_java_checker_command() {
  local with_packages="$1"
  local roots="$2"
  shift 2

  local package_args=()
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_PACKAGES" ]]; then
    package_args+=(--packages "$SPARK_PACKAGES")
  fi
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_JARS_IVY" ]]; then
    package_args+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
  fi

  "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    "${package_args[@]}" \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.recon.inputRoots=$roots" \
    --conf "spark.recon.runDate=$RUN_DATE" \
    "$@" \
    "$CHECKER_JAR"
}

run_java_checker_yaml_command() {
  local with_packages="$1"
  local yaml_file="$2"
  shift 2

  local package_args=()
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_PACKAGES" ]]; then
    package_args+=(--packages "$SPARK_PACKAGES")
  fi
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_JARS_IVY" ]]; then
    package_args+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
  fi

  SPRING_CONFIG_LOCATION="file:$yaml_file" \
  "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    "${package_args[@]}" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    "$CHECKER_JAR"
}

write_java_checker_command() {
  local output_file="$1"
  local with_packages="$2"
  local roots="$3"
  shift 3

  local package_args=()
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_PACKAGES" ]]; then
    package_args+=(--packages "$SPARK_PACKAGES")
  fi
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_JARS_IVY" ]]; then
    package_args+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
  fi

  print_command "$output_file" \
    "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    "${package_args[@]}" \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.recon.inputRoots=$roots" \
    --conf "spark.recon.runDate=$RUN_DATE" \
    "$@" \
    "$CHECKER_JAR"
}

run_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"
  local with_packages="$4"
  shift 4

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_common_env_file "$env_file" "$roots" "$with_packages"
  write_java_checker_command "$command_file" "$with_packages" "$roots" "$@"

  echo "[recon-side-test] scenario=$name expected_exit=$expected"
  run_java_checker_command "$with_packages" "$roots" "$@" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-side-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_yaml_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"
  local with_packages="$4"
  local source_topic="$5"
  local canary_topic="$6"
  local dead_letter_topic="$7"
  local missing_offsets_limit="${8:-1000}"
  shift 8

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local yaml_file="$scenario_dir/application.yml"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"
  local package_args=()

  if [[ "$with_packages" == "with_packages" && -n "$SPARK_PACKAGES" ]]; then
    package_args+=(--packages "$SPARK_PACKAGES")
  fi
  if [[ "$with_packages" == "with_packages" && -n "$SPARK_JARS_IVY" ]]; then
    package_args+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
  fi

  printf '%s\n' "$expected" > "$expected_file"
  write_yaml_side_config "$yaml_file" "$roots" "$source_topic" "$KAFKA_BOOTSTRAP_SERVERS" "$canary_topic" "$dead_letter_topic" "$missing_offsets_limit"
  write_common_env_file "$env_file" "$roots" "$with_packages"
  printf 'SPRING_CONFIG_LOCATION=%q\n' "file:$yaml_file" >> "$env_file"
  print_command "$command_file" \
    env "SPRING_CONFIG_LOCATION=file:$yaml_file" \
    "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    "${package_args[@]}" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    "$CHECKER_JAR"

  echo "[recon-side-test] scenario=$name expected_exit=$expected"
  run_java_checker_yaml_command "$with_packages" "$yaml_file" "$@" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-side-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_wrapper_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  {
    printf 'SPARK_SUBMIT_BIN=%q\n' "$SPARK_SUBMIT_BIN"
    printf 'SPARK_MASTER=%q\n' "$SPARK_MASTER"
    printf 'SPARK_PACKAGES=%q\n' "$SPARK_PACKAGES"
    printf 'SPARK_JARS_IVY=%q\n' "$SPARK_JARS_IVY"
    printf 'CHECKER_JAR=%q\n' "$CHECKER_JAR"
    printf 'INPUT_ROOTS_CSV=%q\n' "$roots"
    printf 'RUN_DATE=%q\n' "$RUN_DATE"
    printf 'FAIL_ON_GAPS=true\n'
    printf 'SOURCE_TOPIC=%q\n' "$SOURCE_TOPIC"
    printf 'KAFKA_BOOTSTRAP_SERVERS=%q\n' "$KAFKA_BOOTSTRAP_SERVERS"
    printf 'CANARY_TOPIC=%q\n' "$CANARY_TOPIC"
    printf 'DEAD_LETTER_TOPIC=%q\n' "$DEAD_LETTER_TOPIC"
  } > "$env_file"
  {
    printf 'env \\\n'
    printf '  SPARK_SUBMIT_BIN=%q \\\n' "$SPARK_SUBMIT_BIN"
    printf '  SPARK_MASTER=%q \\\n' "$SPARK_MASTER"
    printf '  SPARK_PACKAGES=%q \\\n' "$SPARK_PACKAGES"
    printf '  SPARK_JARS_IVY=%q \\\n' "$SPARK_JARS_IVY"
    printf '  CHECKER_JAR=%q \\\n' "$CHECKER_JAR"
    printf '  INPUT_ROOTS_CSV=%q \\\n' "$roots"
    printf '  RUN_DATE=%q \\\n' "$RUN_DATE"
    printf '  FAIL_ON_GAPS=true \\\n'
    printf '  SOURCE_TOPIC=%q \\\n' "$SOURCE_TOPIC"
    printf '  KAFKA_BOOTSTRAP_SERVERS=%q \\\n' "$KAFKA_BOOTSTRAP_SERVERS"
    printf '  CANARY_TOPIC=%q \\\n' "$CANARY_TOPIC"
    printf '  DEAD_LETTER_TOPIC=%q \\\n' "$DEAD_LETTER_TOPIC"
    printf '  %q\n' "$PROD_WRAPPER"
  } > "$command_file"

  echo "[recon-side-test] scenario=$name expected_exit=$expected"
  SPARK_SUBMIT_BIN="$SPARK_SUBMIT_BIN" \
  SPARK_MASTER="$SPARK_MASTER" \
  SPARK_PACKAGES="$SPARK_PACKAGES" \
  SPARK_JARS_IVY="$SPARK_JARS_IVY" \
  CHECKER_JAR="$CHECKER_JAR" \
  INPUT_ROOTS_CSV="$roots" \
  RUN_DATE="$RUN_DATE" \
  FAIL_ON_GAPS=true \
  SOURCE_TOPIC="$SOURCE_TOPIC" \
  KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  CANARY_TOPIC="$CANARY_TOPIC" \
  DEAD_LETTER_TOPIC="$DEAD_LETTER_TOPIC" \
  "$PROD_WRAPPER" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-side-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_wrapper_yaml_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"
  local source_topic="$4"
  local canary_topic="$5"
  local dead_letter_topic="$6"

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local yaml_file="$scenario_dir/application.yml"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_yaml_side_config "$yaml_file" "$roots" "$source_topic" "$KAFKA_BOOTSTRAP_SERVERS" "$canary_topic" "$dead_letter_topic" 1000
  write_common_env_file "$env_file" "$roots" "with_packages"
  {
    printf 'APPLICATION_YML=%q\n' "$yaml_file"
    printf 'ENABLE_SIDE_TOPIC_PACKAGES=true\n'
  } >> "$env_file"
  {
    printf 'env \\\n'
    printf '  SPARK_SUBMIT_BIN=%q \\\n' "$SPARK_SUBMIT_BIN"
    printf '  SPARK_MASTER=%q \\\n' "$SPARK_MASTER"
    printf '  SPARK_PACKAGES=%q \\\n' "$SPARK_PACKAGES"
    printf '  SPARK_JARS_IVY=%q \\\n' "$SPARK_JARS_IVY"
    printf '  CHECKER_JAR=%q \\\n' "$CHECKER_JAR"
    printf '  APPLICATION_YML=%q \\\n' "$yaml_file"
    printf '  ENABLE_SIDE_TOPIC_PACKAGES=true \\\n'
    printf '  %q\n' "$PROD_WRAPPER"
  } > "$command_file"

  echo "[recon-side-test] scenario=$name expected_exit=$expected"
  SPARK_SUBMIT_BIN="$SPARK_SUBMIT_BIN" \
  SPARK_MASTER="$SPARK_MASTER" \
  SPARK_PACKAGES="$SPARK_PACKAGES" \
  SPARK_JARS_IVY="$SPARK_JARS_IVY" \
  CHECKER_JAR="$CHECKER_JAR" \
  APPLICATION_YML="$yaml_file" \
  ENABLE_SIDE_TOPIC_PACKAGES=true \
  "$PROD_WRAPPER" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-side-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

assert_stdout_regex() {
  local name="$1"
  local regex="$2"
  local label="$3"
  local scenario_dir="$SCENARIO_ROOT/$name"
  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local assertion_file="$scenario_dir/assertion_$label.txt"

  if grep -Eq "$regex" "$stdout_file" "$stderr_file"; then
    printf 'pass\nregex=%s\n' "$regex" > "$assertion_file"
    printf '%s\t%s\tpass\t%s\n' "$name" "$label" "$regex" >> "$ASSERTIONS_TSV"
    echo "[recon-side-test] scenario=$name assertion=$label verdict=pass"
    return 0
  fi

  printf 'fail\nregex=%s\nstdout=%s\nstderr=%s\n' "$regex" "$stdout_file" "$stderr_file" > "$assertion_file"
  printf '%s\t%s\tfail\t%s\n' "$name" "$label" "$regex" >> "$ASSERTIONS_TSV"
  echo "[recon-side-test] scenario=$name assertion=$label verdict=fail evidence=$assertion_file"
  return 1
}

assert_stdout_not_regex() {
  local name="$1"
  local regex="$2"
  local label="$3"
  local scenario_dir="$SCENARIO_ROOT/$name"
  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local assertion_file="$scenario_dir/assertion_$label.txt"

  if grep -Eq "$regex" "$stdout_file" "$stderr_file"; then
    printf 'fail\nunexpected_regex=%s\nstdout=%s\nstderr=%s\n' "$regex" "$stdout_file" "$stderr_file" > "$assertion_file"
    printf '%s\t%s\tfail\t%s\n' "$name" "$label" "$regex" >> "$ASSERTIONS_TSV"
    echo "[recon-side-test] scenario=$name assertion=$label verdict=fail evidence=$assertion_file"
    return 1
  fi

  printf 'pass\nunexpected_regex=%s\n' "$regex" > "$assertion_file"
  printf '%s\t%s\tpass\t%s\n' "$name" "$label" "$regex" >> "$ASSERTIONS_TSV"
  echo "[recon-side-test] scenario=$name assertion=$label verdict=pass"
  return 0
}

if [[ -z "$EVIDENCE_ROOT" || "$EVIDENCE_ROOT" == "/" ]]; then
  echo "[recon-side-test] refusing unsafe EVIDENCE_ROOT=$EVIDENCE_ROOT"
  exit 2
fi

if [[ -z "$FIXTURE_ROOT" || "$FIXTURE_ROOT" == "/" ]]; then
  echo "[recon-side-test] refusing unsafe FIXTURE_ROOT=$FIXTURE_ROOT"
  exit 2
fi

case "$SPARK_PACKAGES" in
  *spark-sql-kafka-0-10_2.13*|*spark-sql-kafka-0-10_2.12:4.*)
    echo "[recon-side-test] refusing non-Spark-3.5/Scala-2.12 package set: $SPARK_PACKAGES"
    exit 2
    ;;
esac

if [[ ! -f "$CHECKER_JAR" ]]; then
  echo "[recon-side-test] checker jar not found: $CHECKER_JAR"
  exit 2
fi

rm -rf "$EVIDENCE_ROOT"
mkdir -p "$SCENARIO_ROOT" "$KAFKA_EVIDENCE_ROOT"
printf 'scenario\texpected_exit\tobserved_exit\tverdict\tstdout\tstderr\n' > "$RESULTS_TSV"
printf 'scenario\tassertion\tverdict\tpattern\n' > "$ASSERTIONS_TSV"
trap cleanup_kafka EXIT

version_dir="$EVIDENCE_ROOT/spark_version"
mkdir -p "$version_dir"
print_command "$version_dir/command.txt" "$SPARK_SUBMIT_BIN" --version
"$SPARK_SUBMIT_BIN" --version > "$version_dir/stdout.log" 2> "$version_dir/stderr.log"
version_code=$?
printf '%s\n' "$version_code" > "$version_dir/exit_code.txt"
if [[ "$version_code" -ne 0 ]]; then
  printf 'fail\n' > "$version_dir/verdict.txt"
  echo "[recon-side-test] spark-version observed_exit=$version_code verdict=fail evidence=$version_dir"
  exit 1
fi
if grep -Eiq 'version 3\.5\.|Spark 3\.5\.' "$version_dir/stdout.log" "$version_dir/stderr.log"; then
  printf 'pass\n' > "$version_dir/verdict.txt"
else
  printf 'fail\n' > "$version_dir/verdict.txt"
  echo "[recon-side-test] spark-version did not report Spark 3.5.x evidence=$version_dir"
  exit 1
fi
echo "[recon-side-test] spark-version verdict=pass evidence=$version_dir"

start_kafka_if_requested || exit 1
capture_kafka_version || exit 1
create_kafka_topics || exit 1

parquet_generator_dir="$EVIDENCE_ROOT/parquet_generator"
mkdir -p "$parquet_generator_dir"
print_command "$parquet_generator_dir/command.txt" \
  "$SPARK_SHELL_BIN" \
  --master "$SPARK_MASTER" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "recon.fixtureOutputRoot=$FIXTURE_ROOT" \
  --conf "recon.fixtureRunDate=$RUN_DATE" \
  -i "$PARQUET_GENERATOR"

run_spark_shell_plain_command "$PARQUET_GENERATOR" \
  --conf "recon.fixtureOutputRoot=$FIXTURE_ROOT" \
  --conf "recon.fixtureRunDate=$RUN_DATE" \
  > "$parquet_generator_dir/stdout.log" \
  2> "$parquet_generator_dir/stderr.log"
parquet_generator_code=$?
printf '%s\n' "$parquet_generator_code" > "$parquet_generator_dir/exit_code.txt"
if [[ "$parquet_generator_code" -ne 0 ]]; then
  printf 'fail\n' > "$parquet_generator_dir/verdict.txt"
  echo "[recon-side-test] parquet fixture generation failed evidence=$parquet_generator_dir"
  exit 1
fi
printf 'pass\n' > "$parquet_generator_dir/verdict.txt"
echo "[recon-side-test] parquet fixture generation verdict=pass evidence=$parquet_generator_dir"

side_generator_dir="$EVIDENCE_ROOT/side_topic_generator"
side_manifest="$KAFKA_EVIDENCE_ROOT/side_topic_records.tsv"
mkdir -p "$side_generator_dir"
side_package_args=()
if [[ -n "$SPARK_PACKAGES" ]]; then
  side_package_args+=(--packages "$SPARK_PACKAGES")
fi
if [[ -n "$SPARK_JARS_IVY" ]]; then
  side_package_args+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
fi
print_command "$side_generator_dir/command.txt" \
  "$SPARK_SHELL_BIN" \
  --master "$SPARK_MASTER" \
  "${side_package_args[@]}" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC" \
  --conf "spark.recon.fixtureDeadLetterOnlyTopic=$DEAD_LETTER_ONLY_TOPIC" \
  --conf "spark.recon.fixtureBadCanaryTopic=$BAD_CANARY_TOPIC" \
  --conf "spark.recon.sideTopicManifestPath=$side_manifest" \
  -i "$SIDE_TOPIC_GENERATOR"

run_spark_shell_side_command "$SIDE_TOPIC_GENERATOR" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC" \
  --conf "spark.recon.fixtureDeadLetterOnlyTopic=$DEAD_LETTER_ONLY_TOPIC" \
  --conf "spark.recon.fixtureBadCanaryTopic=$BAD_CANARY_TOPIC" \
  --conf "spark.recon.sideTopicManifestPath=$side_manifest" \
  > "$side_generator_dir/stdout.log" \
  2> "$side_generator_dir/stderr.log"
side_generator_code=$?
printf '%s\n' "$side_generator_code" > "$side_generator_dir/exit_code.txt"
if [[ "$side_generator_code" -ne 0 ]]; then
  printf 'fail\n' > "$side_generator_dir/verdict.txt"
  echo "[recon-side-test] side-topic fixture generation failed evidence=$side_generator_dir"
  exit 1
fi
printf 'pass\n' > "$side_generator_dir/verdict.txt"
echo "[recon-side-test] side-topic fixture generation verdict=pass evidence=$side_generator_dir"

failures=0
gap_root="$FIXTURE_ROOT/gap/root_a"
gap_over_limit_root="$FIXTURE_ROOT/gap_over_limit/root_a"
gap_two_root="$FIXTURE_ROOT/gap_two/root_a"

if run_expected "canary_empty_dead_letter_resolved" 0 "$gap_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$EMPTY_DEAD_LETTER_TOPIC"; then
  assert_stdout_regex "canary_empty_dead_letter_resolved" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_resolved" 'recon.sideTopic.enabled=true' "side_topic_enabled" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_resolved" 'side_topic_read topic=orders-empty-dlq kind=dead_letter decoded_record_count=0' "empty_dead_letter_read" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_resolved" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_resolved" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=1 .*canary_explained_count=1 .*dead_letter_explained_count=0 .*unresolved_count=0 .*canary_record_count=5 .*dead_letter_record_count=0' "canary_empty_dead_letter_summary" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_resolved" 'final_exit_decision code=0 .*side_topic_enabled=true .*unresolved_count=0' "final_exit_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "canary_empty_dead_letter_unresolved" 1 "$gap_two_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$EMPTY_DEAD_LETTER_TOPIC"; then
  assert_stdout_regex "canary_empty_dead_letter_unresolved" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_unresolved" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_unresolved" 'side_topic_bucket=unresolved source_topic=orders side_topic=<none> partition=0 offset_count=1 offsets=\[2\]' "unresolved_offset_2" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_unresolved" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=2 .*canary_explained_count=1 .*dead_letter_explained_count=0 .*unresolved_count=1 .*canary_record_count=5 .*dead_letter_record_count=0' "canary_empty_dead_letter_summary" || failures=$((failures + 1))
  assert_stdout_regex "canary_empty_dead_letter_unresolved" 'final_exit_decision code=1 .*unresolved_count=1' "final_exit_1" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "empty_canary_dead_letter_resolved" 0 "$gap_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$EMPTY_CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_ONLY_TOPIC"; then
  assert_stdout_regex "empty_canary_dead_letter_resolved" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_resolved" 'side_topic_read topic=orders-empty-canary kind=canary decoded_record_count=0' "empty_canary_read" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_resolved" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq-only partition=0 offset_count=1 offsets=\[1\]' "dead_letter_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_resolved" 'side_topic_dead_letter_fields failure_event_id_count=1 reason_msg_count=1 exception_count=1' "dead_letter_fields" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_resolved" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=1 .*canary_explained_count=0 .*dead_letter_explained_count=1 .*unresolved_count=0 .*canary_record_count=0 .*dead_letter_record_count=1' "empty_canary_dead_letter_summary" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_resolved" 'final_exit_decision code=0 .*unresolved_count=0' "final_exit_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "empty_canary_dead_letter_unresolved" 1 "$gap_two_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$EMPTY_CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_ONLY_TOPIC"; then
  assert_stdout_regex "empty_canary_dead_letter_unresolved" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_unresolved" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq-only partition=0 offset_count=1 offsets=\[1\]' "dead_letter_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_unresolved" 'side_topic_bucket=unresolved source_topic=orders side_topic=<none> partition=0 offset_count=1 offsets=\[2\]' "unresolved_offset_2" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_unresolved" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=2 .*canary_explained_count=0 .*dead_letter_explained_count=1 .*unresolved_count=1 .*canary_record_count=0 .*dead_letter_record_count=1' "empty_canary_dead_letter_summary" || failures=$((failures + 1))
  assert_stdout_regex "empty_canary_dead_letter_unresolved" 'final_exit_decision code=1 .*unresolved_count=1' "final_exit_1" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "canary_dead_letter_resolved" 0 "$gap_two_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC"; then
  assert_stdout_regex "canary_dead_letter_resolved" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_resolved" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_resolved" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq partition=0 offset_count=1 offsets=\[2\]' "dead_letter_offset_2" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_resolved" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=2 .*canary_explained_count=1 .*dead_letter_explained_count=1 .*unresolved_count=0 .*canary_record_count=5 .*dead_letter_record_count=4' "canary_dead_letter_summary" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_resolved" 'final_exit_decision code=0 .*unresolved_count=0' "final_exit_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_yaml_expected "yaml_canary_dead_letter_resolved" 0 "$gap_two_root" with_packages "$SOURCE_TOPIC" "$CANARY_TOPIC" "$DEAD_LETTER_TOPIC" 1000; then
  assert_stdout_regex "yaml_canary_dead_letter_resolved" 'recon.runDateSource=application_yml:recon.runDate' "yaml_run_date_source" || failures=$((failures + 1))
  assert_stdout_regex "yaml_canary_dead_letter_resolved" 'recon.sideTopic.enabled=true' "yaml_side_topic_enabled" || failures=$((failures + 1))
  assert_stdout_regex "yaml_canary_dead_letter_resolved" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "yaml_canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "yaml_canary_dead_letter_resolved" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq partition=0 offset_count=1 offsets=\[2\]' "yaml_dead_letter_offset_2" || failures=$((failures + 1))
  assert_stdout_regex "yaml_canary_dead_letter_resolved" 'final_exit_decision code=0 .*unresolved_count=0' "yaml_final_exit_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "canary_dead_letter_truncated_prefix_only" 1 "$gap_over_limit_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC" \
  --conf "spark.recon.missingOffsetsLimit=2"; then
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'recon.missingOffsetsLimit=2' "missing_offsets_limit_2" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'partition=0 .*missing_offset_count=3 .*missing_offsets=\[1,2\] .*missing_offsets_limit=2 .*missing_offsets_truncated=true' "truncated_partition_0" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq partition=0 offset_count=1 offsets=\[2\]' "dead_letter_offset_2" || failures=$((failures + 1))
  assert_stdout_not_regex "canary_dead_letter_truncated_prefix_only" 'side_topic_bucket=unresolved' "no_materialized_unresolved_bucket" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=2 .*canary_explained_count=1 .*dead_letter_explained_count=1 .*unresolved_count=0 .*canary_record_count=5 .*dead_letter_record_count=4 .*missing_offsets_truncated=true' "truncated_side_topic_summary" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'final_exit_decision code=1 .*reason=.*unresolved_offsets_may_remain_beyond_materialized_limit' "final_exit_truncation_reason" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'final_exit_decision code=1 .*unresolved_count=0 .*bounded_missing_offset_count=2 .*missing_offsets_truncated=true' "final_exit_truncation_fields" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_truncated_prefix_only" 'RESULT: FAIL .*unresolved offsets may remain beyond materialized limit' "result_fail_truncation_reason" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "canary_dead_letter_unresolved" 1 "$gap_over_limit_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC"; then
  assert_stdout_regex "canary_dead_letter_unresolved" 'recon.failOnGaps=true' "fail_on_gaps_true" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_unresolved" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_unresolved" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq partition=0 offset_count=1 offsets=\[2\]' "dead_letter_offset_2" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_unresolved" 'side_topic_bucket=unresolved source_topic=orders side_topic=<none> partition=0 offset_count=1 offsets=\[3\]' "unresolved_offset_3" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_unresolved" 'side_topic_summary .*raw_gap_partition_count=1 .*bounded_missing_offset_count=3 .*canary_explained_count=1 .*dead_letter_explained_count=1 .*unresolved_count=1 .*canary_record_count=5 .*dead_letter_record_count=4' "canary_dead_letter_summary" || failures=$((failures + 1))
  assert_stdout_regex "canary_dead_letter_unresolved" 'final_exit_decision code=1 .*unresolved_count=1' "final_exit_1" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "wrong_topic_wrong_partition_nonmatches" 1 "$gap_over_limit_root" with_packages \
  --conf "spark.recon.sourceTopic=shipments" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC" \
  --conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC"; then
  assert_stdout_regex "wrong_topic_wrong_partition_nonmatches" 'side_topic_bucket=unresolved source_topic=shipments side_topic=<none> partition=0 offset_count=3 offsets=\[1,2,3\]' "unresolved_all_offsets" || failures=$((failures + 1))
  assert_stdout_regex "wrong_topic_wrong_partition_nonmatches" 'side_topic_summary .*canary_explained_count=0 .*dead_letter_explained_count=0 .*unresolved_count=3 .*canary_record_count=5 .*dead_letter_record_count=4' "unresolved_summary" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "no_side_topic_regression" 1 "$gap_root" without_packages; then
  assert_stdout_regex "no_side_topic_regression" 'recon.sideTopic.enabled=false' "side_topic_disabled" || failures=$((failures + 1))
  assert_stdout_not_regex "no_side_topic_regression" 'side_topic_reconciliation_begin' "no_side_topic_read" || failures=$((failures + 1))
  assert_stdout_regex "no_side_topic_regression" 'RESULT: FAIL offset gaps detected: gap_partition_count=1' "no_side_topic_fail" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "fail_closed_incomplete_config" 2 "$gap_root" without_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC"; then
  assert_stdout_regex "fail_closed_incomplete_config" 'Incomplete side-topic config' "incomplete_config_error" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "fail_closed_unreachable_bootstrap" 2 "$gap_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=127.0.0.1:1" \
  --conf "spark.recon.canaryTopic=$CANARY_TOPIC"; then
  assert_stdout_regex "fail_closed_unreachable_bootstrap" 'Failed to read side-topic Kafka topic=' "unreachable_bootstrap_error" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_expected "fail_closed_undecodable_payload" 2 "$gap_root" with_packages \
  --conf "spark.recon.sourceTopic=$SOURCE_TOPIC" \
  --conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --conf "spark.recon.canaryTopic=$BAD_CANARY_TOPIC"; then
  assert_stdout_regex "fail_closed_undecodable_payload" 'Failed to decode Avro side-topic payload' "undecodable_payload_error" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_wrapper_expected "production_wrapper_combined_config_capture" 1 "$gap_over_limit_root"; then
  assert_stdout_regex "production_wrapper_combined_config_capture" 'recon.sourceTopic=orders' "wrapper_source_topic" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_combined_config_capture" "recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" "wrapper_bootstrap" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_combined_config_capture" 'recon.canaryTopic=orders-canary' "wrapper_canary_topic" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_combined_config_capture" 'recon.deadLetterTopic=orders-dlq' "wrapper_dead_letter_topic" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_combined_config_capture" 'side_topic_bucket=unresolved .*offsets=\[3\]' "wrapper_unresolved_offset_3" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_combined_config_capture" 'final_exit_decision code=1 .*unresolved_count=1' "wrapper_final_exit_1" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

if run_wrapper_yaml_expected "production_wrapper_yaml_side_topic_config_capture" 0 "$gap_two_root" "$SOURCE_TOPIC" "$CANARY_TOPIC" "$DEAD_LETTER_TOPIC"; then
  assert_stdout_regex "production_wrapper_yaml_side_topic_config_capture" 'recon.runDateSource=application_yml:recon.runDate' "wrapper_yaml_run_date_source" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_yaml_side_topic_config_capture" 'recon.sideTopic.enabled=true' "wrapper_yaml_side_topic_enabled" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_yaml_side_topic_config_capture" 'recon.sourceTopic=orders' "wrapper_yaml_source_topic" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_yaml_side_topic_config_capture" 'side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=\[1\]' "wrapper_yaml_canary_offset_1" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_yaml_side_topic_config_capture" 'side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq partition=0 offset_count=1 offsets=\[2\]' "wrapper_yaml_dead_letter_offset_2" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_yaml_side_topic_config_capture" 'final_exit_decision code=0 .*unresolved_count=0' "wrapper_yaml_final_exit_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

write_listing "$FIXTURE_ROOT" "$EVIDENCE_ROOT/fixture_listing.txt"
write_listing "$FIXTURE_ROOT/cache" "$EVIDENCE_ROOT/cache_listing.txt"

echo "[recon-side-test] scenario_results=$RESULTS_TSV"
echo "[recon-side-test] assertion_results=$ASSERTIONS_TSV"
echo "[recon-side-test] kafka_manifest=$side_manifest"
echo "[recon-side-test] fixture_listing=$EVIDENCE_ROOT/fixture_listing.txt"
echo "[recon-side-test] cache_listing=$EVIDENCE_ROOT/cache_listing.txt"
echo "[recon-side-test] evidence_root=$EVIDENCE_ROOT"

if [[ "$failures" -ne 0 ]]; then
  echo "[recon-side-test] failures=$failures"
  exit 1
fi

echo "[recon-side-test] all side-topic fixture checks matched expected outcomes"
