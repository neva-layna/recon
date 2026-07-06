#!/usr/bin/env bash
set -euo pipefail

# Production wrapper for the Java Spark checker. Build the jar first with:
#   GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"
SPARK_SQL_TIMEZONE="${SPARK_SQL_TIMEZONE:-UTC}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHECKER_JAR="${CHECKER_JAR:-$WORKSPACE_ROOT/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar}"
CHECKER_CLASS="${CHECKER_CLASS:-com.reconciliation.kafka.KafkaOffsetGapChecker}"

METADATA_COLUMN="${METADATA_COLUMN:-cactus__metadata}"
DATE_PARTITION_COLUMN="${DATE_PARTITION_COLUMN:-timestampcolumn}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
NORMALIZED_OFFSETS_PATH="${NORMALIZED_OFFSETS_PATH:-}"
NORMALIZED_OFFSETS_OVERWRITE="${NORMALIZED_OFFSETS_OVERWRITE:-true}"
MISSING_OFFSETS_LIMIT="${MISSING_OFFSETS_LIMIT:-1000}"
FAIL_ON_INVALID_ROWS="${FAIL_ON_INVALID_ROWS:-true}"
FAIL_ON_GAPS="${FAIL_ON_GAPS:-true}"
EXIT_ON_COMPLETION="${EXIT_ON_COMPLETION:-true}"
INPUT_ROOTS_CSV="${INPUT_ROOTS_CSV:-}"
SOURCE_TOPIC="${SOURCE_TOPIC:-}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-}"
CANARY_TOPIC="${CANARY_TOPIC:-}"
DEAD_LETTER_TOPIC="${DEAD_LETTER_TOPIC:-}"
SIDE_TOPIC_STARTING_OFFSETS="${SIDE_TOPIC_STARTING_OFFSETS:-earliest}"
SPARK_PACKAGES="${SPARK_PACKAGES:-}"
SPARK_JARS_IVY="${SPARK_JARS_IVY:-}"
APPLICATION_YML="${APPLICATION_YML:-}"
ENABLE_SIDE_TOPIC_PACKAGES="${ENABLE_SIDE_TOPIC_PACKAGES:-false}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_java_kafka_offset_gap_check_prod.sh ROOT [ROOT ...]
  INPUT_ROOTS_CSV='hdfs:///root-a,hdfs:///root-b' scripts/run_java_kafka_offset_gap_check_prod.sh

Build first:
  GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

Runtime:
  Java checker runtime: Spring Boot 2.7.18 with YAML-first configuration and
  SLF4J/Logback reporting.
  Spark 3.5.x only, Scala 2.12 Spark artifacts, Java 8-compatible checker jar.
  Side-topic reconciliation requires Kafka 3.x brokers and the Spark 3.5 Kafka connector.

YAML-first examples:
  APPLICATION_YML=/etc/recon/application.yml scripts/run_java_kafka_offset_gap_check_prod.sh
  SPRING_CONFIG_LOCATION=file:/etc/recon/application.yml scripts/run_java_kafka_offset_gap_check_prod.sh

Spark-conf override example:
  RUN_DATE=2026-07-02 scripts/run_java_kafka_offset_gap_check_prod.sh hdfs:///root-a hdfs:///root-b

Base environment:
  SPARK_SUBMIT_BIN              default: spark-submit
  SPARK_MASTER                  default: yarn
  CHECKER_JAR                   default: build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
  CHECKER_CLASS                 default: com.reconciliation.kafka.KafkaOffsetGapChecker
  SPARK_SQL_TIMEZONE            default: UTC
  APPLICATION_YML               optional path assigned to SPRING_CONFIG_LOCATION=file:<path>
  SPRING_CONFIG_LOCATION        optional Spring Boot application.yml location
  INPUT_ROOTS_CSV               comma-separated roots when no positional roots are used
  METADATA_COLUMN               default: cactus__metadata
  DATE_PARTITION_COLUMN         default: timestampcolumn
  RUN_DATE                      default: current shell date
  NORMALIZED_OFFSETS_PATH       optional parquet cache path
  NORMALIZED_OFFSETS_OVERWRITE  default: true
  MISSING_OFFSETS_LIMIT         default: 1000
  FAIL_ON_INVALID_ROWS          default: true
  FAIL_ON_GAPS                  default: true
  EXIT_ON_COMPLETION            default: true

Side-topic environment, Java spark-submit feature only:
  SOURCE_TOPIC                  source Kafka topic identity to match
  KAFKA_BOOTSTRAP_SERVERS       Kafka 3.x bootstrap servers
  CANARY_TOPIC                  optional canary/heartbeat side topic
  DEAD_LETTER_TOPIC             optional dead-letter side topic
  SIDE_TOPIC_STARTING_OFFSETS   default: earliest; accepts earliest or beginning
  SPARK_PACKAGES                optional override for spark-submit --packages
  SPARK_JARS_IVY                optional writable Ivy cache forwarded as spark.jars.ivy
  ENABLE_SIDE_TOPIC_PACKAGES    true to add default Kafka/Avro packages for YAML side-topic config

Without positional roots or INPUT_ROOTS_CSV, checker values come from
application.yml. Positional roots or INPUT_ROOTS_CSV switch the wrapper to the
Spark-conf override flow and forward base checker values as spark.recon.* keys;
those Spark conf keys override the same application.yml settings.

Setting any side-topic variable forwards that side-topic value as spark.recon.*
and enables side-topic reconciliation. SOURCE_TOPIC, KAFKA_BOOTSTRAP_SERVERS,
and at least one of CANARY_TOPIC or DEAD_LETTER_TOPIC are then required by the
checker. For side-topic runs, the wrapper adds Spark 3.5 Kafka/Avro packages
unless SPARK_PACKAGES is set. Use ENABLE_SIDE_TOPIC_PACKAGES=true when side-topic
settings are supplied only by application.yml and the cluster does not preload
the Spark Kafka connector.
USAGE
}

case "${1:-}" in
  -h|--help|help)
    usage
    exit 0
    ;;
esac

if [[ "$#" -gt 0 ]]; then
  INPUT_ROOTS_CSV="$(IFS=,; echo "$*")"
fi

if [[ -n "$APPLICATION_YML" ]]; then
  case "$APPLICATION_YML" in
    *:*) export SPRING_CONFIG_LOCATION="$APPLICATION_YML" ;;
    *) export SPRING_CONFIG_LOCATION="file:$APPLICATION_YML" ;;
  esac
fi

yaml_config_requested=false
if [[ -n "${SPRING_CONFIG_LOCATION:-}" || -n "${SPRING_CONFIG_ADDITIONAL_LOCATION:-}" || -n "${SPRING_CONFIG_NAME:-}" ]]; then
  yaml_config_requested=true
fi

if [[ -z "$INPUT_ROOTS_CSV" && "$yaml_config_requested" != "true" ]]; then
  echo "[recon-wrapper] ERROR: provide input roots as positional arguments, INPUT_ROOTS_CSV, APPLICATION_YML, or SPRING_CONFIG_LOCATION" >&2
  exit 2
fi

if [[ ! -f "$CHECKER_JAR" ]]; then
  echo "[recon-wrapper] ERROR: checker jar not found: $CHECKER_JAR" >&2
  echo "[recon-wrapper] build it with: GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar" >&2
  exit 2
fi

spark_conf=(
  --conf "spark.sql.session.timeZone=$SPARK_SQL_TIMEZONE"
)

if [[ -n "$INPUT_ROOTS_CSV" ]]; then
  spark_conf+=(
    --conf "spark.recon.inputRoots=$INPUT_ROOTS_CSV"
    --conf "spark.recon.metadataColumn=$METADATA_COLUMN"
    --conf "spark.recon.datePartitionColumn=$DATE_PARTITION_COLUMN"
    --conf "spark.recon.runDate=$RUN_DATE"
    --conf "spark.recon.normalizedOffsetsOverwrite=$NORMALIZED_OFFSETS_OVERWRITE"
    --conf "spark.recon.failOnInvalidRows=$FAIL_ON_INVALID_ROWS"
    --conf "spark.recon.failOnGaps=$FAIL_ON_GAPS"
    --conf "spark.recon.missingOffsetsLimit=$MISSING_OFFSETS_LIMIT"
    --conf "spark.recon.exitOnCompletion=$EXIT_ON_COMPLETION"
  )
  if [[ -n "$NORMALIZED_OFFSETS_PATH" ]]; then
    spark_conf+=(--conf "spark.recon.normalizedOffsetsPath=$NORMALIZED_OFFSETS_PATH")
  fi
fi
if [[ -n "$SPARK_JARS_IVY" ]]; then
  spark_conf+=(--conf "spark.jars.ivy=$SPARK_JARS_IVY")
fi

side_topic_requested=false
if [[ -n "$SOURCE_TOPIC" || -n "$KAFKA_BOOTSTRAP_SERVERS" || -n "$CANARY_TOPIC" || -n "$DEAD_LETTER_TOPIC" ]]; then
  side_topic_requested=true
  spark_conf+=(--conf "spark.recon.sourceTopic=$SOURCE_TOPIC")
  spark_conf+=(--conf "spark.recon.kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS")
  spark_conf+=(--conf "spark.recon.sideTopicStartingOffsets=$SIDE_TOPIC_STARTING_OFFSETS")
  if [[ -n "$CANARY_TOPIC" ]]; then
    spark_conf+=(--conf "spark.recon.canaryTopic=$CANARY_TOPIC")
  fi
  if [[ -n "$DEAD_LETTER_TOPIC" ]]; then
    spark_conf+=(--conf "spark.recon.deadLetterTopic=$DEAD_LETTER_TOPIC")
  fi
fi

spark_packages_args=()
enable_yaml_side_topic_packages=false
case "$ENABLE_SIDE_TOPIC_PACKAGES" in
  true|TRUE|1|yes|YES|y|Y) enable_yaml_side_topic_packages=true ;;
  false|FALSE|0|no|NO|n|N|"") ;;
  *)
    echo "[recon-wrapper] ERROR: ENABLE_SIDE_TOPIC_PACKAGES must be true or false" >&2
    exit 2
    ;;
esac
if [[ -n "$SPARK_PACKAGES" ]]; then
  spark_packages_args=(--packages "$SPARK_PACKAGES")
elif [[ "$side_topic_requested" == "true" || "$enable_yaml_side_topic_packages" == "true" ]]; then
  spark_packages_args=(--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4")
fi

submit_args=(
  --class "$CHECKER_CLASS"
  --master "$SPARK_MASTER"
)
if [[ "${#spark_packages_args[@]}" -gt 0 ]]; then
  submit_args+=("${spark_packages_args[@]}")
fi
submit_args+=("${spark_conf[@]}" "$CHECKER_JAR")

exec "$SPARK_SUBMIT_BIN" "${submit_args[@]}"
