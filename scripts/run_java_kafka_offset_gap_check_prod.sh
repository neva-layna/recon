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

if [[ "$#" -gt 0 ]]; then
  INPUT_ROOTS_CSV="$(IFS=,; echo "$*")"
fi

if [[ -z "$INPUT_ROOTS_CSV" ]]; then
  echo "[recon-wrapper] ERROR: provide input roots as positional arguments or INPUT_ROOTS_CSV" >&2
  exit 2
fi

if [[ ! -f "$CHECKER_JAR" ]]; then
  echo "[recon-wrapper] ERROR: checker jar not found: $CHECKER_JAR" >&2
  echo "[recon-wrapper] build it with: GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar" >&2
  exit 2
fi

spark_conf=(
  --conf "spark.sql.session.timeZone=$SPARK_SQL_TIMEZONE"
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

exec "$SPARK_SUBMIT_BIN" \
  --class "$CHECKER_CLASS" \
  --master "$SPARK_MASTER" \
  "${spark_conf[@]}" \
  "$CHECKER_JAR"
