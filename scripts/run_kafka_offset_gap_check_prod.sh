#!/usr/bin/env bash
set -euo pipefail

# Production wrapper for the Spark 3.5 Kafka offset gap checker.
#
# Usage:
#   scripts/run_kafka_offset_gap_check_prod.sh hdfs:///data/path/to/parquet1 hdfs:///data/path/to/parquet2 ...
#
# Or:
#   INPUT_ROOTS_CSV='hdfs:///data/path/to/parquet1,hdfs:///data/path/to/parquet2' \
#   scripts/run_kafka_offset_gap_check_prod.sh
#
# Required runtime: Spark 3.5.x with `spark-shell` available on the Hadoop edge
# node. The script exits with the checker exit code: 0 pass, 1 data gaps/invalid
# rows, 2 configuration or unreadable/empty input failure.

SPARK_SHELL_BIN="${SPARK_SHELL_BIN:-spark-shell}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"
SPARK_SQL_TIMEZONE="${SPARK_SQL_TIMEZONE:-UTC}"
METADATA_COLUMN="${METADATA_COLUMN:-cactus__metadata}"
DATE_PARTITION_COLUMN="${DATE_PARTITION_COLUMN:-timestampcolumn}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
MISSING_OFFSETS_LIMIT="${MISSING_OFFSETS_LIMIT:-1000}"
FAIL_ON_INVALID_ROWS="${FAIL_ON_INVALID_ROWS:-true}"
FAIL_ON_GAPS="${FAIL_ON_GAPS:-true}"
NORMALIZED_OFFSETS_PATH="${NORMALIZED_OFFSETS_PATH:-}"
NORMALIZED_OFFSETS_OVERWRITE="${NORMALIZED_OFFSETS_OVERWRITE:-true}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKER="${CHECKER:-$SCRIPT_DIR/check_kafka_offset_gaps.scala}"

# Prints the script header usage block.
usage() {
  sed -n '3,18p' "$0" >&2
}

# Joins positional root arguments into the comma-separated value expected by the checker.
join_by_comma() {
  local IFS=,
  printf '%s' "$*"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$#" -gt 0 ]]; then
  INPUT_ROOTS_CSV="$(join_by_comma "$@")"
else
  INPUT_ROOTS_CSV="${INPUT_ROOTS_CSV:-}"
fi

if [[ -z "$INPUT_ROOTS_CSV" ]]; then
  echo "[recon-prod] ERROR: pass input roots as arguments or set INPUT_ROOTS_CSV" >&2
  usage
  exit 2
fi

cmd=(
  "$SPARK_SHELL_BIN"
  --master "$SPARK_MASTER"
  --conf "spark.sql.session.timeZone=$SPARK_SQL_TIMEZONE"
  --conf "spark.recon.inputRoots=$INPUT_ROOTS_CSV"
  --conf "spark.recon.metadataColumn=$METADATA_COLUMN"
  --conf "spark.recon.datePartitionColumn=$DATE_PARTITION_COLUMN"
  --conf "spark.recon.runDate=$RUN_DATE"
  --conf "spark.recon.missingOffsetsLimit=$MISSING_OFFSETS_LIMIT"
  --conf "spark.recon.failOnInvalidRows=$FAIL_ON_INVALID_ROWS"
  --conf "spark.recon.failOnGaps=$FAIL_ON_GAPS"
  --conf "spark.recon.normalizedOffsetsOverwrite=$NORMALIZED_OFFSETS_OVERWRITE"
)

if [[ -n "$NORMALIZED_OFFSETS_PATH" ]]; then
  cmd+=(--conf "spark.recon.normalizedOffsetsPath=$NORMALIZED_OFFSETS_PATH")
fi

cmd+=(-i "$CHECKER")

echo "[recon-prod] spark_master=$SPARK_MASTER"
echo "[recon-prod] run_date=$RUN_DATE"
echo "[recon-prod] input_roots=$INPUT_ROOTS_CSV"
if [[ -n "$NORMALIZED_OFFSETS_PATH" ]]; then
  echo "[recon-prod] normalized_offsets_path=$NORMALIZED_OFFSETS_PATH"
fi

exec "${cmd[@]}"
