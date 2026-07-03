#!/usr/bin/env bash
set -euo pipefail

# Generates two Hadoop/HDFS sample datasets with Spark 3.5:
#   1. pass/root_a + pass/root_b: continuous after cross-root union.
#   2. gap/root_a + gap/root_b: partition 0 is missing offset 1 after union.
#
# Usage:
#   scripts/generate_hadoop_offset_gap_sample_data.sh hdfs:///tmp/recon-offset-samples
#
# Environment overrides:
#   SPARK_SHELL_BIN=spark-shell
#   SPARK_MASTER=yarn
#   SAMPLE_OUTPUT_ROOT=hdfs:///tmp/recon-kafka-offset-gap-samples
#   SAMPLE_OLD_DATE=yyyy-MM-dd
#   METADATA_COLUMN=cactus__metadata
#   DATE_PARTITION_COLUMN=timestampcolumn

SPARK_SHELL_BIN="${SPARK_SHELL_BIN:-spark-shell}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"
SPARK_SQL_TIMEZONE="${SPARK_SQL_TIMEZONE:-UTC}"
SAMPLE_OUTPUT_ROOT="${1:-${SAMPLE_OUTPUT_ROOT:-hdfs:///tmp/recon-kafka-offset-gap-samples}}"
SAMPLE_OLD_DATE="${SAMPLE_OLD_DATE:-2026-07-01}"
METADATA_COLUMN="${METADATA_COLUMN:-cactus__metadata}"
DATE_PARTITION_COLUMN="${DATE_PARTITION_COLUMN:-timestampcolumn}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATOR="$SCRIPT_DIR/generate_kafka_offset_gap_sample_data.scala"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '3,20p' "$0"
  exit 0
fi

echo "[recon-sample] output_root=$SAMPLE_OUTPUT_ROOT"
echo "[recon-sample] old_date=$SAMPLE_OLD_DATE"

exec "$SPARK_SHELL_BIN" \
  --master "$SPARK_MASTER" \
  --conf "spark.sql.session.timeZone=$SPARK_SQL_TIMEZONE" \
  --conf "spark.recon.sampleOutputRoot=$SAMPLE_OUTPUT_ROOT" \
  --conf "spark.recon.sampleOldDate=$SAMPLE_OLD_DATE" \
  --conf "spark.recon.sampleMetadataColumn=$METADATA_COLUMN" \
  --conf "spark.recon.sampleDatePartitionColumn=$DATE_PARTITION_COLUMN" \
  -i "$GENERATOR"
