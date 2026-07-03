# Operations

Use the Java `spark-submit` wrapper for production-style runs.

## Build Before Running

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

The default wrapper artifact is:

```text
build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

## Run With Positional Roots

```bash
RUN_DATE=2026-07-02 \
NORMALIZED_OFFSETS_PATH=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02 \
scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2 \
  hdfs:///data/path/to/parquet3
```

## Run With A Comma-Separated Root List

```bash
INPUT_ROOTS_CSV='hdfs:///data/path/to/parquet1,hdfs:///data/path/to/parquet2' \
RUN_DATE=2026-07-02 \
scripts/run_java_kafka_offset_gap_check_prod.sh
```

## Wrapper Variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_SUBMIT_BIN` | `spark-submit` | Spark 3.5 submit executable. |
| `SPARK_MASTER` | `yarn` | Spark master passed to `spark-submit --master`. |
| `CHECKER_JAR` | `build/libs/recon-kafka-offset-gap-checker-1.0.0.jar` | Built checker artifact. |
| `CHECKER_CLASS` | `com.reconciliation.kafka.KafkaOffsetGapChecker` | Java main class. |
| `SPARK_SQL_TIMEZONE` | `UTC` | Spark SQL session timezone. |
| `INPUT_ROOTS_CSV` | none | Comma-separated roots when positional roots are not used. |
| `METADATA_COLUMN` | `cactus__metadata` | Metadata JSON column name. |
| `DATE_PARTITION_COLUMN` | `timestampcolumn` | Hive date partition directory prefix. |
| `RUN_DATE` | current shell date | Date directory to skip. |
| `NORMALIZED_OFFSETS_PATH` | none | Optional parquet path for normalized offsets. |
| `NORMALIZED_OFFSETS_OVERWRITE` | `true` | Whether to overwrite the normalized-offset path. |
| `MISSING_OFFSETS_LIMIT` | `1000` | Max missing offsets printed per partition. |
| `FAIL_ON_INVALID_ROWS` | `true` | Exit non-zero when invalid metadata rows exist. |
| `FAIL_ON_GAPS` | `true` | Exit non-zero when gaps exist. |
| `EXIT_ON_COMPLETION` | `true` | Whether the checker exits the JVM with the final checker code. |

The wrapper forwards values as `spark.recon.*` Spark conf keys.

## Direct spark-submit

The wrapper is preferred, but the equivalent direct shape is:

```bash
spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.recon.inputRoots=hdfs:///warehouse/topic/root-a,hdfs:///warehouse/topic/root-b' \
  --conf 'spark.recon.runDate=2026-07-02' \
  --conf 'spark.recon.metadataColumn=cactus__metadata' \
  --conf 'spark.recon.datePartitionColumn=timestampcolumn' \
  --conf 'spark.recon.missingOffsetsLimit=1000' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

## Output

The checker writes machine-readable lines prefixed with `[recon]`.

Important sections:

| Section | Meaning |
| --- | --- |
| `resolved_configuration_begin/end` | Final effective configuration. |
| `partition_scan_begin/end` | Eligible, skipped, and ignored partition paths. |
| `metadata_quality_begin/end` | Invalid metadata row category counts. |
| `partition_gap_stats_begin/end` | Per-partition continuity statistics. |
| `gap_partitions_begin/end` | Condensed list of partitions with gaps. |
| `RESULT: PASS` or `RESULT: FAIL` | Final operator result. |

## Exit Codes

| Code | Meaning |
| ---: | --- |
| 0 | Check completed and no configured failure condition was found. |
| 1 | Valid offset data was read, but gaps or invalid metadata rows failed the check. |
| 2 | Configuration or input data prevented a meaningful check. |

Exit code `2` includes missing roots, invalid boolean/run-date/integer config,
unreadable parquet, no eligible old partitions, empty readable parquet, missing
metadata column, cache read/write failure, and zero valid normalized offsets.

## Operational Notes

- Set `RUN_DATE` to the currently in-progress ingestion date so current-day
  data is skipped.
- Keep `NORMALIZED_OFFSETS_PATH` unique per run if multiple checks can run
  concurrently.
- Do not run each root separately when one Kafka topic can be split across
  roots; the checker unions all roots before analytics.
- Use `MISSING_OFFSETS_LIMIT=0` when you only need missing counts and do not
  want to materialize missing offset values.
- Use `FAIL_ON_GAPS=false` or `FAIL_ON_INVALID_ROWS=false` only for diagnostic
  runs where reporting without failing is intentional.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Wrapper says jar not found | Jar has not been built or `CHECKER_JAR` is wrong. | Run `./gradlew jar` or set `CHECKER_JAR`. |
| Spark rejects dependencies | Runtime is not Spark 3.5.x or has incompatible Scala ABI. | Use Spark 3.5.x built for Scala 2.12. |
| No eligible partitions | `RUN_DATE` equals all fixture/data dates, or directories do not match the date prefix. | Check `RUN_DATE` and `<DATE_PARTITION_COLUMN>=yyyy-MM-dd` names. |
| Gaps are reported per root unexpectedly | Roots were checked separately. | Pass all roots in one run. |
| Cache write fails | Existing path with overwrite disabled, bad path, or permissions. | Use a fresh `NORMALIZED_OFFSETS_PATH` or set overwrite true. |
