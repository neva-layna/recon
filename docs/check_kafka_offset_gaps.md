# Kafka Offset Gap Checker

This page is the full checker reference. For focused operational docs, start
with:

| Document | Purpose |
| --- | --- |
| [README.md](../README.md) | Entry point and quick commands. |
| [BUILD.md](BUILD.md) | Gradle build, Java 8, dependency checks. |
| [TESTING.md](TESTING.md) | Local Docker validation and fixture scenarios. |
| [ARCH.md](ARCH.md) | Runtime architecture and gap algorithm. |
| [OPERATIONS.md](OPERATIONS.md) | Production `spark-submit` wrapper usage. |

`scripts/check_kafka_offset_gaps.scala` is a standalone Spark 3.5.x
`spark-shell -i` Scala script for checking Kafka offset continuity in parquet
data. It is intended for one Kafka topic persisted across one or more
HDFS/parquet roots, where each root has immediate Hive-style date partition
directories such as `timestampcolumn=2026-07-01`.

The workspace also contains a Gradle Java port for `spark-submit` deployments:
`src/main/java/com/reconciliation/kafka/KafkaOffsetGapChecker.java`. The Java
app is compiled for Java 8 bytecode and must run with Spark 3.5.x artifacts
built for Scala 2.12, such as `org.apache.spark:spark-sql_2.12:3.5.6`.

The checker scans only the immediate children of each configured root. It uses
eligible old date partitions from all roots together before computing gaps, so a
continuous offset range can be split across roots. The configured run date is
skipped because the current-day write may still be incomplete.

## Configuration

Pass checker configuration through Spark conf keys. The script accepts canonical
`recon.*` keys and `spark.recon.*` aliases for launchers that only preserve
`spark.*` keys.

| Key | Default | Meaning |
| --- | --- | --- |
| `recon.inputRoots` | required | Comma-separated parquet roots to scan. |
| `recon.metadataColumn` | `cactus__metadata` | String column containing metadata JSON with Kafka `partition` and `offset`. |
| `recon.datePartitionColumn` | `timestampcolumn` | Hive partition directory prefix. |
| `recon.runDate` | driver current date | Optional `yyyy-MM-dd` override for the partition date to skip. |
| `recon.normalizedOffsetsPath` | none | Optional parquet path where normalized offsets are written and read back before analytics. |
| `recon.normalizedOffsetsOverwrite` | `true` | Overwrite the normalized-offset path when persistence is enabled. |
| `recon.failOnInvalidRows` | `true` | Exit non-zero when malformed or incomplete metadata rows are present. |
| `recon.failOnGaps` | `true` | Exit non-zero when any Kafka partition has missing offsets. |
| `recon.missingOffsetsLimit` | `1000` | Per-partition safety limit for materializing actual missing offset values in output. Set `0` to report only counts and mark gapped partitions as truncated. |
| `recon.exitOnCompletion` | `true` | Call `System.exit` so automation does not remain in the interactive shell. |

The fixture generator uses these Spark conf keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `recon.fixtureOutputRoot` | `/tmp/recon-kafka-offset-fixtures` | Local root where generated parquet fixtures are written. |
| `recon.fixtureMetadataColumn` | `cactus__metadata` | Fixture metadata column name. |
| `recon.fixtureDatePartitionColumn` | `timestampcolumn` | Fixture date partition column name. |
| `recon.fixtureOldDate` | `2026-07-01` | Eligible old partition date used in fixtures. |
| `recon.fixtureRunDate` | `2026-07-02` | Run-date partition used for skip fixtures. |

## Production Example

Use Spark 3.5.x and pass all behavior through Spark conf:

```bash
spark-shell \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'recon.inputRoots=hdfs:///warehouse/topic/root-a,hdfs:///warehouse/topic/root-b' \
  --conf 'recon.metadataColumn=cactus__metadata' \
  --conf 'recon.datePartitionColumn=timestampcolumn' \
  --conf 'recon.runDate=2026-07-02' \
  -i scripts/check_kafka_offset_gaps.scala
```

With persisted normalized offsets:

```bash
spark-shell \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'recon.inputRoots=hdfs:///warehouse/topic/root-a,hdfs:///warehouse/topic/root-b' \
  --conf 'recon.normalizedOffsetsPath=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02' \
  --conf 'recon.normalizedOffsetsOverwrite=true' \
  --conf 'recon.runDate=2026-07-02' \
  -i scripts/check_kafka_offset_gaps.scala
```

If a launcher rejects non-`spark.*` keys, use the alias form, for example
`--conf 'spark.recon.inputRoots=hdfs:///warehouse/topic/root-a'`.

### Production Wrapper

Use `scripts/run_kafka_offset_gap_check_prod.sh` from a Hadoop edge node when
you want a repeatable production command instead of a long raw `spark-shell`
invocation. The wrapper still runs the same Scala checker through Spark 3.5
`spark-shell -i`; it only assembles the Spark conf values and passes your input
roots as `spark.recon.*` aliases.

Pass every root as a separate argument:

```bash
RUN_DATE=2026-07-02 \
NORMALIZED_OFFSETS_PATH=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02 \
scripts/run_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2 \
  hdfs:///data/path/to/parquet3
```

You can also provide roots as a comma-separated value:

```bash
INPUT_ROOTS_CSV='hdfs:///data/path/to/parquet1,hdfs:///data/path/to/parquet2' \
scripts/run_kafka_offset_gap_check_prod.sh
```

The wrapper variables are:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_SHELL_BIN` | `spark-shell` | Spark 3.5 shell executable on the edge node. |
| `SPARK_MASTER` | `yarn` | Spark master passed to `spark-shell --master`. |
| `SPARK_SQL_TIMEZONE` | `UTC` | Value for `spark.sql.session.timeZone`. |
| `INPUT_ROOTS_CSV` | none | Comma-separated roots, used when positional roots are not supplied. |
| `METADATA_COLUMN` | `cactus__metadata` | Metadata JSON column name. |
| `DATE_PARTITION_COLUMN` | `timestampcolumn` | Hive date partition directory prefix. |
| `RUN_DATE` | current shell date | Date partition to skip, normally today's in-progress ingestion date. |
| `NORMALIZED_OFFSETS_PATH` | none | Optional HDFS path for normalized offsets written and read before analytics. |
| `NORMALIZED_OFFSETS_OVERWRITE` | `true` | Whether the normalized-offset path is overwritten. |
| `MISSING_OFFSETS_LIMIT` | `1000` | Maximum missing offset values printed per partition. |
| `FAIL_ON_INVALID_ROWS` | `true` | Exit non-zero when eligible metadata rows are invalid. |
| `FAIL_ON_GAPS` | `true` | Exit non-zero when gaps are detected. |

The wrapper uses `spark.recon.*` aliases to avoid Spark warnings while the
checker still prints canonical `recon.*` values. It exits with the checker exit
code: `0` for pass, `1` for gaps or invalid metadata, and `2` for configuration
or unreadable/empty input failures.

### Java Spark Submit Port

The Java checker is the `spark-submit` product path. It preserves the same
`recon.*` and `spark.recon.*` configuration surface, `[recon]` output fields,
and exit code classes as the Scala `spark-shell -i` oracle script.

Build the jar:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

Run with the Java production wrapper:

```bash
RUN_DATE=2026-07-02 \
NORMALIZED_OFFSETS_PATH=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02 \
scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2
```

The wrapper uses `spark-submit --class
com.reconciliation.kafka.KafkaOffsetGapChecker` and the built jar at
`build/libs/recon-kafka-offset-gap-checker-1.0.0.jar` by default. Set
`CHECKER_JAR` when deploying a copied artifact.

Java wrapper variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_SUBMIT_BIN` | `spark-submit` | Spark 3.5 submit executable on the edge node. |
| `SPARK_MASTER` | `yarn` | Spark master passed to `spark-submit --master`. |
| `CHECKER_JAR` | `build/libs/recon-kafka-offset-gap-checker-1.0.0.jar` | Built Java artifact to submit. |
| `CHECKER_CLASS` | `com.reconciliation.kafka.KafkaOffsetGapChecker` | Java main class. |
| `SPARK_SQL_TIMEZONE` | `UTC` | Value for `spark.sql.session.timeZone`. |
| `INPUT_ROOTS_CSV` | none | Comma-separated roots, used when positional roots are not supplied. |
| `METADATA_COLUMN` | `cactus__metadata` | Metadata JSON column name. |
| `DATE_PARTITION_COLUMN` | `timestampcolumn` | Hive date partition directory prefix. |
| `RUN_DATE` | current shell date | Date partition to skip, normally today's in-progress ingestion date. |
| `NORMALIZED_OFFSETS_PATH` | none | Optional HDFS path for normalized offsets written and read before analytics. |
| `NORMALIZED_OFFSETS_OVERWRITE` | `true` | Whether the normalized-offset path is overwritten. |
| `MISSING_OFFSETS_LIMIT` | `1000` | Maximum missing offset values printed per partition. |
| `FAIL_ON_INVALID_ROWS` | `true` | Exit non-zero when eligible metadata rows are invalid. |
| `FAIL_ON_GAPS` | `true` | Exit non-zero when gaps are detected. |
| `EXIT_ON_COMPLETION` | `true` | Whether the checker exits the JVM with the final checker code. |

Production notes:

- `RUN_DATE` is the date directory to skip, not necessarily the data event date.
  For daily runs, set it to the current ingestion date.
- Keep `NORMALIZED_OFFSETS_PATH` unique per run if multiple checks can execute
  concurrently.
- The checker reads only immediate children named
  `<DATE_PARTITION_COLUMN>=yyyy-MM-dd` below each root.
- All roots are unioned before gap analytics; do not run each root separately if
  one Kafka topic can be split across roots.

### Hadoop Sample Data

Use `scripts/generate_hadoop_offset_gap_sample_data.sh` when you want a small
HDFS smoke dataset before running against production paths. It runs
`scripts/generate_kafka_offset_gap_sample_data.scala` with Spark 3.5 and writes
only two scenarios, not the full local fixture matrix.

Generate the sample data:

```bash
scripts/generate_hadoop_offset_gap_sample_data.sh hdfs:///tmp/recon-offset-samples
```

This writes:

- `hdfs:///tmp/recon-offset-samples/pass/root_a`
- `hdfs:///tmp/recon-offset-samples/pass/root_b`
- `hdfs:///tmp/recon-offset-samples/gap/root_a`
- `hdfs:///tmp/recon-offset-samples/gap/root_b`

The generated layout is:

```text
hdfs:///tmp/recon-offset-samples/
  pass/
    root_a/timestampcolumn=2026-07-01/
    root_b/timestampcolumn=2026-07-01/
  gap/
    root_a/timestampcolumn=2026-07-01/
    root_b/timestampcolumn=2026-07-01/
```

The pass roots are continuous only after cross-root union:

- `pass/root_a`: partition `0` offsets `0,2`, partition `1` offset `10`
- `pass/root_b`: partition `0` offset `1`, partition `1` offsets `11,12`

The gap roots are also multi-root, but partition `0` is missing offset `1` after
union:

- `gap/root_a`: partition `0` offset `0`, partition `1` offset `10`
- `gap/root_b`: partition `0` offset `2`, partition `1` offsets `11,12`

Run both sample checks:

```bash
RUN_DATE=2026-07-02 \
scripts/run_hadoop_offset_gap_sample_checks.sh hdfs:///tmp/recon-offset-samples
```

Expected sample-check results:

- `multi_root_pass` exits `0` and prints `RESULT: PASS no gaps detected`.
- `multi_root_gap` exits `1`, prints `missing_offsets=[1]` for partition `0`,
  and the wrapper treats that `1` exit as the expected result for this sample.
- The wrapper exits `0` only when both expected results match.

Sample generator variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_SHELL_BIN` | `spark-shell` | Spark 3.5 shell executable. |
| `SPARK_MASTER` | `yarn` | Spark master for generation. |
| `SPARK_SQL_TIMEZONE` | `UTC` | Spark SQL session timezone. |
| `SAMPLE_OUTPUT_ROOT` | `hdfs:///tmp/recon-kafka-offset-gap-samples` | Output root when no argument is supplied. |
| `SAMPLE_OLD_DATE` | `2026-07-01` | Date partition written in the sample data. |
| `METADATA_COLUMN` | `cactus__metadata` | Metadata column written in parquet. |
| `DATE_PARTITION_COLUMN` | `timestampcolumn` | Hive date partition prefix. |

Sample check variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_SHELL_BIN` | `spark-shell` | Spark 3.5 shell executable. |
| `SPARK_MASTER` | `yarn` | Spark master for checks. |
| `RUN_DATE` | current shell date | Date partition to skip; set this later than `SAMPLE_OLD_DATE`. |
| `MISSING_OFFSETS_LIMIT` | `1000` | Missing values printed per gapped partition. |
| `METADATA_COLUMN` | `cactus__metadata` | Metadata column to read. |
| `DATE_PARTITION_COLUMN` | `timestampcolumn` | Hive date partition prefix to scan. |

For the sample workflow, use `RUN_DATE` different from `SAMPLE_OLD_DATE`.
For example, `SAMPLE_OLD_DATE=2026-07-01` and `RUN_DATE=2026-07-02` makes the
sample partitions eligible for scanning. If the two dates are equal, the checker
will skip the generated partitions as current-day data.

## Script Reference

| File | Purpose |
| --- | --- |
| `scripts/check_kafka_offset_gaps.scala` | Main Spark 3.5 checker run with `spark-shell -i`. |
| `scripts/run_kafka_offset_gap_check_prod.sh` | Production wrapper for checking real HDFS/parquet roots. |
| `src/main/java/com/reconciliation/kafka/KafkaOffsetGapChecker.java` | Java Spark SQL/DataFrame port for `spark-submit`. |
| `scripts/run_java_kafka_offset_gap_check_prod.sh` | Production wrapper for the Java `spark-submit` checker. |
| `scripts/generate_kafka_offset_gap_sample_data.scala` | Small Spark generator for the two Hadoop sample scenarios. |
| `scripts/generate_hadoop_offset_gap_sample_data.sh` | Wrapper around the small sample generator. |
| `scripts/run_hadoop_offset_gap_sample_checks.sh` | Wrapper that verifies the two generated sample scenarios. |
| `tests/fixtures/generate_kafka_offset_gap_fixtures.scala` | Full validation fixture generator used by the local test runner. |
| `scripts/run_kafka_offset_gap_fixture_checks.sh` | Full Docker/local validation runner covering all edge cases. |
| `scripts/run_java_kafka_offset_gap_fixture_checks.sh` | Full Docker/local validation runner for the Java `spark-submit` port. |

## Local Docker Validation

The local validation path uses Spark 3.5.x in Docker, not host-installed Spark.
It writes local fixtures and evidence under `.recon-local/`. Local persistence
validation uses `file://` for the normalized-offset cache path. Production can
use HDFS-style paths such as `hdfs:///tmp/...`; the local command does not
validate a full HDFS cluster.

Build the Java checker first:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

Run the Java `spark-submit` fixture matrix:

```bash
mkdir -p .recon-local
chmod 0777 .recon-local

docker run --rm \
  --entrypoint /bin/bash \
  -e SPARK_SHELL_BIN=/opt/spark/bin/spark-shell \
  -e SPARK_SUBMIT_BIN=/opt/spark/bin/spark-submit \
  -e CHECKER_JAR=/workspace/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
  -e FIXTURE_ROOT=/recon-local/java-fixtures \
  -e EVIDENCE_ROOT=/recon-local/java-evidence \
  -e RUN_DATE=2026-07-02 \
  -v "$PWD":/workspace \
  -v "$PWD/.recon-local":/recon-local \
  -w /workspace \
  apache/spark:3.5.6 \
  -lc 'scripts/run_java_kafka_offset_gap_fixture_checks.sh'
```

The Java helper uses `spark-shell` only to generate deterministic parquet
fixtures from `tests/fixtures/generate_kafka_offset_gap_fixtures.scala`. Every
checker scenario is then run through `spark-submit --class
com.reconciliation.kafka.KafkaOffsetGapChecker` and the built Java jar, not
through `spark-shell -i scripts/check_kafka_offset_gaps.scala`.

To run the Java helper with already available Spark 3.5.x binaries:

```bash
SPARK_SHELL_BIN=spark-shell \
SPARK_SUBMIT_BIN=spark-submit \
CHECKER_JAR=build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
FIXTURE_ROOT=/tmp/recon-kafka-offset-fixtures-java \
EVIDENCE_ROOT=/tmp/recon-kafka-offset-evidence-java \
RUN_DATE=2026-07-02 \
scripts/run_java_kafka_offset_gap_fixture_checks.sh
```

The original Scala fixture runner remains useful as an oracle check for
`scripts/check_kafka_offset_gaps.scala`:

```bash
mkdir -p .recon-local
chmod 0777 .recon-local

docker run --rm \
  --entrypoint /bin/bash \
  -e SPARK_SHELL_BIN=/opt/spark/bin/spark-shell \
  -e FIXTURE_ROOT=/recon-local/fixtures \
  -e EVIDENCE_ROOT=/recon-local/evidence \
  -e RUN_DATE=2026-07-02 \
  -v "$PWD":/workspace \
  -v "$PWD/.recon-local":/recon-local \
  -w /workspace \
  apache/spark:3.5.6 \
  -lc 'scripts/run_kafka_offset_gap_fixture_checks.sh'
```

The helper first runs `spark-shell --version` and requires Spark 3.5.x evidence.
It then runs `tests/fixtures/generate_kafka_offset_gap_fixtures.scala` and the
checker for each scenario.

To run the same helper with an already available Spark 3.5.x shell:

```bash
SPARK_SHELL_BIN=spark-shell \
FIXTURE_ROOT=/tmp/recon-kafka-offset-fixtures \
EVIDENCE_ROOT=/tmp/recon-kafka-offset-evidence \
RUN_DATE=2026-07-02 \
scripts/run_kafka_offset_gap_fixture_checks.sh
```

## Fixture Scenarios

The generator creates deterministic parquet fixtures with immediate
`timestampcolumn=yyyy-MM-dd` directories and a `cactus__metadata` JSON column.
The runner records exact expected and observed exit codes for:

- `continuous_pass`
- `cross_root_split_offsets`
- `missing_offsets`
- `missing_offsets_over_limit`
- `missing_offsets_zero_limit`
- `duplicate_offsets`
- `today_run_date_partition_skipped`
- `scan_ignores_invalid_date_and_nonmatching_children`
- `persisted_normalized_offsets`
- `normalized_offsets_overwrite_false_existing_path`
- `malformed_json`
- `fail_on_invalid_rows_false_allows_invalid_metadata`
- `missing_metadata_value`
- `missing_partition`
- `missing_offset`
- `non_numeric_partition`
- `non_numeric_offset`
- `all_invalid_metadata`
- `empty_readable_parquet`
- `only_run_date_partitions`
- `no_eligible_old_partitions`
- `fail_on_gaps_false_allows_gap`
- `missing_input_roots`
- `invalid_run_date`
- `invalid_fail_flag`
- `invalid_missing_offsets_limit`
- `nonexistent_root`
- `root_not_directory`
- `missing_metadata_column`

Evidence is written as:

- `$EVIDENCE_ROOT/spark_version/{command.txt,stdout.log,stderr.log,exit_code.txt,verdict.txt}`
- `$EVIDENCE_ROOT/generator/{command.txt,stdout.log,stderr.log,exit_code.txt,verdict.txt}`
- `$EVIDENCE_ROOT/scenarios/<scenario>/{command.txt,stdout.log,stderr.log,expected_exit.txt,exit_code.txt,verdict.txt}`
- `$EVIDENCE_ROOT/scenario_results.tsv`
- `$EVIDENCE_ROOT/fixture_listing.txt`
- `$EVIDENCE_ROOT/cache_listing.txt`

## Output And Exit Codes

The checker prints machine-readable lines prefixed with `[recon]`.

Partition scan output includes eligible old partition paths, skipped run-date
paths, ignored invalid-date child directories, and ignored non-matching child
directories.

Metadata quality output includes `eligible_row_count`,
`missing_metadata_count`, `malformed_json_count`, `missing_partition_count`,
`missing_offset_count`, `non_numeric_partition_count`,
`non_numeric_offset_count`, `invalid_row_count`, and
`valid_offset_row_count`.

Gap output includes `partition`, `distinct_offset_count`, `min_offset`,
`max_offset`, `span`, `expected_count`, `missing_offset_count`, `has_gaps`,
`missing_offsets`, `missing_offsets_limit`, and `missing_offsets_truncated`.
Duplicate rows are de-duplicated for gap analytics and reported through
`duplicate_offset_row_count`, so repeated offset rows do not create false
missing offset values.

For a gapped partition, `missing_offsets` contains the actual missing offset
values in ascending order, for example `missing_offsets=[1,4]`. The list is
attributable to the same line's `partition` or `gap_partition` value. If the
true missing value count exceeds `recon.missingOffsetsLimit`, the checker prints
only the first `missing_offsets_limit` values and sets
`missing_offsets_truncated=true`; operators must treat that list as incomplete.
When `missing_offsets_truncated=false`, the list is complete for that partition.

Exit code `0` means the check completed and no configured failure condition was
found. Exit code `1` means the checker read valid offset data but failed because
gaps or invalid metadata rows were detected under the default failure settings.
Exit code `2` means configuration or input data prevented a meaningful check,
including missing input roots, invalid boolean or run-date configuration,
unreadable parquet, no eligible old partitions, empty readable parquet, missing
metadata column, failed cache read/write, or zero valid normalized offsets.
