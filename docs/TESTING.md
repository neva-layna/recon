# Testing

The main local integration test is the Java `spark-submit` fixture matrix. It
uses Docker Spark 3.5.x, generates deterministic parquet fixtures, runs the
Java checker against every scenario, and writes command/output evidence.

## Quick Local Docker Validation

From the workspace root:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

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

Expected final line:

```text
[recon-java-test] all fixture checks matched expected outcomes
```

The matrix file is written to:

```text
.recon-local/java-evidence/scenario_results.tsv
```

All scenario rows should have `verdict` equal to `pass`.

## What The Docker Test Does

The Java fixture runner:

1. Verifies `spark-submit --version` reports Spark 3.5.x.
2. Uses `spark-shell` only to run
   `tests/fixtures/generate_kafka_offset_gap_fixtures.scala` and generate
   parquet fixtures.
3. Runs every checker scenario with:

```text
spark-submit --class com.reconciliation.kafka.KafkaOffsetGapChecker ... build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

It does not run the Java checker through `spark-shell -i`.

## Evidence Layout

With the quick command above, evidence is written below
`.recon-local/java-evidence/`:

| Path | Contents |
| --- | --- |
| `spark_version/` | Spark version command, output, exit code, verdict. |
| `generator/` | Fixture generator command, output, exit code, verdict. |
| `scenarios/<scenario>/command.txt` | Exact `spark-submit` command for the scenario. |
| `scenarios/<scenario>/stdout.log` | Checker stdout. |
| `scenarios/<scenario>/stderr.log` | Checker stderr and Spark logs. |
| `scenarios/<scenario>/expected_exit.txt` | Expected exit code. |
| `scenarios/<scenario>/exit_code.txt` | Observed exit code. |
| `scenarios/<scenario>/verdict.txt` | `pass` or `fail`. |
| `scenario_results.tsv` | Full scenario summary table. |
| `fixture_listing.txt` | Generated fixture file listing. |
| `cache_listing.txt` | Normalized-offset cache listing. |

## Scenario Coverage

The Java matrix currently covers:

| Scenario | Expected exit | Purpose |
| --- | ---: | --- |
| `continuous_pass` | 0 | Continuous offsets across two roots. |
| `cross_root_split_offsets` | 0 | Offsets are continuous only after unioning roots. |
| `missing_offsets` | 1 | Gap detection and exact missing value output. |
| `fail_on_gaps_false_allows_gap` | 0 | Gap is reported but does not fail when disabled. |
| `missing_offsets_over_limit` | 1 | Missing-offset list truncation. |
| `missing_offsets_zero_limit` | 1 | Count-only missing-offset output. |
| `duplicate_offsets` | 0 | Duplicate rows do not create false gaps. |
| `today_run_date_partition_skipped` | 0 | Configured run-date partition is skipped. |
| `scan_ignores_invalid_date_and_nonmatching_children` | 0 | Scan noise is reported and ignored. |
| `persisted_normalized_offsets` | 0 | Normalized offsets are written, read back, and used. |
| `normalized_offsets_overwrite_false_existing_path` | 2 | Existing cache path fails when overwrite is false. |
| `malformed_json` | 1 | Malformed metadata JSON is counted and fails by default. |
| `fail_on_invalid_rows_false_allows_invalid_metadata` | 0 | Invalid rows are reported but allowed when disabled. |
| `missing_metadata_value` | 1 | Null metadata values are counted. |
| `missing_partition` | 1 | Metadata missing `partition` is counted. |
| `missing_offset` | 1 | Metadata missing `offset` is counted. |
| `non_numeric_partition` | 1 | Non-numeric partition is counted. |
| `non_numeric_offset` | 1 | Non-numeric offset is counted. |
| `all_invalid_metadata` | 2 | Zero valid offsets is a configuration/data failure. |
| `empty_readable_parquet` | 2 | Empty parquet input cannot prove continuity. |
| `only_run_date_partitions` | 2 | Current-day-only data is skipped and fails. |
| `no_eligible_old_partitions` | 2 | No eligible date directories. |
| `missing_input_roots` | 2 | Required root config is missing. |
| `invalid_run_date` | 2 | Invalid run-date config. |
| `invalid_fail_flag` | 2 | Invalid boolean config. |
| `invalid_missing_offsets_limit` | 2 | Invalid non-negative integer config. |
| `nonexistent_root` | 2 | Missing filesystem root. |
| `root_not_directory` | 2 | Configured root is not a directory. |
| `missing_metadata_column` | 2 | Configured metadata column is absent. |

## Run With Local Spark Instead Of Docker

If Spark 3.5.x is already available locally:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

SPARK_SHELL_BIN=spark-shell \
SPARK_SUBMIT_BIN=spark-submit \
CHECKER_JAR=build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
FIXTURE_ROOT=/tmp/recon-kafka-offset-fixtures-java \
EVIDENCE_ROOT=/tmp/recon-kafka-offset-evidence-java \
RUN_DATE=2026-07-02 \
scripts/run_java_kafka_offset_gap_fixture_checks.sh
```

The helper rejects non-Spark-3.5 version output.

## Unit Tests

Run Java unit tests with:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew test
```

The tests cover configuration resolution, defaults, invalid booleans, missing
roots, and small formatting helpers. They do not replace the Spark fixture
matrix.

## Scala Oracle Check

The original Scala script remains useful as a behavior oracle. To run its
existing local fixture matrix:

```bash
mkdir -p .recon-local
chmod 0777 .recon-local

docker run --rm \
  --entrypoint /bin/bash \
  -e SPARK_SHELL_BIN=/opt/spark/bin/spark-shell \
  -e FIXTURE_ROOT=/recon-local/scala-fixtures \
  -e EVIDENCE_ROOT=/recon-local/scala-evidence \
  -e RUN_DATE=2026-07-02 \
  -v "$PWD":/workspace \
  -v "$PWD/.recon-local":/recon-local \
  -w /workspace \
  apache/spark:3.5.6 \
  -lc 'scripts/run_kafka_offset_gap_fixture_checks.sh'
```

This validates `scripts/check_kafka_offset_gaps.scala`, not the Java
`spark-submit` checker.

## Common Failures

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `checker jar not found` | Jar was not built or `CHECKER_JAR` points at the wrong path. | Run `GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar`. |
| Spark version check fails | Docker image or local Spark is not Spark 3.5.x. | Use `apache/spark:3.5.6` or a Spark 3.5 local install. |
| Permission errors under `.recon-local` | Docker Spark user cannot write mounted evidence. | Run `mkdir -p .recon-local && chmod 0777 .recon-local`. |
| Scenario exits differ | Behavior regression or stale fixture data. | Remove `.recon-local`, rebuild the jar, and rerun the matrix. |
| Docker cannot find the jar | `CHECKER_JAR` uses a host path inside the container. | Use `/workspace/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar`. |
