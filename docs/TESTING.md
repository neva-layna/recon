# Testing

The main local integration test is the Java `spark-submit` fixture matrix. It
uses Docker Spark 3.5.x, generates deterministic parquet fixtures, runs the
Java checker against every scenario, and writes command/output evidence.

## Quick Local Docker Validation

From the workspace root:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

rtk mkdir -p .recon-local
rtk chmod 0777 .recon-local

rtk docker run --rm \
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
  -lc 'scripts/validation/run_java_kafka_offset_gap_fixture_checks.sh'
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

## YAML And Override Coverage

The Java checker is a Spring Boot 2.7.18 application, so unit and smoke
coverage must include `application.yml` binding as well as Spark-conf override
compatibility. The YAML tests cover base parquet-gap fields, side-topic fields,
defaults, invalid typed values, incomplete side-topic config, unsupported
side-topic offsets, and precedence where `recon.*` or `spark.recon.*` Spark conf
values override YAML.

For real-surface YAML checks, submit the same jar with Spring config
environment:

```bash
rtk env \
  SPRING_CONFIG_LOCATION=file:/tmp/recon/application.yml \
  spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master local[*] \
  --conf 'spark.sql.session.timeZone=UTC' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

Use the wrapper path for the same YAML-first flow:

```bash
rtk env \
  APPLICATION_YML=/tmp/recon/application.yml \
  scripts/check/run_java_kafka_offset_gap_check_prod.sh
```

To prove override compatibility, add positional roots or `INPUT_ROOTS_CSV` and
the wrapper will forward `spark.recon.*` keys over the YAML values.

## Spark+Kafka Docker Side-Topic Validation

Run this matrix when validating the Java side-topic feature. It exists because
canary/heartbeat messages and broken source messages can be routed to Kafka
side topics instead of HDFS parquet, so missing parquet offsets must be checked
against canary and dead-letter topics. The checker scenarios in this matrix use
Java `spark-submit`; `spark-shell` is used only for fixture generation.

Build the jar first, then run the full side-topic matrix with both Kafka 3.x and
Spark 3.5.x in Docker:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

rtk scripts/validation/run_java_kafka_side_topic_docker_checks.sh
```

The Docker wrapper starts `apache/kafka:3.7.0`, creates these topics by
default, and then runs `scripts/validation/run_java_kafka_side_topic_fixture_checks.sh`
inside `apache/spark:3.5.6`:

| Topic | Purpose |
| --- | --- |
| `orders-canary` | Canary records for source offsets, plus false-match records. |
| `orders-dlq` | Dead-letter records for combined canary/dead-letter checks. |
| `orders-dlq-only` | Dead-letter-only scenario. |
| `orders-empty-canary` | Empty canary topic for dead-letter-only exit scenarios. |
| `orders-empty-dlq` | Empty dead-letter topic for canary-only exit scenarios. |
| `orders-bad-canary` | Non-Avro payload for fail-closed decode validation. |

Inside the Spark container, `tests/fixtures/generate_kafka_side_topic_records.scala`
writes Avro object-container side-topic records to those topics and records a
manifest at `kafka/side_topic_records.tsv`. The checker scenarios then run
through Java `spark-submit`. The runner also invokes
`scripts/check/run_java_kafka_offset_gap_check_prod.sh` once to capture production
wrapper side-topic env propagation.

The side-topic matrix file is written to:

```text
.recon-local-side/evidence/run/scenario_results.tsv
```

Use these variables to override the Docker defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_IMAGE` | `apache/spark:3.5.6` | Spark 3.5.x image used to run `spark-submit`. |
| `KAFKA_IMAGE` | `apache/kafka:3.7.0` | Kafka 3.x broker image. |
| `NETWORK_NAME` | `recon-side-topic-net` | Docker network for Spark-to-Kafka communication. |
| `HOST_FIXTURE_ROOT` | `.recon-local-side/fixtures` | Host directory mounted as fixture output. |
| `HOST_EVIDENCE_ROOT` | `.recon-local-side/evidence` | Host directory mounted as evidence output; run output is under `run/`. |
| `CLEANUP` | `true` | Remove the Kafka container and Docker network after the run. |
| `SOURCE_TOPIC` | `orders` | Source topic identity used inside side-topic payloads. |
| `CANARY_TOPIC` | `orders-canary` | Canary topic name. |
| `DEAD_LETTER_TOPIC` | `orders-dlq` | Dead-letter topic name. |

If Spark 3.5.x is installed locally and you only need Kafka in Docker, run the
underlying helper directly:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

rtk env \
  SPARK_SHELL_BIN=spark-shell \
  SPARK_SUBMIT_BIN=spark-submit \
  CHECKER_JAR=build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
  FIXTURE_ROOT=/tmp/recon-kafka-offset-side-topic-fixtures-java \
  EVIDENCE_ROOT=/tmp/recon-kafka-offset-side-topic-evidence-java \
  SPARK_JARS_IVY=/tmp/recon-ivy \
  RUN_DATE=2026-07-02 \
  scripts/validation/run_java_kafka_side_topic_fixture_checks.sh
```

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

Side-topic validation writes the same per-scenario command/stdout/stderr/exit
files under `scenarios/<scenario>/`, plus:

| Path | Contents |
| --- | --- |
| `kafka_version/` | Kafka 3.x version command, output, exit code, verdict. |
| `kafka/kafka_image.txt` | Kafka Docker image used by the wrapper. |
| `kafka/side_topic_records.tsv` | Produced canary/dead-letter/bad-payload manifest. |
| `side_topic_generator/` | Side-topic fixture producer command, output, exit code, verdict. |
| `assertion_results.tsv` | Regex assertion matrix for side-topic buckets and config capture. |

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

The side-topic matrix covers:

| Scenario | Expected exit | Purpose |
| --- | ---: | --- |
| `canary_empty_dead_letter_resolved` | 0 | Canary explains all bounded missing offsets while dead-letter is configured but empty. |
| `canary_empty_dead_letter_unresolved` | 1 | Canary explains one bounded offset and one remains unresolved while dead-letter is empty. |
| `empty_canary_dead_letter_resolved` | 0 | Dead-letter explains all bounded missing offsets while canary is configured but empty. |
| `empty_canary_dead_letter_unresolved` | 1 | Dead-letter explains one bounded offset and one remains unresolved while canary is empty. |
| `canary_dead_letter_resolved` | 0 | Canary and dead-letter together explain all bounded missing offsets. |
| `canary_dead_letter_truncated_prefix_only` | 1 | Real missing offsets exceed `recon.missingOffsetsLimit`; side topics explain only the materialized prefix, so truncation fails closed. |
| `canary_dead_letter_unresolved` | 1 | Canary and dead-letter explain two bounded offsets and one remains unresolved. |
| `wrong_topic_wrong_partition_nonmatches` | 1 | Wrong source-topic/partition records remain nonmatches and all offsets stay unresolved. |
| `no_side_topic_regression` | 1 | Raw parquet gaps still fail when no side-topic config is present. |
| `fail_closed_incomplete_config` | 2 | Partial side-topic config fails before Kafka reads. |
| `fail_closed_unreachable_bootstrap` | 2 | Unreachable Kafka bootstrap fails closed. |
| `fail_closed_undecodable_payload` | 2 | Non-Avro side-topic payload fails closed. |
| `production_wrapper_combined_config_capture` | 1 | Production wrapper env passes side-topic config and preserves unresolved exit `1`. |

## Run With Local Spark Instead Of Docker

If Spark 3.5.x is already available locally:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

rtk env \
  SPARK_SHELL_BIN=spark-shell \
  SPARK_SUBMIT_BIN=spark-submit \
  CHECKER_JAR=build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
  FIXTURE_ROOT=/tmp/recon-kafka-offset-fixtures-java \
  EVIDENCE_ROOT=/tmp/recon-kafka-offset-evidence-java \
  RUN_DATE=2026-07-02 \
  scripts/validation/run_java_kafka_offset_gap_fixture_checks.sh
```

The helper rejects non-Spark-3.5 version output.

## Unit Tests

Run Java unit tests with:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew test
```

The tests cover Spring Boot YAML binding, Spark conf precedence, configuration
resolution, defaults, invalid booleans/numbers/run dates, missing roots,
side-topic config validation, and small formatting helpers. They do not replace
the Spark fixture matrix.

## Scala Oracle Check

The original Scala script remains useful as a behavior oracle. To run its
existing local fixture matrix:

```bash
rtk mkdir -p .recon-local
rtk chmod 0777 .recon-local

rtk docker run --rm \
  --entrypoint /bin/bash \
  -e SPARK_SHELL_BIN=/opt/spark/bin/spark-shell \
  -e FIXTURE_ROOT=/recon-local/scala-fixtures \
  -e EVIDENCE_ROOT=/recon-local/scala-evidence \
  -e RUN_DATE=2026-07-02 \
  -v "$PWD":/workspace \
  -v "$PWD/.recon-local":/recon-local \
  -w /workspace \
  apache/spark:3.5.6 \
  -lc 'scripts/validation/run_kafka_offset_gap_fixture_checks.sh'
```

This validates `scripts/check/check_kafka_offset_gaps.scala`, not the Java
`spark-submit` checker.

## Common Failures

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `checker jar not found` | Jar was not built or `CHECKER_JAR` points at the wrong path. | Run `rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar`. |
| Spark version check fails | Docker image or local Spark is not Spark 3.5.x. | Use `apache/spark:3.5.6` or a Spark 3.5 local install. |
| Kafka version check fails | The side-topic runner is not using Kafka 3.x. | Use the default `apache/kafka:3.7.0` or another Kafka 3.x image. |
| Permission errors under `.recon-local` | Docker Spark user cannot write mounted evidence. | Run `mkdir -p .recon-local && chmod 0777 .recon-local`. |
| Scenario exits differ | Behavior regression or stale fixture data. | Remove `.recon-local`, rebuild the jar, and rerun the matrix. |
| Docker cannot find the jar | `CHECKER_JAR` uses a host path inside the container. | Use `/workspace/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar`. |
| Spark package resolution fails in containers | Ivy cache is not writable. | Set `SPARK_JARS_IVY=/tmp/recon-ivy`. |
