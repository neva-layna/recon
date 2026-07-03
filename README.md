# Reconciliation Kafka Offset Gap Checker

This workspace contains a Spark checker for Kafka offset continuity in parquet
data. The production path is the Gradle Java application submitted with
`spark-submit`; the original Scala `spark-shell -i` script remains as a behavior
oracle and compatibility reference.

Canary/heartbeat records and broken source messages can be redirected away from
the normal HDFS parquet sink into Kafka side topics. When parquet offsets are
missing, operators should reconcile those missing offsets against the configured
canary and dead-letter topics before deciding whether data is truly lost. That
side-topic reconciliation is a Java `spark-submit` feature; do not run it through
the Scala checker.

Compatibility is intentionally narrow: Spark 3.5.x only, Spark artifacts built
for Scala 2.12, Kafka 3.x for side-topic brokers, and Java 8-compatible checker
bytecode. Product deliverables live at workspace-root paths such as
`src/main/java/com/reconciliation/kafka/`,
`scripts/run_java_kafka_offset_gap_check_prod.sh`,
`scripts/run_java_kafka_side_topic_fixture_checks.sh`,
`tests/fixtures/generate_kafka_side_topic_records.scala`, and `docs/`.

## Documentation

| Document | Purpose |
| --- | --- |
| [BUILD.md](docs/BUILD.md) | Build requirements, Gradle commands, Java 8 bytecode checks. |
| [TESTING.md](docs/TESTING.md) | Local Docker validation and fixture matrix. |
| [ARCH.md](docs/ARCH.md) | Runtime architecture, data flow, gap algorithm. |
| [OPERATIONS.md](docs/OPERATIONS.md) | Production wrapper usage, output, exit codes, troubleshooting. |
| [docs/check_kafka_offset_gaps.md](docs/check_kafka_offset_gaps.md) | Full checker reference and legacy Scala-script details. |

## Quick Build

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

## Quick Local Validation

```bash
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
  -lc 'scripts/run_java_kafka_offset_gap_fixture_checks.sh'
```

## Quick Spark+Kafka Docker Side-Topic Validation

Build the jar, then run the full side-topic matrix with Spark 3.5.x and Kafka
3.x in Docker:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

rtk scripts/run_java_kafka_side_topic_docker_checks.sh
```

The wrapper starts `apache/kafka:3.7.0`, creates the canary/dead-letter test
topics, writes Avro side-topic records, and runs the Java checker through
`apache/spark:3.5.6` `spark-submit`. Evidence is written under
`.recon-local-side/evidence/run/`.

## Quick Production-Style Run

```bash
rtk env \
  RUN_DATE=2026-07-02 \
  scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2
```

## Quick Side-Topic Run

Use the Java production wrapper with Kafka 3.x bootstrap servers:

```bash
rtk env \
  SOURCE_TOPIC=orders \
  KAFKA_BOOTSTRAP_SERVERS='broker-a:9092,broker-b:9092' \
  CANARY_TOPIC=orders-canary \
  DEAD_LETTER_TOPIC=orders-dlq \
  SIDE_TOPIC_STARTING_OFFSETS=earliest \
  RUN_DATE=2026-07-02 \
  scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/orders/root-a \
  hdfs:///data/orders/root-b
```

The wrapper submits `build/libs/recon-kafka-offset-gap-checker-1.0.0.jar` with
`spark-submit` and forwards side-topic values as `spark.recon.*` configs. With
`FAIL_ON_GAPS=true`, non-truncated missing offsets fully explained by canary
and/or dead-letter records exit `0`; unresolved offsets or truncated
missing-offset materialization exit `1`. Incomplete side-topic config,
unreadable Kafka side topics, and undecodable Avro payloads fail closed with
exit code `2`.
