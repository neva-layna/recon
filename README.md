# Reconciliation Kafka Offset Gap Checker

This workspace contains a Spark checker for Kafka offset continuity in parquet
data. The production path is the Gradle Java application submitted with
`spark-submit`; the original Scala `spark-shell -i` script remains as a behavior
oracle and compatibility reference.

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
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

## Quick Local Validation

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

## Quick Production-Style Run

```bash
RUN_DATE=2026-07-02 \
scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2
```
