# Build

This project contains a Gradle Java port of the Kafka offset gap checker.
The production artifact is a jar submitted with Spark 3.5.x.

## Requirements

- Java source and bytecode target: Java 8.
- Spark runtime: Spark 3.5.x only.
- Spark dependency ABI: Scala 2.12 only.
- Side-topic broker/runtime validation: Kafka 3.x only.
- Build tool: Gradle.

Use only Spark 3.5.x artifacts built for Scala 2.12 and Kafka 3.x side-topic
brokers or fixtures.

## Project Layout

| Path | Purpose |
| --- | --- |
| `build.gradle` | Gradle Java/application build. |
| `settings.gradle` | Gradle project name. |
| `src/main/java/com/reconciliation/kafka/KafkaOffsetGapChecker.java` | Java Spark checker public entrypoint. |
| `src/main/java/com/reconciliation/kafka/config/` | Configuration loading and Spark conf lookup. |
| `src/main/java/com/reconciliation/kafka/scan/` | Partition discovery. |
| `src/main/java/com/reconciliation/kafka/metadata/` | Parquet read, metadata parsing, normalized-offset persistence. |
| `src/main/java/com/reconciliation/kafka/analytics/` | Offset gap analytics and missing-offset reports. |
| `src/main/java/com/reconciliation/kafka/sidetopic/` | Optional Kafka side-topic Avro decode, matching, and reporting. |
| `src/main/java/com/reconciliation/kafka/model/` | Data carrier classes. |
| `src/main/java/com/reconciliation/kafka/support/` | Shared reporting, constants, exits, and row helpers. |
| `src/test/java/com/reconciliation/kafka/KafkaOffsetGapCheckerTest.java` | Unit tests for config parsing and small helpers. |
| `scripts/run_java_kafka_offset_gap_check_prod.sh` | Production `spark-submit` wrapper. |
| `scripts/run_java_kafka_offset_gap_fixture_checks.sh` | Local Java fixture validation runner. |
| `scripts/run_java_kafka_side_topic_fixture_checks.sh` | Local Kafka 3.x side-topic validation runner for the Java checker. |
| `tests/fixtures/generate_kafka_offset_gap_fixtures.scala` | Spark fixture generator used by local validation. |
| `tests/fixtures/generate_kafka_side_topic_records.scala` | Spark fixture producer for deterministic Avro object-container side-topic messages. |

These are workspace-root product paths. They are not nested under the Zenith
harness package.

## Build The Jar

Use a stable Gradle cache location if you want repeatable local and Docker
validation:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

The jar is written to:

```text
build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

The jar manifest sets:

```text
Main-Class: com.reconciliation.kafka.KafkaOffsetGapChecker
```

The production wrapper still passes the main class explicitly to
`spark-submit`.

## Run Unit Tests

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew test
```

To run with the local Java 8 installation used during validation:

```bash
JAVA_HOME=/Users/nlayna/Library/Java/JavaVirtualMachines/liberica-full-1.8.0_492 \
GRADLE_USER_HOME=/tmp/recon-gradle \
./gradlew --no-daemon test
```

## Verify Java 8 Bytecode

After building, application classes should have classfile major version `52`.
One direct check is:

```bash
javap -verbose build/classes/java/main/com/reconciliation/kafka/KafkaOffsetGapChecker.class \
  | grep 'major version'
```

Expected:

```text
major version: 52
```

## Dependency Constraints

The Spark dependency is intentionally `compileOnly` for production because
Spark supplies its own jars at runtime:

```groovy
compileOnly 'org.apache.spark:spark-sql_2.12:3.5.6'
implementation 'org.apache.avro:avro:1.11.4'
testImplementation 'org.apache.spark:spark-sql_2.12:3.5.6'
```

The Java code reads Kafka through Spark's `format("kafka")` datasource at
runtime. Side-topic runs must make the Spark 3.5 Kafka connector available to
`spark-submit`; the production wrapper adds:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4
```

The wrapper also accepts `SPARK_JARS_IVY` to forward a writable
`spark.jars.ivy` cache path for package resolution in containers or restricted
edge nodes.

Inspect the compile classpath with:

```bash
GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew dependencies --configuration compileClasspath
```

The dependency tree must contain Spark 3.5.x and Scala 2.12 artifacts only for
Spark dependencies. Side-topic runtime packages must stay on Spark 3.5.x,
Scala 2.12, and Kafka 3.x-compatible connector/client artifacts.
The local side-topic runner starts `apache/kafka:3.7.0` by default and rejects
non-3.x Kafka image tags.

## Clean Build Outputs

```bash
./gradlew clean
```

Local Spark fixture evidence is normally written under `.recon-local/`; remove it
when you want a fresh validation run:

```bash
rm -rf .recon-local
```
