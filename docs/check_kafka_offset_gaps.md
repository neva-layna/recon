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

`scripts/check/check_kafka_offset_gaps.scala` is a standalone Spark 3.5.x
`spark-shell -i` Scala script for checking Kafka offset continuity in parquet
data. It is intended for one Kafka topic persisted across one or more
HDFS/parquet roots, where each root has immediate Hive-style date partition
directories such as `timestampcolumn=2026-07-01`.

The workspace also contains a Gradle Java port for `spark-submit` deployments:
`src/main/java/com/reconciliation/kafka/KafkaOffsetGapChecker.java`. The Java
app is compiled for Java 8 bytecode and must run with Spark 3.5.x artifacts
built for Scala 2.12, such as `org.apache.spark:spark-sql_2.12:3.5.6`.
The optional canary/dead-letter side-topic feature is Java `spark-submit` only
and requires Kafka 3.x brokers or fixtures.

The checker scans only the immediate children of each configured root. It uses
eligible old date partitions from all roots together before computing gaps, so a
continuous offset range can be split across roots. The configured run date is
skipped because the current-day write may still be incomplete.

## Configuration

The Java `spark-submit` checker is YAML-first. It is a Spring Boot 2.7.18
application that binds `application.yml` values under the `recon` prefix and
reports through SLF4J with Spring Boot's default Logback backend.

```yaml
recon:
  input-roots:
    - hdfs:///warehouse/topic/root-a
    - hdfs:///warehouse/topic/root-b
  metadata-column: cactus__metadata
  date-partition-column: timestampcolumn
  run-date: "2026-07-02"
  fail-on-gaps: true
```

The existing Spark conf surface remains supported for both Java and Scala.
For Java, `recon.*` and `spark.recon.*` Spark conf values override the same
YAML setting. The Scala `spark-shell -i` checker is separate: it uses Spark conf
only, is not a Spring Boot app, and does not read `application.yml`.

| Key | Default | Meaning |
| --- | --- | --- |
| `recon.inputRoots` | required | Comma-separated parquet roots to scan. |
| `recon.metadataColumn` | `cactus__metadata` | String column containing metadata JSON with Kafka `partition` and `offset`. |
| `recon.datePartitionColumn` | `timestampcolumn` | Hive partition directory prefix. |
| `recon.runDate` | driver current date | Optional `yyyy-MM-dd` override for the partition date to skip. |
| `recon.normalizedOffsetsPath` | none | Optional parquet path where normalized offsets are written and read back before analytics. |
| `recon.normalizedOffsetsOverwrite` | `true` | Overwrite the normalized-offset path when persistence is enabled. |
| `recon.failOnInvalidRows` | `true` | Exit non-zero when malformed or incomplete metadata rows are present. |
| `recon.failOnGaps` | `true` | Exit non-zero for raw gaps without side topics, or for unresolved bounded offsets after Java side-topic reconciliation. |
| `recon.missingOffsetsLimit` | `1000` | Per-partition safety limit for materializing actual missing offset values in output. Set `0` to report only counts and mark gapped partitions as truncated. |
| `recon.exitOnCompletion` | `true` | Call `System.exit` so automation does not remain in the interactive shell. |

Java side-topic reconciliation adds these configs. In YAML, keep side-topic
topic settings in `application.yml`, select broker settings with
`recon.kafka-alias`, and import a separate `kafka-brokers.yml`:

```yaml
spring:
  config:
    import: "file:./kafka-brokers.yml"

recon:
  source-topic: orders
  kafka-alias: main-kafka
  canary-topic: orders-canary
  dead-letter-topic: orders-dlq
  side-topic-starting-offsets: earliest
```

```yaml
kafka-configs:
  broker:
    main-kafka:
      conf:
        "[bootstrap.servers]": broker-a:9092,broker-b:9092
        "[security.protocol]": PLAINTEXT
        "[max.poll.records]": 500
```

Each `recon.*` Spark conf spelling also has the corresponding `spark.recon.*`
alias and overrides YAML. Direct bootstrap servers are retained only as a
legacy Spark-conf/wrapper override, not as the YAML-side side-topic key.

| Key | Alias keys | Default | Meaning |
| --- | --- | --- | --- |
| `recon.sourceTopic` | `recon.sideTopic.sourceTopic` | required when side-topic config is present | Source Kafka topic identity used to match side-topic records. |
| `recon.kafkaAlias` | `recon.kafka.alias` | required for YAML side-topic config | Broker alias selected from `kafka-configs.broker.<alias>.conf`. |
| `recon.kafkaBootstrapServers` | `recon.kafka.bootstrap.servers`, `recon.sideTopic.kafkaBootstrapServers` | legacy Spark-conf override only | Kafka 3.x bootstrap servers forwarded by wrappers or direct `--conf`; do not use this in YAML for the dynamic alias path. |
| `recon.canaryTopic` | `recon.sideTopic.canaryTopic` | none | Optional canary/heartbeat topic containing Avro object-container payloads. |
| `recon.deadLetterTopic` | `recon.deadletterTopic`, `recon.sideTopic.deadLetterTopic` | none | Optional dead-letter topic containing Avro object-container payloads. |
| `recon.sideTopicStartingOffsets` | `recon.sideTopic.startingOffsets`, `recon.sideTopicReadBehavior` | `earliest` | Side-topic read start. `earliest` and `beginning` are accepted and both read from the beginning. |

When any side-topic config is present, `recon.sourceTopic`,
`recon.kafkaAlias` or a legacy Spark-conf bootstrap override, and at least one
of `recon.canaryTopic` or `recon.deadLetterTopic` are required. Partial
side-topic config fails closed with exit code `2`.

The fixture generator uses these Spark conf keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `recon.fixtureOutputRoot` | `/tmp/recon-kafka-offset-fixtures` | Local root where generated parquet fixtures are written. |
| `recon.fixtureMetadataColumn` | `cactus__metadata` | Fixture metadata column name. |
| `recon.fixtureDatePartitionColumn` | `timestampcolumn` | Fixture date partition column name. |
| `recon.fixtureOldDate` | `2026-07-01` | Eligible old partition date used in fixtures. |
| `recon.fixtureRunDate` | `2026-07-02` | Run-date partition used for skip fixtures. |

## Production Example

For the Scala parquet-gap checker, use Spark 3.5.x and pass behavior through
Spark conf:

```bash
rtk spark-shell \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.recon.inputRoots=hdfs:///warehouse/topic/root-a,hdfs:///warehouse/topic/root-b' \
  --conf 'spark.recon.metadataColumn=cactus__metadata' \
  --conf 'spark.recon.datePartitionColumn=timestampcolumn' \
  --conf 'spark.recon.runDate=2026-07-02' \
  -i scripts/check/check_kafka_offset_gaps.scala
```

With persisted normalized offsets:

```bash
rtk spark-shell \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.recon.inputRoots=hdfs:///warehouse/topic/root-a,hdfs:///warehouse/topic/root-b' \
  --conf 'spark.recon.normalizedOffsetsPath=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02' \
  --conf 'spark.recon.normalizedOffsetsOverwrite=true' \
  --conf 'spark.recon.runDate=2026-07-02' \
  -i scripts/check/check_kafka_offset_gaps.scala
```

If a launcher rejects non-`spark.*` keys, use the alias form, for example
`--conf 'spark.recon.inputRoots=hdfs:///warehouse/topic/root-a'`.

### Production Wrapper

Use `scripts/check/run_kafka_offset_gap_check_prod.sh` from a Hadoop edge node when
you want a repeatable production command instead of a long raw `spark-shell`
invocation. The wrapper still runs the same Scala checker through Spark 3.5
`spark-shell -i`; it only assembles the Spark conf values and passes your input
roots as `spark.recon.*` aliases.

Pass every root as a separate argument:

```bash
RUN_DATE=2026-07-02 \
NORMALIZED_OFFSETS_PATH=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02 \
scripts/check/run_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2 \
  hdfs:///data/path/to/parquet3
```

You can also provide roots as a comma-separated value:

```bash
INPUT_ROOTS_CSV='hdfs:///data/path/to/parquet1,hdfs:///data/path/to/parquet2' \
scripts/check/run_kafka_offset_gap_check_prod.sh
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
and exit code classes as the Scala `spark-shell -i` oracle script, but its
preferred operator config is Spring Boot `application.yml`.

Build the jar:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

Run with the Java production wrapper:

```bash
rtk env \
  APPLICATION_YML=/etc/recon/application.yml \
  scripts/check/run_java_kafka_offset_gap_check_prod.sh
```

In that YAML-first wrapper flow, no positional roots are required because the
checker reads `recon.input-roots` from `application.yml`.

Run with wrapper Spark-conf overrides:

```bash
rtk env \
  RUN_DATE=2026-07-02 \
  NORMALIZED_OFFSETS_PATH=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02 \
  scripts/check/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2
```

The wrapper uses `spark-submit --class
com.reconciliation.kafka.KafkaOffsetGapChecker` and the built jar at
`build/libs/recon-kafka-offset-gap-checker-1.0.0.jar` by default. Set
`CHECKER_JAR` when deploying a copied artifact. Positional roots or
`INPUT_ROOTS_CSV` make the wrapper forward base values as `spark.recon.*`,
which override matching YAML settings.

Java wrapper variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPARK_SUBMIT_BIN` | `spark-submit` | Spark 3.5 submit executable on the edge node. |
| `SPARK_MASTER` | `yarn` | Spark master passed to `spark-submit --master`. |
| `CHECKER_JAR` | `build/libs/recon-kafka-offset-gap-checker-1.0.0.jar` | Built Java artifact to submit. |
| `CHECKER_CLASS` | `com.reconciliation.kafka.KafkaOffsetGapChecker` | Java main class. |
| `SPARK_SQL_TIMEZONE` | `UTC` | Value for `spark.sql.session.timeZone`. |
| `APPLICATION_YML` | none | Local YAML file path converted to `SPRING_CONFIG_LOCATION=file:<path>`. |
| `SPRING_CONFIG_LOCATION` | none | Direct Spring Boot config location for YAML-first runs. |
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
| `SOURCE_TOPIC` | none | Source Kafka topic identity used to match side-topic records. |
| `KAFKA_BOOTSTRAP_SERVERS` | none | Kafka 3.x bootstrap servers for configured side-topic reads. |
| `CANARY_TOPIC` | none | Optional canary/heartbeat side topic. |
| `DEAD_LETTER_TOPIC` | none | Optional dead-letter side topic. |
| `SIDE_TOPIC_STARTING_OFFSETS` | `earliest` | Side-topic read start; accepts `earliest` or `beginning`. |
| `SPARK_PACKAGES` | side-topic default only | Optional override for `spark-submit --packages`. |
| `SPARK_JARS_IVY` | none | Optional writable Ivy cache forwarded as `spark.jars.ivy`. |
| `ENABLE_SIDE_TOPIC_PACKAGES` | `false` | Add default Spark Kafka/Avro packages when side-topic config is YAML-only. |

Direct YAML-first Java `spark-submit` uses Spring Boot config environment:

```bash
rtk env \
  SPRING_CONFIG_LOCATION=file:/etc/recon/application.yml \
  spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

Direct Spark-conf override Java `spark-submit` uses explicit `spark.recon.*`
keys:

```bash
rtk spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.recon.inputRoots=hdfs:///data/path/to/parquet1,hdfs:///data/path/to/parquet2' \
  --conf 'spark.recon.runDate=2026-07-02' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

### Java Side-Topic Reconciliation

Canary/heartbeat messages and broken source messages can be redirected away from
HDFS parquet into Kafka side topics. For those pipelines, a missing parquet
offset should be checked against canary and dead-letter topics before it is
treated as unresolved data loss.

Use the Java production wrapper for YAML-first side-topic reconciliation:

```bash
rtk env \
  APPLICATION_YML=/etc/recon/orders-side-topic.yml \
  ENABLE_SIDE_TOPIC_PACKAGES=true \
  scripts/check/run_java_kafka_offset_gap_check_prod.sh
```

The YAML must include `spring.config.import: "file:./kafka-brokers.yml"`,
`recon.kafka-alias`, `source-topic`, and at least one of `canary-topic` or
`dead-letter-topic`. The imported broker file must define
`kafka-configs.broker.<alias>.conf` with a usable `[bootstrap.servers]` entry.

Use wrapper Spark-conf overrides for side-topic reconciliation:

```bash
rtk env \
  SOURCE_TOPIC=orders \
  KAFKA_BOOTSTRAP_SERVERS='broker-a:9092,broker-b:9092' \
  CANARY_TOPIC=orders-canary \
  DEAD_LETTER_TOPIC=orders-dlq \
  SIDE_TOPIC_STARTING_OFFSETS=earliest \
  RUN_DATE=2026-07-02 \
  scripts/check/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/orders/root-a \
  hdfs:///data/orders/root-b
```

The wrapper submits the Java class with `spark-submit`, forwards values as
`spark.recon.*`, and adds these Spark 3.5 runtime packages unless
`SPARK_PACKAGES` is set:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4
```

Direct YAML-first Java `spark-submit` uses the same YAML and side-topic package
requirements:

```bash
rtk env \
  SPRING_CONFIG_LOCATION=file:/etc/recon/orders-side-topic.yml \
  spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4' \
  --conf 'spark.sql.session.timeZone=UTC' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

Direct Java `spark-submit` with legacy Spark-conf bootstrap overrides uses the
same configs:

```bash
rtk spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4' \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.recon.inputRoots=hdfs:///data/orders/root-a,hdfs:///data/orders/root-b' \
  --conf 'spark.recon.runDate=2026-07-02' \
  --conf 'spark.recon.sourceTopic=orders' \
  --conf 'spark.recon.kafkaBootstrapServers=broker-a:9092,broker-b:9092' \
  --conf 'spark.recon.canaryTopic=orders-canary' \
  --conf 'spark.recon.deadLetterTopic=orders-dlq' \
  --conf 'spark.recon.sideTopicStartingOffsets=earliest' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

The Scala `spark-shell -i` checker does not read Kafka side topics. Use
`spark-shell` only for the provided fixture generators when testing this
feature.

Production notes:

- `RUN_DATE` is the date directory to skip, not necessarily the data event date.
  For daily runs, set it to the current ingestion date.
- Keep `NORMALIZED_OFFSETS_PATH` unique per run if multiple checks can execute
  concurrently.
- The checker reads only immediate children named
  `<DATE_PARTITION_COLUMN>=yyyy-MM-dd` below each root.
- All roots are unioned before gap analytics; do not run each root separately if
  one Kafka topic can be split across roots.
- Side-topic matching compares Avro fields `sourceTopic`, `sourcePartition`,
  and `sourceOffset` against materialized missing parquet offsets.
- `MISSING_OFFSETS_LIMIT` bounds the missing offsets available for side-topic
  classification. When it truncates the gap list, side-topic output reports
  `missing_offsets_truncated=true`.

### Hadoop Sample Data

Use `scripts/sample/generate_hadoop_offset_gap_sample_data.sh` when you want a small
HDFS smoke dataset before running against production paths. It runs
`tests/fixtures/generate_kafka_offset_gap_sample_data.scala` with Spark 3.5 and writes
only two scenarios, not the full local fixture matrix.

Generate the sample data:

```bash
scripts/sample/generate_hadoop_offset_gap_sample_data.sh hdfs:///tmp/recon-offset-samples
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
scripts/sample/run_hadoop_offset_gap_sample_checks.sh hdfs:///tmp/recon-offset-samples
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
| `scripts/check/check_kafka_offset_gaps.scala` | Main Spark 3.5 checker run with `spark-shell -i`. |
| `scripts/check/run_kafka_offset_gap_check_prod.sh` | Production wrapper for checking real HDFS/parquet roots. |
| `src/main/java/com/reconciliation/kafka/KafkaOffsetGapChecker.java` | Java Spark SQL/DataFrame port for `spark-submit`. |
| `scripts/check/run_java_kafka_offset_gap_check_prod.sh` | Production wrapper for the Java `spark-submit` checker. |
| `scripts/validation/run_java_kafka_side_topic_fixture_checks.sh` | Local Kafka 3.x side-topic validation runner for the Java checker. |
| `tests/fixtures/generate_kafka_offset_gap_sample_data.scala` | Small Spark generator for the two Hadoop sample scenarios. |
| `scripts/sample/generate_hadoop_offset_gap_sample_data.sh` | Wrapper around the small sample generator. |
| `scripts/sample/run_hadoop_offset_gap_sample_checks.sh` | Wrapper that verifies the two generated sample scenarios. |
| `tests/fixtures/generate_kafka_offset_gap_fixtures.scala` | Full validation fixture generator used by the local test runner. |
| `tests/fixtures/generate_kafka_side_topic_records.scala` | Spark fixture producer for side-topic Avro object-container records. |
| `scripts/validation/run_kafka_offset_gap_fixture_checks.sh` | Full Docker/local validation runner covering all edge cases. |
| `scripts/validation/run_java_kafka_offset_gap_fixture_checks.sh` | Full Docker/local validation runner for the Java `spark-submit` port. |

## Local Docker Validation

The local validation path uses Spark 3.5.x in Docker, not host-installed Spark.
It writes local fixtures and evidence under `.recon-local/`. Local persistence
validation uses `file://` for the normalized-offset cache path. Production can
use HDFS-style paths such as `hdfs:///tmp/...`; the local command does not
validate a full HDFS cluster.

Build the Java checker first:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

Run the Java `spark-submit` fixture matrix:

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
  -lc 'scripts/validation/run_java_kafka_offset_gap_fixture_checks.sh'
```

The Java helper uses `spark-shell` only to generate deterministic parquet
fixtures from `tests/fixtures/generate_kafka_offset_gap_fixtures.scala`. Every
checker scenario is then run through `spark-submit --class
com.reconciliation.kafka.KafkaOffsetGapChecker` and the built Java jar, not
through `spark-shell -i scripts/check/check_kafka_offset_gaps.scala`.

Run the full Java side-topic matrix with Spark 3.5.x and Kafka 3.x in Docker:

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

rtk scripts/validation/run_java_kafka_side_topic_docker_checks.sh
```

The Docker wrapper starts `apache/kafka:3.7.0`, creates the `orders-canary`,
`orders-dlq`, `orders-dlq-only`, `orders-empty-canary`, `orders-empty-dlq`,
and `orders-bad-canary` topics, then runs the
side-topic matrix in `apache/spark:3.5.6`. The Spark-side runner uses
`tests/fixtures/generate_kafka_side_topic_records.scala` only to create Avro
object-container side-topic messages and runs all checker scenarios through
Java `spark-submit`. Evidence is written under `.recon-local-side/evidence/run/`,
including `scenario_results.tsv`, `assertion_results.tsv`,
`kafka/kafka_image.txt`, and `kafka/side_topic_records.tsv`.

The side-topic matrix includes these Java-only exit-semantics scenarios, all
with `recon.failOnGaps=true`:

| Scenario | Expected exit | Meaning |
| --- | ---: | --- |
| `canary_empty_dead_letter_resolved` | 0 | Canary explains all bounded missing offsets; dead-letter is configured but empty. |
| `canary_empty_dead_letter_unresolved` | 1 | Canary explains one bounded offset and another remains unresolved. |
| `empty_canary_dead_letter_resolved` | 0 | Dead-letter explains all bounded missing offsets; canary is configured but empty. |
| `empty_canary_dead_letter_unresolved` | 1 | Dead-letter explains one bounded offset and another remains unresolved. |
| `canary_dead_letter_resolved` | 0 | Canary and dead-letter together explain all bounded missing offsets. |
| `canary_dead_letter_truncated_prefix_only` | 1 | Real missing offsets exceed `recon.missingOffsetsLimit`; side topics explain only the materialized prefix. |
| `canary_dead_letter_unresolved` | 1 | Canary and dead-letter leave one bounded offset unresolved. |

To run the Java helper with already available Spark 3.5.x binaries and only
Kafka in Docker:

```bash
rtk env \
  SPARK_SHELL_BIN=spark-shell \
  SPARK_SUBMIT_BIN=spark-submit \
  CHECKER_JAR=build/libs/recon-kafka-offset-gap-checker-1.0.0.jar \
  FIXTURE_ROOT=/tmp/recon-kafka-offset-fixtures-java \
  EVIDENCE_ROOT=/tmp/recon-kafka-offset-evidence-java \
  RUN_DATE=2026-07-02 \
  scripts/validation/run_java_kafka_offset_gap_fixture_checks.sh
```

The original Scala fixture runner remains useful as an oracle check for
`scripts/check/check_kafka_offset_gaps.scala`:

```bash
rtk mkdir -p .recon-local
rtk chmod 0777 .recon-local

rtk docker run --rm \
  --entrypoint /bin/bash \
  -e SPARK_SHELL_BIN=/opt/spark/bin/spark-shell \
  -e FIXTURE_ROOT=/recon-local/fixtures \
  -e EVIDENCE_ROOT=/recon-local/evidence \
  -e RUN_DATE=2026-07-02 \
  -v "$PWD":/workspace \
  -v "$PWD/.recon-local":/recon-local \
  -w /workspace \
  apache/spark:3.5.6 \
  -lc 'scripts/validation/run_kafka_offset_gap_fixture_checks.sh'
```

The helper first runs `spark-shell --version` and requires Spark 3.5.x evidence.
It then runs `tests/fixtures/generate_kafka_offset_gap_fixtures.scala` and the
checker for each scenario.

To run the same helper with an already available Spark 3.5.x shell:

```bash
rtk env \
  SPARK_SHELL_BIN=spark-shell \
  FIXTURE_ROOT=/tmp/recon-kafka-offset-fixtures \
  EVIDENCE_ROOT=/tmp/recon-kafka-offset-evidence \
  RUN_DATE=2026-07-02 \
  scripts/validation/run_kafka_offset_gap_fixture_checks.sh
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

Side-topic output appears only for Java runs with side-topic config enabled. It
includes `side_topic_reconciliation_begin/end`, one `side_topic_read` line per
configured side topic, `side_topic_bucket=canary_explained`,
`side_topic_bucket=dead_letter_explained`, `side_topic_bucket=unresolved`,
`side_topic_dead_letter_fields`, `side_topic_summary`, and
`final_exit_decision`. Explained buckets show missing parquet offsets found in
the configured side topics; unresolved buckets show materialized missing
parquet offsets not found there. `side_topic_summary` includes raw gap
partition count, bounded missing-offset count, decoded record counts,
explained counts, unresolved count, and truncation state.

For a gapped partition, `missing_offsets` contains the actual missing offset
values in ascending order, for example `missing_offsets=[1,4]`. The list is
attributable to the same line's `partition` or `gap_partition` value. If the
true missing value count exceeds `recon.missingOffsetsLimit`, the checker prints
only the first `missing_offsets_limit` values and sets
`missing_offsets_truncated=true`; operators must treat that list as incomplete.
When `missing_offsets_truncated=false`, the list is complete for that partition.

Exit code `0` means the check completed and no configured failure condition was
found. For Java side-topic runs, raw parquet gaps can still exit `0` when all
materialized missing offsets are explained by canary and/or dead-letter records
and `missing_offsets_truncated=false`.
Exit code `1` means the checker read valid offset data but failed because raw
gaps without side-topic reconciliation, unresolved side-topic offsets,
truncated missing-offset materialization, or invalid metadata rows were
detected under the default failure settings.
Exit code `2` means configuration or input data prevented a meaningful check,
including missing input roots, invalid boolean or run-date configuration,
unreadable parquet, no eligible old partitions, empty readable parquet, missing
metadata column, failed cache read/write, or zero valid normalized offsets.
With side-topic config enabled, exit code `2` also covers incomplete side-topic
config, unreachable/unreadable Kafka side topics, missing Kafka datasource
packages, missing Avro runtime, and undecodable Avro payloads.
