# Operations

Use the Java `spark-submit` wrapper for production-style runs. The Java checker
is a Spring Boot 2.7.18 application: prefer `application.yml` under the `recon`
prefix for operator config, and use Spark conf only when you need launch-time
overrides or legacy compatibility.

Use side-topic reconciliation when the source pipeline can redirect records away
from the HDFS parquet sink. Canary/heartbeat messages and broken source
messages commonly land in Kafka canary or dead-letter topics instead of parquet;
the Java checker can compare missing parquet offsets with those side topics so
operators can distinguish explained gaps from unresolved gaps.

Side-topic reconciliation is only documented for the Java `spark-submit`
checker. Do not use the Scala `spark-shell -i` checker for this feature.

## Build Before Running

```bash
rtk env GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar
```

The default wrapper artifact is:

```text
build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

## Run YAML-First

Create an `application.yml` with base parquet-gap settings:

```yaml
recon:
  input-roots:
    - hdfs:///data/path/to/parquet1
    - hdfs:///data/path/to/parquet2
  metadata-column: cactus__metadata
  date-partition-column: timestampcolumn
  run-date: "2026-07-02"
  normalized-offsets-path: hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02
  normalized-offsets-overwrite: true
  fail-on-invalid-rows: true
  fail-on-gaps: true
  missing-offsets-limit: 1000
  exit-on-completion: true
```

Submit it through the wrapper without positional roots:

```bash
rtk env \
  APPLICATION_YML=/etc/recon/application.yml \
  scripts/run_java_kafka_offset_gap_check_prod.sh
```

`APPLICATION_YML` is converted to `SPRING_CONFIG_LOCATION=file:<path>` before
`spark-submit`. You can also set `SPRING_CONFIG_LOCATION` directly when using a
standard Spring Boot config location.

For side-topic reconciliation in YAML, import a broker alias file and keep
side-topic topic settings in the same `recon` block:

```yaml
spring:
  config:
    import: "file:./kafka-brokers.yml"

recon:
  input-roots:
    - hdfs:///data/orders/root-a
    - hdfs:///data/orders/root-b
  run-date: "2026-07-02"
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

When side-topic settings live only in YAML, ask the wrapper to add the Spark
Kafka and Avro runtime packages unless your cluster preloads them:

```bash
rtk env \
  APPLICATION_YML=/etc/recon/orders-side-topic.yml \
  ENABLE_SIDE_TOPIC_PACKAGES=true \
  scripts/run_java_kafka_offset_gap_check_prod.sh
```

## Run With Spark-Conf Overrides

Positional roots or `INPUT_ROOTS_CSV` make the wrapper forward checker values as
`spark.recon.*` Spark conf keys. These keys override the same values from
`application.yml`.

```bash
rtk env \
  RUN_DATE=2026-07-02 \
  NORMALIZED_OFFSETS_PATH=hdfs:///tmp/recon/topic-normalized-offsets/run_date=2026-07-02 \
  scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/path/to/parquet1 \
  hdfs:///data/path/to/parquet2 \
  hdfs:///data/path/to/parquet3
```

## Run With A Comma-Separated Root List

```bash
rtk env \
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
| `APPLICATION_YML` | none | Local YAML file path converted to `SPRING_CONFIG_LOCATION=file:<path>`. |
| `SPRING_CONFIG_LOCATION` | none | Direct Spring Boot config location for YAML-first runs. |
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
| `SOURCE_TOPIC` | none | Source Kafka topic identity used to match side-topic records. |
| `KAFKA_BOOTSTRAP_SERVERS` | none | Legacy Spark-conf override for Kafka bootstrap servers when side-topic settings are supplied through wrapper environment variables. YAML-first runs should use `recon.kafka-alias` and `kafka-brokers.yml`. |
| `CANARY_TOPIC` | none | Optional Kafka canary topic containing Avro object-container payloads. |
| `DEAD_LETTER_TOPIC` | none | Optional Kafka dead-letter topic containing Avro object-container payloads. |
| `SIDE_TOPIC_STARTING_OFFSETS` | `earliest` | Side-topic read start; `earliest` and `beginning` are accepted. |
| `SPARK_PACKAGES` | side-topic default only | Optional override for `spark-submit --packages`. |
| `SPARK_JARS_IVY` | none | Optional writable Ivy cache path forwarded as `spark.jars.ivy`. |
| `ENABLE_SIDE_TOPIC_PACKAGES` | `false` | Add default Spark Kafka/Avro packages when side-topic config is YAML-only. |

Without positional roots or `INPUT_ROOTS_CSV`, checker values come from
`application.yml`. With positional roots or `INPUT_ROOTS_CSV`, the wrapper
forwards base checker values as `spark.recon.*` Spark conf keys.

When any side-topic variable is set, `SOURCE_TOPIC`,
`KAFKA_BOOTSTRAP_SERVERS`, and at least one of `CANARY_TOPIC` or
`DEAD_LETTER_TOPIC` are required for the legacy Spark-conf override path. The
wrapper adds the Spark 3.5 Kafka source and Avro packages by default:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4
```

Set `SPARK_PACKAGES` when your cluster preloads or mirrors these artifacts.
The Spark Kafka connector resolves Kafka 3.x-compatible client libraries for
Spark 3.5.x.
Set `SPARK_JARS_IVY` when the Spark runtime user cannot write its default Ivy
cache, for example in locked-down containers.

## application.yml Configuration

The checker binds Spring Boot YAML from the `recon` prefix as the preferred
operator config source. A commented sample is packaged at
`src/main/resources/application.yml`.

```yaml
spring:
  config:
    import: "file:./kafka-brokers.yml"

recon:
  input-roots:
    - hdfs:///warehouse/orders/root-a
    - hdfs:///warehouse/orders/root-b
  metadata-column: cactus__metadata
  date-partition-column: timestampcolumn
  run-date: "2026-07-02"
  normalized-offsets-path: hdfs:///tmp/recon-normalized-orders
  normalized-offsets-overwrite: true
  fail-on-invalid-rows: true
  fail-on-gaps: true
  missing-offsets-limit: 1000
  exit-on-completion: true
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

Spring relaxed binding also accepts camelCase names such as `inputRoots` and
`sideTopicStartingOffsets`. Quote `run-date` so YAML passes a string in
`yyyy-MM-dd` form.

Spark conf remains the override layer. Existing `recon.*` and
`spark.recon.*` keys, including wrapper-forwarded values, take precedence over
the same setting from `application.yml`. The alias override spellings are
`recon.kafkaAlias`, `spark.recon.kafkaAlias`, `recon.kafka.alias`, and
`spark.recon.kafka.alias`. Legacy wrapper/direct-bootstrap overrides still use
`spark.recon.kafkaBootstrapServers`. Side-topic `beginning` is normalized to
`earliest`; unsupported starting offsets fail with exit code `2`.

For direct `spark-submit` YAML runs, make the YAML visible to Spring Boot with
your cluster's standard mechanism, for example `SPRING_CONFIG_LOCATION` in
client mode, or a colocated `application.yml`.

## Run With Side-Topic Reconciliation

```bash
rtk env \
  SOURCE_TOPIC=orders \
  KAFKA_BOOTSTRAP_SERVERS='broker-a:9092,broker-b:9092' \
  CANARY_TOPIC=orders-canary \
  DEAD_LETTER_TOPIC=orders-dlq \
  RUN_DATE=2026-07-02 \
  scripts/run_java_kafka_offset_gap_check_prod.sh \
  hdfs:///data/orders/root-a \
  hdfs:///data/orders/root-b
```

The checker reads configured side topics from `earliest`, decodes Avro
object-container payloads, and matches records to missing parquet offsets by
`sourceTopic`, `sourcePartition`, and `sourceOffset`. Side-topic matches do not
disable `FAIL_ON_GAPS`: with `FAIL_ON_GAPS=true`, raw parquet gaps exit `0`
only when the missing-offset materialization is not truncated and every
materialized offset is explained by canary and/or dead-letter records. They
exit `1` when any materialized offset remains unresolved or when
`missing_offsets_truncated=true`. Without side-topic config, raw parquet gaps
still exit `1`.

Use `MISSING_OFFSETS_LIMIT` high enough to materialize the offsets you need to
reconcile. If the gap list is truncated, the side-topic summary prints
`missing_offsets_truncated=true` and only materialized missing offsets are
classified.

## Direct spark-submit

The wrapper is preferred, but direct YAML-first `spark-submit` is also valid:

```bash
rtk env \
  SPRING_CONFIG_LOCATION=file:/etc/recon/application.yml \
  spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --conf 'spark.sql.session.timeZone=UTC' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

For YAML side-topic runs, include the Spark 3.5 Kafka source package:

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

Spark-conf override direct runs use the same Java entrypoint with explicit
`spark.recon.*` values:

```bash
rtk spark-submit \
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

For the side-topic feature, the direct Java `spark-submit` shape is:

```bash
rtk spark-submit \
  --class com.reconciliation.kafka.KafkaOffsetGapChecker \
  --master yarn \
  --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4' \
  --conf 'spark.sql.session.timeZone=UTC' \
  --conf 'spark.recon.inputRoots=hdfs:///warehouse/orders/root-a,hdfs:///warehouse/orders/root-b' \
  --conf 'spark.recon.runDate=2026-07-02' \
  --conf 'spark.recon.sourceTopic=orders' \
  --conf 'spark.recon.kafkaBootstrapServers=broker-a:9092,broker-b:9092' \
  --conf 'spark.recon.canaryTopic=orders-canary' \
  --conf 'spark.recon.deadLetterTopic=orders-dlq' \
  --conf 'spark.recon.sideTopicStartingOffsets=earliest' \
  --conf 'spark.recon.missingOffsetsLimit=1000' \
  build/libs/recon-kafka-offset-gap-checker-1.0.0.jar
```

## Output

The checker writes machine-readable lines prefixed with `[recon]`.
The packaged `logback.xml` intentionally uses `%msg%n`; this keeps checker
messages parseable while still routing reporting through SLF4J and Spring
Boot's default Logback backend. Error lines use SLF4J error level and remain
visible with `[recon] ERROR:`. When Spark's parent logging backend wins the
driver classpath, the reporter still emits the `[recon]` payload on its own line
so machine parsers can match the stable text.

Important sections:

| Section | Meaning |
| --- | --- |
| `resolved_configuration_begin/end` | Final effective configuration. |
| `partition_scan_begin/end` | Eligible, skipped, and ignored partition paths. |
| `metadata_quality_begin/end` | Invalid metadata row category counts. |
| `partition_gap_stats_begin/end` | Per-partition continuity statistics. |
| `gap_partitions_begin/end` | Condensed list of partitions with gaps. |
| `side_topic_reconciliation_begin/end` | Optional side-topic reconciliation. |
| `side_topic_bucket=canary_explained` | Missing offsets found in the canary topic. |
| `side_topic_bucket=dead_letter_explained` | Missing offsets found in the dead-letter topic. |
| `side_topic_bucket=unresolved` | Missing offsets not found in configured side topics. |
| `side_topic_summary` | Raw gap partition count, bounded missing-offset count, explained/unresolved counts, decoded record counts, and `missing_offsets_truncated`. |
| `side_topic_dead_letter_fields` | Counts of matched dead-letter payloads containing failure fields. |
| `final_exit_decision` | Final code, reason, raw gap count, side-topic state, and explained/unresolved counts. |
| `RESULT: PASS` or `RESULT: FAIL` | Final operator result. |

## Exit Codes

| Code | Meaning |
| ---: | --- |
| 0 | Check completed and no configured failure condition was found; side-topic runs with raw gaps may pass when every materialized missing offset is explained and `missing_offsets_truncated=false`. |
| 1 | Valid offset data was read, but raw gaps, unresolved side-topic offsets, truncated missing-offset materialization, or invalid metadata rows failed the check. |
| 2 | Configuration or input data prevented a meaningful check. |

Exit code `2` includes missing roots, invalid boolean/run-date/integer config,
unreadable parquet, no eligible old partitions, empty readable parquet, missing
metadata column, cache read/write failure, and zero valid normalized offsets.
With side-topic config enabled, exit code `2` also includes incomplete
side-topic config, unreadable Kafka side topics, and Avro decode failures.

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
- Use side-topic reconciliation only when the source topic identity is known;
  records from other topics or partitions are ignored for matching.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Wrapper says jar not found | Jar has not been built or `CHECKER_JAR` is wrong. | Run `./gradlew jar` or set `CHECKER_JAR`. |
| Spark rejects dependencies | Runtime is not Spark 3.5.x or has incompatible Scala ABI. | Use Spark 3.5.x built for Scala 2.12. |
| Side-topic run cannot find Kafka datasource | Spark Kafka source package is missing. | Let the wrapper add packages or set `SPARK_PACKAGES` to your cluster mirror. |
| Side-topic run fails with Avro decode error | A configured side-topic record is not an Avro object-container payload with the expected fields. | Fix the side-topic payload/schema or run without side-topic config. |
| No eligible partitions | `RUN_DATE` equals all fixture/data dates, or directories do not match the date prefix. | Check `RUN_DATE` and `<DATE_PARTITION_COLUMN>=yyyy-MM-dd` names. |
| Gaps are reported per root unexpectedly | Roots were checked separately. | Pass all roots in one run. |
| Cache write fails | Existing path with overwrite disabled, bad path, or permissions. | Use a fresh `NORMALIZED_OFFSETS_PATH` or set overwrite true. |
