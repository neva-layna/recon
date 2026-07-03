# Operations

Use the Java `spark-submit` wrapper for production-style runs.

Use side-topic reconciliation when the source pipeline can redirect records away
from the HDFS parquet sink. Canary/heartbeat messages and broken source
messages commonly land in Kafka canary or dead-letter topics instead of parquet;
the Java checker can compare missing parquet offsets with those side topics so
operators can distinguish explained gaps from unresolved gaps.

Side-topic reconciliation is only documented for the Java `spark-submit`
checker. Do not use the Scala `spark-shell -i` checker for this feature.

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
| `SOURCE_TOPIC` | none | Source Kafka topic identity used to match side-topic records. |
| `KAFKA_BOOTSTRAP_SERVERS` | none | Kafka bootstrap servers for configured side-topic reads. |
| `CANARY_TOPIC` | none | Optional Kafka canary topic containing Avro object-container payloads. |
| `DEAD_LETTER_TOPIC` | none | Optional Kafka dead-letter topic containing Avro object-container payloads. |
| `SIDE_TOPIC_STARTING_OFFSETS` | `earliest` | Side-topic read start; `earliest` and `beginning` are accepted. |
| `SPARK_PACKAGES` | side-topic default only | Optional override for `spark-submit --packages`. |
| `SPARK_JARS_IVY` | none | Optional writable Ivy cache path forwarded as `spark.jars.ivy`. |

The wrapper forwards values as `spark.recon.*` Spark conf keys.

When any side-topic variable is set, `SOURCE_TOPIC`,
`KAFKA_BOOTSTRAP_SERVERS`, and at least one of `CANARY_TOPIC` or
`DEAD_LETTER_TOPIC` are required. The wrapper adds the Spark 3.5 Kafka source
and Avro packages by default:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.avro:avro:1.11.4
```

Set `SPARK_PACKAGES` when your cluster preloads or mirrors these artifacts.
The Spark Kafka connector resolves Kafka 3.x-compatible client libraries for
Spark 3.5.x.
Set `SPARK_JARS_IVY` when the Spark runtime user cannot write its default Ivy
cache, for example in locked-down containers.

## Run With Side-Topic Reconciliation

```bash
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
change `FAIL_ON_GAPS`; they explain gaps while preserving the existing exit
class behavior.

Use `MISSING_OFFSETS_LIMIT` high enough to materialize the offsets you need to
reconcile. If the gap list is truncated, the side-topic summary prints
`missing_offsets_truncated=true` and only materialized missing offsets are
classified.

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

For the side-topic feature, the direct Java `spark-submit` shape is:

```bash
spark-submit \
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
| `side_topic_summary` | Counts for explained/unresolved offsets, decoded records, and `missing_offsets_truncated`. |
| `side_topic_dead_letter_fields` | Counts of matched dead-letter payloads containing failure fields. |
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
