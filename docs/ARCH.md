# Architecture

The checker verifies Kafka offset continuity in parquet data written under one
or more root directories. It is implemented in two forms:

- `scripts/check_kafka_offset_gaps.scala`: original Spark 3.5 `spark-shell -i`
  script and behavior oracle.
- `src/main/java/com/reconciliation/kafka/`: Java Spark SQL/DataFrame port
  intended for `spark-submit`.

The Java port is the production artifact for new deployments.
The optional canary/dead-letter side-topic reconciliation exists only in the
Java `spark-submit` checker.

## Runtime Model

```text
operator
  -> scripts/run_java_kafka_offset_gap_check_prod.sh
    -> spark-submit
      -> recon-kafka-offset-gap-checker-1.0.0.jar
        -> KafkaOffsetGapChecker.main
          -> Spark SQL/DataFrame jobs
          -> [recon] stdout/stderr result lines
```

The wrapper assembles Spark conf keys and submits the Java main class:

```text
com.reconciliation.kafka.KafkaOffsetGapChecker
```

Spark supplies the runtime Spark jars. The checker jar is intentionally not a
fat jar.

## Java Packages

| Package | Responsibility |
| --- | --- |
| `com.reconciliation.kafka` | Public `KafkaOffsetGapChecker` entrypoint and high-level orchestration. |
| `com.reconciliation.kafka.config` | `recon.*` / `spark.recon.*` config resolution, validation, and Spark conf lookup. |
| `com.reconciliation.kafka.scan` | Immediate child directory scan, run-date skip, scan reporting. |
| `com.reconciliation.kafka.metadata` | Parquet read, metadata JSON parsing, invalid-row counts, optional normalized parquet persistence. |
| `com.reconciliation.kafka.analytics` | Distinct-offset analytics, gap stats, bounded missing-offset materialization. |
| `com.reconciliation.kafka.sidetopic` | Optional Kafka 3.x side-topic reads, Avro object-container decode, and canary/dead-letter matching. |
| `com.reconciliation.kafka.model` | Small data carriers such as `RootScan`, `EligiblePartition`, `NormalizeResult`, and `MissingOffsetReport`. |
| `com.reconciliation.kafka.support` | Shared constants, result/error reporting, row helpers, and controlled exits. |

## Data Model

Input roots are expected to contain immediate Hive-style date directories:

```text
root/
  timestampcolumn=2026-07-01/
  timestampcolumn=2026-07-02/
```

Only immediate child directories matching:

```text
<datePartitionColumn>=yyyy-MM-dd
```

are eligible. The configured `runDate` is skipped because current-day ingestion
may still be incomplete.

Each eligible parquet row must contain a metadata JSON column, default
`cactus__metadata`, with Kafka fields:

```json
{"partition":0,"offset":123}
```

The checker normalizes valid rows to:

| Column | Type | Meaning |
| --- | --- | --- |
| `partition` | integer | Kafka partition. |
| `offset` | long | Kafka offset. |
| `metadata_json` | string | Original metadata JSON. |
| `source_file` | string | Spark input file name. |

## Processing Pipeline

1. `config.ConfigLoader` resolves configuration from `recon.*` and `spark.recon.*`
   Spark conf keys.
2. `support.ReconReporter` prints resolved configuration as `[recon]` lines.
3. `scan.PartitionScanner` scans immediate child paths under each input root.
4. `scan.PartitionScanner` skips the configured run-date partition and collects
   eligible old partitions.
5. `metadata.MetadataNormalizer` reads all eligible parquet paths as one DataFrame.
6. `metadata.MetadataNormalizer` parses metadata JSON and classifies invalid rows.
7. `metadata.MetadataNormalizer` optionally persists normalized offsets to parquet and
   reads them back.
8. `analytics.OffsetAnalytics` deduplicates `(partition, offset)` pairs for analytics.
9. `analytics.OffsetAnalytics` computes per-partition min/max/span/expected/missing counts.
10. `analytics.OffsetAnalytics` materializes bounded missing offset values per partition.
11. If configured, `sidetopic.SideTopicReconciler` reads Kafka 3.x canary and
    dead-letter topics through the Spark Kafka source.
12. `sidetopic.SideTopicReconciler` decodes Avro object-container payloads and
    buckets materialized missing offsets as canary-explained,
    dead-letter-explained, or unresolved.
13. `support.ReconReporter` prints the final `RESULT`.
14. `KafkaOffsetGapChecker` exits with code `0`, `1`, or `2`.

## Gap Algorithm

For each Kafka partition, the checker uses distinct offsets:

```text
span = max_offset - min_offset + 1
expected_count = span
missing_offset_count = expected_count - distinct_offset_count
has_gaps = missing_offset_count > 0
```

Duplicate rows increase `duplicate_offset_row_count`, but they do not create
false gaps because analytics run on distinct `(partition, offset)` pairs.

Missing offset values are generated from adjacent offset intervals only for
partitions with gaps. The list is bounded by `recon.missingOffsetsLimit` per
partition. When the true missing count exceeds the limit, the checker prints
`missing_offsets_truncated=true`.

## Failure Classes

| Exit code | Class | Examples |
| ---: | --- | --- |
| 0 | Check passed | No gaps and no invalid metadata under default flags. |
| 1 | Data-quality failure | Gaps or invalid metadata rows when the corresponding fail flag is true. |
| 2 | Configuration/input failure | Missing roots, bad config values, no eligible partitions, empty readable parquet, missing metadata column, zero valid offsets, cache write/read failure. |

## Configuration Boundary

The checker reads Spark conf keys only. It supports both canonical and alias
forms:

```text
recon.inputRoots
spark.recon.inputRoots
```

The alias form is preferred in wrappers because some launchers preserve only
`spark.*` keys.

## Side-Topic Boundary

Broken source records and canary/heartbeat records may be intentionally routed
away from the HDFS parquet sink into Kafka side topics. In that case, a missing
parquet offset is not fully understood until the Java checker has compared it
with the configured canary and dead-letter topics.

Side-topic reconciliation is intentionally bounded:

- It is a Java `spark-submit` feature only; the Scala `spark-shell -i` checker
  remains a parquet-gap oracle and does not read side topics.
- Runtime Spark must be Spark 3.5.x, Spark connector artifacts must use Scala
  2.12, and Kafka brokers/fixtures must be Kafka 3.x.
- `recon.sourceTopic`, `recon.kafkaBootstrapServers`, and at least one of
  `recon.canaryTopic` or `recon.deadLetterTopic` are required when any
  side-topic config is present.
- `recon.sideTopicStartingOffsets` accepts `earliest` or `beginning`; both read
  from the beginning of the side topics.
- The side-topic classifier uses the bounded `missing_offsets` values from the
  gap analysis. If `recon.missingOffsetsLimit` truncates that list, the
  side-topic summary prints `missing_offsets_truncated=true`.
- Kafka read failures, incomplete side-topic config, and undecodable Avro
  payloads fail closed with exit code `2`.

## Artifact Boundary

The Java checker depends on:

```text
org.apache.spark:spark-sql_2.12:3.5.6
```

That dependency is `compileOnly` for the application jar. The Spark cluster or
Docker image supplies Spark at runtime.
Side-topic runs also require the Spark 3.5 Kafka source package
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6` and Avro
`org.apache.avro:avro:1.11.4`; the Java production wrapper adds those packages
for side-topic runs unless `SPARK_PACKAGES` overrides them.

## Oracle And Parity

The Scala script is the behavior oracle for the Java port. Local parity is
validated by:

- running the current Scala fixture matrix;
- running the Java `spark-submit` fixture matrix;
- comparing required `[recon]` fields and expected exit codes.

This workspace is not a Git repository, so historical oracle immutability cannot
be proven from Git. Current oracle checksums and the accepted mission decision
are recorded in the Zenith mission artifacts from the porting work.
