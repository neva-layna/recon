# Synthetic Data Utilities

Standalone Java CLI utility project for generating deterministic synthetic data
used by reconciliation fixtures and local checks.

This project is intentionally separate from the root checker Gradle build. It
has its own `settings.gradle`, `build.gradle`, and Gradle wrapper under
`synth-data/`, and it is not included from the workspace root `settings.gradle`.

## Build And Install

Run utility commands from this directory:

```bash
cd synth-data
```

Build, test, and create the local distribution:

```bash
rtk ./gradlew clean test installDist --no-daemon --console=plain
```

The installed CLI is:

```text
build/install/synth-data/bin/synth-data
```

Show the top-level help:

```bash
rtk build/install/synth-data/bin/synth-data --help
```

Show the Kafka producer help:

```bash
rtk build/install/synth-data/bin/synth-data kafka-side-topic --help
```

## Local Parquet Generator

The default CLI writes one local Parquet row under a checker-shaped Hive-style
date partition:

```text
<output-dir>/<relative-root>/<date-partition-column>=yyyy-MM-dd/part-00000.parquet
```

Copy-pasteable example:

```bash
rtk build/install/synth-data/bin/synth-data \
  --output-dir generated_data \
  --relative-root orders/root_a \
  --date-partition-column timestampcolumn \
  --date 2026-07-01 \
  --metadata-column cactus__metadata \
  --topic orders \
  --partition 0 \
  --offset 123 \
  --payload sample-payload \
  --extra source=manual
```

The generated file is:

```text
generated_data/orders/root_a/timestampcolumn=2026-07-01/part-00000.parquet
```

The metadata column defaults to `cactus__metadata` and contains checker-compatible
JSON such as:

```json
{"partition":0,"offset":123}
```

Successful parquet runs print manifest lines that identify the generated paths
and values:

```text
[synth-data] output_root=/absolute/path/to/generated_data
[synth-data] relative_root=orders/root_a
[synth-data] partition_path=/absolute/path/to/generated_data/orders/root_a/timestampcolumn=2026-07-01
[synth-data] parquet_file=/absolute/path/to/generated_data/orders/root_a/timestampcolumn=2026-07-01/part-00000.parquet
[synth-data] metadata_json={"partition":0,"offset":123}
[synth-data] hdfs_required=false
```

The generator writes only to the local filesystem. It does not upload to HDFS or
require HDFS client configuration. After generation, operators can copy the
local tree manually, for example:

```bash
hdfs dfs -put generated_data/ /tmp/my_synth
```

After that manual copy, the root checker can read HDFS paths that include the
copied relative root, such as `hdfs:///tmp/my_synth/orders/root_a`.

## Parquet CLI Arguments

Required:

- `--output-dir DIR`: local output directory.
- `--relative-root PATH`: relative checker input root/source path below the output directory.
- `--date-partition-column NAME`: Hive-style date partition column.
- `--date yyyy-MM-dd`: partition date.
- `--topic NAME`: source topic value written as a data column.
- `--partition N`: numeric Kafka partition used in metadata JSON.
- `--offset N`: numeric Kafka offset used in metadata JSON.

Optional:

- `--metadata-column NAME`: metadata JSON column, default `cactus__metadata`.
- `--payload VALUE`: add a `payload` string column.
- `--extra NAME=VALUE`: add a string column; may be repeated.

## Kafka Side-Topic Producer

The same distribution can produce checker-compatible canary or dead-letter
records to Kafka side topics. Use the `kafka-side-topic` subcommand when
invoking the installed CLI:

```bash
rtk build/install/synth-data/bin/synth-data kafka-side-topic \
  --bootstrap-server localhost:9092 \
  --conf security.protocol=PLAINTEXT \
  --conf client.id=synth-data-docs \
  --destination-topic orders-canary \
  --kind canary \
  --source-topic orders \
  --source-partition 0 \
  --source-offset 123 \
  --source-timestamp 1710000000123 \
  --source-key order-123 \
  --source-value payload-123 \
  --source-header trace=manual
```

`--conf key=value` may be repeated and is passed through to Kafka producer
configuration. The CLI always overrides `key.serializer` and `value.serializer`
with Kafka byte-array serializers and writes Avro object-container values,
matching the checker side-topic decoder.

Successful dry runs print the Avro manifest and the producer configuration
without connecting to Kafka:

```bash
rtk build/install/synth-data/bin/synth-data kafka-side-topic \
  --bootstrap-server localhost:9092 \
  --conf security.protocol=PLAINTEXT \
  --conf client.id=synth-data-docs \
  --destination-topic orders-canary \
  --kind canary \
  --source-topic orders \
  --source-partition 0 \
  --source-offset 123 \
  --source-timestamp 1710000000123 \
  --source-key order-123 \
  --source-value payload-123 \
  --source-header trace=manual \
  --dry-run \
  --payload-file generated_payloads/orders-canary-123.avro
```

Expected stdout includes:

```text
[synth-data] manifest destination_topic=orders-canary kind=canary source_topic=orders source_partition=0 source_offset=123 payload_bytes=...
[synth-data] producer_config bootstrap.servers=localhost:9092 key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer conf.security.protocol=PLAINTEXT conf.client.id=synth-data-docs
[synth-data] dry_run=true
```

An SSL/SASL-style producer invocation uses the same repeated `--conf` shape:

```bash
rtk build/install/synth-data/bin/synth-data kafka-side-topic \
  --bootstrap-server broker-a.example:9093,broker-b.example:9093 \
  --conf security.protocol=SASL_SSL \
  --conf sasl.mechanism=SCRAM-SHA-512 \
  --conf 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";' \
  --conf ssl.truststore.location=/etc/security/kafka.client.truststore.jks \
  --conf ssl.truststore.password=changeit \
  --destination-topic orders-dead-letter \
  --kind dead-letter \
  --source-topic orders \
  --source-partition 0 \
  --source-offset 456 \
  --source-timestamp 1710000000456 \
  --source-key order-456 \
  --source-value broken-payload \
  --source-headers trace=manual,error=synthetic \
  --failure-event-id failure-456 \
  --reason-msg deserialize-failed \
  --exception com.example.DeserializationException \
  --dry-run
```

Dead-letter records require:

- `--failure-event-id TEXT`
- `--reason-msg TEXT`
- `--exception TEXT`

For payload-only validation without connecting to Kafka, add `--dry-run` and
optionally `--payload-file PATH`.

## Non-Goals

- No HDFS upload: parquet generation is local only; use `hdfs dfs -put` yourself when needed.
- No broker-alias parsing: Kafka producer settings are explicit `--bootstrap-server` and repeated `--conf` values.
- No root Gradle module: `synth-data/` remains a standalone utility project and is not included by the root build.
