---
name: spark-offset-checker
description: Implement or validate the standalone Spark 3.5 parquet Kafka offset gap checker mission artifacts.
---

# Spark Offset Checker Mission Procedure

Use this skill for work on the standalone parquet Kafka offset checker. The product is not part of the nested Zenith harness package.

## Required Constraints

- Keep product files at the workspace root, such as `scripts/`, `tests/fixtures/`, `docs/`, and optional `docker/`.
- Do not edit `zenith/zenith/` for product behavior.
- Use Spark 3.5.x only. Do not use Spark 4 in scripts, Docker images, docs, or validation.
- The main checker must be a Scala script runnable with `spark-shell -i`.
- Configuration must work through Spark conf keys because `spark-shell -i` has awkward argument passing.
- Shell commands should use `rtk`.

## Checker Behavior

- Accept multiple input roots.
- Discover immediate Hive-style date partition directories named `<datePartitionColumn>=yyyy-MM-dd`.
- Skip the configured run date, defaulting to the driver current date.
- Parse the configured metadata JSON column, default `cactus__metadata`.
- Extract Kafka `partition` and `offset`.
- Combine offsets across all roots before analytics.
- Count distinct offsets per Kafka partition.
- Compute and print distinct offsets, min offset, max offset, span, expected count, gap flag, and missing offsets.
- Exit non-zero by default for gaps and for invalid/missing metadata that compromises the check.
- Exit non-zero by default for no eligible old partitions, empty readable data, or zero valid normalized offsets.
- If a persisted normalized-offset path is configured, write combined normalized offsets there and use the persisted dataset for analytics.

## Validation Expectations

- Prove behavior through Docker with Spark 3.5.x in local mode.
- Generated fixtures must include pass, gap, split-across-roots, duplicate, today-skipped, invalid metadata, empty/no-valid-data, and persisted-cache scenarios.
- Capture Spark version, commands, stdout/stderr, exit codes, and relevant generated artifact paths in handoff reports.
