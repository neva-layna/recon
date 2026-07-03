---
name: java-spark-offset-checker
description: Implement or validate the Java Gradle Spark 3.5 Kafka offset gap checker and spark-submit wrapper.
---

# Java Spark Offset Checker Procedure

Use this skill for this mission's Java application port of
`scripts/check_kafka_offset_gaps.scala`.

## Constraints

- Product files belong at the workspace root, not under `zenith/zenith`.
- Use Gradle.
- Compile Java source for Java 8 bytecode.
- Use Spark 3.5.x artifacts only.
- Use Spark artifacts built for Scala 2.12; do not use Scala 2.13 artifacts.
- Provide a shell script that runs the Java checker with `spark-submit`.
- Shell commands should use `rtk`.

## Behavior Oracle

The Scala script `scripts/check_kafka_offset_gaps.scala` defines the behavior to
port. Preserve configuration keys, partition discovery, metadata validation,
optional normalized-offset persistence, gap analytics, output fields, and exit
codes unless a mission decision explicitly accepts a divergence.

## Implementation Notes

- Prefer Spark SQL/DataFrame APIs in Java over manual parquet parsing.
- Keep invalid metadata categorization fail-closed.
- Keep cross-root union before gap analytics.
- Keep bounded missing-offset output and truncation reporting.
- Keep `[recon]` output lines suitable for existing shell validators.
- Avoid adding runtime dependencies that conflict with Spark's provided jars.

## Validation Notes

- Prove the app with Gradle tests and real Spark 3.5 fixture runs.
- Validation evidence must include Spark version, exact command or wrapper call,
  stdout/stderr, exit code, and scenario result matrix.
- Fixture scenarios should cover pass, gap, split roots, duplicates, run-date
  skip, scan noise, persisted normalized offsets, invalid metadata variants,
  all-invalid data, empty data, only-run-date partitions, and no eligible
  partitions.
