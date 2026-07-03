/**
 * Spark 3.5 fixture generator for the full local validation matrix.
 *
 * This script is intentionally broader than the Hadoop sample generator. It
 * creates pass, cross-root split, gap, duplicate, invalid metadata, empty, and
 * partition-scan edge cases for `scripts/run_kafka_offset_gap_fixture_checks.sh`.
 */
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

val FixturePrefix = "[recon-fixtures]"

/** Reads `recon.*` or `spark.recon.*` fixture configuration. */
def confOption(key: String): Option[String] = {
  val aliases = Seq(key, s"spark.$key").distinct

  def fromSparkConf(candidate: String): Option[String] =
    scala.util.Try(spark.conf.get(candidate)).toOption.map(_.trim).filter(_.nonEmpty)

  def fromSparkShellCommand(candidate: String): Option[String] = {
    val command = Option(System.getProperty("sun.java.command")).getOrElse("")
    val tokens = command.split("\\s+").toSeq.filter(_.nonEmpty)
    val pairPrefix = candidate + "="
    val inlinePrefix = "--conf=" + candidate + "="

    tokens.sliding(2).collectFirst {
      case Seq("--conf", value) if value.startsWith(pairPrefix) =>
        value.substring(pairPrefix.length)
    }.orElse {
      tokens.collectFirst {
        case value if value.startsWith(inlinePrefix) =>
          value.substring(inlinePrefix.length)
      }
    }.map(_.trim).filter(_.nonEmpty)
  }

  aliases.view.flatMap(candidate => fromSparkConf(candidate).orElse(fromSparkShellCommand(candidate))).headOption
}

val outputRoot = confOption("recon.fixtureOutputRoot").getOrElse("/tmp/recon-kafka-offset-fixtures")
val metadataColumn = confOption("recon.fixtureMetadataColumn").getOrElse("cactus__metadata")
val datePartitionColumn = confOption("recon.fixtureDatePartitionColumn").getOrElse("timestampcolumn")
val oldDate = confOption("recon.fixtureOldDate").getOrElse("2026-07-01")
val runDate = confOption("recon.fixtureRunDate").getOrElse("2026-07-02")

val outputRootForSafety = outputRoot.trim.stripSuffix("/")
if (outputRootForSafety.isEmpty || outputRootForSafety == "/" || outputRootForSafety == "file:" || outputRootForSafety == "file://") {
  Console.err.println(s"$FixturePrefix refusing unsafe recon.fixtureOutputRoot=$outputRoot")
  System.exit(2)
}

/** Builds a path below the configured fixture output root. */
def fixturePath(parts: String*): String =
  (Seq(outputRoot.stripSuffix("/")) ++ parts.map(_.stripPrefix("/").stripSuffix("/"))).mkString("/")

/** Builds an immediate Hive-style date partition path below a fixture root. */
def partitionPath(root: String, date: String): String =
  fixturePath(root, s"$datePartitionColumn=$date")

/** Builds numeric JSON metadata matching the production checker schema. */
def metadata(partition: String, offset: String): String =
  s"""{"partition":$partition,"offset":$offset}"""

/** Builds string JSON metadata used for non-numeric validation cases. */
def metadataString(partition: String, offset: String): String =
  s"""{"partition":"$partition","offset":"$offset"}"""

/** Builds JSON metadata missing one required Kafka field. */
def metadataMissingPartition(offset: String): String =
  s"""{"offset":$offset}"""

def metadataMissingOffset(partition: String): String =
  s"""{"partition":$partition}"""

/** Creates a fixture DataFrame with the configured metadata column. */
def frame(values: Seq[Option[String]]): DataFrame = {
  val rows = spark.sparkContext.parallelize(values.zipWithIndex.map {
    case (metadataValue, index) => Row(metadataValue.orNull, s"payload-$index")
  })
  val schema = StructType(Seq(
    StructField(metadataColumn, StringType, nullable = true),
    StructField("payload", StringType, nullable = false)
  ))
  spark.createDataFrame(rows, schema)
}

/** Writes one fixture parquet partition. */
def writeValues(root: String, date: String, values: Seq[Option[String]]): Unit = {
  frame(values).coalesce(1).write.mode("overwrite").parquet(partitionPath(root, date))
}

/** Writes an empty but readable parquet partition. */
def writeEmpty(root: String, date: String): Unit = {
  frame(Seq.empty[Option[String]]).coalesce(1).write.mode("overwrite").parquet(partitionPath(root, date))
}

val hadoopConf = spark.sparkContext.hadoopConfiguration
val outPath = new Path(outputRoot)
val fs = outPath.getFileSystem(hadoopConf)
if (fs.exists(outPath)) {
  fs.delete(outPath, true)
}
fs.mkdirs(outPath)

writeValues(
  "pass/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "1")), Some(metadata("1", "10")))
)
writeValues(
  "pass/root_b",
  oldDate,
  Seq(Some(metadata("0", "2")), Some(metadata("1", "11")), Some(metadata("1", "12")))
)

writeValues(
  "split/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "2")), Some(metadata("1", "10")))
)
writeValues(
  "split/root_b",
  oldDate,
  Seq(Some(metadata("0", "1")), Some(metadata("1", "11")), Some(metadata("1", "12")))
)

writeValues(
  "gap/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "2")), Some(metadata("1", "5")))
)

writeValues(
  "gap_over_limit/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "4")), Some(metadata("1", "5")))
)

writeValues(
  "duplicate/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "1")), Some(metadata("0", "1")), Some(metadata("0", "2")))
)

writeValues(
  "today_skipped/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "1")), Some(metadata("0", "2")))
)
writeValues(
  "today_skipped/root_a",
  runDate,
  Seq(Some(metadata("0", "10")), Some(metadata("0", "12")))
)
fs.mkdirs(new Path(fixturePath("today_skipped/root_a", s"$datePartitionColumn=not-a-date")))
fs.mkdirs(new Path(fixturePath("today_skipped/root_a", "not_a_partition=2026-07-01")))

writeValues(
  "scan_noise/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadata("0", "1")), Some(metadata("0", "2")))
)
fs.mkdirs(new Path(fixturePath("scan_noise/root_a", s"$datePartitionColumn=2026-13-99")))
fs.mkdirs(new Path(fixturePath("scan_noise/root_a", s"${datePartitionColumn}_backup=$oldDate")))
fs.mkdirs(new Path(fixturePath("scan_noise/root_a", "random_child")))

writeValues(
  "invalid/malformed_json/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some("{bad-json"))
)
writeValues(
  "invalid/missing_metadata/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), None)
)
writeValues(
  "invalid/missing_partition/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadataMissingPartition("1")))
)
writeValues(
  "invalid/missing_offset/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadataMissingOffset("0")))
)
writeValues(
  "invalid/non_numeric_partition/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadataString("abc", "1")))
)
writeValues(
  "invalid/non_numeric_offset/root_a",
  oldDate,
  Seq(Some(metadata("0", "0")), Some(metadataString("0", "abc")))
)
writeValues(
  "invalid/all_invalid/root_a",
  oldDate,
  Seq(Some("{bad-json"), Some(metadataString("x", "y")), None)
)

writeEmpty("empty/readable_empty/root_a", oldDate)
writeValues("empty/only_run_date/root_a", runDate, Seq(Some(metadata("0", "0"))))
fs.mkdirs(new Path(fixturePath("empty/no_eligible/root_a", s"$datePartitionColumn=not-a-date")))
fs.mkdirs(new Path(fixturePath("empty/no_eligible/root_a", "misc=2026-07-01")))

println(s"$FixturePrefix output_root=$outputRoot")
println(s"$FixturePrefix metadata_column=$metadataColumn")
println(s"$FixturePrefix date_partition_column=$datePartitionColumn")
println(s"$FixturePrefix old_date=$oldDate")
println(s"$FixturePrefix run_date=$runDate")
println(s"$FixturePrefix pass_roots=${fixturePath("pass/root_a")},${fixturePath("pass/root_b")}")
println(s"$FixturePrefix split_roots=${fixturePath("split/root_a")},${fixturePath("split/root_b")}")
println(s"$FixturePrefix gap_root=${fixturePath("gap/root_a")}")
println(s"$FixturePrefix gap_over_limit_root=${fixturePath("gap_over_limit/root_a")}")
println(s"$FixturePrefix duplicate_root=${fixturePath("duplicate/root_a")}")
println(s"$FixturePrefix today_skipped_root=${fixturePath("today_skipped/root_a")}")
println(s"$FixturePrefix scan_noise_root=${fixturePath("scan_noise/root_a")}")
println(s"$FixturePrefix cache_path=${fixturePath("cache/normalized_offsets")}")
println(s"$FixturePrefix generation_complete=true")

System.exit(0)
