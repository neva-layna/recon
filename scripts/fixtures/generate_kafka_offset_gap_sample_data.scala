/**
 * Spark 3.5 `spark-shell -i` sample-data generator for Hadoop/HDFS validation.
 *
 * This intentionally creates only two multi-root scenarios:
 *   - `pass/root_a` + `pass/root_b`: all offsets are present after cross-root union.
 *   - `gap/root_a` + `gap/root_b`: partition 0 is missing offset 1 after union.
 *
 * Configure through Spark conf keys:
 *   - `recon.sampleOutputRoot`, default `hdfs:///tmp/recon-kafka-offset-gap-samples`
 *   - `recon.sampleMetadataColumn`, default `cactus__metadata`
 *   - `recon.sampleDatePartitionColumn`, default `timestampcolumn`
 *   - `recon.sampleOldDate`, default `2026-07-01`
 */
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

val SamplePrefix = "[recon-sample]"

/** Reads `recon.*` or `spark.recon.*` values from Spark conf or spark-shell command text. */
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

val outputRoot = confOption("recon.sampleOutputRoot").getOrElse("hdfs:///tmp/recon-kafka-offset-gap-samples")
val metadataColumn = confOption("recon.sampleMetadataColumn").getOrElse("cactus__metadata")
val datePartitionColumn = confOption("recon.sampleDatePartitionColumn").getOrElse("timestampcolumn")
val oldDate = confOption("recon.sampleOldDate").getOrElse("2026-07-01")

val outputRootForSafety = outputRoot.trim.stripSuffix("/")
if (
  outputRootForSafety.isEmpty ||
    outputRootForSafety == "/" ||
    outputRootForSafety == "file:" ||
    outputRootForSafety == "file://" ||
    outputRootForSafety == "hdfs:" ||
    outputRootForSafety == "hdfs:/"
) {
  Console.err.println(s"$SamplePrefix refusing unsafe recon.sampleOutputRoot=$outputRoot")
  System.exit(2)
}

/** Joins path fragments below `outputRoot` without introducing local filesystem assumptions. */
def samplePath(parts: String*): String =
  (Seq(outputRoot.stripSuffix("/")) ++ parts.map(_.stripPrefix("/").stripSuffix("/"))).mkString("/")

/** Returns the Hive-style partition path for a sample root and date. */
def partitionPath(root: String, date: String): String =
  samplePath(root, s"$datePartitionColumn=$date")

/** Builds the JSON payload expected by the checker's metadata parser. */
def metadata(partition: String, offset: String): String =
  s"""{"partition":$partition,"offset":$offset}"""

/** Creates a small DataFrame with the configured metadata column and a payload column. */
def frame(values: Seq[String]): DataFrame = {
  val rows = spark.sparkContext.parallelize(values.zipWithIndex.map {
    case (metadataValue, index) => Row(metadataValue, s"sample-payload-$index")
  })
  val schema = StructType(Seq(
    StructField(metadataColumn, StringType, nullable = false),
    StructField("payload", StringType, nullable = false)
  ))
  spark.createDataFrame(rows, schema)
}

/** Writes one parquet Hive-date partition for a sample root. */
def writeValues(root: String, date: String, values: Seq[String]): Unit = {
  frame(values).coalesce(1).write.mode("overwrite").parquet(partitionPath(root, date))
}

val outputPath = new Path(outputRoot)
val fs = outputPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
if (fs.exists(outputPath)) {
  fs.delete(outputPath, true)
}
fs.mkdirs(outputPath)

writeValues(
  "pass/root_a",
  oldDate,
  Seq(metadata("0", "0"), metadata("0", "2"), metadata("1", "10"))
)
writeValues(
  "pass/root_b",
  oldDate,
  Seq(metadata("0", "1"), metadata("1", "11"), metadata("1", "12"))
)

writeValues(
  "gap/root_a",
  oldDate,
  Seq(metadata("0", "0"), metadata("1", "10"))
)
writeValues(
  "gap/root_b",
  oldDate,
  Seq(metadata("0", "2"), metadata("1", "11"), metadata("1", "12"))
)

val passRoots = Seq(samplePath("pass/root_a"), samplePath("pass/root_b"))
val gapRoots = Seq(samplePath("gap/root_a"), samplePath("gap/root_b"))

println(s"$SamplePrefix output_root=$outputRoot")
println(s"$SamplePrefix metadata_column=$metadataColumn")
println(s"$SamplePrefix date_partition_column=$datePartitionColumn")
println(s"$SamplePrefix old_date=$oldDate")
println(s"$SamplePrefix pass_input_roots=${passRoots.mkString(",")}")
println(s"$SamplePrefix gap_input_roots=${gapRoots.mkString(",")}")
println(s"$SamplePrefix expected_pass_exit=0")
println(s"$SamplePrefix expected_gap_exit=1")
println(s"$SamplePrefix expected_gap_missing_offsets=[1]")
println(s"$SamplePrefix generation_complete=true")

System.exit(0)
