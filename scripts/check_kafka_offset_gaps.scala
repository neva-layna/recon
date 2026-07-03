/**
 * Spark 3.5 `spark-shell -i` script that checks Kafka offset continuity across
 * multiple parquet/HDFS roots.
 *
 * Input roots are expected to contain immediate Hive-style date directories such
 * as `timestampcolumn=yyyy-MM-dd`. The script skips the configured run date,
 * parses Kafka `partition` and `offset` from the configured metadata JSON
 * column, unions all eligible roots, and reports offset gaps per Kafka
 * partition.
 *
 * Configure through Spark conf keys:
 *   - `recon.inputRoots` comma-separated roots, required
 *   - `recon.metadataColumn`, default `cactus__metadata`
 *   - `recon.datePartitionColumn`, default `timestampcolumn`
 *   - `recon.runDate`, default driver current date
 *   - `recon.normalizedOffsetsPath`, optional parquet cache path
 *   - `recon.missingOffsetsLimit`, default `1000`
 */
import java.time.{LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Runtime configuration resolved from `recon.*` or `spark.recon.*` Spark conf keys. */
case class CheckerConfig(
    inputRoots: Seq[String],
    metadataColumn: String,
    datePartitionColumn: String,
    runDate: LocalDate,
    runDateSource: String,
    normalizedOffsetsPath: Option[String],
    normalizedOffsetsOverwrite: Boolean,
    failOnInvalidRows: Boolean,
    failOnGaps: Boolean,
    missingOffsetsLimit: Long,
    exitOnCompletion: Boolean
)

/** Bounded materialized missing offset values for one gapped Kafka partition. */
case class MissingOffsetReport(offsets: Seq[Long], truncated: Boolean)

/** One eligible old Hive date partition discovered under an input root. */
case class EligiblePartition(root: String, date: LocalDate, path: String)

/** Discovery result for one configured input root. */
case class RootScan(
    root: String,
    eligible: Seq[EligiblePartition],
    skippedRunDate: Seq[String],
    ignoredInvalidDate: Seq[String],
    ignoredNonMatching: Seq[String]
)

val ReconPrefix = "[recon]"
val DateFormatter = DateTimeFormatter.ISO_LOCAL_DATE

/**
 * Reads a config value from Spark conf or the spark-shell command line.
 *
 * Spark 3.5 warns for non-`spark.*` keys and can omit plain `recon.*` keys from
 * `SparkConf`, so this also parses `sun.java.command` as a fallback. Prefer
 * `spark.recon.*` aliases when a launcher strips arbitrary keys.
 */
def confOption(key: String): Option[String] = {
  val aliases = Seq(key, s"spark.$key").distinct

  def fromSparkConf(candidate: String): Option[String] = {
    Try(spark.conf.get(candidate)).toOption
      .orElse(spark.sparkContext.getConf.getOption(candidate))
      .map(_.trim)
      .filter(_.nonEmpty)
  }

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

/** Prints a final result and terminates the spark-shell JVM with the supplied exit code. */
def stopNow(code: Int, message: String): Nothing = {
  if (code == 0) {
    println(s"$ReconPrefix RESULT: PASS $message")
  } else {
    Console.err.println(s"$ReconPrefix ERROR: $message")
    println(s"$ReconPrefix RESULT: FAIL $message")
  }
  System.exit(code)
  throw new RuntimeException(message)
}

/** Parses permissive boolean Spark conf values such as true/false, 1/0, and yes/no. */
def parseBoolean(key: String, defaultValue: Boolean): Boolean = {
  confOption(key) match {
    case None => defaultValue
    case Some(value) =>
      value.toLowerCase match {
        case "true" | "1" | "yes" | "y" => true
        case "false" | "0" | "no" | "n" => false
        case other => stopNow(2, s"Invalid boolean Spark conf $key=$other")
      }
  }
}

/** Parses a non-negative long Spark conf value or fails with an operator-readable error. */
def parseNonNegativeLong(key: String, defaultValue: Long): Long = {
  confOption(key) match {
    case None => defaultValue
    case Some(value) =>
      Try(value.toLong) match {
        case Success(parsed) if parsed >= 0L => parsed
        case Success(parsed) =>
          stopNow(2, s"Invalid non-negative integer Spark conf $key=$parsed")
        case Failure(error) =>
          stopNow(2, s"Invalid non-negative integer Spark conf $key=$value: ${error.getMessage}")
      }
  }
}

/** Returns the configured run date, or the Spark driver current date if unset. */
def parseRunDate(): (LocalDate, String) = {
  confOption("recon.runDate") match {
    case Some(value) =>
      Try(LocalDate.parse(value, DateFormatter)) match {
        case Success(parsed) => (parsed, "spark_conf:recon.runDate")
        case Failure(error) =>
          stopNow(2, s"Invalid Spark conf recon.runDate=$value; expected yyyy-MM-dd: ${error.getMessage}")
      }
    case None => (LocalDate.now(ZoneId.systemDefault()), "driver_current_date")
  }
}

/** Resolves and validates the checker configuration. */
def loadConfig(): CheckerConfig = {
  val roots = confOption("recon.inputRoots")
    .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
    .getOrElse(Seq.empty[String])

  if (roots.isEmpty) {
    stopNow(
      2,
      "Missing required Spark conf recon.inputRoots; provide a comma-separated list of root paths"
    )
  }

  val (runDate, runDateSource) = parseRunDate()

  CheckerConfig(
    inputRoots = roots,
    metadataColumn = confOption("recon.metadataColumn").getOrElse("cactus__metadata"),
    datePartitionColumn = confOption("recon.datePartitionColumn").getOrElse("timestampcolumn"),
    runDate = runDate,
    runDateSource = runDateSource,
    normalizedOffsetsPath = confOption("recon.normalizedOffsetsPath"),
    normalizedOffsetsOverwrite = parseBoolean("recon.normalizedOffsetsOverwrite", true),
    failOnInvalidRows = parseBoolean("recon.failOnInvalidRows", true),
    failOnGaps = parseBoolean("recon.failOnGaps", true),
    missingOffsetsLimit = parseNonNegativeLong("recon.missingOffsetsLimit", 1000L),
    exitOnCompletion = parseBoolean("recon.exitOnCompletion", true)
  )
}

/** Prints resolved configuration as machine-readable `[recon]` lines. */
def printConfig(config: CheckerConfig): Unit = {
  println(s"$ReconPrefix resolved_configuration_begin")
  println(s"$ReconPrefix recon.inputRoots=${config.inputRoots.mkString(",")}")
  println(s"$ReconPrefix recon.metadataColumn=${config.metadataColumn}")
  println(s"$ReconPrefix recon.datePartitionColumn=${config.datePartitionColumn}")
  println(s"$ReconPrefix recon.runDate=${config.runDate.format(DateFormatter)}")
  println(s"$ReconPrefix recon.runDateSource=${config.runDateSource}")
  println(s"$ReconPrefix recon.normalizedOffsetsPath=${config.normalizedOffsetsPath.getOrElse("<none>")}")
  println(s"$ReconPrefix recon.normalizedOffsetsOverwrite=${config.normalizedOffsetsOverwrite}")
  println(s"$ReconPrefix recon.failOnInvalidRows=${config.failOnInvalidRows}")
  println(s"$ReconPrefix recon.failOnGaps=${config.failOnGaps}")
  println(s"$ReconPrefix recon.missingOffsetsLimit=${config.missingOffsetsLimit}")
  println(s"$ReconPrefix recon.exitOnCompletion=${config.exitOnCompletion}")
  println(s"$ReconPrefix resolved_configuration_end")
}

/** Lists immediate children for a root through Hadoop FileSystem APIs. */
def listImmediateChildren(rootText: String): Array[FileStatus] = {
  val rootPath = new Path(rootText)
  val fs = rootPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

  if (!fs.exists(rootPath)) {
    stopNow(2, s"Configured input root does not exist: $rootText")
  }
  if (!fs.getFileStatus(rootPath).isDirectory) {
    stopNow(2, s"Configured input root is not a directory: $rootText")
  }

  Try(fs.listStatus(rootPath)) match {
    case Success(statuses) => statuses
    case Failure(error) =>
      stopNow(2, s"Failed to list configured input root $rootText: ${error.getMessage}")
  }
}

/** Scans one root for immediate `<datePartitionColumn>=yyyy-MM-dd` children not equal to run date. */
def scanRoot(rootText: String, config: CheckerConfig): RootScan = {
  val prefix = config.datePartitionColumn + "="
  val statuses = listImmediateChildren(rootText)
  val eligible = scala.collection.mutable.ArrayBuffer.empty[EligiblePartition]
  val skippedRunDate = scala.collection.mutable.ArrayBuffer.empty[String]
  val ignoredInvalidDate = scala.collection.mutable.ArrayBuffer.empty[String]
  val ignoredNonMatching = scala.collection.mutable.ArrayBuffer.empty[String]
  val datePattern = "^\\d{4}-\\d{2}-\\d{2}$"

  statuses.foreach { status =>
    val childPath = status.getPath
    val name = childPath.getName
    if (status.isDirectory && name.startsWith(prefix)) {
      val dateText = name.substring(prefix.length)
      if (!dateText.matches(datePattern)) {
        ignoredInvalidDate += childPath.toString
      } else {
        Try(LocalDate.parse(dateText, DateFormatter)) match {
          case Success(partitionDate) if partitionDate == config.runDate =>
            skippedRunDate += childPath.toString
          case Success(partitionDate) =>
            eligible += EligiblePartition(rootText, partitionDate, childPath.toString)
          case Failure(_) =>
            ignoredInvalidDate += childPath.toString
        }
      }
    } else {
      ignoredNonMatching += childPath.toString
    }
  }

  RootScan(rootText, eligible.toSeq.sortBy(_.date.toString), skippedRunDate.toSeq, ignoredInvalidDate.toSeq, ignoredNonMatching.toSeq)
}

/** Prints partition discovery details, including skipped and ignored child paths. */
def printScan(scans: Seq[RootScan]): Unit = {
  println(s"$ReconPrefix partition_scan_begin")
  scans.foreach { scan =>
    println(s"$ReconPrefix root=${scan.root}")
    println(s"$ReconPrefix root=${scan.root} eligible_count=${scan.eligible.size}")
    scan.eligible.foreach { item =>
      println(s"$ReconPrefix eligible_path root=${scan.root} date=${item.date.format(DateFormatter)} path=${item.path}")
    }
    scan.skippedRunDate.foreach { path =>
      println(s"$ReconPrefix skipped_run_date_path root=${scan.root} path=$path")
    }
    scan.ignoredInvalidDate.foreach { path =>
      println(s"$ReconPrefix ignored_invalid_date_path root=${scan.root} path=$path")
    }
    scan.ignoredNonMatching.foreach { path =>
      println(s"$ReconPrefix ignored_non_matching_path root=${scan.root} path=$path")
    }
  }
  println(s"$ReconPrefix partition_scan_end")
}

/** Returns a Spark SQL column reference with backticks escaped for arbitrary metadata column names. */
def quotedColumn(name: String): Column = {
  col("`" + name.replace("`", "``") + "`")
}

/** Aggregation helper for counting rows matching a predicate. */
def countWhen(condition: Column, alias: String): Column = {
  sum(when(condition, lit(1L)).otherwise(lit(0L))).cast(LongType).as(alias)
}

/** Reads all eligible parquet paths as one DataFrame, failing closed on read errors. */
def readEligibleParquet(paths: Seq[String]): DataFrame = {
  Try(spark.read.parquet(paths: _*)) match {
    case Success(df) => df
    case Failure(error) =>
      stopNow(2, s"Failed to read eligible parquet data: ${error.getClass.getSimpleName}: ${error.getMessage}")
  }
}

/**
 * Parses metadata JSON and returns normalized valid `(partition, offset)` rows.
 *
 * Invalid eligible rows are categorized and counted before any continuity
 * analytics so bad metadata cannot silently prove the dataset complete.
 */
def normalizeOffsets(input: DataFrame, config: CheckerConfig): (DataFrame, Long, Long) = {
  val inputRows = input.count()
  println(s"$ReconPrefix eligible_row_count=$inputRows")

  if (inputRows == 0L) {
    stopNow(2, "Eligible parquet data contained zero rows")
  }
  if (!input.columns.contains(config.metadataColumn)) {
    stopNow(2, s"Metadata column '${config.metadataColumn}' not found in eligible parquet data")
  }

  val metadataSchema = new StructType()
    .add("partition", StringType, nullable = true)
    .add("offset", StringType, nullable = true)
    .add("_recon_corrupt_record", StringType, nullable = true)

  val numericPattern = "^[0-9]+$"
  val withParsedMetadata = input
    .withColumn("_recon_metadata_raw", quotedColumn(config.metadataColumn).cast(StringType))
    .withColumn(
      "_recon_metadata_json",
      from_json(
        col("_recon_metadata_raw"),
        metadataSchema,
        Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" -> "_recon_corrupt_record")
      )
    )
    .withColumn("_recon_partition_raw", trim(col("_recon_metadata_json.partition").cast(StringType)))
    .withColumn("_recon_offset_raw", trim(col("_recon_metadata_json.offset").cast(StringType)))
    .withColumn("_recon_partition_value", when(col("_recon_partition_raw").rlike(numericPattern), col("_recon_partition_raw").cast(IntegerType)).otherwise(lit(null).cast(IntegerType)))
    .withColumn("_recon_offset_value", when(col("_recon_offset_raw").rlike(numericPattern), col("_recon_offset_raw").cast(LongType)).otherwise(lit(null).cast(LongType)))
    .withColumn("_recon_source_file", input_file_name())

  val missingMetadata = col("_recon_metadata_raw").isNull
  val malformedJson =
    col("_recon_metadata_raw").isNotNull &&
      (col("_recon_metadata_json").isNull || col("_recon_metadata_json._recon_corrupt_record").isNotNull)
  val missingPartition = !missingMetadata && !malformedJson && col("_recon_partition_raw").isNull
  val missingOffset = !missingMetadata && !malformedJson && col("_recon_offset_raw").isNull
  val nonNumericPartition = !missingMetadata && !malformedJson && col("_recon_partition_raw").isNotNull && col("_recon_partition_value").isNull
  val nonNumericOffset = !missingMetadata && !malformedJson && col("_recon_offset_raw").isNotNull && col("_recon_offset_value").isNull
  val invalidRow =
    missingMetadata || malformedJson || missingPartition || missingOffset || nonNumericPartition || nonNumericOffset
  val validRow = !invalidRow

  val quality = withParsedMetadata.agg(
    count(lit(1L)).cast(LongType).as("eligible_row_count"),
    countWhen(missingMetadata, "missing_metadata_count"),
    countWhen(malformedJson, "malformed_json_count"),
    countWhen(missingPartition, "missing_partition_count"),
    countWhen(missingOffset, "missing_offset_count"),
    countWhen(nonNumericPartition, "non_numeric_partition_count"),
    countWhen(nonNumericOffset, "non_numeric_offset_count"),
    countWhen(invalidRow, "invalid_row_count"),
    countWhen(validRow, "valid_offset_row_count")
  )

  println(s"$ReconPrefix metadata_quality_begin")
  quality.show(false)
  println(s"$ReconPrefix metadata_quality_end")

  val qualityRow = quality.collect()(0)
  val invalidRows = qualityRow.getAs[Long]("invalid_row_count")
  val validRows = qualityRow.getAs[Long]("valid_offset_row_count")

  if (validRows == 0L) {
    stopNow(2, "Zero valid partition/offset pairs were extracted from eligible parquet data")
  }

  val normalized = withParsedMetadata
    .filter(validRow)
    .select(
      col("_recon_partition_value").as("partition"),
      col("_recon_offset_value").as("offset"),
      col("_recon_metadata_raw").as("metadata_json"),
      col("_recon_source_file").as("source_file")
    )

  (normalized, invalidRows, validRows)
}

/**
 * Optionally writes normalized offsets to a parquet path and reads them back
 * before analytics. This supports a production HDFS temp path or local `file://`
 * validation with the same Spark code path.
 */
def persistIfConfigured(normalized: DataFrame, config: CheckerConfig): DataFrame = {
  config.normalizedOffsetsPath match {
    case None =>
      println(s"$ReconPrefix normalized_offsets_persisted=false")
      normalized
    case Some(path) =>
      val mode = if (config.normalizedOffsetsOverwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
      println(s"$ReconPrefix normalized_offsets_persisted=true path=$path mode=$mode")
      Try(normalized.write.mode(mode).parquet(path)) match {
        case Success(_) =>
          println(s"$ReconPrefix normalized_offsets_write_complete path=$path")
        case Failure(error) =>
          stopNow(2, s"Failed to write normalized offsets to $path: ${error.getClass.getSimpleName}: ${error.getMessage}")
      }

      val persisted = Try(spark.read.parquet(path)) match {
        case Success(df) =>
          println(s"$ReconPrefix normalized_offsets_read_complete path=$path")
          df
        case Failure(error) =>
          stopNow(2, s"Failed to read normalized offsets from $path: ${error.getClass.getSimpleName}: ${error.getMessage}")
      }

      persisted.select(
        col("partition").cast(IntegerType).as("partition"),
        col("offset").cast(LongType).as("offset"),
        col("metadata_json"),
        col("source_file")
      )
  }
}

/** Formats a bounded sequence of missing offsets as `[1,2,3]`. */
def formatMissingOffsets(offsets: Seq[Long]): String =
  offsets.mkString("[", ",", "]")

/**
 * Builds bounded missing-offset value lists from distinct offsets.
 *
 * Missing values are materialized only for partitions with gaps and only up to
 * `limit` per partition. The `truncated` flag is true when more missing values
 * exist than were printed.
 */
def buildMissingOffsetReports(distinctOffsets: DataFrame, stats: DataFrame, limit: Long): Map[Int, MissingOffsetReport] = {
  val gapStats = stats
    .filter(col("has_gaps"))
    .select(col("partition"), col("missing_offset_count"))
    .collect()

  val offsetsByPartition: Map[Int, Seq[Long]] =
    if (limit == 0L || gapStats.isEmpty) {
      Map.empty[Int, Seq[Long]]
    } else {
      val offsetWindow = Window.partitionBy(col("partition")).orderBy(col("offset").asc)
      val intervalWindow = Window
        .partitionBy(col("partition"))
        .orderBy(col("gap_start").asc, col("gap_end").asc)
        .rowsBetween(Window.unboundedPreceding, -1L)

      val materializedMissing = distinctOffsets
        .withColumn("next_offset", lead(col("offset"), 1).over(offsetWindow))
        .filter(col("next_offset").isNotNull && col("next_offset") > col("offset") + lit(1L))
        .select(
          col("partition"),
          (col("offset") + lit(1L)).as("gap_start"),
          (col("next_offset") - lit(1L)).as("gap_end")
        )
        .withColumn("gap_size", col("gap_end") - col("gap_start") + lit(1L))
        .withColumn("prior_missing_count", coalesce(sum(col("gap_size")).over(intervalWindow), lit(0L)))
        .withColumn("remaining_limit", lit(limit) - col("prior_missing_count"))
        .withColumn("take_count", least(col("gap_size"), col("remaining_limit")))
        .filter(col("take_count") > lit(0L))
        .withColumn("missing_offset", explode(sequence(col("gap_start"), col("gap_start") + col("take_count") - lit(1L))))
        .select(col("partition"), col("missing_offset").cast(LongType).as("missing_offset"))
        .orderBy(col("partition").asc, col("missing_offset").asc)

      materializedMissing
        .collect()
        .groupBy(_.getAs[Int]("partition"))
        .map {
          case (partition, rows) =>
            partition -> rows.map(_.getAs[Long]("missing_offset")).toSeq
        }
    }

  gapStats.map { row =>
    val partition = row.getAs[Int]("partition")
    val missingCount = row.getAs[Long]("missing_offset_count")
    partition -> MissingOffsetReport(
      offsets = offsetsByPartition.getOrElse(partition, Seq.empty[Long]),
      truncated = missingCount > limit
    )
  }.toMap
}

/** Computes, prints, and returns the number of Kafka partitions with gaps. */
def printGapStats(analyticsInput: DataFrame, config: CheckerConfig): Long = {
  val normalizedRowCount = analyticsInput.count()
  if (normalizedRowCount == 0L) {
    stopNow(2, "Normalized offset dataset contained zero rows before analytics")
  }

  val distinctOffsets = analyticsInput.select(col("partition"), col("offset")).distinct()
  val distinctPairCount = distinctOffsets.count()
  println(s"$ReconPrefix normalized_offset_row_count=$normalizedRowCount")
  println(s"$ReconPrefix distinct_partition_offset_count=$distinctPairCount")
  println(s"$ReconPrefix duplicate_offset_row_count=${normalizedRowCount - distinctPairCount}")

  val stats = distinctOffsets
    .groupBy(col("partition"))
    .agg(
      count(lit(1L)).cast(LongType).as("distinct_offset_count"),
      min(col("offset")).cast(LongType).as("min_offset"),
      max(col("offset")).cast(LongType).as("max_offset")
    )
    .withColumn("span", col("max_offset") - col("min_offset") + lit(1L))
    .withColumn("expected_count", col("span"))
    .withColumn("missing_offset_count", col("expected_count") - col("distinct_offset_count"))
    .withColumn("has_gaps", col("missing_offset_count") > lit(0L))
    .select(
      col("partition"),
      col("distinct_offset_count"),
      col("min_offset"),
      col("max_offset"),
      col("span"),
      col("expected_count"),
      col("missing_offset_count"),
      col("has_gaps")
    )
    .orderBy(col("partition").asc)

  val rows = stats.collect()
  if (rows.isEmpty) {
    stopNow(2, "No Kafka partitions were available for gap analytics")
  }
  val missingOffsetReports = buildMissingOffsetReports(distinctOffsets, stats, config.missingOffsetsLimit)

  println(s"$ReconPrefix partition_gap_stats_begin")
  rows.foreach { row =>
    val report = missingOffsetReports.getOrElse(row.getAs[Int]("partition"), MissingOffsetReport(Seq.empty[Long], truncated = false))
    println(
      s"$ReconPrefix partition=${row.getAs[Int]("partition")} " +
        s"distinct_offset_count=${row.getAs[Long]("distinct_offset_count")} " +
        s"min_offset=${row.getAs[Long]("min_offset")} " +
        s"max_offset=${row.getAs[Long]("max_offset")} " +
        s"span=${row.getAs[Long]("span")} " +
        s"expected_count=${row.getAs[Long]("expected_count")} " +
        s"missing_offset_count=${row.getAs[Long]("missing_offset_count")} " +
        s"has_gaps=${row.getAs[Boolean]("has_gaps")} " +
        s"missing_offsets=${formatMissingOffsets(report.offsets)} " +
        s"missing_offsets_limit=${config.missingOffsetsLimit} " +
        s"missing_offsets_truncated=${report.truncated}"
    )
  }
  println(s"$ReconPrefix partition_gap_stats_end")

  val gapCount = rows.count(_.getAs[Boolean]("has_gaps")).toLong
  println(s"$ReconPrefix gap_partition_count=$gapCount")
  if (gapCount > 0L) {
    println(s"$ReconPrefix gap_partitions_begin")
    rows.filter(_.getAs[Boolean]("has_gaps")).foreach { row =>
      val report = missingOffsetReports(row.getAs[Int]("partition"))
      println(
        s"$ReconPrefix gap_partition=${row.getAs[Int]("partition")} " +
          s"missing_offset_count=${row.getAs[Long]("missing_offset_count")} " +
          s"min_offset=${row.getAs[Long]("min_offset")} " +
          s"max_offset=${row.getAs[Long]("max_offset")} " +
          s"missing_offsets=${formatMissingOffsets(report.offsets)} " +
          s"missing_offsets_limit=${config.missingOffsetsLimit} " +
          s"missing_offsets_truncated=${report.truncated}"
      )
    }
    println(s"$ReconPrefix gap_partitions_end")
  }

  gapCount
}

/** Emits the final PASS/FAIL line and exits or throws according to config. */
def finish(config: CheckerConfig, code: Int, reasons: Seq[String]): Unit = {
  if (reasons.nonEmpty) {
    Console.err.println(s"$ReconPrefix ERROR: ${reasons.mkString("; ")}")
    println(s"$ReconPrefix RESULT: FAIL ${reasons.mkString("; ")}")
  } else {
    println(s"$ReconPrefix RESULT: PASS no gaps detected")
  }

  if (config.exitOnCompletion) {
    System.exit(code)
  } else if (code != 0) {
    throw new RuntimeException(reasons.mkString("; "))
  }
}

val config = loadConfig()
printConfig(config)

val scans = config.inputRoots.map(scanRoot(_, config))
printScan(scans)

val eligiblePartitions = scans.flatMap(_.eligible)
if (eligiblePartitions.isEmpty) {
  val skippedRunDateCount = scans.map(_.skippedRunDate.size).sum
  val detail =
    if (skippedRunDateCount > 0) s"; skipped_run_date_partition_count=$skippedRunDateCount"
    else ""
  stopNow(2, s"No eligible old date partition directories found$detail")
}

val eligiblePaths = eligiblePartitions.map(_.path).distinct.sorted
println(s"$ReconPrefix eligible_path_count=${eligiblePaths.size}")

val parquetInput = readEligibleParquet(eligiblePaths)
val (normalizedOffsets, invalidRows, validRows) = normalizeOffsets(parquetInput, config)
println(s"$ReconPrefix valid_offset_row_count=$validRows")

val analyticsInput = persistIfConfigured(normalizedOffsets, config)
val gapCount = printGapStats(analyticsInput, config)

val failureReasons = scala.collection.mutable.ArrayBuffer.empty[String]
if (invalidRows > 0L && config.failOnInvalidRows) {
  failureReasons += s"invalid metadata rows detected: invalid_row_count=$invalidRows"
}
if (gapCount > 0L && config.failOnGaps) {
  failureReasons += s"offset gaps detected: gap_partition_count=$gapCount"
}

finish(config, if (failureReasons.nonEmpty) 1 else 0, failureReasons.toSeq)
