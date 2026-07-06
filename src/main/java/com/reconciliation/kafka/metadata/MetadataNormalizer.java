package com.reconciliation.kafka.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.NormalizeResult;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconReporter;
import com.reconciliation.kafka.support.RowValues;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * Reads eligible parquet partitions and normalizes embedded Kafka metadata into
 * partition/offset rows.
 */
public final class MetadataNormalizer {
    /**
     * Numeric-only pattern used before casting metadata strings to Spark numbers.
     */
    private static final String NUMERIC_PATTERN = "^[0-9]+$";

    /**
     * Prevents construction of the metadata utility.
     */
    private MetadataNormalizer() {
    }

    /**
     * Reads all eligible parquet paths as a single Spark dataset.
     *
     * @param spark active Spark session
     * @param paths parquet paths selected by the partition scanner
     * @return input parquet dataset
     * @throws com.reconciliation.kafka.support.ReconExit when Spark cannot read
     *         the selected paths
     */
    public static Dataset<Row> readEligibleParquet(SparkSession spark, List<String> paths) {
        try {
            return spark.read().parquet(paths.toArray(new String[paths.size()]));
        } catch (Exception error) {
            ReconReporter.stopNow(2, "Failed to read eligible parquet data: " + error.getClass().getSimpleName() + ": " + error.getMessage());
            return null;
        }
    }

    /**
     * Parses the configured metadata JSON column, reports quality counts, and
     * returns only valid partition/offset rows.
     *
     * @param input eligible parquet rows
     * @param config checker configuration with metadata column settings
     * @return normalized offsets plus valid and invalid row counts
     * @throws com.reconciliation.kafka.support.ReconExit when input data is
     *         empty, lacks the metadata column, or has no valid offsets
     */
    public static NormalizeResult normalizeOffsets(Dataset<Row> input, CheckerConfig config) {
        long inputRows = input.count();
        ReconReporter.info(ReconConstants.RECON_PREFIX + " eligible_row_count=" + inputRows);

        if (inputRows == 0L) {
            ReconReporter.stopNow(2, "Eligible parquet data contained zero rows");
        }
        if (!Arrays.asList(input.columns()).contains(config.metadataColumn)) {
            ReconReporter.stopNow(2, "Metadata column '" + config.metadataColumn + "' not found in eligible parquet data");
        }

        StructType metadataSchema = new StructType()
            .add("partition", DataTypes.StringType, true)
            .add("offset", DataTypes.StringType, true)
            .add("_recon_corrupt_record", DataTypes.StringType, true);

        Map<String, String> jsonOptions = new HashMap<String, String>();
        jsonOptions.put("mode", "PERMISSIVE");
        jsonOptions.put("columnNameOfCorruptRecord", "_recon_corrupt_record");

        Dataset<Row> withParsedMetadata = input
            .withColumn("_recon_metadata_raw", quotedColumn(config.metadataColumn).cast(DataTypes.StringType))
            .withColumn("_recon_metadata_json", from_json(col("_recon_metadata_raw"), metadataSchema, jsonOptions))
            .withColumn("_recon_partition_raw", trim(col("_recon_metadata_json.partition").cast(DataTypes.StringType)))
            .withColumn("_recon_offset_raw", trim(col("_recon_metadata_json.offset").cast(DataTypes.StringType)))
            .withColumn(
                "_recon_partition_value",
                when(col("_recon_partition_raw").rlike(NUMERIC_PATTERN), col("_recon_partition_raw").cast(DataTypes.IntegerType))
                    .otherwise(lit(null).cast(DataTypes.IntegerType))
            )
            .withColumn(
                "_recon_offset_value",
                when(col("_recon_offset_raw").rlike(NUMERIC_PATTERN), col("_recon_offset_raw").cast(DataTypes.LongType))
                    .otherwise(lit(null).cast(DataTypes.LongType))
            )
            .withColumn("_recon_source_file", input_file_name());

        Column missingMetadata = col("_recon_metadata_raw").isNull();
        Column malformedJson = col("_recon_metadata_raw").isNotNull()
            .and(col("_recon_metadata_json").isNull().or(col("_recon_metadata_json._recon_corrupt_record").isNotNull()));
        Column missingPartition = not(missingMetadata).and(not(malformedJson)).and(col("_recon_partition_raw").isNull());
        Column missingOffset = not(missingMetadata).and(not(malformedJson)).and(col("_recon_offset_raw").isNull());
        Column nonNumericPartition = not(missingMetadata).and(not(malformedJson))
            .and(col("_recon_partition_raw").isNotNull())
            .and(col("_recon_partition_value").isNull());
        Column nonNumericOffset = not(missingMetadata).and(not(malformedJson))
            .and(col("_recon_offset_raw").isNotNull())
            .and(col("_recon_offset_value").isNull());
        Column invalidRow = missingMetadata
            .or(malformedJson)
            .or(missingPartition)
            .or(missingOffset)
            .or(nonNumericPartition)
            .or(nonNumericOffset);
        Column validRow = not(invalidRow);

        Dataset<Row> quality = withParsedMetadata.agg(
            count(lit(1L)).cast(DataTypes.LongType).as("eligible_row_count"),
            countWhen(missingMetadata, "missing_metadata_count"),
            countWhen(malformedJson, "malformed_json_count"),
            countWhen(missingPartition, "missing_partition_count"),
            countWhen(missingOffset, "missing_offset_count"),
            countWhen(nonNumericPartition, "non_numeric_partition_count"),
            countWhen(nonNumericOffset, "non_numeric_offset_count"),
            countWhen(invalidRow, "invalid_row_count"),
            countWhen(validRow, "valid_offset_row_count")
        );

        ReconReporter.info(ReconConstants.RECON_PREFIX + " metadata_quality_begin");
        quality.show(false);
        ReconReporter.info(ReconConstants.RECON_PREFIX + " metadata_quality_end");

        Row qualityRow = quality.collectAsList().get(0);
        long invalidRows = RowValues.getLong(qualityRow, "invalid_row_count");
        long validRows = RowValues.getLong(qualityRow, "valid_offset_row_count");

        if (validRows == 0L) {
            ReconReporter.stopNow(2, "Zero valid partition/offset pairs were extracted from eligible parquet data");
        }

        Dataset<Row> normalized = withParsedMetadata
            .filter(validRow)
            .select(
                col("_recon_partition_value").as("partition"),
                col("_recon_offset_value").as("offset"),
                col("_recon_metadata_raw").as("metadata_json"),
                col("_recon_source_file").as("source_file")
            );

        return new NormalizeResult(normalized, invalidRows, validRows);
    }

    /**
     * Builds a Spark column reference that safely quotes arbitrary column names.
     *
     * @param name source column name
     * @return quoted Spark SQL column
     */
    public static Column quotedColumn(String name) {
        return col("`" + name.replace("`", "``") + "`");
    }

    /**
     * Produces a long count aggregation for rows matching a condition.
     *
     * @param condition Spark boolean condition to count
     * @param alias output column alias
     * @return aggregation column cast to long
     */
    public static Column countWhen(Column condition, String alias) {
        return sum(when(condition, lit(1L)).otherwise(lit(0L))).cast(DataTypes.LongType).as(alias);
    }

    /**
     * Optionally writes normalized offsets to parquet and re-reads them for
     * downstream analytics.
     *
     * @param spark active Spark session
     * @param normalized normalized partition/offset rows
     * @param config checker configuration with optional persistence path
     * @return original dataset when persistence is disabled, otherwise the
     *         persisted dataset projected back to the expected schema
     * @throws com.reconciliation.kafka.support.ReconExit when configured write or
     *         readback fails
     */
    public static Dataset<Row> persistIfConfigured(SparkSession spark, Dataset<Row> normalized, CheckerConfig config) {
        if (!config.normalizedOffsetsPath.isPresent()) {
            ReconReporter.info(ReconConstants.RECON_PREFIX + " normalized_offsets_persisted=false");
            return normalized;
        }

        String path = config.normalizedOffsetsPath.get();
        SaveMode mode = config.normalizedOffsetsOverwrite ? SaveMode.Overwrite : SaveMode.ErrorIfExists;
        ReconReporter.info(ReconConstants.RECON_PREFIX + " normalized_offsets_persisted=true path=" + path + " mode=" + mode);
        try {
            normalized.write().mode(mode).parquet(path);
            ReconReporter.info(ReconConstants.RECON_PREFIX + " normalized_offsets_write_complete path=" + path);
        } catch (Exception error) {
            ReconReporter.stopNow(2, "Failed to write normalized offsets to " + path + ": " + error.getClass().getSimpleName() + ": " + error.getMessage());
        }

        try {
            Dataset<Row> persisted = spark.read().parquet(path);
            ReconReporter.info(ReconConstants.RECON_PREFIX + " normalized_offsets_read_complete path=" + path);
            return persisted.select(
                col("partition").cast(DataTypes.IntegerType).as("partition"),
                col("offset").cast(DataTypes.LongType).as("offset"),
                col("metadata_json"),
                col("source_file")
            );
        } catch (Exception error) {
            ReconReporter.stopNow(2, "Failed to read normalized offsets from " + path + ": " + error.getClass().getSimpleName() + ": " + error.getMessage());
            return null;
        }
    }
}
