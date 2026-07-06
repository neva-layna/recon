package com.reconciliation.kafka.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.model.MissingOffsetReport;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconReporter;
import com.reconciliation.kafka.support.RowValues;

import lombok.experimental.UtilityClass;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lead;
import static org.apache.spark.sql.functions.least;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sequence;
import static org.apache.spark.sql.functions.sum;

/**
 * Computes partition-level Kafka offset statistics from normalized parquet rows.
 */
@UtilityClass
public final class OffsetAnalytics {
    /**
     * Prints row counts, duplicate counts, per-partition gap statistics, and gap
     * summaries for normalized partition/offset pairs.
     *
     * @param analyticsInput normalized rows with partition and offset columns
     * @param config checker configuration controlling output limits
     * @return gap count and bounded missing-offset reports by partition
     * @throws com.reconciliation.kafka.support.ReconExit when analytics has no
     *         usable rows
     */
    public static GapAnalysisResult printGapStats(Dataset<Row> analyticsInput, CheckerConfig config) {
        long normalizedRowCount = analyticsInput.count();
        if (normalizedRowCount == 0L) {
            ReconReporter.stopNow(2, "Normalized offset dataset contained zero rows before analytics");
        }

        Dataset<Row> distinctOffsets = analyticsInput.select(col("partition"), col("offset")).distinct();
        long distinctPairCount = distinctOffsets.count();
        ReconReporter.info(ReconConstants.RECON_PREFIX + " normalized_offset_row_count=" + normalizedRowCount);
        ReconReporter.info(ReconConstants.RECON_PREFIX + " distinct_partition_offset_count=" + distinctPairCount);
        ReconReporter.info(ReconConstants.RECON_PREFIX + " duplicate_offset_row_count=" + (normalizedRowCount - distinctPairCount));

        Dataset<Row> stats = distinctOffsets
            .groupBy(col("partition"))
            .agg(
                count(lit(1L)).cast(DataTypes.LongType).as("distinct_offset_count"),
                min(col("offset")).cast(DataTypes.LongType).as("min_offset"),
                max(col("offset")).cast(DataTypes.LongType).as("max_offset")
            )
            .withColumn("span", col("max_offset").minus(col("min_offset")).plus(lit(1L)))
            .withColumn("expected_count", col("span"))
            .withColumn("missing_offset_count", col("expected_count").minus(col("distinct_offset_count")))
            .withColumn("has_gaps", col("missing_offset_count").gt(lit(0L)))
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
            .orderBy(col("partition").asc());

        List<Row> rows = stats.collectAsList();
        if (rows.isEmpty()) {
            ReconReporter.stopNow(2, "No Kafka partitions were available for gap analytics");
        }
        Map<Integer, MissingOffsetReport> missingOffsetReports =
            buildMissingOffsetReports(distinctOffsets, stats, config.getMissingOffsetsLimit());

        ReconReporter.info(ReconConstants.RECON_PREFIX + " partition_gap_stats_begin");
        for (Row row : rows) {
            int partition = RowValues.getInt(row, "partition");
            MissingOffsetReport report = missingOffsetReports.containsKey(partition)
                ? missingOffsetReports.get(partition)
                : new MissingOffsetReport(Collections.emptyList(), false);
            ReconReporter.info(
                ReconConstants.RECON_PREFIX + " partition=" + partition
                    + " distinct_offset_count=" + RowValues.getLong(row, "distinct_offset_count")
                    + " min_offset=" + RowValues.getLong(row, "min_offset")
                    + " max_offset=" + RowValues.getLong(row, "max_offset")
                    + " span=" + RowValues.getLong(row, "span")
                    + " expected_count=" + RowValues.getLong(row, "expected_count")
                    + " missing_offset_count=" + RowValues.getLong(row, "missing_offset_count")
                    + " has_gaps=" + RowValues.getBoolean(row, "has_gaps")
                    + " missing_offsets=" + formatMissingOffsets(report.getOffsets())
                    + " missing_offsets_limit=" + config.getMissingOffsetsLimit()
                    + " missing_offsets_truncated=" + report.isTruncated()
            );
        }
        ReconReporter.info(ReconConstants.RECON_PREFIX + " partition_gap_stats_end");

        long gapCount = 0L;
        for (Row row : rows) {
            if (RowValues.getBoolean(row, "has_gaps")) {
                gapCount++;
            }
        }
        ReconReporter.info(ReconConstants.RECON_PREFIX + " gap_partition_count=" + gapCount);
        if (gapCount > 0L) {
            ReconReporter.info(ReconConstants.RECON_PREFIX + " gap_partitions_begin");
            for (Row row : rows) {
                if (RowValues.getBoolean(row, "has_gaps")) {
                    int partition = RowValues.getInt(row, "partition");
                    MissingOffsetReport report = missingOffsetReports.get(partition);
                    ReconReporter.info(
                        ReconConstants.RECON_PREFIX + " gap_partition=" + partition
                            + " missing_offset_count=" + RowValues.getLong(row, "missing_offset_count")
                            + " min_offset=" + RowValues.getLong(row, "min_offset")
                            + " max_offset=" + RowValues.getLong(row, "max_offset")
                            + " missing_offsets=" + formatMissingOffsets(report.getOffsets())
                            + " missing_offsets_limit=" + config.getMissingOffsetsLimit()
                            + " missing_offsets_truncated=" + report.isTruncated()
                    );
                }
            }
            ReconReporter.info(ReconConstants.RECON_PREFIX + " gap_partitions_end");
        }

        return new GapAnalysisResult(gapCount, missingOffsetReports);
    }

    /**
     * Builds ordered missing-offset reports for partitions that have gaps.
     *
     * @param distinctOffsets distinct partition/offset pairs
     * @param stats per-partition statistics containing has_gaps and
     *        missing_offset_count
     * @param limit maximum concrete offsets to materialize per partition
     * @return reports keyed by partition, including truncation flags
     */
    public static Map<Integer, MissingOffsetReport> buildMissingOffsetReports(
        Dataset<Row> distinctOffsets,
        Dataset<Row> stats,
        long limit
    ) {
        List<Row> gapStats = stats
            .filter(col("has_gaps"))
            .select(col("partition"), col("missing_offset_count"))
            .collectAsList();

        Map<Integer, List<Long>> offsetsByPartition = new HashMap<>();
        if (limit != 0L && !gapStats.isEmpty()) {
            WindowSpec offsetWindow = Window.partitionBy(col("partition")).orderBy(col("offset").asc());
            WindowSpec intervalWindow = Window
                .partitionBy(col("partition"))
                .orderBy(col("gap_start").asc(), col("gap_end").asc())
                .rowsBetween(Window.unboundedPreceding(), -1L);

            Dataset<Row> materializedMissing = distinctOffsets
                .withColumn("next_offset", lead(col("offset"), 1).over(offsetWindow))
                .filter(col("next_offset").isNotNull().and(col("next_offset").gt(col("offset").plus(lit(1L)))))
                .select(
                    col("partition"),
                    col("offset").plus(lit(1L)).as("gap_start"),
                    col("next_offset").minus(lit(1L)).as("gap_end")
                )
                .withColumn("gap_size", col("gap_end").minus(col("gap_start")).plus(lit(1L)))
                .withColumn("prior_missing_count", coalesce(sum(col("gap_size")).over(intervalWindow), lit(0L)))
                .withColumn("remaining_limit", lit(limit).minus(col("prior_missing_count")))
                .withColumn("take_count", least(col("gap_size"), col("remaining_limit")))
                .filter(col("take_count").gt(lit(0L)))
                .withColumn("missing_offset", explode(sequence(col("gap_start"), col("gap_start").plus(col("take_count")).minus(lit(1L)))))
                .select(col("partition"), col("missing_offset").cast(DataTypes.LongType).as("missing_offset"))
                .orderBy(col("partition").asc(), col("missing_offset").asc());

            for (Row row : materializedMissing.collectAsList()) {
                int partition = RowValues.getInt(row, "partition");
                Long missingOffset = RowValues.getLong(row, "missing_offset");
                if (!offsetsByPartition.containsKey(partition)) {
                    offsetsByPartition.put(partition, new ArrayList<>());
                }
                offsetsByPartition.get(partition).add(missingOffset);
            }
        }

        Map<Integer, MissingOffsetReport> reports = new LinkedHashMap<>();
        for (Row row : gapStats) {
            int partition = RowValues.getInt(row, "partition");
            long missingCount = RowValues.getLong(row, "missing_offset_count");
            List<Long> offsets = offsetsByPartition.containsKey(partition)
                ? offsetsByPartition.get(partition)
                : Collections.emptyList();
            reports.put(partition, new MissingOffsetReport(offsets, missingCount > limit));
        }
        return reports;
    }

    /**
     * Formats missing offsets as the compact bracketed list used in recon logs.
     *
     * @param offsets missing offsets in display order
     * @return bracketed comma-separated list without spaces
     */
    public static String formatMissingOffsets(List<Long> offsets) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < offsets.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(offsets.get(i));
        }
        builder.append(']');
        return builder.toString();
    }
}
