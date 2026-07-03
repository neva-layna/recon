package com.reconciliation.kafka;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.reconciliation.kafka.analytics.OffsetAnalytics;
import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.ConfigLoader;
import com.reconciliation.kafka.config.SparkConfLookup;
import com.reconciliation.kafka.metadata.MetadataNormalizer;
import com.reconciliation.kafka.model.EligiblePartition;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.model.NormalizeResult;
import com.reconciliation.kafka.model.RootScan;
import com.reconciliation.kafka.scan.PartitionScanner;
import com.reconciliation.kafka.sidetopic.SideTopicClassification;
import com.reconciliation.kafka.sidetopic.SideTopicReconciler;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconExit;
import com.reconciliation.kafka.support.ReconReporter;

/**
 * Spark application entrypoint for scanning Kafka metadata in parquet partitions
 * and reporting offset gaps.
 */
public final class KafkaOffsetGapChecker {
    /**
     * Prevents construction of the command-line application class.
     */
    private KafkaOffsetGapChecker() {
    }

    /**
     * Starts the Spark session, runs reconciliation, stops Spark, and exits with
     * the code requested by the checker.
     *
     * @param args command-line arguments accepted by Spark but not read here
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Kafka Offset Gap Checker")
            .getOrCreate();

        ReconExit requestedExit = null;
        try {
            run(spark);
        } catch (ReconExit exit) {
            requestedExit = exit;
        } finally {
            spark.stop();
        }

        if (requestedExit != null && requestedExit.exitJvm) {
            System.exit(requestedExit.code);
        }
    }

    /**
     * Executes the full checker flow: load config, scan eligible partitions,
     * normalize metadata, analyze gaps, optionally reconcile side topics, and
     * report the final result.
     *
     * @param spark active Spark session used for configuration, filesystem, and
     *        DataFrame work
     * @throws ReconExit when the run should stop with an operator-visible exit
     *         code
     */
    static void run(SparkSession spark) {
        CheckerConfig config = ConfigLoader.loadConfig(
            new SparkConfLookup(spark),
            new Supplier<LocalDate>() {
                @Override
                public LocalDate get() {
                    return LocalDate.now(ZoneId.systemDefault());
                }
            }
        );
        ReconReporter.printConfig(config);

        List<RootScan> scans = new ArrayList<RootScan>();
        for (String root : config.inputRoots) {
            scans.add(PartitionScanner.scanRoot(spark, root, config));
        }
        PartitionScanner.printScan(scans);

        List<EligiblePartition> eligiblePartitions = new ArrayList<EligiblePartition>();
        for (RootScan scan : scans) {
            eligiblePartitions.addAll(scan.eligible);
        }
        if (eligiblePartitions.isEmpty()) {
            int skippedRunDateCount = 0;
            for (RootScan scan : scans) {
                skippedRunDateCount += scan.skippedRunDate.size();
            }
            String detail = skippedRunDateCount > 0
                ? "; skipped_run_date_partition_count=" + skippedRunDateCount
                : "";
            ReconReporter.stopNow(2, "No eligible old date partition directories found" + detail);
        }

        List<String> eligiblePaths = PartitionScanner.distinctSortedPaths(eligiblePartitions);
        System.out.println(ReconConstants.RECON_PREFIX + " eligible_path_count=" + eligiblePaths.size());

        Dataset<Row> parquetInput = MetadataNormalizer.readEligibleParquet(spark, eligiblePaths);
        NormalizeResult normalizeResult = MetadataNormalizer.normalizeOffsets(parquetInput, config);
        System.out.println(ReconConstants.RECON_PREFIX + " valid_offset_row_count=" + normalizeResult.validRows);

        Dataset<Row> analyticsInput = MetadataNormalizer.persistIfConfigured(spark, normalizeResult.normalizedOffsets, config);
        GapAnalysisResult gapResult = OffsetAnalytics.printGapStats(analyticsInput, config);
        Optional<SideTopicClassification> sideTopicClassification =
            SideTopicReconciler.reconcileIfConfigured(spark, config, gapResult);

        List<String> failureReasons = new ArrayList<String>();
        if (normalizeResult.invalidRows > 0L && config.failOnInvalidRows) {
            failureReasons.add("invalid metadata rows detected: invalid_row_count=" + normalizeResult.invalidRows);
        }
        Optional<String> gapFailureReason = gapFailureReason(config, gapResult, sideTopicClassification);
        if (gapFailureReason.isPresent()) {
            failureReasons.add(gapFailureReason.get());
        }

        int finalCode = failureReasons.isEmpty() ? 0 : 1;
        String passMessage = passMessage(config, gapResult, sideTopicClassification);
        printFinalExitDecision(config, gapResult, sideTopicClassification, finalCode, failureReasons, passMessage);
        ReconReporter.finish(config, finalCode, failureReasons, passMessage);
    }

    /**
     * Determines whether gap analytics should fail the run after optional
     * side-topic classification has had a chance to explain bounded missing
     * offsets.
     *
     * @param config resolved checker configuration
     * @param gapResult raw parquet gap analytics
     * @param sideTopicClassification optional side-topic classification result
     * @return failure reason when gap-related exit code {@code 1} is required
     */
    static Optional<String> gapFailureReason(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification
    ) {
        if (!config.failOnGaps || gapResult.gapPartitionCount == 0L) {
            return Optional.empty();
        }

        if (sideTopicClassification.isPresent()) {
            SideTopicClassification classification = sideTopicClassification.get();
            if (classification.unresolvedCount > 0L) {
                return Optional.of(
                    "unresolved offset gaps after side-topic reconciliation: raw_gap_partition_count="
                        + gapResult.gapPartitionCount
                        + " unresolved_count=" + classification.unresolvedCount
                );
            }
            if (classification.missingOffsetsTruncated) {
                return Optional.of(
                    "missing offsets truncated after side-topic reconciliation: unresolved offsets may remain beyond materialized limit"
                        + "; raw_gap_partition_count=" + gapResult.gapPartitionCount
                        + " bounded_missing_offset_count=" + classification.boundedMissingOffsetCount
                        + " missing_offsets_truncated=true"
                        + " unresolved_count=0"
                );
            }
            return Optional.empty();
        }

        return Optional.of("offset gaps detected: gap_partition_count=" + gapResult.gapPartitionCount);
    }

    /**
     * Builds the success result text so side-topic-resolved raw gaps are not
     * reported as if no raw parquet gaps were found.
     *
     * @param config resolved checker configuration
     * @param gapResult raw parquet gap analytics
     * @param sideTopicClassification optional side-topic classification result
     * @return operator-facing pass reason
     */
    private static String passMessage(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification
    ) {
        if (gapResult.gapPartitionCount > 0L && sideTopicClassification.isPresent()) {
            return "side-topic reconciliation resolved all bounded missing offsets";
        }
        if (gapResult.gapPartitionCount > 0L && !config.failOnGaps) {
            return "offset gaps detected but recon.failOnGaps=false";
        }
        return "no gaps detected";
    }

    /**
     * Emits a compact final decision line before the stable RESULT line.
     *
     * @param config resolved checker configuration
     * @param gapResult raw parquet gap analytics
     * @param sideTopicClassification optional side-topic classification result
     * @param finalCode final process-style exit code
     * @param failureReasons failure reasons collected for the run
     * @param passMessage pass reason used when no failure reason exists
     */
    private static void printFinalExitDecision(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification,
        int finalCode,
        List<String> failureReasons,
        String passMessage
    ) {
        String reason = failureReasons.isEmpty()
            ? passMessage
            : String.join("; ", failureReasons);
        StringBuilder builder = new StringBuilder(ReconConstants.RECON_PREFIX)
            .append(" final_exit_decision")
            .append(" code=").append(finalCode)
            .append(" reason=").append(reason.replace(' ', '_'))
            .append(" fail_on_gaps=").append(config.failOnGaps)
            .append(" raw_gap_partition_count=").append(gapResult.gapPartitionCount)
            .append(" side_topic_enabled=").append(sideTopicClassification.isPresent());
        if (sideTopicClassification.isPresent()) {
            SideTopicClassification classification = sideTopicClassification.get();
            builder
                .append(" canary_explained_count=").append(classification.canaryExplainedCount)
                .append(" dead_letter_explained_count=").append(classification.deadLetterExplainedCount)
                .append(" unresolved_count=").append(classification.unresolvedCount)
                .append(" bounded_missing_offset_count=").append(classification.boundedMissingOffsetCount)
                .append(" missing_offsets_truncated=").append(classification.missingOffsetsTruncated);
        }
        System.out.println(builder.toString());
    }
}
