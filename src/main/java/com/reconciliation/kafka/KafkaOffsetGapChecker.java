package com.reconciliation.kafka;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
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
import com.reconciliation.kafka.model.NormalizeResult;
import com.reconciliation.kafka.model.RootScan;
import com.reconciliation.kafka.scan.PartitionScanner;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconExit;
import com.reconciliation.kafka.support.ReconReporter;

public final class KafkaOffsetGapChecker {
    private KafkaOffsetGapChecker() {
    }

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
        long gapCount = OffsetAnalytics.printGapStats(analyticsInput, config);

        List<String> failureReasons = new ArrayList<String>();
        if (normalizeResult.invalidRows > 0L && config.failOnInvalidRows) {
            failureReasons.add("invalid metadata rows detected: invalid_row_count=" + normalizeResult.invalidRows);
        }
        if (gapCount > 0L && config.failOnGaps) {
            failureReasons.add("offset gaps detected: gap_partition_count=" + gapCount);
        }

        ReconReporter.finish(config, failureReasons.isEmpty() ? 0 : 1, failureReasons);
    }
}
