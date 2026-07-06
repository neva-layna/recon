package com.reconciliation.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import com.reconciliation.kafka.analytics.OffsetAnalyticsService;
import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.CheckerConfigurationService;
import com.reconciliation.kafka.decision.ExitDecisionService;
import com.reconciliation.kafka.metadata.MetadataNormalizerService;
import com.reconciliation.kafka.model.EligiblePartition;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.model.NormalizeResult;
import com.reconciliation.kafka.model.RootScan;
import com.reconciliation.kafka.scan.PartitionScanService;
import com.reconciliation.kafka.sidetopic.SideTopicClassification;
import com.reconciliation.kafka.sidetopic.SideTopicReconciliationService;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconReporterService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Spring-managed orchestration for one offset-gap checker run.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaOffsetGapCheckerRunner implements CheckerJob {
    private final CheckerConfigurationService configurationService;
    private final PartitionScanService partitionScanService;
    private final MetadataNormalizerService metadataNormalizerService;
    private final OffsetAnalyticsService offsetAnalyticsService;
    private final SideTopicReconciliationService sideTopicReconciliationService;
    private final ExitDecisionService exitDecisionService;
    private final ReconReporterService reporterService;

    /**
     * Executes the full checker flow: load config, scan eligible partitions,
     * normalize metadata, analyze gaps, optionally reconcile side topics, and
     * report the final result.
     */
    @Override
    public void run() {
        log.debug("Starting Kafka offset gap checker runner");
        CheckerConfig config = configurationService.load();
        reporterService.printConfig(config);

        List<RootScan> scans = partitionScanService.scan(config);
        List<EligiblePartition> eligiblePartitions = eligiblePartitions(scans);
        if (eligiblePartitions.isEmpty()) {
            reporterService.stopNow(2, "No eligible old date partition directories found" + skippedRunDateDetail(scans));
        }

        List<String> eligiblePaths = partitionScanService.distinctSortedPaths(eligiblePartitions);
        reporterService.info(ReconConstants.RECON_PREFIX + " eligible_path_count=" + eligiblePaths.size());

        Dataset<Row> parquetInput = metadataNormalizerService.readEligibleParquet(eligiblePaths);
        NormalizeResult normalizeResult = metadataNormalizerService.normalizeOffsets(parquetInput, config);
        reporterService.info(ReconConstants.RECON_PREFIX + " valid_offset_row_count=" + normalizeResult.getValidRows());

        Dataset<Row> analyticsInput = metadataNormalizerService.persistIfConfigured(normalizeResult.getNormalizedOffsets(), config);
        GapAnalysisResult gapResult = offsetAnalyticsService.printGapStats(analyticsInput, config);
        Optional<SideTopicClassification> sideTopicClassification =
            sideTopicReconciliationService.reconcileIfConfigured(config, gapResult);

        List<String> failureReasons = new ArrayList<>();
        if (normalizeResult.getInvalidRows() > 0L && config.isFailOnInvalidRows()) {
            failureReasons.add("invalid metadata rows detected: invalid_row_count=" + normalizeResult.getInvalidRows());
        }
        Optional<String> gapFailureReason = exitDecisionService.determineGapFailureReason(
            config,
            gapResult,
            sideTopicClassification
        );
        if (gapFailureReason.isPresent()) {
            failureReasons.add(gapFailureReason.get());
        }

        int finalCode = failureReasons.isEmpty() ? 0 : 1;
        String passMessage = exitDecisionService.passMessage(config, gapResult, sideTopicClassification);
        exitDecisionService.printFinalExitDecision(
            config,
            gapResult,
            sideTopicClassification,
            finalCode,
            failureReasons,
            passMessage
        );
        reporterService.finish(config, finalCode, failureReasons, passMessage);
    }

    private List<EligiblePartition> eligiblePartitions(List<RootScan> scans) {
        List<EligiblePartition> eligiblePartitions = new ArrayList<>();
        for (RootScan scan : scans) {
            eligiblePartitions.addAll(scan.getEligible());
        }
        return eligiblePartitions;
    }

    private String skippedRunDateDetail(List<RootScan> scans) {
        int skippedRunDateCount = 0;
        for (RootScan scan : scans) {
            skippedRunDateCount += scan.getSkippedRunDate().size();
        }
        return skippedRunDateCount > 0
            ? "; skipped_run_date_partition_count=" + skippedRunDateCount
            : "";
    }
}
