package com.reconciliation.kafka.decision;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.sidetopic.SideTopicClassification;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconReporter;

/**
 * Computes final checker pass/fail decisions and operator-facing reason text.
 */
@Component
public class ExitDecisionService {
    /**
     * Instance wrapper for gap-failure decisions.
     *
     * @param config resolved checker configuration
     * @param gapResult raw parquet gap analytics
     * @param sideTopicClassification optional side-topic classification result
     * @return failure reason when gap-related exit code {@code 1} is required
     */
    public Optional<String> determineGapFailureReason(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification
    ) {
        return gapFailureReason(config, gapResult, sideTopicClassification);
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
    public static Optional<String> gapFailureReason(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification
    ) {
        if (!config.isFailOnGaps() || gapResult.getGapPartitionCount() == 0L) {
            return Optional.empty();
        }

        if (sideTopicClassification.isPresent()) {
            SideTopicClassification classification = sideTopicClassification.get();
            if (classification.getUnresolvedCount() > 0L) {
                return Optional.of(
                    "unresolved offset gaps after side-topic reconciliation: raw_gap_partition_count="
                        + gapResult.getGapPartitionCount()
                        + " unresolved_count=" + classification.getUnresolvedCount()
                );
            }
            if (classification.isMissingOffsetsTruncated()) {
                return Optional.of(
                    "missing offsets truncated after side-topic reconciliation: unresolved offsets may remain beyond materialized limit"
                        + "; raw_gap_partition_count=" + gapResult.getGapPartitionCount()
                        + " bounded_missing_offset_count=" + classification.getBoundedMissingOffsetCount()
                        + " missing_offsets_truncated=true"
                        + " unresolved_count=0"
                );
            }
            return Optional.empty();
        }

        return Optional.of("offset gaps detected: gap_partition_count=" + gapResult.getGapPartitionCount());
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
    public String passMessage(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification
    ) {
        if (gapResult.getGapPartitionCount() > 0L && sideTopicClassification.isPresent()) {
            return "side-topic reconciliation resolved all bounded missing offsets";
        }
        if (gapResult.getGapPartitionCount() > 0L && !config.isFailOnGaps()) {
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
    public void printFinalExitDecision(
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
            .append(" fail_on_gaps=").append(config.isFailOnGaps())
            .append(" raw_gap_partition_count=").append(gapResult.getGapPartitionCount())
            .append(" side_topic_enabled=").append(sideTopicClassification.isPresent());
        if (sideTopicClassification.isPresent()) {
            SideTopicClassification classification = sideTopicClassification.get();
            builder
                .append(" canary_explained_count=").append(classification.getCanaryExplainedCount())
                .append(" dead_letter_explained_count=").append(classification.getDeadLetterExplainedCount())
                .append(" unresolved_count=").append(classification.getUnresolvedCount())
                .append(" bounded_missing_offset_count=").append(classification.getBoundedMissingOffsetCount())
                .append(" missing_offsets_truncated=").append(classification.isMissingOffsetsTruncated());
        }
        ReconReporter.info(builder.toString());
    }
}
