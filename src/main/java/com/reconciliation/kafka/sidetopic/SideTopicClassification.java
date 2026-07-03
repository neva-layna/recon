package com.reconciliation.kafka.sidetopic;

import java.util.List;
import java.util.Map;

/**
 * Bucketed explanation of missing offsets using canary and dead-letter records.
 */
public final class SideTopicClassification {
    /**
     * Source Kafka topic whose gaps were classified.
     */
    public final String sourceTopic;
    /**
     * Missing offsets explained by records in the canary topic.
     */
    public final Map<Integer, List<Long>> canaryExplainedOffsets;
    /**
     * Missing offsets explained by records in the dead-letter topic.
     */
    public final Map<Integer, List<Long>> deadLetterExplainedOffsets;
    /**
     * Missing offsets not found in any configured side topic.
     */
    public final Map<Integer, List<Long>> unresolvedOffsets;
    /**
     * Count of missing offsets explained by canary records.
     */
    public final long canaryExplainedCount;
    /**
     * Count of missing offsets explained by dead-letter records.
     */
    public final long deadLetterExplainedCount;
    /**
     * Count of missing offsets not explained by side-topic records.
     */
    public final long unresolvedCount;
    /**
     * Total decoded canary records read from Kafka.
     */
    public final long canaryRecordCount;
    /**
     * Total decoded dead-letter records read from Kafka.
     */
    public final long deadLetterRecordCount;
    /**
     * Matched dead-letter records with failureEventId present.
     */
    public final long deadLetterFailureEventIdCount;
    /**
     * Matched dead-letter records with reasonMsg present.
     */
    public final long deadLetterReasonMsgCount;
    /**
     * Matched dead-letter records with exception present.
     */
    public final long deadLetterExceptionCount;
    /**
     * Whether gap analytics truncated the missing-offset set before matching.
     */
    public final boolean missingOffsetsTruncated;

    /**
     * Creates a side-topic classification result.
     *
     * @param sourceTopic source topic whose gaps were classified
     * @param canaryExplainedOffsets canary matches by partition
     * @param deadLetterExplainedOffsets dead-letter matches by partition
     * @param unresolvedOffsets unmatched missing offsets by partition
     * @param canaryExplainedCount count of canary-explained offsets
     * @param deadLetterExplainedCount count of dead-letter-explained offsets
     * @param unresolvedCount count of unexplained offsets
     * @param canaryRecordCount decoded canary record count
     * @param deadLetterRecordCount decoded dead-letter record count
     * @param deadLetterFailureEventIdCount matched failureEventId count
     * @param deadLetterReasonMsgCount matched reasonMsg count
     * @param deadLetterExceptionCount matched exception count
     * @param missingOffsetsTruncated whether missing offsets were truncated
     */
    public SideTopicClassification(
        String sourceTopic,
        Map<Integer, List<Long>> canaryExplainedOffsets,
        Map<Integer, List<Long>> deadLetterExplainedOffsets,
        Map<Integer, List<Long>> unresolvedOffsets,
        long canaryExplainedCount,
        long deadLetterExplainedCount,
        long unresolvedCount,
        long canaryRecordCount,
        long deadLetterRecordCount,
        long deadLetterFailureEventIdCount,
        long deadLetterReasonMsgCount,
        long deadLetterExceptionCount,
        boolean missingOffsetsTruncated
    ) {
        this.sourceTopic = sourceTopic;
        this.canaryExplainedOffsets = canaryExplainedOffsets;
        this.deadLetterExplainedOffsets = deadLetterExplainedOffsets;
        this.unresolvedOffsets = unresolvedOffsets;
        this.canaryExplainedCount = canaryExplainedCount;
        this.deadLetterExplainedCount = deadLetterExplainedCount;
        this.unresolvedCount = unresolvedCount;
        this.canaryRecordCount = canaryRecordCount;
        this.deadLetterRecordCount = deadLetterRecordCount;
        this.deadLetterFailureEventIdCount = deadLetterFailureEventIdCount;
        this.deadLetterReasonMsgCount = deadLetterReasonMsgCount;
        this.deadLetterExceptionCount = deadLetterExceptionCount;
        this.missingOffsetsTruncated = missingOffsetsTruncated;
    }
}
