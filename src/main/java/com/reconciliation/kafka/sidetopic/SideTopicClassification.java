package com.reconciliation.kafka.sidetopic;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Bucketed explanation of missing offsets using canary and dead-letter records.
 */
@Getter
@RequiredArgsConstructor
public final class SideTopicClassification {
    /**
     * Source Kafka topic whose gaps were classified.
     */
    private final String sourceTopic;
    /**
     * Missing offsets explained by records in the canary topic.
     */
    private final Map<Integer, List<Long>> canaryExplainedOffsets;
    /**
     * Missing offsets explained by records in the dead-letter topic.
     */
    private final Map<Integer, List<Long>> deadLetterExplainedOffsets;
    /**
     * Missing offsets not found in any configured side topic.
     */
    private final Map<Integer, List<Long>> unresolvedOffsets;
    /**
     * Count of missing offsets explained by canary records.
     */
    private final long canaryExplainedCount;
    /**
     * Count of missing offsets explained by dead-letter records.
     */
    private final long deadLetterExplainedCount;
    /**
     * Count of missing offsets not explained by side-topic records.
     */
    private final long unresolvedCount;
    /**
     * Number of raw parquet partitions with gaps before side-topic matching.
     */
    private final long rawGapPartitionCount;
    /**
     * Number of bounded missing offsets available for exact side-topic matching.
     */
    private final long boundedMissingOffsetCount;
    /**
     * Total decoded canary records read from Kafka.
     */
    private final long canaryRecordCount;
    /**
     * Total decoded dead-letter records read from Kafka.
     */
    private final long deadLetterRecordCount;
    /**
     * Matched dead-letter records with failureEventId present.
     */
    private final long deadLetterFailureEventIdCount;
    /**
     * Matched dead-letter records with reasonMsg present.
     */
    private final long deadLetterReasonMsgCount;
    /**
     * Matched dead-letter records with exception present.
     */
    private final long deadLetterExceptionCount;
    /**
     * Whether gap analytics truncated the missing-offset set before matching.
     */
    private final boolean missingOffsetsTruncated;
}
