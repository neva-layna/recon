package com.reconciliation.kafka.sidetopic;

import java.util.List;
import java.util.Map;

import lombok.RequiredArgsConstructor;

/**
 * Bucketed explanation of missing offsets using canary and dead-letter records.
 */
@RequiredArgsConstructor
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
     * Number of raw parquet partitions with gaps before side-topic matching.
     */
    public final long rawGapPartitionCount;
    /**
     * Number of bounded missing offsets available for exact side-topic matching.
     */
    public final long boundedMissingOffsetCount;
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
}
