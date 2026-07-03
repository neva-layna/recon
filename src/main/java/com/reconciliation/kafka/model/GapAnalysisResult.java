package com.reconciliation.kafka.model;

import java.util.Map;

/**
 * Summary returned by offset gap analytics.
 */
public final class GapAnalysisResult {
    /**
     * Number of Kafka partitions that contain at least one missing offset.
     */
    public final long gapPartitionCount;
    /**
     * Missing-offset reports keyed by Kafka partition id.
     */
    public final Map<Integer, MissingOffsetReport> missingOffsetsByPartition;

    /**
     * Creates an analytics result.
     *
     * @param gapPartitionCount count of partitions with gaps
     * @param missingOffsetsByPartition bounded missing offsets by partition
     */
    public GapAnalysisResult(long gapPartitionCount, Map<Integer, MissingOffsetReport> missingOffsetsByPartition) {
        this.gapPartitionCount = gapPartitionCount;
        this.missingOffsetsByPartition = missingOffsetsByPartition;
    }
}
