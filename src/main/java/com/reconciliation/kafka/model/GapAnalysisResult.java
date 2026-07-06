package com.reconciliation.kafka.model;

import java.util.Map;

import lombok.RequiredArgsConstructor;

/**
 * Summary returned by offset gap analytics.
 */
@RequiredArgsConstructor
public final class GapAnalysisResult {
    /**
     * Number of Kafka partitions that contain at least one missing offset.
     */
    public final long gapPartitionCount;
    /**
     * Missing-offset reports keyed by Kafka partition id.
     */
    public final Map<Integer, MissingOffsetReport> missingOffsetsByPartition;
}
