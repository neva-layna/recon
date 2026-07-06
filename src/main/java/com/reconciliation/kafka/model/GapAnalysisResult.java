package com.reconciliation.kafka.model;

import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Summary returned by offset gap analytics.
 */
@Getter
@RequiredArgsConstructor
public final class GapAnalysisResult {
    /**
     * Number of Kafka partitions that contain at least one missing offset.
     */
    private final long gapPartitionCount;
    /**
     * Missing-offset reports keyed by Kafka partition id.
     */
    private final Map<Integer, MissingOffsetReport> missingOffsetsByPartition;
}
