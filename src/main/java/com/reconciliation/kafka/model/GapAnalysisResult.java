package com.reconciliation.kafka.model;

import java.util.Map;

public final class GapAnalysisResult {
    public final long gapPartitionCount;
    public final Map<Integer, MissingOffsetReport> missingOffsetsByPartition;

    public GapAnalysisResult(long gapPartitionCount, Map<Integer, MissingOffsetReport> missingOffsetsByPartition) {
        this.gapPartitionCount = gapPartitionCount;
        this.missingOffsetsByPartition = missingOffsetsByPartition;
    }
}
