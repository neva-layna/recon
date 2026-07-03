package com.reconciliation.kafka.sidetopic;

import java.util.List;
import java.util.Map;

public final class SideTopicClassification {
    public final String sourceTopic;
    public final Map<Integer, List<Long>> canaryExplainedOffsets;
    public final Map<Integer, List<Long>> deadLetterExplainedOffsets;
    public final Map<Integer, List<Long>> unresolvedOffsets;
    public final long canaryExplainedCount;
    public final long deadLetterExplainedCount;
    public final long unresolvedCount;
    public final long canaryRecordCount;
    public final long deadLetterRecordCount;
    public final long deadLetterFailureEventIdCount;
    public final long deadLetterReasonMsgCount;
    public final long deadLetterExceptionCount;
    public final boolean missingOffsetsTruncated;

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
