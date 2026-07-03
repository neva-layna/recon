package com.reconciliation.kafka.sidetopic;

import java.util.Optional;

public final class SideTopicRecord {
    public final SideTopicKind kind;
    public final String sideTopic;
    public final String sourceTopic;
    public final int sourcePartition;
    public final long sourceOffset;
    public final Optional<String> failureEventId;
    public final Optional<String> reasonMsg;
    public final Optional<String> exception;

    public SideTopicRecord(
        SideTopicKind kind,
        String sideTopic,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        Optional<String> failureEventId,
        Optional<String> reasonMsg,
        Optional<String> exception
    ) {
        this.kind = kind;
        this.sideTopic = sideTopic;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.failureEventId = failureEventId;
        this.reasonMsg = reasonMsg;
        this.exception = exception;
    }

    public MissingOffsetKey key() {
        return new MissingOffsetKey(sourceTopic, sourcePartition, sourceOffset);
    }
}
