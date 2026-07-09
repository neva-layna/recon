package com.reconciliation.synthdata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

final class SideTopicDecodedRecord {
    private final KafkaSideTopicKind kind;
    private final String sideTopic;
    private final String sourceKey;
    private final String sourceValue;
    private final Map<String, String> sourceHeaders;
    private final String sourceTopic;
    private final int sourcePartition;
    private final long sourceOffset;
    private final long sourceTimestamp;
    private final String failureEventId;
    private final String reasonMsg;
    private final String exception;

    SideTopicDecodedRecord(
        KafkaSideTopicKind kind,
        String sideTopic,
        String sourceKey,
        String sourceValue,
        Map<String, String> sourceHeaders,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        long sourceTimestamp,
        String failureEventId,
        String reasonMsg,
        String exception
    ) {
        this.kind = kind;
        this.sideTopic = sideTopic;
        this.sourceKey = sourceKey;
        this.sourceValue = sourceValue;
        this.sourceHeaders = Collections.unmodifiableMap(new LinkedHashMap<String, String>(sourceHeaders));
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.sourceTimestamp = sourceTimestamp;
        this.failureEventId = failureEventId;
        this.reasonMsg = reasonMsg;
        this.exception = exception;
    }

    KafkaSideTopicKind getKind() {
        return kind;
    }

    String getSideTopic() {
        return sideTopic;
    }

    String getSourceKey() {
        return sourceKey;
    }

    String getSourceValue() {
        return sourceValue;
    }

    Map<String, String> getSourceHeaders() {
        return sourceHeaders;
    }

    String getSourceTopic() {
        return sourceTopic;
    }

    int getSourcePartition() {
        return sourcePartition;
    }

    long getSourceOffset() {
        return sourceOffset;
    }

    long getSourceTimestamp() {
        return sourceTimestamp;
    }

    String getFailureEventId() {
        return failureEventId;
    }

    String getReasonMsg() {
        return reasonMsg;
    }

    String getException() {
        return exception;
    }
}
