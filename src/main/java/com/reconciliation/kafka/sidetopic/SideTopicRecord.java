package com.reconciliation.kafka.sidetopic;

import java.util.Optional;

/**
 * Decoded side-topic record with the original source Kafka coordinates.
 */
public final class SideTopicRecord {
    /**
     * Side-topic kind the record was decoded from.
     */
    public final SideTopicKind kind;
    /**
     * Kafka side-topic name that contained the record.
     */
    public final String sideTopic;
    /**
     * Original source topic carried by the side-topic payload.
     */
    public final String sourceTopic;
    /**
     * Original source partition carried by the side-topic payload.
     */
    public final int sourcePartition;
    /**
     * Original source offset carried by the side-topic payload.
     */
    public final long sourceOffset;
    /**
     * Optional dead-letter failure event id.
     */
    public final Optional<String> failureEventId;
    /**
     * Optional dead-letter reason message.
     */
    public final Optional<String> reasonMsg;
    /**
     * Optional dead-letter exception text.
     */
    public final Optional<String> exception;

    /**
     * Creates a decoded side-topic record.
     *
     * @param kind side-topic kind
     * @param sideTopic Kafka topic that contained the record
     * @param sourceTopic original source topic
     * @param sourcePartition original source partition
     * @param sourceOffset original source offset
     * @param failureEventId optional dead-letter failure event id
     * @param reasonMsg optional dead-letter reason message
     * @param exception optional dead-letter exception text
     */
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

    /**
     * Builds the matching key used to compare this record with missing offsets.
     *
     * @return source topic, partition, and offset key
     */
    public MissingOffsetKey key() {
        return new MissingOffsetKey(sourceTopic, sourcePartition, sourceOffset);
    }
}
