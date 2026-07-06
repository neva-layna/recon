package com.reconciliation.kafka.sidetopic;

import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Decoded side-topic record with the original source Kafka coordinates.
 */
@Getter
@RequiredArgsConstructor
public final class SideTopicRecord {
    /**
     * Side-topic kind the record was decoded from.
     */
    private final SideTopicKind kind;
    /**
     * Kafka side-topic name that contained the record.
     */
    private final String sideTopic;
    /**
     * Original source topic carried by the side-topic payload.
     */
    private final String sourceTopic;
    /**
     * Original source partition carried by the side-topic payload.
     */
    private final int sourcePartition;
    /**
     * Original source offset carried by the side-topic payload.
     */
    private final long sourceOffset;
    /**
     * Optional dead-letter failure event id.
     */
    private final Optional<String> failureEventId;
    /**
     * Optional dead-letter reason message.
     */
    private final Optional<String> reasonMsg;
    /**
     * Optional dead-letter exception text.
     */
    private final Optional<String> exception;

    /**
     * Builds the matching key used to compare this record with missing offsets.
     *
     * @return source topic, partition, and offset key
     */
    public MissingOffsetKey key() {
        return new MissingOffsetKey(sourceTopic, sourcePartition, sourceOffset);
    }
}
