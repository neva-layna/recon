package com.reconciliation.kafka.sidetopic;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Source topic, partition, and offset tuple used to match side-topic records to
 * missing offsets.
 */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode
public final class MissingOffsetKey implements Comparable<MissingOffsetKey> {
    /**
     * Source Kafka topic name.
     */
    private final String sourceTopic;
    /**
     * Source Kafka partition id.
     */
    private final int sourcePartition;
    /**
     * Source Kafka offset.
     */
    private final long sourceOffset;

    /**
     * Orders keys by topic, partition, then offset for stable bucket output.
     *
     * @param other key to compare with
     * @return negative, zero, or positive comparison result
     */
    @Override
    public int compareTo(MissingOffsetKey other) {
        int topicCompare = getSourceTopic().compareTo(other.getSourceTopic());
        if (topicCompare != 0) {
            return topicCompare;
        }
        int partitionCompare = Integer.compare(getSourcePartition(), other.getSourcePartition());
        if (partitionCompare != 0) {
            return partitionCompare;
        }
        return Long.compare(getSourceOffset(), other.getSourceOffset());
    }
}
