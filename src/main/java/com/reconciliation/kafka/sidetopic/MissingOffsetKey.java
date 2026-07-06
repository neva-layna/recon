package com.reconciliation.kafka.sidetopic;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * Source topic, partition, and offset tuple used to match side-topic records to
 * missing offsets.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public final class MissingOffsetKey implements Comparable<MissingOffsetKey> {
    /**
     * Source Kafka topic name.
     */
    public final String sourceTopic;
    /**
     * Source Kafka partition id.
     */
    public final int sourcePartition;
    /**
     * Source Kafka offset.
     */
    public final long sourceOffset;

    /**
     * Orders keys by topic, partition, then offset for stable bucket output.
     *
     * @param other key to compare with
     * @return negative, zero, or positive comparison result
     */
    @Override
    public int compareTo(MissingOffsetKey other) {
        int topicCompare = sourceTopic.compareTo(other.sourceTopic);
        if (topicCompare != 0) {
            return topicCompare;
        }
        int partitionCompare = Integer.compare(sourcePartition, other.sourcePartition);
        if (partitionCompare != 0) {
            return partitionCompare;
        }
        return Long.compare(sourceOffset, other.sourceOffset);
    }
}
