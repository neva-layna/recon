package com.reconciliation.kafka.sidetopic;

/**
 * Source topic, partition, and offset tuple used to match side-topic records to
 * missing offsets.
 */
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
     * Creates a side-topic matching key.
     *
     * @param sourceTopic source Kafka topic name
     * @param sourcePartition source Kafka partition id
     * @param sourceOffset source Kafka offset
     */
    public MissingOffsetKey(String sourceTopic, int sourcePartition, long sourceOffset) {
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }

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

    /**
     * Compares keys by source topic, partition, and offset.
     *
     * @param value candidate object
     * @return true when the candidate identifies the same source offset
     */
    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        }
        if (!(value instanceof MissingOffsetKey)) {
            return false;
        }
        MissingOffsetKey other = (MissingOffsetKey) value;
        return sourcePartition == other.sourcePartition
            && sourceOffset == other.sourceOffset
            && sourceTopic.equals(other.sourceTopic);
    }

    /**
     * Hashes the same source topic, partition, and offset fields used by equals.
     *
     * @return stable hash code for map and set membership
     */
    @Override
    public int hashCode() {
        int result = sourceTopic.hashCode();
        result = 31 * result + sourcePartition;
        result = 31 * result + (int) (sourceOffset ^ (sourceOffset >>> 32));
        return result;
    }
}
