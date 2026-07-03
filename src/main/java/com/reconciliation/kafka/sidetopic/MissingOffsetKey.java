package com.reconciliation.kafka.sidetopic;

public final class MissingOffsetKey implements Comparable<MissingOffsetKey> {
    public final String sourceTopic;
    public final int sourcePartition;
    public final long sourceOffset;

    public MissingOffsetKey(String sourceTopic, int sourcePartition, long sourceOffset) {
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
    }

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

    @Override
    public int hashCode() {
        int result = sourceTopic.hashCode();
        result = 31 * result + sourcePartition;
        result = 31 * result + (int) (sourceOffset ^ (sourceOffset >>> 32));
        return result;
    }
}
