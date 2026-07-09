package com.reconciliation.synthdata;

/**
 * Kafka delivery metadata returned after the broker acknowledges a record.
 */
public final class KafkaSideTopicDelivery {
    private final int partition;
    private final long offset;

    public KafkaSideTopicDelivery(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
