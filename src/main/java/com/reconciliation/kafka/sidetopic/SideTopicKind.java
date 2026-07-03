package com.reconciliation.kafka.sidetopic;

/**
 * Side-topic source used when decoding and classifying Kafka records.
 */
public enum SideTopicKind {
    /**
     * Canary side topic containing rerouted source records.
     */
    CANARY,
    /**
     * Dead-letter side topic containing failed source records and diagnostics.
     */
    DEAD_LETTER
}
