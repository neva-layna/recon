package com.reconciliation.kafka.config;

import java.util.Optional;

import lombok.RequiredArgsConstructor;

/**
 * Configuration for optional Kafka side-topic reconciliation.
 */
@RequiredArgsConstructor
public final class SideTopicConfig {
    /**
     * Original source topic whose missing offsets are being explained.
     */
    public final String sourceTopic;
    /**
     * Kafka bootstrap server list used by Spark's Kafka reader.
     */
    public final String kafkaBootstrapServers;
    /**
     * Optional canary side topic that may contain rerouted source records.
     */
    public final Optional<String> canaryTopic;
    /**
     * Optional dead-letter side topic that may contain failed source records.
     */
    public final Optional<String> deadLetterTopic;
    /**
     * Spark Kafka starting-offset mode, currently normalized to earliest.
     */
    public final String startingOffsets;
}
