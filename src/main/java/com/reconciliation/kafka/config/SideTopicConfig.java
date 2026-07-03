package com.reconciliation.kafka.config;

import java.util.Optional;

/**
 * Configuration for optional Kafka side-topic reconciliation.
 */
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

    /**
     * Creates side-topic reconciliation settings.
     *
     * @param sourceTopic source topic matched against missing offsets
     * @param kafkaBootstrapServers Kafka bootstrap server list
     * @param canaryTopic optional canary topic name
     * @param deadLetterTopic optional dead-letter topic name
     * @param startingOffsets Spark Kafka starting offset mode
     */
    public SideTopicConfig(
        String sourceTopic,
        String kafkaBootstrapServers,
        Optional<String> canaryTopic,
        Optional<String> deadLetterTopic,
        String startingOffsets
    ) {
        this.sourceTopic = sourceTopic;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.canaryTopic = canaryTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.startingOffsets = startingOffsets;
    }
}
