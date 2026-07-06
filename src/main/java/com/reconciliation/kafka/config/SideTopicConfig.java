package com.reconciliation.kafka.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;

/**
 * Configuration for optional Kafka side-topic reconciliation.
 */
@Getter
public final class SideTopicConfig {
    /**
     * Original source topic whose missing offsets are being explained.
     */
    private final String sourceTopic;
    /**
     * Optional broker alias selected from kafka-brokers.yml.
     */
    private final Optional<String> kafkaAlias;
    /**
     * Kafka consumer properties from the selected broker alias.
     */
    private final Map<String, String> kafkaConsumerConfig;
    /**
     * Kafka bootstrap server list used by Spark's Kafka reader.
     */
    private final String kafkaBootstrapServers;
    /**
     * Optional canary side topic that may contain rerouted source records.
     */
    private final Optional<String> canaryTopic;
    /**
     * Optional dead-letter side topic that may contain failed source records.
     */
    private final Optional<String> deadLetterTopic;
    /**
     * Spark Kafka starting-offset mode, currently normalized to earliest.
     */
    private final String startingOffsets;

    /**
     * Creates immutable side-topic configuration.
     *
     * @param sourceTopic original source topic
     * @param kafkaAlias selected broker alias, or empty for legacy Spark-conf
     *        bootstrap override
     * @param kafkaConsumerConfig selected Kafka consumer properties
     * @param canaryTopic optional canary side topic
     * @param deadLetterTopic optional dead-letter side topic
     * @param startingOffsets normalized Spark Kafka starting offset mode
     */
    public SideTopicConfig(
        String sourceTopic,
        Optional<String> kafkaAlias,
        Map<String, String> kafkaConsumerConfig,
        Optional<String> canaryTopic,
        Optional<String> deadLetterTopic,
        String startingOffsets
    ) {
        this.sourceTopic = sourceTopic;
        this.kafkaAlias = kafkaAlias;
        this.kafkaConsumerConfig = Collections.unmodifiableMap(new LinkedHashMap<>(kafkaConsumerConfig));
        this.kafkaBootstrapServers = this.kafkaConsumerConfig.get("bootstrap.servers");
        this.canaryTopic = canaryTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.startingOffsets = startingOffsets;
    }
}
