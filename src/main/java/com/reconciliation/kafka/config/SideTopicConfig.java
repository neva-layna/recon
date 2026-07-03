package com.reconciliation.kafka.config;

import java.util.Optional;

public final class SideTopicConfig {
    public final String sourceTopic;
    public final String kafkaBootstrapServers;
    public final Optional<String> canaryTopic;
    public final Optional<String> deadLetterTopic;
    public final String startingOffsets;

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
