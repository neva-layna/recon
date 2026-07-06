package com.reconciliation.kafka.sidetopic;

import java.util.LinkedHashMap;
import java.util.Map;

import com.reconciliation.kafka.config.SideTopicConfig;

import lombok.experimental.UtilityClass;

/**
 * Builds Spark Kafka reader options for side-topic reads.
 */
@UtilityClass
public final class SideTopicReaderOptions {
    /**
     * Builds the full Spark reader option map for one side topic.
     *
     * @param config resolved side-topic configuration
     * @param topic Kafka side topic to read
     * @return reader options in application order
     */
    public static Map<String, String> build(SideTopicConfig config, String topic) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("kafka.request.timeout.ms", "10000");
        options.put("kafka.default.api.timeout.ms", "10000");
        for (Map.Entry<String, String> entry : config.getKafkaConsumerConfig().entrySet()) {
            options.put("kafka." + entry.getKey(), entry.getValue());
        }
        options.put("subscribe", topic);
        options.put("startingOffsets", config.getStartingOffsets());
        options.put("endingOffsets", "latest");
        options.put("failOnDataLoss", "true");
        return options;
    }
}
