package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.format.annotation.DateTimeFormat;

import lombok.Getter;
import lombok.Setter;

/**
 * Spring Boot YAML properties under the {@code recon} prefix.
 */
@ConfigurationProperties(prefix = "recon")
@Getter
@Setter
public class ReconProperties {
    /**
     * Root directories whose immediate date partitions are scanned.
     */
    private List<String> inputRoots;
    /**
     * Column that contains Kafka metadata JSON.
     */
    private String metadataColumn;
    /**
     * Hive-style partition column name used in child directory names.
     */
    private String datePartitionColumn;
    /**
     * Business date whose partition is skipped during reconciliation.
     */
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    private LocalDate runDate;
    /**
     * Optional parquet path for writing normalized partition/offset rows.
     */
    private String normalizedOffsetsPath;
    /**
     * Whether a configured normalized-offset path is overwritten.
     */
    private Boolean normalizedOffsetsOverwrite;
    /**
     * Whether invalid metadata rows make the final result fail.
     */
    private Boolean failOnInvalidRows;
    /**
     * Whether detected offset gaps make the final result fail.
     */
    private Boolean failOnGaps;
    /**
     * Maximum number of concrete missing offsets printed per partition.
     */
    private Long missingOffsetsLimit;
    /**
     * Whether the checker exits the JVM after printing the final result.
     */
    private Boolean exitOnCompletion;
    /**
     * Original source topic whose missing offsets are being explained.
     */
    private String sourceTopic;
    /**
     * Broker alias used to select Kafka consumer settings.
     */
    private String kafkaAlias;
    /**
     * Optional canary side topic that may contain rerouted source records.
     */
    private String canaryTopic;
    /**
     * Optional dead-letter side topic that may contain failed source records.
     */
    private String deadLetterTopic;
    /**
     * Spark Kafka starting-offset mode.
     */
    private String sideTopicStartingOffsets;
}
