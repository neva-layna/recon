package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import lombok.RequiredArgsConstructor;

/**
 * Immutable configuration resolved from Spark settings for one checker run.
 */
@RequiredArgsConstructor
public final class CheckerConfig {
    /**
     * Root directories whose immediate date partitions are scanned.
     */
    public final List<String> inputRoots;
    /**
     * Column that contains Kafka metadata JSON.
     */
    public final String metadataColumn;
    /**
     * Hive-style partition column name used in child directory names.
     */
    public final String datePartitionColumn;
    /**
     * Business date whose partition is skipped during reconciliation.
     */
    public final LocalDate runDate;
    /**
     * Human-readable source used to derive {@link #runDate}.
     */
    public final String runDateSource;
    /**
     * Optional parquet path for writing normalized partition/offset rows.
     */
    public final Optional<String> normalizedOffsetsPath;
    /**
     * Whether a configured normalized-offset path is overwritten.
     */
    public final boolean normalizedOffsetsOverwrite;
    /**
     * Whether invalid metadata rows make the final result fail.
     */
    public final boolean failOnInvalidRows;
    /**
     * Whether detected offset gaps make the final result fail.
     */
    public final boolean failOnGaps;
    /**
     * Maximum number of concrete missing offsets printed per partition.
     */
    public final long missingOffsetsLimit;
    /**
     * Whether the checker exits the JVM after printing the final result.
     */
    public final boolean exitOnCompletion;
    /**
     * Optional Kafka side-topic settings used to explain missing offsets.
     */
    public final Optional<SideTopicConfig> sideTopicConfig;
}
