package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Immutable configuration resolved from Spark settings for one checker run.
 */
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

    /**
     * Creates a fully resolved checker configuration.
     *
     * @param inputRoots root directories to scan
     * @param metadataColumn column that stores metadata JSON
     * @param datePartitionColumn date partition column name
     * @param runDate current run date to skip
     * @param runDateSource description of how the run date was resolved
     * @param normalizedOffsetsPath optional output path for normalized offsets
     * @param normalizedOffsetsOverwrite whether to overwrite that output path
     * @param failOnInvalidRows whether invalid metadata fails the run
     * @param failOnGaps whether offset gaps fail the run
     * @param missingOffsetsLimit concrete missing offsets to print per partition
     * @param exitOnCompletion whether final reporting should exit the JVM
     * @param sideTopicConfig optional side-topic reconciliation config
     */
    public CheckerConfig(
        List<String> inputRoots,
        String metadataColumn,
        String datePartitionColumn,
        LocalDate runDate,
        String runDateSource,
        Optional<String> normalizedOffsetsPath,
        boolean normalizedOffsetsOverwrite,
        boolean failOnInvalidRows,
        boolean failOnGaps,
        long missingOffsetsLimit,
        boolean exitOnCompletion,
        Optional<SideTopicConfig> sideTopicConfig
    ) {
        this.inputRoots = inputRoots;
        this.metadataColumn = metadataColumn;
        this.datePartitionColumn = datePartitionColumn;
        this.runDate = runDate;
        this.runDateSource = runDateSource;
        this.normalizedOffsetsPath = normalizedOffsetsPath;
        this.normalizedOffsetsOverwrite = normalizedOffsetsOverwrite;
        this.failOnInvalidRows = failOnInvalidRows;
        this.failOnGaps = failOnGaps;
        this.missingOffsetsLimit = missingOffsetsLimit;
        this.exitOnCompletion = exitOnCompletion;
        this.sideTopicConfig = sideTopicConfig;
    }
}
