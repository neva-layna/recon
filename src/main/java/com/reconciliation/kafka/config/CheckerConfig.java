package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public final class CheckerConfig {
    public final List<String> inputRoots;
    public final String metadataColumn;
    public final String datePartitionColumn;
    public final LocalDate runDate;
    public final String runDateSource;
    public final Optional<String> normalizedOffsetsPath;
    public final boolean normalizedOffsetsOverwrite;
    public final boolean failOnInvalidRows;
    public final boolean failOnGaps;
    public final long missingOffsetsLimit;
    public final boolean exitOnCompletion;
    public final Optional<SideTopicConfig> sideTopicConfig;

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
