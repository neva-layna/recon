package com.reconciliation.synthdata;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable options for one local parquet generation run.
 */
public final class GenerationOptions {
    public static final String DEFAULT_METADATA_COLUMN = "cactus__metadata";

    private final Path outputDirectory;
    private final String relativeRoot;
    private final String datePartitionColumn;
    private final LocalDate date;
    private final String metadataColumn;
    private final String topic;
    private final int partition;
    private final long offset;
    private final String payload;
    private final Map<String, String> extraValues;

    public GenerationOptions(
        Path outputDirectory,
        String relativeRoot,
        String datePartitionColumn,
        LocalDate date,
        String metadataColumn,
        String topic,
        int partition,
        long offset,
        String payload,
        Map<String, String> extraValues
    ) {
        this.outputDirectory = requireOutputDirectory(outputDirectory);
        this.relativeRoot = requireNonBlank("relative root", relativeRoot);
        this.datePartitionColumn = requireColumnName("date partition column", datePartitionColumn);
        if (date == null) {
            throw new IllegalArgumentException("date must be provided");
        }
        this.date = date;
        this.metadataColumn = requireColumnName("metadata column", metadataColumn);
        this.topic = requireNonBlank("topic", topic);
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        if (offset < 0L) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        this.partition = partition;
        this.offset = offset;
        this.payload = payload;
        this.extraValues = copyExtraValues(extraValues);
    }

    public Path getOutputDirectory() {
        return outputDirectory;
    }

    public String getRelativeRoot() {
        return relativeRoot;
    }

    public String getDatePartitionColumn() {
        return datePartitionColumn;
    }

    public LocalDate getDate() {
        return date;
    }

    public String getMetadataColumn() {
        return metadataColumn;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getExtraValues() {
        return extraValues;
    }

    public String getPartitionDirectoryName() {
        return datePartitionColumn + "=" + date;
    }

    public String getMetadataJson() {
        return "{\"partition\":" + partition + ",\"offset\":" + offset + "}";
    }

    private static Path requireOutputDirectory(Path outputDirectory) {
        if (outputDirectory == null) {
            throw new IllegalArgumentException("output directory must be provided");
        }
        return outputDirectory;
    }

    private static String requireNonBlank(String label, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " must not be empty");
        }
        return value.trim();
    }

    private static String requireColumnName(String label, String value) {
        String trimmed = requireNonBlank(label, value);
        if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(label + " must match [A-Za-z_][A-Za-z0-9_]*: " + value);
        }
        return trimmed;
    }

    private static Map<String, String> copyExtraValues(Map<String, String> extraValues) {
        if (extraValues == null || extraValues.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, String>(extraValues));
    }
}
