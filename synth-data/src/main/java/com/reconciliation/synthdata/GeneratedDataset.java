package com.reconciliation.synthdata;

import java.nio.file.Path;

/**
 * Paths and values produced by a parquet generation run.
 */
public final class GeneratedDataset {
    private final Path outputRoot;
    private final Path partitionDirectory;
    private final Path parquetFile;
    private final String metadataJson;

    public GeneratedDataset(Path outputRoot, Path partitionDirectory, Path parquetFile, String metadataJson) {
        this.outputRoot = outputRoot;
        this.partitionDirectory = partitionDirectory;
        this.parquetFile = parquetFile;
        this.metadataJson = metadataJson;
    }

    public Path getOutputRoot() {
        return outputRoot;
    }

    public Path getPartitionDirectory() {
        return partitionDirectory;
    }

    public Path getParquetFile() {
        return parquetFile;
    }

    public String getMetadataJson() {
        return metadataJson;
    }
}
