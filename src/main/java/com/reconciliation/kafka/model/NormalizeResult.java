package com.reconciliation.kafka.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Result of converting metadata JSON into normalized partition/offset rows.
 */
@Getter
@RequiredArgsConstructor
public final class NormalizeResult {
    /**
     * Dataset containing valid partition, offset, metadata_json, and source_file
     * columns.
     */
    private final Dataset<Row> normalizedOffsets;
    /**
     * Number of eligible rows rejected by metadata validation.
     */
    private final long invalidRows;
    /**
     * Number of eligible rows accepted as valid offsets.
     */
    private final long validRows;
}
