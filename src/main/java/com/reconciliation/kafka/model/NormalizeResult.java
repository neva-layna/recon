package com.reconciliation.kafka.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import lombok.RequiredArgsConstructor;

/**
 * Result of converting metadata JSON into normalized partition/offset rows.
 */
@RequiredArgsConstructor
public final class NormalizeResult {
    /**
     * Dataset containing valid partition, offset, metadata_json, and source_file
     * columns.
     */
    public final Dataset<Row> normalizedOffsets;
    /**
     * Number of eligible rows rejected by metadata validation.
     */
    public final long invalidRows;
    /**
     * Number of eligible rows accepted as valid offsets.
     */
    public final long validRows;
}
