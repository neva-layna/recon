package com.reconciliation.kafka.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of converting metadata JSON into normalized partition/offset rows.
 */
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

    /**
     * Creates a normalization result.
     *
     * @param normalizedOffsets valid normalized offset rows
     * @param invalidRows count of invalid metadata rows
     * @param validRows count of valid metadata rows
     */
    public NormalizeResult(Dataset<Row> normalizedOffsets, long invalidRows, long validRows) {
        this.normalizedOffsets = normalizedOffsets;
        this.invalidRows = invalidRows;
        this.validRows = validRows;
    }
}
