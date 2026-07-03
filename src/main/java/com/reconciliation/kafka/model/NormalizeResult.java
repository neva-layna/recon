package com.reconciliation.kafka.model;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class NormalizeResult {
    public final Dataset<Row> normalizedOffsets;
    public final long invalidRows;
    public final long validRows;

    public NormalizeResult(Dataset<Row> normalizedOffsets, long invalidRows, long validRows) {
        this.normalizedOffsets = normalizedOffsets;
        this.invalidRows = invalidRows;
        this.validRows = validRows;
    }
}
