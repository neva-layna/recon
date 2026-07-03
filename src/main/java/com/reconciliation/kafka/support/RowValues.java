package com.reconciliation.kafka.support;

import org.apache.spark.sql.Row;

public final class RowValues {
    private RowValues() {
    }

    public static int getInt(Row row, String fieldName) {
        Object value = row.getAs(fieldName);
        return ((Number) value).intValue();
    }

    public static long getLong(Row row, String fieldName) {
        Object value = row.getAs(fieldName);
        return ((Number) value).longValue();
    }

    public static boolean getBoolean(Row row, String fieldName) {
        return (Boolean) row.getAs(fieldName);
    }
}
