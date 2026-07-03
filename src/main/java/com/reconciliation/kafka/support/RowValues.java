package com.reconciliation.kafka.support;

import org.apache.spark.sql.Row;

/**
 * Typed access helpers for Spark rows produced by this checker.
 */
public final class RowValues {
    /**
     * Prevents construction of the row helper utility.
     */
    private RowValues() {
    }

    /**
     * Reads a numeric row field as an int.
     *
     * @param row Spark row containing the field
     * @param fieldName field to read
     * @return field value converted with Number.intValue
     */
    public static int getInt(Row row, String fieldName) {
        Object value = row.getAs(fieldName);
        return ((Number) value).intValue();
    }

    /**
     * Reads a numeric row field as a long.
     *
     * @param row Spark row containing the field
     * @param fieldName field to read
     * @return field value converted with Number.longValue
     */
    public static long getLong(Row row, String fieldName) {
        Object value = row.getAs(fieldName);
        return ((Number) value).longValue();
    }

    /**
     * Reads a boolean row field.
     *
     * @param row Spark row containing the field
     * @param fieldName field to read
     * @return field value as a boolean
     */
    public static boolean getBoolean(Row row, String fieldName) {
        return (Boolean) row.getAs(fieldName);
    }
}
