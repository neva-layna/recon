package com.reconciliation.kafka.model;

import java.time.LocalDate;

/**
 * Eligible parquet partition directory selected for offset analysis.
 */
public final class EligiblePartition implements Comparable<EligiblePartition> {
    /**
     * Configured input root that contained the partition.
     */
    public final String root;
    /**
     * Parsed date from the partition directory name.
     */
    public final LocalDate date;
    /**
     * Full filesystem path to the partition directory.
     */
    public final String path;

    /**
     * Creates an eligible partition record.
     *
     * @param root configured input root
     * @param date parsed partition date
     * @param path full partition path
     */
    public EligiblePartition(String root, LocalDate date, String path) {
        this.root = root;
        this.date = date;
        this.path = path;
    }

    /**
     * Orders partitions by date and then by path for stable scan output.
     *
     * @param other partition to compare with
     * @return negative, zero, or positive comparison result
     */
    @Override
    public int compareTo(EligiblePartition other) {
        int dateCompare = this.date.compareTo(other.date);
        if (dateCompare != 0) {
            return dateCompare;
        }
        return this.path.compareTo(other.path);
    }
}
