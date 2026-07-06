package com.reconciliation.kafka.model;

import java.time.LocalDate;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Eligible parquet partition directory selected for offset analysis.
 */
@Getter
@RequiredArgsConstructor
public final class EligiblePartition implements Comparable<EligiblePartition> {
    /**
     * Configured input root that contained the partition.
     */
    private final String root;
    /**
     * Parsed date from the partition directory name.
     */
    private final LocalDate date;
    /**
     * Full filesystem path to the partition directory.
     */
    private final String path;

    /**
     * Orders partitions by date and then by path for stable scan output.
     *
     * @param other partition to compare with
     * @return negative, zero, or positive comparison result
     */
    @Override
    public int compareTo(EligiblePartition other) {
        int dateCompare = getDate().compareTo(other.getDate());
        if (dateCompare != 0) {
            return dateCompare;
        }
        return getPath().compareTo(other.getPath());
    }
}
