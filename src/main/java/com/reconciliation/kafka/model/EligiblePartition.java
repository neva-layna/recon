package com.reconciliation.kafka.model;

import java.time.LocalDate;

public final class EligiblePartition implements Comparable<EligiblePartition> {
    public final String root;
    public final LocalDate date;
    public final String path;

    public EligiblePartition(String root, LocalDate date, String path) {
        this.root = root;
        this.date = date;
        this.path = path;
    }

    @Override
    public int compareTo(EligiblePartition other) {
        int dateCompare = this.date.compareTo(other.date);
        if (dateCompare != 0) {
            return dateCompare;
        }
        return this.path.compareTo(other.path);
    }
}
