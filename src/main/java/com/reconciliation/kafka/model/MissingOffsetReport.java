package com.reconciliation.kafka.model;

import java.util.List;

/**
 * Bounded list of concrete missing offsets for one Kafka partition.
 */
public final class MissingOffsetReport {
    /**
     * Materialized missing offsets up to the configured output limit.
     */
    public final List<Long> offsets;
    /**
     * Whether additional missing offsets exist beyond {@link #offsets}.
     */
    public final boolean truncated;

    /**
     * Creates a missing-offset report.
     *
     * @param offsets materialized missing offsets
     * @param truncated whether the full missing-offset set exceeded the limit
     */
    public MissingOffsetReport(List<Long> offsets, boolean truncated) {
        this.offsets = offsets;
        this.truncated = truncated;
    }
}
