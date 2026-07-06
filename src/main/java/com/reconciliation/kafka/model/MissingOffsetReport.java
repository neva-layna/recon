package com.reconciliation.kafka.model;

import java.util.List;

import lombok.RequiredArgsConstructor;

/**
 * Bounded list of concrete missing offsets for one Kafka partition.
 */
@RequiredArgsConstructor
public final class MissingOffsetReport {
    /**
     * Materialized missing offsets up to the configured output limit.
     */
    public final List<Long> offsets;
    /**
     * Whether additional missing offsets exist beyond {@link #offsets}.
     */
    public final boolean truncated;
}
