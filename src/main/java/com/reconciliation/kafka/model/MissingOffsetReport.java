package com.reconciliation.kafka.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Bounded list of concrete missing offsets for one Kafka partition.
 */
@Getter
@RequiredArgsConstructor
public final class MissingOffsetReport {
    /**
     * Materialized missing offsets up to the configured output limit.
     */
    private final List<Long> offsets;
    /**
     * Whether additional missing offsets exist beyond {@link #offsets}.
     */
    private final boolean truncated;
}
