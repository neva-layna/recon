package com.reconciliation.kafka.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Classified immediate-child scan result for one configured input root.
 */
@Getter
@RequiredArgsConstructor
public final class RootScan {
    /**
     * Configured root path that was scanned.
     */
    private final String root;
    /**
     * Date partitions older or newer than the run date and selected for input.
     */
    private final List<EligiblePartition> eligible;
    /**
     * Partition paths whose date equals the run date.
     */
    private final List<String> skippedRunDate;
    /**
     * Partition-like paths whose date text failed validation.
     */
    private final List<String> ignoredInvalidDate;
    /**
     * Immediate children that did not match the configured date partition shape.
     */
    private final List<String> ignoredNonMatching;
}
