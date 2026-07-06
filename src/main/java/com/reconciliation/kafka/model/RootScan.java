package com.reconciliation.kafka.model;

import java.util.List;

import lombok.RequiredArgsConstructor;

/**
 * Classified immediate-child scan result for one configured input root.
 */
@RequiredArgsConstructor
public final class RootScan {
    /**
     * Configured root path that was scanned.
     */
    public final String root;
    /**
     * Date partitions older or newer than the run date and selected for input.
     */
    public final List<EligiblePartition> eligible;
    /**
     * Partition paths whose date equals the run date.
     */
    public final List<String> skippedRunDate;
    /**
     * Partition-like paths whose date text failed validation.
     */
    public final List<String> ignoredInvalidDate;
    /**
     * Immediate children that did not match the configured date partition shape.
     */
    public final List<String> ignoredNonMatching;
}
