package com.reconciliation.kafka.model;

import java.util.List;

/**
 * Classified immediate-child scan result for one configured input root.
 */
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

    /**
     * Creates a root scan result.
     *
     * @param root scanned root path
     * @param eligible selected eligible partitions
     * @param skippedRunDate paths skipped because they match the run date
     * @param ignoredInvalidDate paths ignored because date text was invalid
     * @param ignoredNonMatching paths ignored because they did not match
     */
    public RootScan(
        String root,
        List<EligiblePartition> eligible,
        List<String> skippedRunDate,
        List<String> ignoredInvalidDate,
        List<String> ignoredNonMatching
    ) {
        this.root = root;
        this.eligible = eligible;
        this.skippedRunDate = skippedRunDate;
        this.ignoredInvalidDate = ignoredInvalidDate;
        this.ignoredNonMatching = ignoredNonMatching;
    }
}
