package com.reconciliation.kafka.model;

import java.util.List;

public final class RootScan {
    public final String root;
    public final List<EligiblePartition> eligible;
    public final List<String> skippedRunDate;
    public final List<String> ignoredInvalidDate;
    public final List<String> ignoredNonMatching;

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
