package com.reconciliation.kafka.model;

import java.util.List;

public final class MissingOffsetReport {
    public final List<Long> offsets;
    public final boolean truncated;

    public MissingOffsetReport(List<Long> offsets, boolean truncated) {
        this.offsets = offsets;
        this.truncated = truncated;
    }
}
