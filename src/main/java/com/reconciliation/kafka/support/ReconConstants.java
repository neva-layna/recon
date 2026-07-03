package com.reconciliation.kafka.support;

import java.time.format.DateTimeFormatter;

public final class ReconConstants {
    public static final String RECON_PREFIX = "[recon]";
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private ReconConstants() {
    }
}
