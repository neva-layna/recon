package com.reconciliation.kafka.support;

import java.time.format.DateTimeFormatter;

/**
 * Shared constants for operator-visible reconciliation output.
 */
public final class ReconConstants {
    /**
     * Prefix used on structured checker output lines.
     */
    public static final String RECON_PREFIX = "[recon]";
    /**
     * Date formatter used for run-date config and Hive partition dates.
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    /**
     * Prevents construction of the constants holder.
     */
    private ReconConstants() {
    }
}
