package com.reconciliation.kafka.support;

import java.time.format.DateTimeFormatter;

import lombok.experimental.UtilityClass;

/**
 * Shared constants for operator-visible reconciliation output.
 */
@UtilityClass
public final class ReconConstants {
    /**
     * Prefix used on structured checker output lines.
     */
    public static final String RECON_PREFIX = "[recon]";
    /**
     * Date formatter used for run-date config and Hive partition dates.
     */
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

}
