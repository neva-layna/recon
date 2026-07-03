package com.reconciliation.kafka.config;

import java.time.LocalDate;

/**
 * Internal holder for the resolved run date and how it was chosen.
 */
final class RunDateResult {
    /**
     * Date whose matching partition is skipped by the scanner.
     */
    final LocalDate date;
    /**
     * Source label printed in the resolved configuration.
     */
    final String source;

    /**
     * Creates a run-date resolution result.
     *
     * @param date resolved run date
     * @param source source label for reporting
     */
    RunDateResult(LocalDate date, String source) {
        this.date = date;
        this.source = source;
    }
}
