package com.reconciliation.kafka;

/**
 * Spring-managed checker job boundary used by the Boot startup runner.
 */
public interface CheckerJob {
    /**
     * Runs one checker invocation.
     */
    void run();
}
