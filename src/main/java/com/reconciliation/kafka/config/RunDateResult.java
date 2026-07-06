package com.reconciliation.kafka.config;

import java.time.LocalDate;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Internal holder for the resolved run date and how it was chosen.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class RunDateResult {
    /**
     * Date whose matching partition is skipped by the scanner.
     */
    private final LocalDate date;
    /**
     * Source label printed in the resolved configuration.
     */
    private final String source;
}
