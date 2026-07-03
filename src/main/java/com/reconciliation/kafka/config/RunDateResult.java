package com.reconciliation.kafka.config;

import java.time.LocalDate;

final class RunDateResult {
    final LocalDate date;
    final String source;

    RunDateResult(LocalDate date, String source) {
        this.date = date;
        this.source = source;
    }
}
