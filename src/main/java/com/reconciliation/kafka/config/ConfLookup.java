package com.reconciliation.kafka.config;

import java.util.Optional;

public interface ConfLookup {
    Optional<String> get(String key);
}
