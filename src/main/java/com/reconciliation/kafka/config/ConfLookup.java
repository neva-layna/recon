package com.reconciliation.kafka.config;

import java.util.Optional;

/**
 * Reads checker configuration values from a backing configuration source.
 */
public interface ConfLookup {
    /**
     * Looks up a non-empty value for a configuration key.
     *
     * @param key exact configuration key to read
     * @return configured value, or empty when the key is absent or blank
     */
    Optional<String> get(String key);
}
