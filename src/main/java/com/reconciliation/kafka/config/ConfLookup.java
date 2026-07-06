package com.reconciliation.kafka.config;

import java.util.List;
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

    /**
     * Looks up the first configured value from an already ordered key list.
     *
     * @param keys exact keys to test in lookup order
     * @return resolved value plus source label, or empty when no key is set
     */
    default Optional<ResolvedConfValue> getFirst(List<String> keys) {
        for (String key : keys) {
            Optional<String> value = get(key);
            if (value.isPresent()) {
                return Optional.of(new ResolvedConfValue(value.get(), sourceLabel(key)));
            }
        }
        return Optional.empty();
    }

    /**
     * Describes the source for a matched key in resolved-configuration output.
     *
     * @param key exact matched key
     * @return stable source label
     */
    default String sourceLabel(String key) {
        return "spark_conf:" + (key.startsWith("spark.") ? key.substring("spark.".length()) : key);
    }
}
