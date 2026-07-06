package com.reconciliation.kafka.config;

import java.util.List;
import java.util.Optional;

import lombok.RequiredArgsConstructor;

/**
 * Resolves Spark configuration before falling back to application YAML.
 */
@RequiredArgsConstructor
public final class LayeredConfLookup implements ConfLookup {
    private final ConfLookup sparkConfLookup;
    private final ConfLookup applicationYamlLookup;

    @Override
    public Optional<String> get(String key) {
        Optional<String> sparkValue = sparkConfLookup.get(key);
        return sparkValue.isPresent() ? sparkValue : applicationYamlLookup.get(key);
    }

    @Override
    public Optional<ResolvedConfValue> getFirst(List<String> keys) {
        Optional<ResolvedConfValue> sparkValue = sparkConfLookup.getFirst(keys);
        return sparkValue.isPresent() ? sparkValue : applicationYamlLookup.getFirst(keys);
    }
}
