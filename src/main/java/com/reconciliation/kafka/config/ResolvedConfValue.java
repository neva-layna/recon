package com.reconciliation.kafka.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A resolved checker configuration value and the layer that supplied it.
 */
@Getter
@RequiredArgsConstructor
public final class ResolvedConfValue {
    /**
     * String form consumed by the existing checker parsers.
     */
    private final String value;
    /**
     * Stable source label printed for operator diagnostics.
     */
    private final String source;
}
