package com.reconciliation.kafka.config;

import lombok.RequiredArgsConstructor;

/**
 * A resolved checker configuration value and the layer that supplied it.
 */
@RequiredArgsConstructor
public final class ResolvedConfValue {
    /**
     * String form consumed by the existing checker parsers.
     */
    public final String value;
    /**
     * Stable source label printed for operator diagnostics.
     */
    public final String source;
}
