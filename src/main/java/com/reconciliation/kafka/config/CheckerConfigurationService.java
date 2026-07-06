package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

/**
 * Spring-managed loader for checker configuration.
 */
@Component
@RequiredArgsConstructor
public class CheckerConfigurationService {
    private final ConfLookup confLookup;
    private final KafkaConfigsProperties kafkaConfigsProperties;
    private final Supplier<LocalDate> currentDateSupplier;

    /**
     * Loads the effective checker configuration from Spark settings.
     *
     * @return resolved checker configuration
     */
    public CheckerConfig load() {
        return ConfigLoader.loadConfig(confLookup, kafkaConfigsProperties, currentDateSupplier);
    }
}
