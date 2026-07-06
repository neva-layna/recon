package com.reconciliation.kafka.config;

import java.util.Optional;

import com.reconciliation.kafka.support.ReconConstants;

import lombok.RequiredArgsConstructor;

/**
 * Configuration lookup backed by Spring Boot {@code application.yml}.
 */
@RequiredArgsConstructor
public final class ApplicationYamlLookup implements ConfLookup {
    private final ReconProperties properties;

    @Override
    public Optional<String> get(String key) {
        if ("recon.inputRoots".equals(key)) {
            return properties.getInputRoots() == null || properties.getInputRoots().isEmpty()
                ? Optional.<String>empty()
                : Optional.of(String.join(",", properties.getInputRoots()));
        }
        if ("recon.metadataColumn".equals(key)) {
            return clean(properties.getMetadataColumn());
        }
        if ("recon.datePartitionColumn".equals(key)) {
            return clean(properties.getDatePartitionColumn());
        }
        if ("recon.runDate".equals(key)) {
            return properties.getRunDate() == null
                ? Optional.<String>empty()
                : Optional.of(properties.getRunDate().format(ReconConstants.DATE_FORMATTER));
        }
        if ("recon.normalizedOffsetsPath".equals(key)) {
            return clean(properties.getNormalizedOffsetsPath());
        }
        if ("recon.normalizedOffsetsOverwrite".equals(key)) {
            return bool(properties.getNormalizedOffsetsOverwrite());
        }
        if ("recon.failOnInvalidRows".equals(key)) {
            return bool(properties.getFailOnInvalidRows());
        }
        if ("recon.failOnGaps".equals(key)) {
            return bool(properties.getFailOnGaps());
        }
        if ("recon.missingOffsetsLimit".equals(key)) {
            return properties.getMissingOffsetsLimit() == null
                ? Optional.<String>empty()
                : Optional.of(String.valueOf(properties.getMissingOffsetsLimit()));
        }
        if ("recon.exitOnCompletion".equals(key)) {
            return bool(properties.getExitOnCompletion());
        }
        if ("recon.sourceTopic".equals(key)) {
            return clean(properties.getSourceTopic());
        }
        if ("recon.kafkaAlias".equals(key)) {
            return raw(properties.getKafkaAlias());
        }
        if ("recon.canaryTopic".equals(key)) {
            return clean(properties.getCanaryTopic());
        }
        if ("recon.deadLetterTopic".equals(key)) {
            return clean(properties.getDeadLetterTopic());
        }
        if ("recon.sideTopicStartingOffsets".equals(key)) {
            return clean(properties.getSideTopicStartingOffsets());
        }
        return Optional.empty();
    }

    @Override
    public String sourceLabel(String key) {
        return "application_yml:" + key;
    }

    private Optional<String> bool(Boolean value) {
        return value == null ? Optional.<String>empty() : Optional.of(String.valueOf(value));
    }

    private Optional<String> clean(String value) {
        if (value == null) {
            return Optional.empty();
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? Optional.<String>empty() : Optional.of(trimmed);
    }

    private Optional<String> raw(String value) {
        return value == null ? Optional.<String>empty() : Optional.of(value);
    }
}
