package com.reconciliation.kafka.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

/**
 * Spring Boot YAML properties under the {@code kafka-configs} prefix.
 */
@ConfigurationProperties(prefix = "kafka-configs")
@Getter
@Setter
public class KafkaConfigsProperties {
    /**
     * Broker aliases keyed by operator-selected name.
     */
    private Map<String, BrokerProperties> broker = new LinkedHashMap<>();

    /**
     * Returns whether the imported broker config defines an alias.
     *
     * @param alias broker alias selected by recon.kafka-alias
     * @return true when the alias exists
     */
    public boolean hasBroker(String alias) {
        return broker != null && broker.containsKey(alias);
    }

    /**
     * Returns sorted broker aliases for diagnostics.
     *
     * @return broker aliases in stable display order
     */
    public List<String> brokerAliases() {
        if (broker == null || broker.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> aliases = new ArrayList<>(broker.keySet());
        Collections.sort(aliases);
        return aliases;
    }

    /**
     * Resolves an alias to normalized Kafka consumer property names.
     *
     * @param alias broker alias selected by recon.kafka-alias
     * @return immutable normalized config, or empty when the alias is absent
     */
    public Map<String, String> normalizedConf(String alias) {
        if (broker == null) {
            return Collections.emptyMap();
        }
        BrokerProperties properties = broker.get(alias);
        return properties == null ? Collections.emptyMap() : properties.normalizedConf();
    }

    /**
     * Normalizes broker YAML keys into Kafka consumer property names.
     *
     * @param rawKey raw YAML map key
     * @return usable Kafka property key, or blank when unusable
     */
    public static String normalizeKafkaPropertyName(String rawKey) {
        if (rawKey == null) {
            return "";
        }
        String key = rawKey.trim();
        if (key.startsWith("[") && key.endsWith("]") && key.length() >= 2) {
            key = key.substring(1, key.length() - 1).trim();
        }
        if (key.startsWith("kafka.")) {
            key = key.substring("kafka.".length()).trim();
        }
        return key;
    }

    /**
     * One broker alias entry from {@code kafka-configs.broker.<alias>}.
     */
    @Getter
    @Setter
    public static class BrokerProperties {
        /**
         * Kafka consumer configuration map under {@code conf}.
         */
        private Map<String, String> conf = new LinkedHashMap<>();

        /**
         * Returns normalized, non-blank Kafka consumer configuration entries.
         *
         * @return immutable normalized config map
         */
        public Map<String, String> normalizedConf() {
            if (conf == null || conf.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<String, String> normalized = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : conf.entrySet()) {
                String key = normalizeKafkaPropertyName(entry.getKey());
                String value = entry.getValue() == null ? "" : entry.getValue().trim();
                if (!key.isEmpty() && !value.isEmpty()) {
                    normalized.put(key, value);
                }
            }
            return Collections.unmodifiableMap(normalized);
        }
    }
}
