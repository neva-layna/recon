package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconReporter;

/**
 * Parses Spark configuration into checker configuration objects.
 */
public final class ConfigLoader {
    /**
     * Prevents construction of the configuration utility.
     */
    private ConfigLoader() {
    }

    /**
     * Loads required and optional checker settings, applying defaults and
     * failing fast for invalid operator input.
     *
     * @param lookup configuration lookup source
     * @param currentDateSupplier supplies the driver date when recon.runDate is
     *        omitted
     * @return resolved checker configuration
     * @throws com.reconciliation.kafka.support.ReconExit when required or typed
     *         configuration is invalid
     */
    public static CheckerConfig loadConfig(ConfLookup lookup, Supplier<LocalDate> currentDateSupplier) {
        return loadConfig(lookup, new KafkaConfigsProperties(), currentDateSupplier);
    }

    /**
     * Loads required and optional checker settings, applying defaults and
     * resolving imported Kafka broker aliases.
     *
     * @param lookup configuration lookup source
     * @param kafkaConfigs Spring-bound broker alias configuration
     * @param currentDateSupplier supplies the driver date when recon.runDate is
     *        omitted
     * @return resolved checker configuration
     * @throws com.reconciliation.kafka.support.ReconExit when required or typed
     *         configuration is invalid
     */
    public static CheckerConfig loadConfig(
        ConfLookup lookup,
        KafkaConfigsProperties kafkaConfigs,
        Supplier<LocalDate> currentDateSupplier
    ) {
        Optional<String> rootsValue = confOption("recon.inputRoots", lookup);
        List<String> roots = rootsValue.isPresent()
            ? splitCsv(rootsValue.get())
            : Collections.<String>emptyList();

        if (roots.isEmpty()) {
            ReconReporter.stopNow(
                2,
                "Missing required Spark conf recon.inputRoots; provide a comma-separated list of root paths"
            );
        }

        RunDateResult runDate = parseRunDate(lookup, currentDateSupplier);

        return new CheckerConfig(
            roots,
            confOption("recon.metadataColumn", lookup).orElse("cactus__metadata"),
            confOption("recon.datePartitionColumn", lookup).orElse("timestampcolumn"),
            runDate.date,
            runDate.source,
            confOption("recon.normalizedOffsetsPath", lookup),
            parseBoolean("recon.normalizedOffsetsOverwrite", true, lookup),
            parseBoolean("recon.failOnInvalidRows", true, lookup),
            parseBoolean("recon.failOnGaps", true, lookup),
            parseNonNegativeLong("recon.missingOffsetsLimit", 1000L, lookup),
            parseBoolean("recon.exitOnCompletion", true, lookup),
            loadSideTopicConfig(lookup, kafkaConfigs)
        );
    }

    /**
     * Loads optional side-topic reconciliation settings and validates that a
     * partial configuration is not silently ignored.
     *
     * @param lookup configuration lookup source
     * @return side-topic configuration, or empty when no side-topic keys are set
     * @throws com.reconciliation.kafka.support.ReconExit when side-topic config
     *         is incomplete or unsupported
     */
    public static Optional<SideTopicConfig> loadSideTopicConfig(ConfLookup lookup) {
        return loadSideTopicConfig(lookup, new KafkaConfigsProperties());
    }

    /**
     * Loads optional side-topic reconciliation settings and validates the
     * selected broker alias configuration.
     *
     * @param lookup configuration lookup source
     * @param kafkaConfigs Spring-bound broker alias configuration
     * @return side-topic configuration, or empty when no side-topic keys are set
     * @throws com.reconciliation.kafka.support.ReconExit when side-topic config
     *         is incomplete or unsupported
     */
    public static Optional<SideTopicConfig> loadSideTopicConfig(ConfLookup lookup, KafkaConfigsProperties kafkaConfigs) {
        Optional<String> sourceTopic = firstConfOption(lookup, "recon.sourceTopic", "recon.sideTopic.sourceTopic");
        Optional<ResolvedConfValue> alias = firstConfValue(lookup, "recon.kafkaAlias", "recon.kafka.alias");
        Optional<ResolvedConfValue> legacyBootstrapServers = sparkConfOnly(firstConfValue(
            lookup,
            "recon.kafkaBootstrapServers",
            "recon.kafka.bootstrap.servers",
            "recon.sideTopic.kafkaBootstrapServers"
        ));
        Optional<String> canaryTopic = firstConfOption(lookup, "recon.canaryTopic", "recon.sideTopic.canaryTopic");
        Optional<String> deadLetterTopic = firstConfOption(
            lookup,
            "recon.deadLetterTopic",
            "recon.deadletterTopic",
            "recon.sideTopic.deadLetterTopic"
        );
        Optional<String> rawStartingOffsets = firstConfOption(
            lookup,
            "recon.sideTopicStartingOffsets",
            "recon.sideTopic.startingOffsets",
            "recon.sideTopicReadBehavior"
        );

        boolean anySideTopicConfig = sourceTopic.isPresent()
            || alias.isPresent()
            || legacyBootstrapServers.isPresent()
            || canaryTopic.isPresent()
            || deadLetterTopic.isPresent()
            || rawStartingOffsets.isPresent();
        if (!anySideTopicConfig) {
            return Optional.empty();
        }

        List<String> missing = new ArrayList<String>();
        if (!sourceTopic.isPresent()) {
            missing.add("recon.sourceTopic");
        }
        if (!alias.isPresent() && !legacyBootstrapServers.isPresent()) {
            missing.add("recon.kafkaAlias");
        }
        if (!canaryTopic.isPresent() && !deadLetterTopic.isPresent()) {
            missing.add("one of recon.canaryTopic or recon.deadLetterTopic");
        }
        if (!missing.isEmpty()) {
            ReconReporter.stopNow(2, "Incomplete side-topic config; missing " + String.join(", ", missing));
        }

        String startingOffsets = rawStartingOffsets.orElse("earliest").toLowerCase(Locale.ROOT);
        if ("beginning".equals(startingOffsets)) {
            startingOffsets = "earliest";
        }
        if (!"earliest".equals(startingOffsets)) {
            ReconReporter.stopNow(
                2,
                "Invalid Spark conf recon.sideTopicStartingOffsets=" + rawStartingOffsets.get()
                    + "; expected earliest or beginning"
            );
        }

        BrokerResolution broker = alias.isPresent()
            ? resolveBrokerAlias(alias.get().value, kafkaConfigs)
            : legacyBootstrapBroker(legacyBootstrapServers.get().value);

        return Optional.of(new SideTopicConfig(
            sourceTopic.get(),
            broker.alias,
            broker.conf,
            canaryTopic,
            deadLetterTopic,
            startingOffsets
        ));
    }

    private static Optional<ResolvedConfValue> sparkConfOnly(Optional<ResolvedConfValue> value) {
        if (!value.isPresent()) {
            return value;
        }
        return value.get().source.startsWith("application_yml:")
            ? Optional.<ResolvedConfValue>empty()
            : value;
    }

    private static BrokerResolution resolveBrokerAlias(String rawAlias, KafkaConfigsProperties kafkaConfigs) {
        String alias = rawAlias == null ? "" : rawAlias.trim();
        if (alias.isEmpty()) {
            ReconReporter.stopNow(2, "Invalid side-topic config; recon.kafkaAlias is blank or whitespace");
        }

        if (!kafkaConfigs.hasBroker(alias)) {
            ReconReporter.stopNow(
                2,
                "Unknown recon.kafkaAlias=" + alias
                    + "; define kafka-configs.broker." + alias + ".conf in kafka-brokers.yml"
                    + knownAliases(kafkaConfigs)
            );
        }

        Map<String, String> conf = kafkaConfigs.normalizedConf(alias);
        if (conf.isEmpty()) {
            ReconReporter.stopNow(2, "Broker alias " + alias + " has empty kafka-configs.broker." + alias + ".conf");
        }
        if (!conf.containsKey("bootstrap.servers")) {
            ReconReporter.stopNow(
                2,
                "Broker alias " + alias
                    + " missing required kafka-configs.broker." + alias + ".conf[bootstrap.servers]"
            );
        }
        return new BrokerResolution(Optional.of(alias), conf);
    }

    private static BrokerResolution legacyBootstrapBroker(String bootstrapServers) {
        Map<String, String> conf = new LinkedHashMap<String, String>();
        conf.put("bootstrap.servers", bootstrapServers.trim());
        return new BrokerResolution(Optional.<String>empty(), conf);
    }

    private static String knownAliases(KafkaConfigsProperties kafkaConfigs) {
        List<String> aliases = kafkaConfigs.brokerAliases();
        return aliases.isEmpty() ? "" : "; known aliases=" + String.join(",", aliases);
    }

    private static final class BrokerResolution {
        private final Optional<String> alias;
        private final Map<String, String> conf;

        private BrokerResolution(Optional<String> alias, Map<String, String> conf) {
            this.alias = alias;
            this.conf = conf;
        }
    }

    /**
     * Returns the first configured value from an ordered alias list.
     *
     * @param lookup configuration lookup source
     * @param keys aliases in precedence order
     * @return first present value, or empty when no alias is configured
     */
    public static Optional<String> firstConfOption(ConfLookup lookup, String... keys) {
        Optional<ResolvedConfValue> value = firstConfValue(lookup, keys);
        return value.isPresent() ? Optional.of(value.get().value) : Optional.<String>empty();
    }

    /**
     * Looks up a configuration key, accepting both plain recon.* and spark.recon.*
     * spellings.
     *
     * @param key logical checker key
     * @param lookup configuration lookup source
     * @return configured value, or empty when neither spelling is present
     */
    public static Optional<String> confOption(String key, ConfLookup lookup) {
        Optional<ResolvedConfValue> value = firstConfValue(lookup, key);
        return value.isPresent() ? Optional.of(value.get().value) : Optional.<String>empty();
    }

    /**
     * Looks up a configuration key, accepting Spark aliases and preserving the
     * source label for diagnostics.
     *
     * @param lookup configuration lookup source
     * @param keys logical checker keys
     * @return resolved value, or empty when no alias is configured
     */
    public static Optional<ResolvedConfValue> firstConfValue(ConfLookup lookup, String... keys) {
        List<String> aliases = new ArrayList<String>();
        for (String key : keys) {
            aliases.add(key);
            String sparkAlias = "spark." + key;
            if (!sparkAlias.equals(key)) {
                aliases.add(sparkAlias);
            }
        }
        return lookup.getFirst(aliases);
    }

    /**
     * Parses tolerant boolean text used by Spark conf values.
     *
     * @param key configuration key to read
     * @param defaultValue value returned when the key is absent
     * @param lookup configuration lookup source
     * @return parsed boolean value
     * @throws com.reconciliation.kafka.support.ReconExit when the configured
     *         value is not a supported boolean token
     */
    public static boolean parseBoolean(String key, boolean defaultValue, ConfLookup lookup) {
        Optional<String> raw = confOption(key, lookup);
        if (!raw.isPresent()) {
            return defaultValue;
        }

        String normalized = raw.get().toLowerCase(Locale.ROOT);
        if ("true".equals(normalized) || "1".equals(normalized) || "yes".equals(normalized) || "y".equals(normalized)) {
            return true;
        }
        if ("false".equals(normalized) || "0".equals(normalized) || "no".equals(normalized) || "n".equals(normalized)) {
            return false;
        }
        ReconReporter.stopNow(2, "Invalid boolean Spark conf " + key + "=" + normalized);
        return defaultValue;
    }

    /**
     * Parses a non-negative long Spark conf value.
     *
     * @param key configuration key to read
     * @param defaultValue value returned when the key is absent
     * @param lookup configuration lookup source
     * @return parsed non-negative value
     * @throws com.reconciliation.kafka.support.ReconExit when the value is
     *         negative or not numeric
     */
    public static long parseNonNegativeLong(String key, long defaultValue, ConfLookup lookup) {
        Optional<String> raw = confOption(key, lookup);
        if (!raw.isPresent()) {
            return defaultValue;
        }

        try {
            long parsed = Long.parseLong(raw.get());
            if (parsed >= 0L) {
                return parsed;
            }
            ReconReporter.stopNow(2, "Invalid non-negative integer Spark conf " + key + "=" + parsed);
        } catch (NumberFormatException error) {
            ReconReporter.stopNow(2, "Invalid non-negative integer Spark conf " + key + "=" + raw.get() + ": " + error.getMessage());
        }
        return defaultValue;
    }

    /**
     * Resolves the run date from configuration or the supplied driver date.
     *
     * @param lookup configuration lookup source
     * @param currentDateSupplier fallback supplier used when recon.runDate is
     *        absent
     * @return resolved run date plus source label for reporting
     * @throws com.reconciliation.kafka.support.ReconExit when recon.runDate is
     *         not yyyy-MM-dd
     */
    static RunDateResult parseRunDate(ConfLookup lookup, Supplier<LocalDate> currentDateSupplier) {
        Optional<ResolvedConfValue> raw = firstConfValue(lookup, "recon.runDate");
        if (!raw.isPresent()) {
            return new RunDateResult(currentDateSupplier.get(), "driver_current_date");
        }

        try {
            return new RunDateResult(LocalDate.parse(raw.get().value, ReconConstants.DATE_FORMATTER), raw.get().source);
        } catch (DateTimeParseException error) {
            ReconReporter.stopNow(
                2,
                "Invalid Spark conf recon.runDate=" + raw.get().value + "; expected yyyy-MM-dd: " + error.getMessage()
            );
            return null;
        }
    }

    /**
     * Splits a comma-separated configuration value and drops blank items.
     *
     * @param text comma-separated text
     * @return trimmed non-empty values in input order
     */
    public static List<String> splitCsv(String text) {
        List<String> values = new ArrayList<String>();
        for (String item : text.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
    }
}
