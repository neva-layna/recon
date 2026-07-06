package com.reconciliation.kafka.config;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.spark.sql.SparkSession;

import lombok.RequiredArgsConstructor;
import scala.Option;

/**
 * Configuration lookup backed by Spark runtime conf, SparkConf, and the
 * submitted Java command line.
 */
@RequiredArgsConstructor
public final class SparkConfLookup implements ConfLookup {
    /**
     * Active Spark session used for runtime and SparkConf configuration lookups.
     */
    private final SparkSession spark;

    /**
     * Resolves a key from runtime SQL conf, SparkConf, then sun.java.command
     * --conf tokens.
     *
     * @param key exact key to resolve
     * @return first non-blank value found in fallback order
     */
    @Override
    public Optional<String> get(String key) {
        Optional<String> runtimeValue = fromRuntimeConf(key);
        if (runtimeValue.isPresent()) {
            return runtimeValue;
        }

        Optional<String> sparkConfValue = fromSparkConf(key);
        if (sparkConfValue.isPresent()) {
            return sparkConfValue;
        }

        return fromJavaCommand(key);
    }

    /**
     * Reads Spark SQL runtime configuration, treating absent or invalid keys as
     * empty.
     *
     * @param key exact runtime configuration key
     * @return non-blank runtime value, or empty
     */
    private Optional<String> fromRuntimeConf(String key) {
        try {
            return clean(spark.conf().get(key));
        } catch (NoSuchElementException error) {
            return Optional.empty();
        } catch (IllegalArgumentException error) {
            return Optional.empty();
        }
    }

    /**
     * Reads the lower-level SparkConf when SQL runtime conf did not contain a
     * value.
     *
     * @param key exact SparkConf key
     * @return non-blank SparkConf value, or empty
     */
    private Optional<String> fromSparkConf(String key) {
        Option<String> option = spark.sparkContext().getConf().getOption(key);
        if (option.isDefined()) {
            return clean(option.get());
        }
        return Optional.empty();
    }

    /**
     * Recovers --conf key=value settings from sun.java.command for environments
     * where Spark filters plain recon.* keys from runtime conf.
     *
     * @param key exact key to search for in command tokens
     * @return non-blank command-line value, or empty
     */
    private Optional<String> fromJavaCommand(String key) {
        String command = System.getProperty("sun.java.command", "");
        List<String> tokens = Arrays.asList(command.split("\\s+"));
        String pairPrefix = key + "=";
        String inlinePrefix = "--conf=" + key + "=";

        for (int i = 0; i + 1 < tokens.size(); i++) {
            if ("--conf".equals(tokens.get(i)) && tokens.get(i + 1).startsWith(pairPrefix)) {
                return clean(tokens.get(i + 1).substring(pairPrefix.length()));
            }
        }
        for (String token : tokens) {
            if (token.startsWith(inlinePrefix)) {
                return clean(token.substring(inlinePrefix.length()));
            }
        }
        return Optional.empty();
    }

    /**
     * Trims a configuration value and treats null or blank text as absent.
     *
     * @param value raw configuration value
     * @return trimmed value, or empty
     */
    private Optional<String> clean(String value) {
        if (value == null) {
            return Optional.empty();
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? Optional.<String>empty() : Optional.of(trimmed);
    }
}
