package com.reconciliation.kafka.config;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.spark.sql.SparkSession;

import scala.Option;

public final class SparkConfLookup implements ConfLookup {
    private final SparkSession spark;

    public SparkConfLookup(SparkSession spark) {
        this.spark = spark;
    }

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

    private Optional<String> fromRuntimeConf(String key) {
        try {
            return clean(spark.conf().get(key));
        } catch (NoSuchElementException error) {
            return Optional.empty();
        } catch (IllegalArgumentException error) {
            return Optional.empty();
        }
    }

    private Optional<String> fromSparkConf(String key) {
        Option<String> option = spark.sparkContext().getConf().getOption(key);
        if (option.isDefined()) {
            return clean(option.get());
        }
        return Optional.empty();
    }

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

    private Optional<String> clean(String value) {
        if (value == null) {
            return Optional.empty();
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? Optional.<String>empty() : Optional.of(trimmed);
    }
}
