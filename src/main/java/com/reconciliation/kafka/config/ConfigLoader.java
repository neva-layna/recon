package com.reconciliation.kafka.config;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconReporter;

public final class ConfigLoader {
    private ConfigLoader() {
    }

    public static CheckerConfig loadConfig(ConfLookup lookup, Supplier<LocalDate> currentDateSupplier) {
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
            parseBoolean("recon.exitOnCompletion", true, lookup)
        );
    }

    public static Optional<String> confOption(String key, ConfLookup lookup) {
        List<String> aliases = new ArrayList<String>();
        aliases.add(key);
        String sparkAlias = "spark." + key;
        if (!sparkAlias.equals(key)) {
            aliases.add(sparkAlias);
        }

        for (String candidate : aliases) {
            Optional<String> value = lookup.get(candidate);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

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

    static RunDateResult parseRunDate(ConfLookup lookup, Supplier<LocalDate> currentDateSupplier) {
        Optional<String> raw = confOption("recon.runDate", lookup);
        if (!raw.isPresent()) {
            return new RunDateResult(currentDateSupplier.get(), "driver_current_date");
        }

        try {
            return new RunDateResult(LocalDate.parse(raw.get(), ReconConstants.DATE_FORMATTER), "spark_conf:recon.runDate");
        } catch (DateTimeParseException error) {
            ReconReporter.stopNow(
                2,
                "Invalid Spark conf recon.runDate=" + raw.get() + "; expected yyyy-MM-dd: " + error.getMessage()
            );
            return null;
        }
    }

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
