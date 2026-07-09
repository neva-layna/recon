package com.reconciliation.synthdata;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Plain Java command-line entrypoint for local synthetic parquet generation.
 */
public final class SynthDataCli {
    private static final String COLUMN_NAME_PATTERN = "[A-Za-z_][A-Za-z0-9_]*";

    private SynthDataCli() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            if (KafkaSideTopicProducerCli.isKafkaInvocation(args)) {
                return KafkaSideTopicProducerCli.run(args, out, err);
            }
            if (hasHelp(args)) {
                printUsage(out);
                return 0;
            }

            GenerationOptions options = parseOptions(args);
            GeneratedDataset generated = new SyntheticRecordGenerator().generate(options);
            printManifest(out, options, generated);
            return 0;
        } catch (IllegalArgumentException error) {
            err.println("[synth-data] ERROR: " + error.getMessage());
            printUsage(err);
            return 2;
        } catch (IOException error) {
            err.println("[synth-data] ERROR: failed to write parquet: " + error.getMessage());
            return 1;
        }
    }

    private static boolean hasHelp(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static GenerationOptions parseOptions(String[] args) {
        String outputDirectory = null;
        String relativeRoot = null;
        String datePartitionColumn = null;
        String date = null;
        String metadataColumn = GenerationOptions.DEFAULT_METADATA_COLUMN;
        String topic = null;
        String partition = null;
        String offset = null;
        String payload = null;
        Map<String, String> extraValues = new LinkedHashMap<String, String>();

        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if ("--output-dir".equals(arg)) {
                outputDirectory = requireValue(args, ++index, arg);
            } else if ("--relative-root".equals(arg)) {
                relativeRoot = requireValue(args, ++index, arg);
            } else if ("--date-partition-column".equals(arg)) {
                datePartitionColumn = requireValue(args, ++index, arg);
            } else if ("--date".equals(arg)) {
                date = requireValue(args, ++index, arg);
            } else if ("--metadata-column".equals(arg)) {
                metadataColumn = requireValue(args, ++index, arg);
            } else if ("--topic".equals(arg)) {
                topic = requireValue(args, ++index, arg);
            } else if ("--partition".equals(arg)) {
                partition = requireValue(args, ++index, arg);
            } else if ("--offset".equals(arg)) {
                offset = requireValue(args, ++index, arg);
            } else if ("--payload".equals(arg)) {
                payload = requireValue(args, ++index, arg);
            } else if ("--extra".equals(arg)) {
                addExtraValue(extraValues, requireValue(args, ++index, arg));
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        requirePresent("--output-dir", outputDirectory);
        requirePresent("--relative-root", relativeRoot);
        requirePresent("--date-partition-column", datePartitionColumn);
        requirePresent("--date", date);
        requirePresent("--topic", topic);
        requirePresent("--partition", partition);
        requirePresent("--offset", offset);

        validateLocalOutputDirectory(outputDirectory);
        validateRelativeRoot(relativeRoot);
        validateColumnName("date partition column", datePartitionColumn);
        validateColumnName("metadata column", metadataColumn);
        validateColumnConflicts(datePartitionColumn, metadataColumn, extraValues);

        return new GenerationOptions(
            Paths.get(outputDirectory),
            relativeRoot,
            datePartitionColumn,
            parseDate(date),
            metadataColumn,
            topic,
            parseInt("--partition", partition),
            parseLong("--offset", offset),
            payload,
            extraValues
        );
    }

    private static void printManifest(PrintStream out, GenerationOptions options, GeneratedDataset generated) {
        out.println("[synth-data] output_root=" + generated.getOutputRoot());
        out.println("[synth-data] relative_root=" + options.getRelativeRoot());
        out.println("[synth-data] partition_path=" + generated.getPartitionDirectory());
        out.println("[synth-data] parquet_file=" + generated.getParquetFile());
        out.println("[synth-data] date_partition_column=" + options.getDatePartitionColumn());
        out.println("[synth-data] date=" + options.getDate());
        out.println("[synth-data] metadata_column=" + options.getMetadataColumn());
        out.println("[synth-data] metadata_json=" + generated.getMetadataJson());
        out.println("[synth-data] topic=" + options.getTopic());
        out.println("[synth-data] partition=" + options.getPartition());
        out.println("[synth-data] offset=" + options.getOffset());
        out.println("[synth-data] payload_column=" + (options.getPayload() != null));
        out.println("[synth-data] extra_columns=" + String.join(",", options.getExtraValues().keySet()));
        out.println("[synth-data] hdfs_required=false");
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        String value = args[index];
        if (value == null || value.trim().isEmpty() || value.startsWith("--")) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return value;
    }

    private static void requirePresent(String flag, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing required argument " + flag);
        }
    }

    private static void validateLocalOutputDirectory(String value) {
        if (value.contains("://") || value.startsWith("hdfs:")) {
            throw new IllegalArgumentException("--output-dir must be a local filesystem path, not a URI: " + value);
        }
    }

    private static void validateRelativeRoot(String value) {
        if (value.contains("://") || value.startsWith("hdfs:")) {
            throw new IllegalArgumentException("--relative-root must be a relative local path, not a URI: " + value);
        }
        Path path = Paths.get(value);
        if (path.isAbsolute()) {
            throw new IllegalArgumentException("--relative-root must be relative: " + value);
        }
        Path normalized = path.normalize();
        if (normalized.toString().isEmpty() || ".".equals(normalized.toString())) {
            throw new IllegalArgumentException("--relative-root must name a source/root path below the output directory");
        }
        for (Path part : normalized) {
            if ("..".equals(part.toString())) {
                throw new IllegalArgumentException("--relative-root must not contain '..': " + value);
            }
        }
    }

    private static void validateColumnName(String label, String value) {
        if (value == null || !value.matches(COLUMN_NAME_PATTERN)) {
            throw new IllegalArgumentException(label + " must match " + COLUMN_NAME_PATTERN + ": " + value);
        }
    }

    private static void validateColumnConflicts(
        String datePartitionColumn,
        String metadataColumn,
        Map<String, String> extraValues
    ) {
        if ("topic".equals(metadataColumn) || "payload".equals(metadataColumn)) {
            throw new IllegalArgumentException("--metadata-column conflicts with reserved column: " + metadataColumn);
        }
        if ("topic".equals(datePartitionColumn) || "payload".equals(datePartitionColumn) || metadataColumn.equals(datePartitionColumn)) {
            throw new IllegalArgumentException("--date-partition-column conflicts with a data column: " + datePartitionColumn);
        }
        for (String column : extraValues.keySet()) {
            if ("topic".equals(column) || "payload".equals(column) || metadataColumn.equals(column) || datePartitionColumn.equals(column)) {
                throw new IllegalArgumentException("--extra conflicts with generated column: " + column);
            }
        }
    }

    private static void addExtraValue(Map<String, String> extraValues, String rawValue) {
        int separator = rawValue.indexOf('=');
        if (separator <= 0) {
            throw new IllegalArgumentException("--extra must use NAME=VALUE: " + rawValue);
        }
        String key = rawValue.substring(0, separator).trim();
        String value = rawValue.substring(separator + 1);
        validateColumnName("extra column", key);
        if (extraValues.containsKey(key)) {
            throw new IllegalArgumentException("duplicate --extra column: " + key);
        }
        extraValues.put(key, value);
    }

    private static LocalDate parseDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException error) {
            throw new IllegalArgumentException("--date must be yyyy-MM-dd: " + value, error);
        }
    }

    private static int parseInt(String flag, String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(flag + " must be non-negative: " + value);
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(flag + " must be an integer: " + value, error);
        }
    }

    private static long parseLong(String flag, String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0L) {
                throw new IllegalArgumentException(flag + " must be non-negative: " + value);
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(flag + " must be an integer: " + value, error);
        }
    }

    private static void printUsage(PrintStream stream) {
        stream.println("Usage: synth-data --output-dir DIR --relative-root PATH --date-partition-column NAME --date yyyy-MM-dd --topic NAME --partition N --offset N [--metadata-column NAME] [--payload VALUE] [--extra NAME=VALUE]...");
        stream.println("       synth-data kafka-side-topic --bootstrap-server HOST:PORT --destination-topic TOPIC --kind canary|dead-letter ...");
        stream.println("       synth-data --bootstrap-server HOST:PORT --destination-topic TOPIC --kind canary|dead-letter ...");
    }
}
