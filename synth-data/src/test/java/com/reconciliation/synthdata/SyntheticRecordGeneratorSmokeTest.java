package com.reconciliation.synthdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Smoke test run by Gradle's cliSmokeTest task.
 */
public final class SyntheticRecordGeneratorSmokeTest {
    private SyntheticRecordGeneratorSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        Path outputDirectory = Files.createTempDirectory("synth-data-parquet-smoke");
        try {
            Map<String, String> extras = new LinkedHashMap<String, String>();
            extras.put("region", "emea");
            extras.put("source", "smoke");

            GenerationOptions options = new GenerationOptions(
                outputDirectory,
                "orders/root_a",
                "timestampcolumn",
                LocalDate.parse("2026-07-01"),
                GenerationOptions.DEFAULT_METADATA_COLUMN,
                "orders",
                2,
                10L,
                "payload-10",
                extras
            );

            GeneratedDataset generated = new SyntheticRecordGenerator().generate(options);
            require(Files.isDirectory(generated.getPartitionDirectory()), "partition directory");
            require(Files.isRegularFile(generated.getParquetFile()), "parquet file");
            require(Files.size(generated.getParquetFile()) > 0L, "parquet file has bytes");
            require(generated.getPartitionDirectory().endsWith("timestampcolumn=2026-07-01"), "Hive-style date path");
            require("{\"partition\":2,\"offset\":10}".equals(generated.getMetadataJson()), "metadata JSON");

            assertCliMissingArgumentFails(outputDirectory);
        } finally {
            deleteRecursively(outputDirectory);
        }
    }

    private static void assertCliMissingArgumentFails(Path outputDirectory) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        int exit = SynthDataCli.run(
            new String[] {"--output-dir", outputDirectory.toString()},
            new PrintStream(stdout),
            new PrintStream(stderr)
        );
        String error = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
        require(exit == 2, "missing-arg exit");
        require(error.contains("missing required argument --relative-root"), "missing-arg error");
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        Files.walk(path)
            .sorted((left, right) -> right.compareTo(left))
            .forEach(SyntheticRecordGeneratorSmokeTest::deleteIfExists);
    }

    private static void deleteIfExists(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException error) {
            throw new IllegalStateException("failed to delete " + path, error);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
