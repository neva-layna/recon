package com.reconciliation.kafka;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Test;

import com.reconciliation.kafka.analytics.OffsetAnalytics;
import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.ConfLookup;
import com.reconciliation.kafka.config.ConfigLoader;
import com.reconciliation.kafka.support.ReconExit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaOffsetGapCheckerTest {
    @Test
    public void loadsCanonicalAndAliasConfiguration() {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("spark.recon.inputRoots", "/data/root-a, /data/root-b");
        conf.put("spark.recon.runDate", "2026-07-02");
        conf.put("recon.metadataColumn", "meta_json");
        conf.put("spark.recon.datePartitionColumn", "dt");
        conf.put("recon.failOnGaps", "no");
        conf.put("spark.recon.failOnInvalidRows", "0");
        conf.put("spark.recon.normalizedOffsetsPath", "file:///tmp/recon-normalized");
        conf.put("spark.recon.normalizedOffsetsOverwrite", "false");
        conf.put("spark.recon.missingOffsetsLimit", "0");
        conf.put("spark.recon.exitOnCompletion", "n");

        CheckerConfig config = ConfigLoader.loadConfig(
            new MapLookup(conf),
            fixedDate("2026-07-03")
        );

        assertEquals(Arrays.asList("/data/root-a", "/data/root-b"), config.inputRoots);
        assertEquals("meta_json", config.metadataColumn);
        assertEquals("dt", config.datePartitionColumn);
        assertEquals(LocalDate.parse("2026-07-02"), config.runDate);
        assertEquals("spark_conf:recon.runDate", config.runDateSource);
        assertEquals("file:///tmp/recon-normalized", config.normalizedOffsetsPath.get());
        assertFalse(config.normalizedOffsetsOverwrite);
        assertFalse(config.failOnInvalidRows);
        assertFalse(config.failOnGaps);
        assertEquals(0L, config.missingOffsetsLimit);
        assertFalse(config.exitOnCompletion);
    }

    @Test
    public void defaultsRunDateWhenUnset() {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("recon.inputRoots", "/data/root-a");

        CheckerConfig config = ConfigLoader.loadConfig(
            new MapLookup(conf),
            fixedDate("2026-07-04")
        );

        assertEquals(LocalDate.parse("2026-07-04"), config.runDate);
        assertEquals("driver_current_date", config.runDateSource);
        assertEquals("cactus__metadata", config.metadataColumn);
        assertEquals("timestampcolumn", config.datePartitionColumn);
        assertTrue(config.normalizedOffsetsOverwrite);
        assertTrue(config.failOnInvalidRows);
        assertTrue(config.failOnGaps);
        assertEquals(1000L, config.missingOffsetsLimit);
        assertTrue(config.exitOnCompletion);
    }

    @Test
    public void rejectsMissingInputRoots() {
        try {
            ConfigLoader.loadConfig(new MapLookup(new HashMap<String, String>()), fixedDate("2026-07-04"));
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("Missing required Spark conf recon.inputRoots"));
        }
    }

    @Test
    public void rejectsInvalidBoolean() {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("recon.failOnGaps", "maybe");
        try {
            ConfigLoader.parseBoolean("recon.failOnGaps", true, new MapLookup(conf));
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("Invalid boolean Spark conf recon.failOnGaps=maybe"));
        }
    }

    @Test
    public void formatsMissingOffsetsWithoutSpaces() {
        assertEquals("[1,2,3]", OffsetAnalytics.formatMissingOffsets(Arrays.asList(1L, 2L, 3L)));
        assertEquals("[]", OffsetAnalytics.formatMissingOffsets(Arrays.<Long>asList()));
    }

    private static Supplier<LocalDate> fixedDate(final String date) {
        return new Supplier<LocalDate>() {
            @Override
            public LocalDate get() {
                return LocalDate.parse(date);
            }
        };
    }

    private static final class MapLookup implements ConfLookup {
        private final Map<String, String> values;

        private MapLookup(Map<String, String> values) {
            this.values = values;
        }

        @Override
        public Optional<String> get(String key) {
            String value = values.get(key);
            return value == null || value.trim().isEmpty()
                ? Optional.<String>empty()
                : Optional.of(value.trim());
        }
    }
}
