package com.reconciliation.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.reconciliation.kafka.analytics.OffsetAnalytics;
import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.ConfLookup;
import com.reconciliation.kafka.config.ConfigLoader;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.model.MissingOffsetReport;
import com.reconciliation.kafka.sidetopic.SideTopicAvroDecoder;
import com.reconciliation.kafka.sidetopic.SideTopicClassification;
import com.reconciliation.kafka.sidetopic.SideTopicClassifier;
import com.reconciliation.kafka.sidetopic.SideTopicKind;
import com.reconciliation.kafka.sidetopic.SideTopicRecord;
import com.reconciliation.kafka.sidetopic.SideTopicReconciler;
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
        assertFalse(config.sideTopicConfig.isPresent());
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

    @Test
    public void loadsSideTopicConfigurationWithAliases() {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("recon.inputRoots", "/data/root-a");
        conf.put("spark.recon.sideTopic.sourceTopic", "orders");
        conf.put("spark.recon.kafka.bootstrap.servers", "broker-a:9092");
        conf.put("spark.recon.sideTopic.canaryTopic", "orders-canary");
        conf.put("spark.recon.deadLetterTopic", "orders-dlq");
        conf.put("spark.recon.sideTopicStartingOffsets", "beginning");

        CheckerConfig config = ConfigLoader.loadConfig(new MapLookup(conf), fixedDate("2026-07-04"));

        assertTrue(config.sideTopicConfig.isPresent());
        assertEquals("orders", config.sideTopicConfig.get().sourceTopic);
        assertEquals("broker-a:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("orders-canary", config.sideTopicConfig.get().canaryTopic.get());
        assertEquals("orders-dlq", config.sideTopicConfig.get().deadLetterTopic.get());
        assertEquals("earliest", config.sideTopicConfig.get().startingOffsets);
    }

    @Test
    public void rejectsIncompleteSideTopicConfiguration() {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("recon.inputRoots", "/data/root-a");
        conf.put("spark.recon.canaryTopic", "orders-canary");

        try {
            ConfigLoader.loadConfig(new MapLookup(conf), fixedDate("2026-07-04"));
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("Incomplete side-topic config"));
            assertTrue(exit.getMessage().contains("recon.sourceTopic"));
            assertTrue(exit.getMessage().contains("recon.kafkaBootstrapServers"));
        }
    }

    @Test
    public void rejectsNonEarliestSideTopicReadBehavior() {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("recon.inputRoots", "/data/root-a");
        conf.put("spark.recon.sourceTopic", "orders");
        conf.put("spark.recon.kafkaBootstrapServers", "broker-a:9092");
        conf.put("spark.recon.canaryTopic", "orders-canary");
        conf.put("spark.recon.sideTopicStartingOffsets", "latest");

        try {
            ConfigLoader.loadConfig(new MapLookup(conf), fixedDate("2026-07-04"));
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("expected earliest or beginning"));
        }
    }

    @Test
    public void decodesCanaryAvroObjectContainer() throws Exception {
        byte[] payload = avroPayload(false, "orders", 0, 3L, null, null, null);

        List<SideTopicRecord> records = SideTopicAvroDecoder.decodeContainer(payload, SideTopicKind.CANARY, "orders-canary");

        assertEquals(1, records.size());
        assertEquals("orders", records.get(0).sourceTopic);
        assertEquals(0, records.get(0).sourcePartition);
        assertEquals(3L, records.get(0).sourceOffset);
        assertFalse(records.get(0).failureEventId.isPresent());
    }

    @Test
    public void decodesDeadLetterAvroObjectContainerWithFailureFields() throws Exception {
        byte[] payload = avroPayload(true, "orders", 1, 5L, "evt-1", "bad value", "IllegalStateException");

        List<SideTopicRecord> records = SideTopicAvroDecoder.decodeContainer(payload, SideTopicKind.DEAD_LETTER, "orders-dlq");

        assertEquals(1, records.size());
        assertEquals("orders", records.get(0).sourceTopic);
        assertEquals(1, records.get(0).sourcePartition);
        assertEquals(5L, records.get(0).sourceOffset);
        assertEquals("evt-1", records.get(0).failureEventId.get());
        assertEquals("bad value", records.get(0).reasonMsg.get());
        assertEquals("IllegalStateException", records.get(0).exception.get());
    }

    @Test
    public void classifiesSideTopicsBySourceTopicPartitionAndOffset() {
        Map<Integer, MissingOffsetReport> missing = new LinkedHashMap<Integer, MissingOffsetReport>();
        missing.put(0, new MissingOffsetReport(Arrays.asList(1L, 2L, 3L), false));
        missing.put(1, new MissingOffsetReport(Arrays.asList(7L), false));
        GapAnalysisResult gaps = new GapAnalysisResult(2L, missing);

        List<SideTopicRecord> canary = new ArrayList<SideTopicRecord>();
        canary.add(record(SideTopicKind.CANARY, "orders-canary", "orders", 0, 1L));
        canary.add(record(SideTopicKind.CANARY, "orders-canary", "orders", 0, 1L));
        canary.add(record(SideTopicKind.CANARY, "orders-canary", "other-topic", 0, 2L));
        canary.add(record(SideTopicKind.CANARY, "orders-canary", "orders", 9, 7L));
        canary.add(record(SideTopicKind.CANARY, "orders-canary", "orders", 0, 99L));
        List<SideTopicRecord> deadLetter = new ArrayList<SideTopicRecord>();
        deadLetter.add(new SideTopicRecord(
            SideTopicKind.DEAD_LETTER,
            "orders-dlq",
            "orders",
            0,
            2L,
            Optional.of("evt-2"),
            Optional.of("reason"),
            Optional.of("Exception")
        ));

        SideTopicClassification classification = SideTopicClassifier.classify("orders", gaps, canary, deadLetter);

        assertEquals(Arrays.asList(1L), classification.canaryExplainedOffsets.get(0));
        assertEquals(Arrays.asList(2L), classification.deadLetterExplainedOffsets.get(0));
        assertEquals(Arrays.asList(3L), classification.unresolvedOffsets.get(0));
        assertEquals(Arrays.asList(7L), classification.unresolvedOffsets.get(1));
        assertEquals(1L, classification.canaryExplainedCount);
        assertEquals(1L, classification.deadLetterExplainedCount);
        assertEquals(2L, classification.unresolvedCount);
        assertEquals(2L, classification.rawGapPartitionCount);
        assertEquals(4L, classification.boundedMissingOffsetCount);
        assertEquals(5L, classification.canaryRecordCount);
        assertEquals(1L, classification.deadLetterFailureEventIdCount);
    }

    @Test
    public void gapExitDecisionUsesUnresolvedSideTopicOffsets() {
        Map<Integer, MissingOffsetReport> missing = new LinkedHashMap<Integer, MissingOffsetReport>();
        missing.put(0, new MissingOffsetReport(Arrays.asList(1L, 2L), false));
        GapAnalysisResult gaps = new GapAnalysisResult(1L, missing);

        Optional<String> rawGapFailure = KafkaOffsetGapChecker.gapFailureReason(
            checkerConfig(true),
            gaps,
            Optional.<SideTopicClassification>empty()
        );
        assertTrue(rawGapFailure.isPresent());
        assertTrue(rawGapFailure.get().contains("gap_partition_count=1"));

        Optional<String> resolvedSideTopicFailure = KafkaOffsetGapChecker.gapFailureReason(
            checkerConfig(true),
            gaps,
            Optional.of(classification(1L, 1L, 0L))
        );
        assertFalse(resolvedSideTopicFailure.isPresent());

        Optional<String> truncatedSideTopicFailure = KafkaOffsetGapChecker.gapFailureReason(
            checkerConfig(true),
            gaps,
            Optional.of(classification(1L, 1L, 0L, true))
        );
        assertTrue(truncatedSideTopicFailure.isPresent());
        assertTrue(truncatedSideTopicFailure.get().contains("unresolved offsets may remain beyond materialized limit"));
        assertTrue(truncatedSideTopicFailure.get().contains("missing_offsets_truncated=true"));
        assertTrue(truncatedSideTopicFailure.get().contains("unresolved_count=0"));

        Optional<String> unresolvedSideTopicFailure = KafkaOffsetGapChecker.gapFailureReason(
            checkerConfig(true),
            gaps,
            Optional.of(classification(1L, 0L, 1L))
        );
        assertTrue(unresolvedSideTopicFailure.isPresent());
        assertTrue(unresolvedSideTopicFailure.get().contains("unresolved_count=1"));

        Optional<String> disabledGapFailure = KafkaOffsetGapChecker.gapFailureReason(
            checkerConfig(false),
            gaps,
            Optional.of(classification(0L, 0L, 2L))
        );
        assertFalse(disabledGapFailure.isPresent());
    }

    @Test
    public void formatsSideTopicOffsetsWithoutSpaces() {
        assertEquals("[4,5]", SideTopicReconciler.formatOffsets(Arrays.asList(4L, 5L)));
        assertEquals("[]", SideTopicReconciler.formatOffsets(Collections.<Long>emptyList()));
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

    private static SideTopicRecord record(
        SideTopicKind kind,
        String sideTopic,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset
    ) {
        return new SideTopicRecord(
            kind,
            sideTopic,
            sourceTopic,
            sourcePartition,
            sourceOffset,
            Optional.<String>empty(),
            Optional.<String>empty(),
            Optional.<String>empty()
        );
    }

    private static CheckerConfig checkerConfig(boolean failOnGaps) {
        return new CheckerConfig(
            Arrays.asList("/data/root-a"),
            "cactus__metadata",
            "timestampcolumn",
            LocalDate.parse("2026-07-02"),
            "test",
            Optional.<String>empty(),
            true,
            true,
            failOnGaps,
            1000L,
            true,
            Optional.empty()
        );
    }

    private static SideTopicClassification classification(
        long canaryExplainedCount,
        long deadLetterExplainedCount,
        long unresolvedCount
    ) {
        return classification(canaryExplainedCount, deadLetterExplainedCount, unresolvedCount, false);
    }

    private static SideTopicClassification classification(
        long canaryExplainedCount,
        long deadLetterExplainedCount,
        long unresolvedCount,
        boolean missingOffsetsTruncated
    ) {
        return new SideTopicClassification(
            "orders",
            Collections.<Integer, List<Long>>emptyMap(),
            Collections.<Integer, List<Long>>emptyMap(),
            unresolvedCount > 0L
                ? Collections.singletonMap(Integer.valueOf(0), Arrays.asList(2L))
                : Collections.<Integer, List<Long>>emptyMap(),
            canaryExplainedCount,
            deadLetterExplainedCount,
            unresolvedCount,
            1L,
            canaryExplainedCount + deadLetterExplainedCount + unresolvedCount,
            canaryExplainedCount,
            deadLetterExplainedCount,
            0L,
            0L,
            0L,
            missingOffsetsTruncated
        );
    }

    private static byte[] avroPayload(
        boolean deadLetter,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        String failureEventId,
        String reasonMsg,
        String exception
    ) throws IOException {
        Schema schema = new Schema.Parser().parse(deadLetter ? deadLetterSchema() : canarySchema());
        GenericRecord record = new GenericData.Record(schema);
        record.put("sourceKey", "key-1");
        record.put("sourceValue", "value-1");
        record.put("sourceHeaders", Collections.<String, String>emptyMap());
        record.put("sourceTopic", sourceTopic);
        record.put("sourcePartition", sourcePartition);
        record.put("sourceOffset", sourceOffset);
        record.put("sourceKafkaTimestamp", 123456789L);
        if (deadLetter) {
            record.put("failureEventId", failureEventId);
            record.put("reasonMsg", reasonMsg);
            record.put("exception", exception);
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter);
        writer.create(schema, output);
        writer.append(record);
        writer.close();
        return output.toByteArray();
    }

    private static String canarySchema() {
        return "{"
            + "\"type\":\"record\","
            + "\"name\":\"CanaryRecord\","
            + "\"fields\":["
            + "{\"name\":\"sourceKey\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sourceValue\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sourceHeaders\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
            + "{\"name\":\"sourceTopic\",\"type\":\"string\"},"
            + "{\"name\":\"sourcePartition\",\"type\":\"int\"},"
            + "{\"name\":\"sourceOffset\",\"type\":\"long\"},"
            + "{\"name\":\"sourceKafkaTimestamp\",\"type\":\"long\"}"
            + "]"
            + "}";
    }

    private static String deadLetterSchema() {
        return "{"
            + "\"type\":\"record\","
            + "\"name\":\"DeadLetterRecord\","
            + "\"fields\":["
            + "{\"name\":\"sourceKey\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sourceValue\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sourceHeaders\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
            + "{\"name\":\"sourceTopic\",\"type\":\"string\"},"
            + "{\"name\":\"sourcePartition\",\"type\":\"int\"},"
            + "{\"name\":\"sourceOffset\",\"type\":\"long\"},"
            + "{\"name\":\"sourceKafkaTimestamp\",\"type\":\"long\"},"
            + "{\"name\":\"failureEventId\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"reasonMsg\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"exception\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]"
            + "}";
    }
}
