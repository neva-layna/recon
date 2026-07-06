package com.reconciliation.kafka.sidetopic;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.reconciliation.kafka.config.SideTopicConfig;
import com.reconciliation.kafka.support.ReconReporter;

import static org.junit.Assert.assertTrue;

public class SideTopicReconcilerTest {
    private Logger reporterLogger;
    private ListAppender<ILoggingEvent> appender;

    @Before
    public void attachLogCapture() {
        reporterLogger = (Logger) LoggerFactory.getLogger(ReconReporter.class);
        appender = new ListAppender<ILoggingEvent>();
        appender.start();
        reporterLogger.addAppender(appender);
    }

    @After
    public void detachLogCapture() {
        reporterLogger.detachAppender(appender);
        appender.stop();
    }

    @Test
    public void printsMachineReadableSideTopicBucketsAndSummary() throws Exception {
        SideTopicConfig config = new SideTopicConfig(
            "orders",
            Optional.of("main-kafka"),
            kafkaConf(),
            Optional.of("orders-canary"),
            Optional.of("orders-dlq"),
            "earliest"
        );
        Map<Integer, List<Long>> canary = new LinkedHashMap<Integer, List<Long>>();
        canary.put(0, Arrays.asList(1L));
        Map<Integer, List<Long>> deadLetter = new LinkedHashMap<Integer, List<Long>>();
        deadLetter.put(0, Arrays.asList(2L));
        Map<Integer, List<Long>> unresolved = new LinkedHashMap<Integer, List<Long>>();
        unresolved.put(1, Arrays.asList(9L));
        SideTopicClassification classification = new SideTopicClassification(
            "orders",
            canary,
            deadLetter,
            unresolved,
            1L,
            1L,
            1L,
            2L,
            3L,
            2L,
            3L,
            1L,
            1L,
            1L,
            true
        );

        SideTopicReconciler.printClassification(config, classification);

        String text = loggedText();
        assertTrue(text.contains(
            "[recon] side_topic_bucket=canary_explained source_topic=orders side_topic=orders-canary partition=0 offset_count=1 offsets=[1]"
        ));
        assertTrue(text.contains(
            "[recon] side_topic_bucket=dead_letter_explained source_topic=orders side_topic=orders-dlq partition=0 offset_count=1 offsets=[2]"
        ));
        assertTrue(text.contains(
            "[recon] side_topic_bucket=unresolved source_topic=orders side_topic=<none> partition=1 offset_count=1 offsets=[9]"
        ));
        assertTrue(text.contains(
            "[recon] side_topic_summary source_topic=orders raw_gap_partition_count=2 bounded_missing_offset_count=3 canary_explained_count=1 dead_letter_explained_count=1 unresolved_count=1"
        ));
        assertTrue(text.contains("missing_offsets_truncated=true"));
        assertTrue(text.contains(
            "[recon] side_topic_dead_letter_fields failure_event_id_count=1 reason_msg_count=1 exception_count=1"
        ));
    }

    @Test
    public void buildsSparkKafkaReaderOptionsFromBrokerConf() {
        SideTopicConfig config = new SideTopicConfig(
            "orders",
            Optional.of("main-kafka"),
            kafkaConf(),
            Optional.of("orders-canary"),
            Optional.of("orders-dlq"),
            "earliest"
        );

        Map<String, String> options = SideTopicReaderOptions.build(config, "orders-canary");

        assertTrue(options.containsKey("kafka.bootstrap.servers"));
        assertTrue(options.containsKey("kafka.security.protocol"));
        assertTrue(options.containsKey("kafka.max.poll.records"));
        assertTrue(options.containsKey("subscribe"));
        assertTrue(options.containsKey("startingOffsets"));
        assertTrue(options.containsKey("endingOffsets"));
        assertTrue(options.containsKey("failOnDataLoss"));
        org.junit.Assert.assertEquals("broker-a:9092", options.get("kafka.bootstrap.servers"));
        org.junit.Assert.assertEquals("SASL_SSL", options.get("kafka.security.protocol"));
        org.junit.Assert.assertEquals("500", options.get("kafka.max.poll.records"));
        org.junit.Assert.assertEquals("orders-canary", options.get("subscribe"));
        org.junit.Assert.assertEquals("earliest", options.get("startingOffsets"));
        org.junit.Assert.assertEquals("latest", options.get("endingOffsets"));
        org.junit.Assert.assertEquals("true", options.get("failOnDataLoss"));
    }

    private String loggedText() {
        StringBuilder builder = new StringBuilder();
        for (ILoggingEvent event : appender.list) {
            builder.append(event.getFormattedMessage()).append('\n');
        }
        return builder.toString();
    }

    private static Map<String, String> kafkaConf() {
        Map<String, String> conf = new LinkedHashMap<String, String>();
        conf.put("bootstrap.servers", "broker-a:9092");
        conf.put("security.protocol", "SASL_SSL");
        conf.put("max.poll.records", "500");
        return conf;
    }
}
