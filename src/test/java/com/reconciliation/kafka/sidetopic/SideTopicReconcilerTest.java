package com.reconciliation.kafka.sidetopic;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.reconciliation.kafka.config.SideTopicConfig;

import static org.junit.Assert.assertTrue;

public class SideTopicReconcilerTest {
    @Test
    public void printsMachineReadableSideTopicBucketsAndSummary() throws Exception {
        SideTopicConfig config = new SideTopicConfig(
            "orders",
            "broker-a:9092",
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
            1L,
            1L,
            1L,
            false
        );

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(output, true, "UTF-8"));
            SideTopicReconciler.printClassification(config, classification);
        } finally {
            System.setOut(originalOut);
        }

        String text = output.toString("UTF-8");
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
            "[recon] side_topic_summary source_topic=orders canary_explained_count=1 dead_letter_explained_count=1 unresolved_count=1"
        ));
        assertTrue(text.contains(
            "[recon] side_topic_dead_letter_fields failure_event_id_count=1 reason_msg_count=1 exception_count=1"
        ));
    }
}
