package com.reconciliation.synthdata;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Smoke tests Kafka side-topic parsing and Avro object-container compatibility
 * without requiring a broker.
 */
public final class KafkaSideTopicPayloadSmokeTest {
    private KafkaSideTopicPayloadSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        KafkaSideTopicProducerOptions canary = KafkaSideTopicProducerOptions.parse(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--conf", "security.protocol=PLAINTEXT",
            "--conf", "acks=all",
            "--conf", "key.serializer=example.WrongSerializer",
            "--destination-topic", "orders-canary",
            "--kind", "canary",
            "--source-topic", "orders",
            "--source-partition", "2",
            "--source-offset", "42",
            "--source-timestamp", "1710000000042",
            "--source-key", "order-42",
            "--source-value", "payload-42",
            "--source-header", "trace=abc",
            "--source-header", "attempt=1",
            "--dry-run"
        });
        require("PLAINTEXT".equals(canary.getProducerConf().get("security.protocol")), "security.protocol conf");
        require("all".equals(canary.getProducerConf().get("acks")), "acks conf");
        Properties properties = canary.producerProperties();
        require(ByteArraySerializer.class.getName().equals(properties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)),
            "key serializer preserved");
        require(ByteArraySerializer.class.getName().equals(properties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)),
            "value serializer preserved");
        SideTopicDecodedRecord canaryDecoded = SideTopicPayloadDecodeSupport.decodeSingle(
            SideTopicAvroPayloads.create(canary),
            KafkaSideTopicKind.CANARY,
            "orders-canary"
        );
        requireCanary(canaryDecoded);
        SideTopicPayloadDecodeSupport.printDecoded(System.out, canaryDecoded, "[synth-data-test] decoded_payload");

        KafkaSideTopicProducerOptions deadLetter = KafkaSideTopicProducerOptions.parse(new String[] {
            "kafka-side-topic",
            "--bootstrap-server", "localhost:9092",
            "--conf", "security.protocol=PLAINTEXT",
            "--destination-topic", "orders-dlq",
            "--kind", "dead-letter",
            "--source-topic", "orders",
            "--source-partition", "3",
            "--source-offset", "77",
            "--source-timestamp", "1710000000077",
            "--source-key", "order-77",
            "--source-value", "payload-77",
            "--source-headers", "trace=dead,attempt=2",
            "--failure-event-id", "evt-77",
            "--reason-msg", "validation failed",
            "--exception", "IllegalStateException",
            "--dry-run"
        });
        SideTopicDecodedRecord deadLetterDecoded = SideTopicPayloadDecodeSupport.decodeSingle(
            SideTopicAvroPayloads.create(deadLetter),
            KafkaSideTopicKind.DEAD_LETTER,
            "orders-dlq"
        );
        requireDeadLetter(deadLetterDecoded);
        SideTopicPayloadDecodeSupport.printDecoded(System.out, deadLetterDecoded, "[synth-data-test] decoded_payload");

        expectParseFailure(new String[] {"--bootstrap-server", "localhost:9092"}, "missing required args");
        expectParseFailure(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--conf", "security.protocol",
            "--destination-topic", "orders-canary",
            "--kind", "canary",
            "--source-topic", "orders",
            "--source-partition", "0",
            "--source-offset", "1",
            "--source-timestamp", "1710000000001",
            "--source-key", "order-1",
            "--source-value", "payload-1",
            "--source-headers", "none"
        }, "invalid conf");
        expectParseFailure(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--destination-topic", "orders-canary",
            "--kind", "bad-kind",
            "--source-topic", "orders",
            "--source-partition", "0",
            "--source-offset", "1",
            "--source-timestamp", "1710000000001",
            "--source-key", "order-1",
            "--source-value", "payload-1",
            "--source-headers", "none"
        }, "invalid kind");
        expectParseFailure(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--destination-topic", "orders-canary",
            "--kind", "canary",
            "--source-topic", "orders",
            "--source-partition", "0",
            "--source-offset", "not-a-number",
            "--source-timestamp", "1710000000001",
            "--source-key", "order-1",
            "--source-value", "payload-1",
            "--source-headers", "none"
        }, "malformed numeric");
    }

    static void requireCanary(SideTopicDecodedRecord record) {
        require(record.getKind() == KafkaSideTopicKind.CANARY, "canary kind");
        require("orders-canary".equals(record.getSideTopic()), "canary side topic");
        require("orders".equals(record.getSourceTopic()), "canary source topic");
        require(record.getSourcePartition() == 2, "canary source partition");
        require(record.getSourceOffset() == 42L, "canary source offset");
        require(record.getSourceTimestamp() == 1710000000042L, "canary timestamp");
        require("order-42".equals(record.getSourceKey()), "canary source key");
        require("payload-42".equals(record.getSourceValue()), "canary source value");
        require("abc".equals(record.getSourceHeaders().get("trace")), "canary trace header");
        require("1".equals(record.getSourceHeaders().get("attempt")), "canary attempt header");
        require(record.getFailureEventId() == null, "canary failure event absent");
    }

    static void requireDeadLetter(SideTopicDecodedRecord record) {
        require(record.getKind() == KafkaSideTopicKind.DEAD_LETTER, "dead-letter kind");
        require("orders-dlq".equals(record.getSideTopic()), "dead-letter side topic");
        require("orders".equals(record.getSourceTopic()), "dead-letter source topic");
        require(record.getSourcePartition() == 3, "dead-letter source partition");
        require(record.getSourceOffset() == 77L, "dead-letter source offset");
        require(record.getSourceTimestamp() == 1710000000077L, "dead-letter timestamp");
        require("order-77".equals(record.getSourceKey()), "dead-letter source key");
        require("payload-77".equals(record.getSourceValue()), "dead-letter source value");
        require("dead".equals(record.getSourceHeaders().get("trace")), "dead-letter trace header");
        require("2".equals(record.getSourceHeaders().get("attempt")), "dead-letter attempt header");
        require("evt-77".equals(record.getFailureEventId()), "failure event id");
        require("validation failed".equals(record.getReasonMsg()), "reason msg");
        require("IllegalStateException".equals(record.getException()), "exception");
    }

    private static void expectParseFailure(String[] args, String label) {
        try {
            KafkaSideTopicProducerOptions.parse(args);
            throw new AssertionError("expected parse failure: " + label);
        } catch (IllegalArgumentException expected) {
            System.out.println("[synth-data-test] expected_error label=" + label + " message=" + expected.getMessage());
        }
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
