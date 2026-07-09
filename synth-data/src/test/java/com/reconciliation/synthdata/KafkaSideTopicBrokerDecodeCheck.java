package com.reconciliation.synthdata;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Test utility that consumes one Kafka side-topic record and decodes its Avro
 * object-container value.
 */
public final class KafkaSideTopicBrokerDecodeCheck {
    private KafkaSideTopicBrokerDecodeCheck() {
    }

    public static void main(String[] args) throws Exception {
        ExpectedRecord expected = ExpectedRecord.parse(args);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, expected.bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "synth-data-side-topic-check-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(properties);
        try {
            consumer.subscribe(Collections.singleton(expected.topic));
            long deadline = System.currentTimeMillis() + expected.timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500L));
                for (ConsumerRecord<byte[], byte[]> kafkaRecord : records) {
                    SideTopicDecodedRecord decoded = SideTopicPayloadDecodeSupport.decodeSingle(
                        kafkaRecord.value(),
                        expected.kind,
                        kafkaRecord.topic()
                    );
                    if (expected.matches(decoded)) {
                        System.out.println(
                            "[synth-data-test] broker_record"
                                + " topic=" + kafkaRecord.topic()
                                + " kafka_partition=" + kafkaRecord.partition()
                                + " kafka_offset=" + kafkaRecord.offset()
                                + " key_bytes=" + (kafkaRecord.key() == null ? 0 : kafkaRecord.key().length)
                                + " payload_bytes=" + (kafkaRecord.value() == null ? 0 : kafkaRecord.value().length)
                        );
                        SideTopicPayloadDecodeSupport.printDecoded(
                            System.out,
                            decoded,
                            "[synth-data-test] decoded_broker_payload"
                        );
                        return;
                    }
                }
            }
            throw new AssertionError("timed out waiting for matching record on " + expected.topic);
        } finally {
            consumer.close();
        }
    }

    private static final class ExpectedRecord {
        private String bootstrapServer;
        private String topic;
        private KafkaSideTopicKind kind;
        private String sourceTopic;
        private Integer sourcePartition;
        private Long sourceOffset;
        private Long sourceTimestamp;
        private String sourceKey;
        private String sourceValue;
        private String headerKey;
        private String headerValue;
        private String failureEventId;
        private String reasonMsg;
        private String exception;
        private long timeoutMs = 30000L;

        static ExpectedRecord parse(String[] args) {
            ExpectedRecord expected = new ExpectedRecord();
            for (int index = 0; index < args.length; index++) {
                String arg = args[index];
                if ("--bootstrap-server".equals(arg)) {
                    expected.bootstrapServer = value(args, ++index, arg);
                } else if ("--topic".equals(arg)) {
                    expected.topic = value(args, ++index, arg);
                } else if ("--kind".equals(arg)) {
                    expected.kind = KafkaSideTopicKind.parse(value(args, ++index, arg));
                } else if ("--source-topic".equals(arg)) {
                    expected.sourceTopic = value(args, ++index, arg);
                } else if ("--source-partition".equals(arg)) {
                    expected.sourcePartition = Integer.valueOf(value(args, ++index, arg));
                } else if ("--source-offset".equals(arg)) {
                    expected.sourceOffset = Long.valueOf(value(args, ++index, arg));
                } else if ("--source-timestamp".equals(arg)) {
                    expected.sourceTimestamp = Long.valueOf(value(args, ++index, arg));
                } else if ("--source-key".equals(arg)) {
                    expected.sourceKey = value(args, ++index, arg);
                } else if ("--source-value".equals(arg)) {
                    expected.sourceValue = value(args, ++index, arg);
                } else if ("--source-header".equals(arg)) {
                    String header = value(args, ++index, arg);
                    int separator = header.indexOf('=');
                    if (separator <= 0) {
                        throw new IllegalArgumentException("--source-header must be key=value: " + header);
                    }
                    expected.headerKey = header.substring(0, separator);
                    expected.headerValue = header.substring(separator + 1);
                } else if ("--failure-event-id".equals(arg)) {
                    expected.failureEventId = value(args, ++index, arg);
                } else if ("--reason-msg".equals(arg)) {
                    expected.reasonMsg = value(args, ++index, arg);
                } else if ("--exception".equals(arg)) {
                    expected.exception = value(args, ++index, arg);
                } else if ("--timeout-ms".equals(arg)) {
                    expected.timeoutMs = Long.parseLong(value(args, ++index, arg));
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            expected.require();
            return expected;
        }

        boolean matches(SideTopicDecodedRecord record) {
            return kind == record.getKind()
                && topic.equals(record.getSideTopic())
                && sourceTopic.equals(record.getSourceTopic())
                && sourcePartition.intValue() == record.getSourcePartition()
                && sourceOffset.longValue() == record.getSourceOffset()
                && sourceTimestamp.longValue() == record.getSourceTimestamp()
                && sourceKey.equals(record.getSourceKey())
                && sourceValue.equals(record.getSourceValue())
                && (headerKey == null || headerValue.equals(record.getSourceHeaders().get(headerKey)))
                && (failureEventId == null || failureEventId.equals(record.getFailureEventId()))
                && (reasonMsg == null || reasonMsg.equals(record.getReasonMsg()))
                && (exception == null || exception.equals(record.getException()));
        }

        private void require() {
            requireText(bootstrapServer, "--bootstrap-server");
            requireText(topic, "--topic");
            if (kind == null) {
                throw new IllegalArgumentException("missing --kind");
            }
            requireText(sourceTopic, "--source-topic");
            if (sourcePartition == null) {
                throw new IllegalArgumentException("missing --source-partition");
            }
            if (sourceOffset == null) {
                throw new IllegalArgumentException("missing --source-offset");
            }
            if (sourceTimestamp == null) {
                throw new IllegalArgumentException("missing --source-timestamp");
            }
            requireText(sourceKey, "--source-key");
            requireText(sourceValue, "--source-value");
        }

        private static void requireText(String value, String flag) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("missing " + flag);
            }
        }

        private static String value(String[] args, int index, String flag) {
            if (index >= args.length || args[index].startsWith("--")) {
                throw new IllegalArgumentException("missing value for " + flag);
            }
            return args[index];
        }
    }
}
