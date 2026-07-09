package com.reconciliation.synthdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/**
 * Builds checker-compatible Avro object-container Kafka values.
 */
public final class SideTopicAvroPayloads {
    private static final String CANARY_SCHEMA_TEXT =
        "{"
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

    private static final String DEAD_LETTER_SCHEMA_TEXT =
        "{"
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

    private SideTopicAvroPayloads() {
    }

    public static byte[] create(KafkaSideTopicProducerOptions options) throws IOException {
        Schema schema = schemaFor(options.getKind());
        GenericRecord record = new GenericData.Record(schema);
        record.put("sourceKey", options.getSourceKey());
        record.put("sourceValue", options.getSourceValue());
        record.put("sourceHeaders", new LinkedHashMap<String, String>(options.getSourceHeaders()));
        record.put("sourceTopic", options.getSourceTopic());
        record.put("sourcePartition", options.getSourcePartition());
        record.put("sourceOffset", options.getSourceOffset());
        record.put("sourceKafkaTimestamp", options.getSourceTimestamp());
        if (options.getKind() == KafkaSideTopicKind.DEAD_LETTER) {
            record.put("failureEventId", options.getFailureEventId());
            record.put("reasonMsg", options.getReasonMsg());
            record.put("exception", options.getException());
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter);
        writer.create(schema, output);
        writer.append(record);
        writer.close();
        return output.toByteArray();
    }

    static Schema schemaFor(KafkaSideTopicKind kind) {
        String schemaText = kind == KafkaSideTopicKind.DEAD_LETTER ? DEAD_LETTER_SCHEMA_TEXT : CANARY_SCHEMA_TEXT;
        return new Schema.Parser().parse(schemaText);
    }
}
