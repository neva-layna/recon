package com.reconciliation.synthdata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

final class SideTopicPayloadDecodeSupport {
    private SideTopicPayloadDecodeSupport() {
    }

    static SideTopicDecodedRecord decodeSingle(byte[] payload, KafkaSideTopicKind kind, String sideTopic)
        throws IOException {
        require(payload != null && payload.length >= 4, "payload too short for Avro object-container");
        require(payload[0] == 'O' && payload[1] == 'b' && payload[2] == 'j' && payload[3] == 1,
            "payload is not an Avro object-container");

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(
            new ByteArrayInputStream(payload),
            datumReader
        );
        try {
            require(stream.hasNext(), "Avro object-container payload contained zero records");
            GenericRecord record = stream.next();
            require(!stream.hasNext(), "expected exactly one record in payload");
            requireField(record, "sourceKey");
            requireField(record, "sourceValue");
            requireField(record, "sourceHeaders");
            requireField(record, "sourceTopic");
            requireField(record, "sourcePartition");
            requireField(record, "sourceOffset");
            requireField(record, "sourceKafkaTimestamp");
            if (kind == KafkaSideTopicKind.DEAD_LETTER) {
                requireField(record, "failureEventId");
                requireField(record, "reasonMsg");
                requireField(record, "exception");
            }

            return new SideTopicDecodedRecord(
                kind,
                sideTopic,
                requiredString(record, "sourceKey"),
                requiredString(record, "sourceValue"),
                mapString(record, "sourceHeaders"),
                requiredString(record, "sourceTopic"),
                requiredInt(record, "sourcePartition"),
                requiredLong(record, "sourceOffset"),
                requiredLong(record, "sourceKafkaTimestamp"),
                optionalString(record, "failureEventId"),
                optionalString(record, "reasonMsg"),
                optionalString(record, "exception")
            );
        } finally {
            stream.close();
        }
    }

    static void printDecoded(PrintStream out, SideTopicDecodedRecord record, String prefix) {
        out.println(
            prefix
                + " object_container=true"
                + " kind=" + record.getKind().getCliName()
                + " side_topic=" + record.getSideTopic()
                + " source_topic=" + record.getSourceTopic()
                + " source_partition=" + record.getSourcePartition()
                + " source_offset=" + record.getSourceOffset()
                + " source_timestamp=" + record.getSourceTimestamp()
                + " source_key=" + record.getSourceKey()
                + " source_value=" + record.getSourceValue()
                + " source_headers=" + headersToText(record.getSourceHeaders())
                + " failure_event_id=" + valueOrNone(record.getFailureEventId())
                + " reason_msg=" + valueOrNone(record.getReasonMsg())
                + " exception=" + valueOrNone(record.getException())
        );
    }

    static String headersToText(Map<String, String> headers) {
        if (headers.isEmpty()) {
            return "<empty>";
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return builder.toString();
    }

    static String valueOrNone(String value) {
        return value == null || value.trim().isEmpty() ? "<none>" : value;
    }

    private static void requireField(GenericRecord record, String fieldName) throws IOException {
        Schema.Field field = record.getSchema().getField(fieldName);
        require(field != null, "missing Avro field " + fieldName);
    }

    private static String requiredString(GenericRecord record, String fieldName) throws IOException {
        String value = optionalString(record, fieldName);
        require(value != null, "missing Avro value " + fieldName);
        return value;
    }

    private static String optionalString(GenericRecord record, String fieldName) throws IOException {
        if (record.getSchema().getField(fieldName) == null) {
            return null;
        }
        Object value = record.get(fieldName);
        if (value == null) {
            return null;
        }
        String text;
        if (value instanceof Utf8 || value instanceof CharSequence) {
            text = value.toString();
        } else if (value instanceof ByteBuffer) {
            ByteBuffer duplicate = ((ByteBuffer) value).asReadOnlyBuffer();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            text = new String(bytes, "UTF-8");
        } else if (value instanceof byte[]) {
            text = new String((byte[]) value, "UTF-8");
        } else {
            text = value.toString();
        }
        String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static int requiredInt(GenericRecord record, String fieldName) throws IOException {
        long value = requiredLong(record, fieldName);
        require(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE,
            "Avro field " + fieldName + " is outside integer range: " + value);
        return (int) value;
    }

    private static long requiredLong(GenericRecord record, String fieldName) throws IOException {
        Object value = record.get(fieldName);
        require(value != null, "missing Avro value " + fieldName);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof CharSequence) {
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException error) {
                throw new IOException("Avro field " + fieldName + " is not numeric: " + value, error);
            }
        }
        throw new IOException("Avro field " + fieldName + " has unsupported numeric type " + value.getClass().getName());
    }

    private static Map<String, String> mapString(GenericRecord record, String fieldName) throws IOException {
        Object value = record.get(fieldName);
        require(value instanceof Map, "Avro field " + fieldName + " is not a map");
        Map<?, ?> raw = (Map<?, ?>) value;
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return result;
    }

    static void require(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }
}
