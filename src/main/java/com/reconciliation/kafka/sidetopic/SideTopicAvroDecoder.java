package com.reconciliation.kafka.sidetopic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import lombok.experimental.UtilityClass;

/**
 * Decodes Avro object-container payloads read from configured Kafka side topics.
 */
@UtilityClass
public final class SideTopicAvroDecoder {
    /**
     * Decodes a Kafka value that contains one or more Avro generic records.
     *
     * @param payload Kafka value bytes in Avro object-container format
     * @param kind side-topic kind being decoded
     * @param sideTopic Kafka topic the payload came from
     * @return decoded side-topic records
     * @throws IOException when the payload is empty, malformed, lacks required
     *         fields, or contains no records
     */
    public static List<SideTopicRecord> decodeContainer(
        byte[] payload,
        SideTopicKind kind,
        String sideTopic
    ) throws IOException {
        if (payload == null || payload.length == 0) {
            throw new IOException("empty Avro object-container payload");
        }

        List<SideTopicRecord> records = new ArrayList<>();
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(
            new ByteArrayInputStream(payload),
            datumReader
        )) {
            while (stream.hasNext()) {
                GenericRecord record = stream.next();
                records.add(toSideTopicRecord(record, kind, sideTopic));
            }
        }

        if (records.isEmpty()) {
            throw new IOException("Avro object-container payload contained zero records");
        }
        return records;
    }

    /**
     * Validates required source fields and converts one Avro record to the
     * checker model. Dead-letter records must also carry diagnostic fields.
     *
     * @param record Avro generic record from the container stream
     * @param kind side-topic kind being decoded
     * @param sideTopic Kafka topic name the record came from
     * @return normalized side-topic record
     * @throws IOException when required schema fields or values are missing or
     *         typed incorrectly
     */
    private static SideTopicRecord toSideTopicRecord(GenericRecord record, SideTopicKind kind, String sideTopic) throws IOException {
        requireField(record, "sourceKey");
        requireField(record, "sourceValue");
        requireField(record, "sourceHeaders");
        requireField(record, "sourceTopic");
        requireField(record, "sourcePartition");
        requireField(record, "sourceOffset");
        requireField(record, "sourceKafkaTimestamp");
        if (kind == SideTopicKind.DEAD_LETTER) {
            requireField(record, "failureEventId");
            requireField(record, "reasonMsg");
            requireField(record, "exception");
        }

        String sourceTopic = requiredString(record, "sourceTopic");
        int sourcePartition = requiredInt(record, "sourcePartition");
        long sourceOffset = requiredLong(record, "sourceOffset");

        return new SideTopicRecord(
            kind,
            sideTopic,
            sourceTopic,
            sourcePartition,
            sourceOffset,
            optionalString(record, "failureEventId"),
            optionalString(record, "reasonMsg"),
            optionalString(record, "exception")
        );
    }

    /**
     * Ensures an Avro schema contains a required field.
     *
     * @param record Avro record whose schema is checked
     * @param fieldName required field name
     * @throws IOException when the field is absent from the schema
     */
    private static void requireField(GenericRecord record, String fieldName) throws IOException {
        Schema.Field field = record.getSchema().getField(fieldName);
        if (field == null) {
            throw new IOException("missing Avro field " + fieldName);
        }
    }

    /**
     * Reads a required string-like field after trimming blank values to empty.
     *
     * @param record Avro record to read
     * @param fieldName field name to read
     * @return non-blank string value
     * @throws IOException when the value is absent or blank
     */
    private static String requiredString(GenericRecord record, String fieldName) throws IOException {
        Optional<String> value = optionalString(record, fieldName);
        if (!value.isPresent()) {
            throw new IOException("missing Avro value " + fieldName);
        }
        return value.get();
    }

    /**
     * Reads a required integer field, accepting numeric strings when present.
     *
     * @param record Avro record to read
     * @param fieldName field name to read
     * @return value within Java int range
     * @throws IOException when the value is missing, non-numeric, or out of range
     */
    private static int requiredInt(GenericRecord record, String fieldName) throws IOException {
        long value = requiredLong(record, fieldName);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IOException("Avro field " + fieldName + " is outside integer range: " + value);
        }
        return (int) value;
    }

    /**
     * Reads a required long field from numeric Avro values or numeric text.
     *
     * @param record Avro record to read
     * @param fieldName field name to read
     * @return long value
     * @throws IOException when the value is missing, non-numeric, or unsupported
     */
    private static long requiredLong(GenericRecord record, String fieldName) throws IOException {
        Object value = record.get(fieldName);
        if (value == null) {
            throw new IOException("missing Avro value " + fieldName);
        }
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

    /**
     * Reads an optional field as trimmed UTF-8 text.
     *
     * @param record Avro record to read
     * @param fieldName optional field name
     * @return trimmed text, or empty when the field or value is absent or blank
     * @throws IOException when byte decoding fails
     */
    private static Optional<String> optionalString(GenericRecord record, String fieldName) throws IOException {
        if (record.getSchema().getField(fieldName) == null) {
            return Optional.empty();
        }
        Object value = record.get(fieldName);
        if (value == null) {
            return Optional.empty();
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
        return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    }
}
