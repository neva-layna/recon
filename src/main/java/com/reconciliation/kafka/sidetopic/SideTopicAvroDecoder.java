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

public final class SideTopicAvroDecoder {
    private SideTopicAvroDecoder() {
    }

    public static List<SideTopicRecord> decodeContainer(
        byte[] payload,
        SideTopicKind kind,
        String sideTopic
    ) throws IOException {
        if (payload == null || payload.length == 0) {
            throw new IOException("empty Avro object-container payload");
        }

        List<SideTopicRecord> records = new ArrayList<SideTopicRecord>();
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        try (DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(
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

    private static void requireField(GenericRecord record, String fieldName) throws IOException {
        Schema.Field field = record.getSchema().getField(fieldName);
        if (field == null) {
            throw new IOException("missing Avro field " + fieldName);
        }
    }

    private static String requiredString(GenericRecord record, String fieldName) throws IOException {
        Optional<String> value = optionalString(record, fieldName);
        if (!value.isPresent()) {
            throw new IOException("missing Avro value " + fieldName);
        }
        return value.get();
    }

    private static int requiredInt(GenericRecord record, String fieldName) throws IOException {
        long value = requiredLong(record, fieldName);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IOException("Avro field " + fieldName + " is outside integer range: " + value);
        }
        return (int) value;
    }

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
        return trimmed.isEmpty() ? Optional.<String>empty() : Optional.of(trimmed);
    }
}
