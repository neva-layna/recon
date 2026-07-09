package com.reconciliation.synthdata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes checker-compatible parquet data without external services.
 */
public final class SyntheticRecordGenerator {
    private static final String PARQUET_FILE_NAME = "part-00000.parquet";

    public GeneratedDataset generate(GenerationOptions options) throws IOException {
        Path outputRoot = options.getOutputDirectory().toAbsolutePath().normalize();
        validateOutputRoot(outputRoot);

        Path partitionDirectory = outputRoot
            .resolve(Paths.get(options.getRelativeRoot()))
            .resolve(options.getPartitionDirectoryName())
            .normalize();
        if (!partitionDirectory.startsWith(outputRoot)) {
            throw new IllegalArgumentException("relative root must stay below output directory: " + options.getRelativeRoot());
        }

        Files.createDirectories(partitionDirectory);
        Path parquetFile = partitionDirectory.resolve(PARQUET_FILE_NAME);
        if (Files.isDirectory(parquetFile)) {
            throw new IllegalArgumentException("parquet file path is a directory: " + parquetFile);
        }

        SyntheticRecord record = new SyntheticRecord(
            options.getMetadataJson(),
            options.getTopic(),
            options.getPayload(),
            options.getExtraValues()
        );
        Schema schema = schemaFor(options);
        writeRecord(parquetFile, schema, options, record);

        return new GeneratedDataset(outputRoot, partitionDirectory, parquetFile, options.getMetadataJson());
    }

    private static void validateOutputRoot(Path outputRoot) {
        if (Files.exists(outputRoot) && !Files.isDirectory(outputRoot)) {
            throw new IllegalArgumentException("output directory path exists but is not a directory: " + outputRoot);
        }
    }

    private static Schema schemaFor(GenerationOptions options) {
        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(requiredStringField(options.getMetadataColumn()));
        fields.add(requiredStringField("topic"));
        if (options.getPayload() != null) {
            fields.add(requiredStringField("payload"));
        }
        for (String column : options.getExtraValues().keySet()) {
            fields.add(requiredStringField(column));
        }

        Schema schema = Schema.createRecord("SyntheticCheckerRecord", null, "com.reconciliation.synthdata", false);
        schema.setFields(fields);
        return schema;
    }

    private static Schema.Field requiredStringField(String name) {
        return new Schema.Field(name, Schema.create(Schema.Type.STRING), null, (Object) null);
    }

    private static void writeRecord(
        Path parquetFile,
        Schema schema,
        GenerationOptions options,
        SyntheticRecord record
    ) throws IOException {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put(options.getMetadataColumn(), record.getMetadataJson());
        avroRecord.put("topic", record.getTopic());
        if (record.getPayload() != null) {
            avroRecord.put("payload", record.getPayload());
        }
        for (Map.Entry<String, String> extra : record.getExtraValues().entrySet()) {
            avroRecord.put(extra.getKey(), extra.getValue());
        }

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(parquetFile))
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()) {
            writer.write(avroRecord);
        }
    }
}
