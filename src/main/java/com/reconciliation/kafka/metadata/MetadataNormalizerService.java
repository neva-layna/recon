package com.reconciliation.kafka.metadata;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.NormalizeResult;

import lombok.RequiredArgsConstructor;

/**
 * Spring bean facade for parquet read, metadata normalization, and optional
 * normalized-offset persistence.
 */
@Component
@RequiredArgsConstructor
public class MetadataNormalizerService {
    private final SparkSession spark;

    /**
     * Reads eligible parquet paths.
     *
     * @param paths eligible partition paths
     * @return parquet dataset
     */
    public Dataset<Row> readEligibleParquet(List<String> paths) {
        return MetadataNormalizer.readEligibleParquet(spark, paths);
    }

    /**
     * Normalizes metadata JSON into partition/offset rows.
     *
     * @param input eligible parquet rows
     * @param config checker configuration
     * @return normalization result
     */
    public NormalizeResult normalizeOffsets(Dataset<Row> input, CheckerConfig config) {
        return MetadataNormalizer.normalizeOffsets(input, config);
    }

    /**
     * Optionally persists normalized offsets and reads them back.
     *
     * @param normalized normalized offsets
     * @param config checker configuration
     * @return analytics input dataset
     */
    public Dataset<Row> persistIfConfigured(Dataset<Row> normalized, CheckerConfig config) {
        return MetadataNormalizer.persistIfConfigured(spark, normalized, config);
    }
}
