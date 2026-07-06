package com.reconciliation.kafka.sidetopic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.SideTopicConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconExit;
import com.reconciliation.kafka.support.ReconReporter;
import com.reconciliation.kafka.support.RowValues;

import lombok.experimental.UtilityClass;

import static org.apache.spark.sql.functions.col;

/**
 * Reads configured Kafka side topics and reports how they explain missing
 * offsets.
 */
@UtilityClass
public final class SideTopicReconciler {
    /**
     * Runs side-topic reconciliation when side-topic config is present and
     * returns the classification needed by final exit-code decisions.
     *
     * @param spark active Spark session used to read Kafka
     * @param config checker configuration with optional side-topic settings
     * @param gaps gap analytics result to explain
     * @return classification when side-topic config is present
     * @throws ReconExit when Kafka reads or Avro decoding fail
     */
    public static Optional<SideTopicClassification> reconcileIfConfigured(
        SparkSession spark,
        CheckerConfig config,
        GapAnalysisResult gaps
    ) {
        if (!config.getSideTopicConfig().isPresent()) {
            ReconReporter.info(ReconConstants.RECON_PREFIX + " side_topic_reconciliation state=disabled");
            return Optional.empty();
        }

        SideTopicConfig sideTopicConfig = config.getSideTopicConfig().get();
        ReconReporter.info(ReconConstants.RECON_PREFIX + " side_topic_reconciliation_begin");
        ReconReporter.info(
            ReconConstants.RECON_PREFIX + " side_topic source_topic=" + sideTopicConfig.getSourceTopic()
                + " kafka_alias=" + sideTopicConfig.getKafkaAlias().orElse("<legacy-spark-conf-bootstrap>")
                + " kafka_bootstrap_servers=" + sideTopicConfig.getKafkaBootstrapServers()
                + " starting_offsets=" + sideTopicConfig.getStartingOffsets()
                + " canary_topic=" + sideTopicConfig.getCanaryTopic().orElse("<none>")
                + " dead_letter_topic=" + sideTopicConfig.getDeadLetterTopic().orElse("<none>")
        );

        List<SideTopicRecord> canaryRecords = sideTopicConfig.getCanaryTopic().isPresent()
            ? readTopic(spark, sideTopicConfig, sideTopicConfig.getCanaryTopic().get(), SideTopicKind.CANARY)
            : Collections.emptyList();
        List<SideTopicRecord> deadLetterRecords = sideTopicConfig.getDeadLetterTopic().isPresent()
            ? readTopic(spark, sideTopicConfig, sideTopicConfig.getDeadLetterTopic().get(), SideTopicKind.DEAD_LETTER)
            : Collections.emptyList();

        SideTopicClassification classification = SideTopicClassifier.classify(
            sideTopicConfig.getSourceTopic(),
            gaps,
            canaryRecords,
            deadLetterRecords
        );
        printClassification(sideTopicConfig, classification);
        ReconReporter.info(ReconConstants.RECON_PREFIX + " side_topic_reconciliation_end");
        return Optional.of(classification);
    }

    /**
     * Reads one Kafka side topic to the latest offset and decodes Avro payloads.
     *
     * @param spark active Spark session used to read Kafka
     * @param config side-topic Kafka connection settings
     * @param topic side-topic name to read
     * @param kind side-topic kind used by the decoder
     * @return decoded side-topic records
     * @throws ReconExit when Kafka read or payload decoding fails
     */
    private static List<SideTopicRecord> readTopic(
        SparkSession spark,
        SideTopicConfig config,
        String topic,
        SideTopicKind kind
    ) {
        List<SideTopicRecord> decoded = new ArrayList<>();
        try {
            DataFrameReader reader = spark.read().format("kafka");
            for (Map.Entry<String, String> entry : SideTopicReaderOptions.build(config, topic).entrySet()) {
                reader = reader.option(entry.getKey(), entry.getValue());
            }
            Dataset<Row> rows = reader.load()
                .select(col("topic"), col("partition"), col("offset"), col("value"));

            for (Row row : rows.collectAsList()) {
                byte[] payload = row.getAs("value");
                String kafkaTopic = row.getAs("topic");
                long kafkaOffset = RowValues.getLong(row, "offset");
                try {
                    decoded.addAll(SideTopicAvroDecoder.decodeContainer(payload, kind, topic));
                } catch (IOException error) {
                    ReconReporter.stopNow(
                        2,
                        "Failed to decode Avro side-topic payload topic=" + kafkaTopic
                            + " offset=" + kafkaOffset + ": " + error.getMessage()
                    );
                } catch (LinkageError error) {
                    ReconReporter.stopNow(
                        2,
                        "Failed to decode Avro side-topic payload topic=" + kafkaTopic
                            + " offset=" + kafkaOffset
                            + ": Avro runtime unavailable: " + error.getClass().getSimpleName()
                            + ": " + error.getMessage()
                    );
                }
            }
        } catch (ReconExit exit) {
            throw exit;
        } catch (Exception error) {
            ReconReporter.stopNow(
                2,
                "Failed to read side-topic Kafka topic=" + topic
                    + " bootstrap_servers=" + config.getKafkaBootstrapServers()
                    + ": " + error.getClass().getSimpleName() + ": " + error.getMessage()
            );
        }

        ReconReporter.info(
            ReconConstants.RECON_PREFIX + " side_topic_read topic=" + topic
                + " kind=" + kind.name().toLowerCase()
                + " decoded_record_count=" + decoded.size()
        );
        return decoded;
    }

    /**
     * Prints bucketed side-topic classification and summary counts.
     *
     * @param config side-topic configuration used for topic names
     * @param classification bucketed classification to print
     */
    static void printClassification(SideTopicConfig config, SideTopicClassification classification) {
        printBucket(classification.getSourceTopic(), "canary_explained", config.getCanaryTopic().orElse("<none>"), classification.getCanaryExplainedOffsets());
        printBucket(classification.getSourceTopic(), "dead_letter_explained", config.getDeadLetterTopic().orElse("<none>"), classification.getDeadLetterExplainedOffsets());
        printBucket(classification.getSourceTopic(), "unresolved", "<none>", classification.getUnresolvedOffsets());
        ReconReporter.info(
            ReconConstants.RECON_PREFIX + " side_topic_dead_letter_fields"
                + " failure_event_id_count=" + classification.getDeadLetterFailureEventIdCount()
                + " reason_msg_count=" + classification.getDeadLetterReasonMsgCount()
                + " exception_count=" + classification.getDeadLetterExceptionCount()
        );
        ReconReporter.info(
            ReconConstants.RECON_PREFIX + " side_topic_summary source_topic=" + classification.getSourceTopic()
                + " raw_gap_partition_count=" + classification.getRawGapPartitionCount()
                + " bounded_missing_offset_count=" + classification.getBoundedMissingOffsetCount()
                + " canary_explained_count=" + classification.getCanaryExplainedCount()
                + " dead_letter_explained_count=" + classification.getDeadLetterExplainedCount()
                + " unresolved_count=" + classification.getUnresolvedCount()
                + " canary_record_count=" + classification.getCanaryRecordCount()
                + " dead_letter_record_count=" + classification.getDeadLetterRecordCount()
                + " missing_offsets_truncated=" + classification.isMissingOffsetsTruncated()
        );
    }

    /**
     * Prints one non-empty classification bucket by partition in stable order.
     *
     * @param sourceTopic source topic whose offsets are being printed
     * @param bucket bucket name used in recon output
     * @param sideTopic side-topic name, or &lt;none&gt; for unresolved offsets
     * @param offsetsByPartition offsets grouped by source partition
     */
    private static void printBucket(String sourceTopic, String bucket, String sideTopic, Map<Integer, List<Long>> offsetsByPartition) {
        List<Integer> partitions = new ArrayList<>(offsetsByPartition.keySet());
        Collections.sort(partitions);
        for (Integer partition : partitions) {
            List<Long> offsets = new ArrayList<>(offsetsByPartition.get(partition));
            Collections.sort(offsets);
            ReconReporter.info(
                ReconConstants.RECON_PREFIX + " side_topic_bucket=" + bucket
                    + " source_topic=" + sourceTopic
                    + " side_topic=" + sideTopic
                    + " partition=" + partition
                    + " offset_count=" + offsets.size()
                    + " offsets=" + formatOffsets(offsets)
            );
        }
    }

    /**
     * Formats offsets as the compact bracketed list used in side-topic logs.
     *
     * @param offsets offsets in display order
     * @return bracketed comma-separated list without spaces
     */
    public static String formatOffsets(List<Long> offsets) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < offsets.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(offsets.get(i));
        }
        builder.append(']');
        return builder.toString();
    }
}
