package com.reconciliation.kafka.sidetopic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.SideTopicConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconExit;
import com.reconciliation.kafka.support.ReconReporter;
import com.reconciliation.kafka.support.RowValues;

import static org.apache.spark.sql.functions.col;

public final class SideTopicReconciler {
    private SideTopicReconciler() {
    }

    public static void reconcileIfConfigured(SparkSession spark, CheckerConfig config, GapAnalysisResult gaps) {
        if (!config.sideTopicConfig.isPresent()) {
            return;
        }

        SideTopicConfig sideTopicConfig = config.sideTopicConfig.get();
        System.out.println(ReconConstants.RECON_PREFIX + " side_topic_reconciliation_begin");
        System.out.println(
            ReconConstants.RECON_PREFIX + " side_topic source_topic=" + sideTopicConfig.sourceTopic
                + " kafka_bootstrap_servers=" + sideTopicConfig.kafkaBootstrapServers
                + " starting_offsets=" + sideTopicConfig.startingOffsets
                + " canary_topic=" + sideTopicConfig.canaryTopic.orElse("<none>")
                + " dead_letter_topic=" + sideTopicConfig.deadLetterTopic.orElse("<none>")
        );

        List<SideTopicRecord> canaryRecords = sideTopicConfig.canaryTopic.isPresent()
            ? readTopic(spark, sideTopicConfig, sideTopicConfig.canaryTopic.get(), SideTopicKind.CANARY)
            : Collections.<SideTopicRecord>emptyList();
        List<SideTopicRecord> deadLetterRecords = sideTopicConfig.deadLetterTopic.isPresent()
            ? readTopic(spark, sideTopicConfig, sideTopicConfig.deadLetterTopic.get(), SideTopicKind.DEAD_LETTER)
            : Collections.<SideTopicRecord>emptyList();

        SideTopicClassification classification = SideTopicClassifier.classify(
            sideTopicConfig.sourceTopic,
            gaps,
            canaryRecords,
            deadLetterRecords
        );
        printClassification(sideTopicConfig, classification);
        System.out.println(ReconConstants.RECON_PREFIX + " side_topic_reconciliation_end");
    }

    private static List<SideTopicRecord> readTopic(
        SparkSession spark,
        SideTopicConfig config,
        String topic,
        SideTopicKind kind
    ) {
        List<SideTopicRecord> decoded = new ArrayList<SideTopicRecord>();
        try {
            Dataset<Row> rows = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", config.startingOffsets)
                .option("endingOffsets", "latest")
                .option("failOnDataLoss", "true")
                .option("kafka.request.timeout.ms", "10000")
                .option("kafka.default.api.timeout.ms", "10000")
                .load()
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
                    + " bootstrap_servers=" + config.kafkaBootstrapServers
                    + ": " + error.getClass().getSimpleName() + ": " + error.getMessage()
            );
        }

        System.out.println(
            ReconConstants.RECON_PREFIX + " side_topic_read topic=" + topic
                + " kind=" + kind.name().toLowerCase()
                + " decoded_record_count=" + decoded.size()
        );
        return decoded;
    }

    static void printClassification(SideTopicConfig config, SideTopicClassification classification) {
        printBucket(classification.sourceTopic, "canary_explained", config.canaryTopic.orElse("<none>"), classification.canaryExplainedOffsets);
        printBucket(classification.sourceTopic, "dead_letter_explained", config.deadLetterTopic.orElse("<none>"), classification.deadLetterExplainedOffsets);
        printBucket(classification.sourceTopic, "unresolved", "<none>", classification.unresolvedOffsets);
        System.out.println(
            ReconConstants.RECON_PREFIX + " side_topic_dead_letter_fields"
                + " failure_event_id_count=" + classification.deadLetterFailureEventIdCount
                + " reason_msg_count=" + classification.deadLetterReasonMsgCount
                + " exception_count=" + classification.deadLetterExceptionCount
        );
        System.out.println(
            ReconConstants.RECON_PREFIX + " side_topic_summary source_topic=" + classification.sourceTopic
                + " canary_explained_count=" + classification.canaryExplainedCount
                + " dead_letter_explained_count=" + classification.deadLetterExplainedCount
                + " unresolved_count=" + classification.unresolvedCount
                + " canary_record_count=" + classification.canaryRecordCount
                + " dead_letter_record_count=" + classification.deadLetterRecordCount
                + " missing_offsets_truncated=" + classification.missingOffsetsTruncated
        );
    }

    private static void printBucket(String sourceTopic, String bucket, String sideTopic, Map<Integer, List<Long>> offsetsByPartition) {
        List<Integer> partitions = new ArrayList<Integer>(offsetsByPartition.keySet());
        Collections.sort(partitions);
        for (Integer partition : partitions) {
            List<Long> offsets = new ArrayList<Long>(offsetsByPartition.get(partition));
            Collections.sort(offsets);
            System.out.println(
                ReconConstants.RECON_PREFIX + " side_topic_bucket=" + bucket
                    + " source_topic=" + sourceTopic
                    + " side_topic=" + sideTopic
                    + " partition=" + partition
                    + " offset_count=" + offsets.size()
                    + " offsets=" + formatOffsets(offsets)
            );
        }
    }

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
