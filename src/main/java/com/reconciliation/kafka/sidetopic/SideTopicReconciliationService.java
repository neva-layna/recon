package com.reconciliation.kafka.sidetopic;

import java.util.Optional;

import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;

import lombok.RequiredArgsConstructor;

/**
 * Spring bean facade for optional Kafka side-topic reconciliation.
 */
@Component
@RequiredArgsConstructor
public class SideTopicReconciliationService {
    private final SparkSession spark;

    /**
     * Runs side-topic reconciliation when enabled.
     *
     * @param config checker configuration
     * @param gaps gap analytics result
     * @return side-topic classification when enabled
     */
    public Optional<SideTopicClassification> reconcileIfConfigured(CheckerConfig config, GapAnalysisResult gaps) {
        return SideTopicReconciler.reconcileIfConfigured(spark, config, gaps);
    }
}
