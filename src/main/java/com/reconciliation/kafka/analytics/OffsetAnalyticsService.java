package com.reconciliation.kafka.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;

/**
 * Spring bean facade for offset gap analytics.
 */
@Component
public class OffsetAnalyticsService {
    /**
     * Prints gap statistics for normalized offsets.
     *
     * @param analyticsInput normalized offsets
     * @param config checker configuration
     * @return gap analysis result
     */
    public GapAnalysisResult printGapStats(Dataset<Row> analyticsInput, CheckerConfig config) {
        return OffsetAnalytics.printGapStats(analyticsInput, config);
    }
}
