package com.reconciliation.kafka.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.EligiblePartition;
import com.reconciliation.kafka.model.RootScan;

import lombok.RequiredArgsConstructor;

/**
 * Spring bean facade for filesystem partition scanning.
 */
@Component
@RequiredArgsConstructor
public class PartitionScanService {
    private final SparkSession spark;

    /**
     * Scans all configured roots and prints the stable scan report.
     *
     * @param config checker configuration
     * @return scan result for each configured root
     */
    public List<RootScan> scan(CheckerConfig config) {
        List<RootScan> scans = new ArrayList<RootScan>();
        for (String root : config.inputRoots) {
            scans.add(PartitionScanner.scanRoot(spark, root, config));
        }
        PartitionScanner.printScan(scans);
        return scans;
    }

    /**
     * Returns unique eligible parquet paths sorted lexicographically.
     *
     * @param eligiblePartitions eligible partitions from all roots
     * @return sorted distinct path strings
     */
    public List<String> distinctSortedPaths(List<EligiblePartition> eligiblePartitions) {
        return PartitionScanner.distinctSortedPaths(eligiblePartitions);
    }
}
