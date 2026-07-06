package com.reconciliation.kafka.scan;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.EligiblePartition;
import com.reconciliation.kafka.model.RootScan;
import com.reconciliation.kafka.support.ReconConstants;
import com.reconciliation.kafka.support.ReconExit;
import com.reconciliation.kafka.support.ReconReporter;

/**
 * Discovers eligible Hive-style date partitions under configured input roots.
 */
public final class PartitionScanner {
    /**
     * Directory name date pattern accepted after the configured partition prefix.
     */
    private static final Pattern HIVE_DATE_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");

    /**
     * Prevents construction of the scanner utility.
     */
    private PartitionScanner() {
    }

    /**
     * Scans one root directory and classifies immediate children as eligible,
     * skipped run-date, invalid date, or non-matching paths.
     *
     * @param spark active Spark session used for Hadoop filesystem access
     * @param rootText configured input root path
     * @param config checker configuration with partition column and run date
     * @return scan result for the root
     * @throws ReconExit when the root cannot be listed or is not a directory
     */
    public static RootScan scanRoot(SparkSession spark, String rootText, CheckerConfig config) {
        String prefix = config.datePartitionColumn + "=";
        FileStatus[] statuses = listImmediateChildren(spark, rootText);

        List<EligiblePartition> eligible = new ArrayList<EligiblePartition>();
        List<String> skippedRunDate = new ArrayList<String>();
        List<String> ignoredInvalidDate = new ArrayList<String>();
        List<String> ignoredNonMatching = new ArrayList<String>();

        for (FileStatus status : statuses) {
            Path childPath = status.getPath();
            String childPathText = childPath.toString();
            String name = childPath.getName();
            if (status.isDirectory() && name.startsWith(prefix)) {
                String dateText = name.substring(prefix.length());
                if (!HIVE_DATE_PATTERN.matcher(dateText).matches()) {
                    ignoredInvalidDate.add(childPathText);
                    continue;
                }
                try {
                    LocalDate partitionDate = LocalDate.parse(dateText, ReconConstants.DATE_FORMATTER);
                    if (partitionDate.equals(config.runDate)) {
                        skippedRunDate.add(childPathText);
                    } else {
                        eligible.add(new EligiblePartition(rootText, partitionDate, childPathText));
                    }
                } catch (DateTimeParseException error) {
                    ignoredInvalidDate.add(childPathText);
                }
            } else {
                ignoredNonMatching.add(childPathText);
            }
        }

        Collections.sort(eligible);
        return new RootScan(rootText, eligible, skippedRunDate, ignoredInvalidDate, ignoredNonMatching);
    }

    /**
     * Lists direct children of a configured root after validating that the root
     * exists and is a directory.
     *
     * @param spark active Spark session used for Hadoop configuration
     * @param rootText configured root path
     * @return immediate Hadoop file statuses
     * @throws ReconExit when the root is missing, not a directory, or cannot be
     *         listed
     */
    static FileStatus[] listImmediateChildren(SparkSession spark, String rootText) {
        try {
            Path rootPath = new Path(rootText);
            FileSystem fs = rootPath.getFileSystem(spark.sparkContext().hadoopConfiguration());

            if (!fs.exists(rootPath)) {
                ReconReporter.stopNow(2, "Configured input root does not exist: " + rootText);
            }
            if (!fs.getFileStatus(rootPath).isDirectory()) {
                ReconReporter.stopNow(2, "Configured input root is not a directory: " + rootText);
            }
            return fs.listStatus(rootPath);
        } catch (ReconExit exit) {
            throw exit;
        } catch (Exception error) {
            ReconReporter.stopNow(2, "Failed to list configured input root " + rootText + ": " + error.getMessage());
            return new FileStatus[0];
        }
    }

    /**
     * Prints the scan result buckets in stable recon-log form.
     *
     * @param scans root scan results to print
     */
    public static void printScan(List<RootScan> scans) {
        ReconReporter.info(ReconConstants.RECON_PREFIX + " partition_scan_begin");
        for (RootScan scan : scans) {
            ReconReporter.info(ReconConstants.RECON_PREFIX + " root=" + scan.root);
            ReconReporter.info(ReconConstants.RECON_PREFIX + " root=" + scan.root + " eligible_count=" + scan.eligible.size());
            for (EligiblePartition item : scan.eligible) {
                ReconReporter.info(
                    ReconConstants.RECON_PREFIX + " eligible_path root=" + scan.root
                        + " date=" + item.date.format(ReconConstants.DATE_FORMATTER)
                        + " path=" + item.path
                );
            }
            for (String path : scan.skippedRunDate) {
                ReconReporter.info(ReconConstants.RECON_PREFIX + " skipped_run_date_path root=" + scan.root + " path=" + path);
            }
            for (String path : scan.ignoredInvalidDate) {
                ReconReporter.info(ReconConstants.RECON_PREFIX + " ignored_invalid_date_path root=" + scan.root + " path=" + path);
            }
            for (String path : scan.ignoredNonMatching) {
                ReconReporter.info(ReconConstants.RECON_PREFIX + " ignored_non_matching_path root=" + scan.root + " path=" + path);
            }
        }
        ReconReporter.info(ReconConstants.RECON_PREFIX + " partition_scan_end");
    }

    /**
     * Returns unique eligible parquet paths sorted lexicographically.
     *
     * @param eligiblePartitions eligible partitions from all roots
     * @return sorted distinct path strings
     */
    public static List<String> distinctSortedPaths(List<EligiblePartition> eligiblePartitions) {
        List<String> paths = new ArrayList<String>();
        for (EligiblePartition partition : eligiblePartitions) {
            if (!paths.contains(partition.path)) {
                paths.add(partition.path);
            }
        }
        Collections.sort(paths);
        return paths;
    }
}
