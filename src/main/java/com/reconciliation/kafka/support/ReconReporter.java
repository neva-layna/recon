package com.reconciliation.kafka.support;

import java.util.List;

import com.reconciliation.kafka.config.CheckerConfig;

/**
 * Emits structured reconciliation status lines and final result signals.
 */
public final class ReconReporter {
    /**
     * Prevents construction of the reporting utility.
     */
    private ReconReporter() {
    }

    /**
     * Prints the resolved checker configuration in recon-log format.
     *
     * @param config resolved configuration to print
     */
    public static void printConfig(CheckerConfig config) {
        System.out.println(ReconConstants.RECON_PREFIX + " resolved_configuration_begin");
        System.out.println(ReconConstants.RECON_PREFIX + " recon.inputRoots=" + String.join(",", config.inputRoots));
        System.out.println(ReconConstants.RECON_PREFIX + " recon.metadataColumn=" + config.metadataColumn);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.datePartitionColumn=" + config.datePartitionColumn);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.runDate=" + config.runDate.format(ReconConstants.DATE_FORMATTER));
        System.out.println(ReconConstants.RECON_PREFIX + " recon.runDateSource=" + config.runDateSource);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.normalizedOffsetsPath=" + config.normalizedOffsetsPath.orElse("<none>"));
        System.out.println(ReconConstants.RECON_PREFIX + " recon.normalizedOffsetsOverwrite=" + config.normalizedOffsetsOverwrite);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.failOnInvalidRows=" + config.failOnInvalidRows);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.failOnGaps=" + config.failOnGaps);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.missingOffsetsLimit=" + config.missingOffsetsLimit);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.exitOnCompletion=" + config.exitOnCompletion);
        System.out.println(ReconConstants.RECON_PREFIX + " recon.sideTopic.enabled=" + config.sideTopicConfig.isPresent());
        if (config.sideTopicConfig.isPresent()) {
            System.out.println(ReconConstants.RECON_PREFIX + " recon.sourceTopic=" + config.sideTopicConfig.get().sourceTopic);
            System.out.println(ReconConstants.RECON_PREFIX + " recon.kafkaBootstrapServers=" + config.sideTopicConfig.get().kafkaBootstrapServers);
            System.out.println(ReconConstants.RECON_PREFIX + " recon.canaryTopic=" + config.sideTopicConfig.get().canaryTopic.orElse("<none>"));
            System.out.println(ReconConstants.RECON_PREFIX + " recon.deadLetterTopic=" + config.sideTopicConfig.get().deadLetterTopic.orElse("<none>"));
            System.out.println(ReconConstants.RECON_PREFIX + " recon.sideTopicStartingOffsets=" + config.sideTopicConfig.get().startingOffsets);
        }
        System.out.println(ReconConstants.RECON_PREFIX + " resolved_configuration_end");
    }

    /**
     * Prints the final pass/fail result and either requests JVM exit or throws on
     * failure when exit-on-completion is disabled.
     *
     * @param config resolved configuration controlling exit behavior
     * @param code final process-style result code
     * @param reasons failure reasons; empty means pass
     * @throws ReconExit when exit-on-completion is enabled
     * @throws RuntimeException when the run failed and exit-on-completion is
     *         disabled
     */
    public static void finish(CheckerConfig config, int code, List<String> reasons) {
        finish(config, code, reasons, "no gaps detected");
    }

    /**
     * Prints the final pass/fail result and either requests JVM exit or throws on
     * failure when exit-on-completion is disabled.
     *
     * @param config resolved configuration controlling exit behavior
     * @param code final process-style result code
     * @param reasons failure reasons; empty means pass
     * @param passMessage operator-facing pass reason when reasons is empty
     * @throws ReconExit when exit-on-completion is enabled
     * @throws RuntimeException when the run failed and exit-on-completion is
     *         disabled
     */
    public static void finish(CheckerConfig config, int code, List<String> reasons, String passMessage) {
        if (!reasons.isEmpty()) {
            String message = String.join("; ", reasons);
            System.err.println(ReconConstants.RECON_PREFIX + " ERROR: " + message);
            System.out.println(ReconConstants.RECON_PREFIX + " RESULT: FAIL " + message);
        } else {
            System.out.println(ReconConstants.RECON_PREFIX + " RESULT: PASS " + passMessage);
        }

        if (config.exitOnCompletion) {
            throw new ReconExit(code, reasons.isEmpty() ? passMessage : String.join("; ", reasons), true);
        } else if (code != 0) {
            throw new RuntimeException(String.join("; ", reasons));
        }
    }

    /**
     * Immediately prints a terminal result and raises a JVM-exit signal.
     *
     * @param code process-style result code
     * @param message operator-facing reason
     * @throws ReconExit always, carrying the requested code and message
     */
    public static void stopNow(int code, String message) {
        if (code == 0) {
            System.out.println(ReconConstants.RECON_PREFIX + " RESULT: PASS " + message);
        } else {
            System.err.println(ReconConstants.RECON_PREFIX + " ERROR: " + message);
            System.out.println(ReconConstants.RECON_PREFIX + " RESULT: FAIL " + message);
        }
        throw new ReconExit(code, message, true);
    }
}
