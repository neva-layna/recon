package com.reconciliation.kafka.support;

import java.util.List;

import com.reconciliation.kafka.config.CheckerConfig;

public final class ReconReporter {
    private ReconReporter() {
    }

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

    public static void finish(CheckerConfig config, int code, List<String> reasons) {
        if (!reasons.isEmpty()) {
            String message = String.join("; ", reasons);
            System.err.println(ReconConstants.RECON_PREFIX + " ERROR: " + message);
            System.out.println(ReconConstants.RECON_PREFIX + " RESULT: FAIL " + message);
        } else {
            System.out.println(ReconConstants.RECON_PREFIX + " RESULT: PASS no gaps detected");
        }

        if (config.exitOnCompletion) {
            throw new ReconExit(code, reasons.isEmpty() ? "no gaps detected" : String.join("; ", reasons), true);
        } else if (code != 0) {
            throw new RuntimeException(String.join("; ", reasons));
        }
    }

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
