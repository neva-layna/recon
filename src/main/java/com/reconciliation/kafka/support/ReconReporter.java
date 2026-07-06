package com.reconciliation.kafka.support;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.SideTopicConfig;

import lombok.experimental.UtilityClass;

/**
 * Emits structured reconciliation status lines and final result signals.
 */
@UtilityClass
public final class ReconReporter {
    private static final Logger LOG = LoggerFactory.getLogger(ReconReporter.class);
    private static final boolean LOGBACK_ACTIVE = LoggerFactory.getILoggerFactory()
        .getClass()
        .getName()
        .startsWith("ch.qos.logback.");

    /**
     * Emits one stable operator-facing info line through SLF4J.
     *
     * @param message full message text
     */
    public static void info(String message) {
        LOG.info(parseable(message));
    }

    /**
     * Emits one stable operator-facing error line through SLF4J.
     *
     * @param message full message text
     */
    public static void error(String message) {
        LOG.error(parseable(message));
    }

    private static String parseable(String message) {
        return LOGBACK_ACTIVE ? message : System.lineSeparator() + message;
    }

    /**
     * Prints the resolved checker configuration in recon-log format.
     *
     * @param config resolved configuration to print
     */
    public static void printConfig(CheckerConfig config) {
        info(ReconConstants.RECON_PREFIX + " resolved_configuration_begin");
        info(ReconConstants.RECON_PREFIX + " recon.inputRoots=" + String.join(",", config.getInputRoots()));
        info(ReconConstants.RECON_PREFIX + " recon.metadataColumn=" + config.getMetadataColumn());
        info(ReconConstants.RECON_PREFIX + " recon.datePartitionColumn=" + config.getDatePartitionColumn());
        info(ReconConstants.RECON_PREFIX + " recon.runDate=" + config.getRunDate().format(ReconConstants.DATE_FORMATTER));
        info(ReconConstants.RECON_PREFIX + " recon.runDateSource=" + config.getRunDateSource());
        info(ReconConstants.RECON_PREFIX + " recon.normalizedOffsetsPath=" + config.getNormalizedOffsetsPath().orElse("<none>"));
        info(ReconConstants.RECON_PREFIX + " recon.normalizedOffsetsOverwrite=" + config.isNormalizedOffsetsOverwrite());
        info(ReconConstants.RECON_PREFIX + " recon.failOnInvalidRows=" + config.isFailOnInvalidRows());
        info(ReconConstants.RECON_PREFIX + " recon.failOnGaps=" + config.isFailOnGaps());
        info(ReconConstants.RECON_PREFIX + " recon.missingOffsetsLimit=" + config.getMissingOffsetsLimit());
        info(ReconConstants.RECON_PREFIX + " recon.exitOnCompletion=" + config.isExitOnCompletion());
        info(ReconConstants.RECON_PREFIX + " recon.sideTopic.enabled=" + config.getSideTopicConfig().isPresent());
        if (config.getSideTopicConfig().isPresent()) {
            SideTopicConfig sideTopicConfig = config.getSideTopicConfig().get();
            info(ReconConstants.RECON_PREFIX + " recon.sourceTopic=" + sideTopicConfig.getSourceTopic());
            info(ReconConstants.RECON_PREFIX + " recon.kafkaAlias=" + sideTopicConfig.getKafkaAlias().orElse("<legacy-spark-conf-bootstrap>"));
            info(ReconConstants.RECON_PREFIX + " recon.kafkaBootstrapServers=" + sideTopicConfig.getKafkaBootstrapServers());
            info(ReconConstants.RECON_PREFIX + " recon.canaryTopic=" + sideTopicConfig.getCanaryTopic().orElse("<none>"));
            info(ReconConstants.RECON_PREFIX + " recon.deadLetterTopic=" + sideTopicConfig.getDeadLetterTopic().orElse("<none>"));
            info(ReconConstants.RECON_PREFIX + " recon.sideTopicStartingOffsets=" + sideTopicConfig.getStartingOffsets());
        }
        info(ReconConstants.RECON_PREFIX + " resolved_configuration_end");
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
            error(ReconConstants.RECON_PREFIX + " ERROR: " + message);
            info(ReconConstants.RECON_PREFIX + " RESULT: FAIL " + message);
        } else {
            info(ReconConstants.RECON_PREFIX + " RESULT: PASS " + passMessage);
        }

        if (config.isExitOnCompletion()) {
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
            info(ReconConstants.RECON_PREFIX + " RESULT: PASS " + message);
        } else {
            error(ReconConstants.RECON_PREFIX + " ERROR: " + message);
            info(ReconConstants.RECON_PREFIX + " RESULT: FAIL " + message);
        }
        throw new ReconExit(code, message, true);
    }
}
