package com.reconciliation.kafka.support;

import java.util.List;

import org.springframework.stereotype.Component;

import com.reconciliation.kafka.config.CheckerConfig;

/**
 * Spring bean facade for stable operator-facing recon output.
 */
@Component
public class ReconReporterService {
    /**
     * Emits one stable operator-facing info line.
     *
     * @param message full message text
     */
    public void info(String message) {
        ReconReporter.info(message);
    }

    /**
     * Emits one stable operator-facing error line.
     *
     * @param message full message text
     */
    public void error(String message) {
        ReconReporter.error(message);
    }

    /**
     * Prints the resolved checker configuration.
     *
     * @param config resolved checker configuration
     */
    public void printConfig(CheckerConfig config) {
        ReconReporter.printConfig(config);
    }

    /**
     * Prints the final result and raises the configured terminal signal.
     *
     * @param config resolved checker configuration
     * @param code final exit code
     * @param reasons failure reasons
     * @param passMessage pass message
     */
    public void finish(CheckerConfig config, int code, List<String> reasons, String passMessage) {
        ReconReporter.finish(config, code, reasons, passMessage);
    }

    /**
     * Immediately terminates the run with an operator-visible result.
     *
     * @param code exit code
     * @param message result message
     */
    public void stopNow(int code, String message) {
        ReconReporter.stopNow(code, message);
    }
}
