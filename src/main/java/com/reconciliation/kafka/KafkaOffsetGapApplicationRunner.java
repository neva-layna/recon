package com.reconciliation.kafka;

import java.util.Optional;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.reconciliation.kafka.support.ReconExit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Spring Boot startup hook that invokes the checker job and captures intended
 * process exits without letting Boot skip context shutdown.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaOffsetGapApplicationRunner implements ApplicationRunner {
    private final CheckerJob checkerJob;
    private ReconExit requestedExit;

    @Override
    public void run(ApplicationArguments args) {
        try {
            checkerJob.run();
        } catch (ReconExit exit) {
            requestedExit = exit;
            log.debug("Checker requested process exit code {}", exit.code);
        }
    }

    /**
     * Returns the checker-requested process exit after startup runner execution.
     *
     * @return requested exit, or empty when the checker completed without one
     */
    public Optional<ReconExit> requestedExit() {
        return Optional.ofNullable(requestedExit);
    }
}
