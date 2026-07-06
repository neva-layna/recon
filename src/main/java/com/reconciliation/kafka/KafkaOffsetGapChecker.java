package com.reconciliation.kafka;

import java.util.Optional;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindException;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

import com.reconciliation.kafka.decision.ExitDecisionService;
import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.sidetopic.SideTopicClassification;
import com.reconciliation.kafka.support.ReconExit;
import com.reconciliation.kafka.support.ReconReporter;

import lombok.extern.slf4j.Slf4j;

/**
 * Spring Boot application entrypoint for the Spark offset gap checker.
 */
@Slf4j
public class KafkaOffsetGapChecker {
    /**
     * Starts the Spring Boot application context that owns Spark and checker
     * collaborators, then exits with the code requested by the checker.
     *
     * @param args command-line arguments accepted by Spark but not read here
     */
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(KafkaOffsetGapCheckerConfiguration.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        application.setLogStartupInfo(false);
        application.setRegisterShutdownHook(false);

        ConfigurableApplicationContext context = null;
        ReconExit requestedExit = null;
        try {
            context = application.run(args);
            requestedExit = context
                .getBean(KafkaOffsetGapApplicationRunner.class)
                .requestedExit()
                .orElse(null);
        } catch (ReconExit exit) {
            requestedExit = exit;
            log.debug("Spring startup surfaced checker exit code {}", exit.getCode());
        } catch (RuntimeException error) {
            if (!isConfigurationBindingFailure(error)) {
                throw error;
            }
            requestedExit = reportConfigurationBindingFailure(error);
        } finally {
            if (context != null) {
                context.close();
            }
        }

        if (requestedExit != null && requestedExit.isExitJvm()) {
            System.exit(requestedExit.getCode());
        }
    }

    /**
     * Compatibility helper for focused exit-decision tests.
     *
     * @param config resolved checker configuration
     * @param gapResult raw parquet gap analytics
     * @param sideTopicClassification optional side-topic classification result
     * @return failure reason when gap-related exit code {@code 1} is required
     */
    static Optional<String> gapFailureReason(
        CheckerConfig config,
        GapAnalysisResult gapResult,
        Optional<SideTopicClassification> sideTopicClassification
    ) {
        return ExitDecisionService.gapFailureReason(config, gapResult, sideTopicClassification);
    }

    /**
     * Identifies Spring Boot configuration-binding failures that should map to
     * the checker's stable operator-input exit class.
     *
     * @param error startup failure
     * @return true when the failure came from {@code application.yml} binding
     */
    static boolean isConfigurationBindingFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ConfigurationPropertiesBindException || current instanceof BindException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static ReconExit reportConfigurationBindingFailure(Throwable error) {
        try {
            ReconReporter.stopNow(2, "Invalid application.yml recon configuration: " + rootMessage(error));
        } catch (ReconExit exit) {
            return exit;
        }
        return new ReconExit(2, "Invalid application.yml recon configuration", true);
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        Throwable lastWithMessage = error;
        while (current != null) {
            if (current.getMessage() != null && !current.getMessage().trim().isEmpty()) {
                lastWithMessage = current;
            }
            current = current.getCause();
        }
        return lastWithMessage.getMessage();
    }
}
