package com.reconciliation.kafka;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Test;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

import com.reconciliation.kafka.config.ApplicationYamlLookup;
import com.reconciliation.kafka.config.CheckerConfig;
import com.reconciliation.kafka.config.ConfLookup;
import com.reconciliation.kafka.config.ConfigLoader;
import com.reconciliation.kafka.config.LayeredConfLookup;
import com.reconciliation.kafka.config.ReconProperties;
import com.reconciliation.kafka.support.ReconExit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaOffsetGapConfigurationTest {
    @Test
    public void bindsYamlAndLoadsCompleteConfig() {
        CheckerConfig config = loadFromYaml("full-application.yml");

        assertEquals(Arrays.asList("/yaml/root-a", "/yaml/root-b"), config.inputRoots);
        assertEquals("yaml_meta", config.metadataColumn);
        assertEquals("business_date", config.datePartitionColumn);
        assertEquals(LocalDate.parse("2026-07-05"), config.runDate);
        assertEquals("application_yml:recon.runDate", config.runDateSource);
        assertEquals("file:///tmp/yaml-normalized", config.normalizedOffsetsPath.get());
        assertFalse(config.normalizedOffsetsOverwrite);
        assertFalse(config.failOnInvalidRows);
        assertFalse(config.failOnGaps);
        assertEquals(7L, config.missingOffsetsLimit);
        assertFalse(config.exitOnCompletion);
        assertTrue(config.sideTopicConfig.isPresent());
        assertEquals("yaml-orders", config.sideTopicConfig.get().sourceTopic);
        assertEquals("yaml-broker:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("yaml-orders-canary", config.sideTopicConfig.get().canaryTopic.get());
        assertEquals("yaml-orders-dlq", config.sideTopicConfig.get().deadLetterTopic.get());
        assertEquals("earliest", config.sideTopicConfig.get().startingOffsets);
    }

    @Test
    public void yamlUsesExistingDefaultsWhenOptionalValuesAreAbsent() {
        CheckerConfig config = loadFromYaml("defaults-application.yml");

        assertEquals(Arrays.asList("/yaml/default-root"), config.inputRoots);
        assertEquals("cactus__metadata", config.metadataColumn);
        assertEquals("timestampcolumn", config.datePartitionColumn);
        assertEquals(LocalDate.parse("2026-07-06"), config.runDate);
        assertEquals("driver_current_date", config.runDateSource);
        assertFalse(config.normalizedOffsetsPath.isPresent());
        assertTrue(config.normalizedOffsetsOverwrite);
        assertTrue(config.failOnInvalidRows);
        assertTrue(config.failOnGaps);
        assertEquals(1000L, config.missingOffsetsLimit);
        assertTrue(config.exitOnCompletion);
        assertFalse(config.sideTopicConfig.isPresent());
    }

    @Test
    public void rejectsYamlWithoutInputRoots() {
        try {
            loadFromYaml("missing-roots-application.yml");
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("Missing required Spark conf recon.inputRoots"));
        }
    }

    @Test
    public void rejectsInvalidYamlTypedValuesAsConfigurationFailures() {
        assertYamlBindingFailure("invalid-boolean-application.yml");
        assertYamlBindingFailure("invalid-number-application.yml");
        assertYamlBindingFailure("invalid-run-date-application.yml");
    }

    @Test
    public void rejectsIncompleteSideTopicYaml() {
        try {
            loadFromYaml("incomplete-side-topic-application.yml");
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("Incomplete side-topic config"));
            assertTrue(exit.getMessage().contains("recon.sourceTopic"));
            assertTrue(exit.getMessage().contains("recon.kafkaBootstrapServers"));
        }
    }

    @Test
    public void rejectsUnsupportedSideTopicYamlStartingOffsets() {
        try {
            loadFromYaml("invalid-side-topic-offsets-application.yml");
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            assertTrue(exit.getMessage().contains("expected earliest or beginning"));
        }
    }

    @Test
    public void sparkConfOverridesYamlForRepresentativeFields() {
        ReconProperties yaml = loadProperties("full-application.yml");
        Map<String, String> spark = new HashMap<String, String>();
        spark.put("spark.recon.metadataColumn", "spark_meta");
        spark.put("spark.recon.runDate", "2026-07-04");
        spark.put("spark.recon.failOnGaps", "true");
        spark.put("spark.recon.missingOffsetsLimit", "99");
        spark.put("spark.recon.sideTopic.sourceTopic", "spark-orders");
        spark.put("spark.recon.kafkaBootstrapServers", "spark-broker:9092");
        spark.put("spark.recon.deadLetterTopic", "spark-orders-dlq");
        spark.put("spark.recon.sideTopicStartingOffsets", "earliest");

        CheckerConfig config = ConfigLoader.loadConfig(
            new LayeredConfLookup(new MapLookup(spark), new ApplicationYamlLookup(yaml)),
            fixedDate("2026-07-06")
        );

        assertEquals(Arrays.asList("/yaml/root-a", "/yaml/root-b"), config.inputRoots);
        assertEquals("spark_meta", config.metadataColumn);
        assertEquals(LocalDate.parse("2026-07-04"), config.runDate);
        assertEquals("spark_conf:recon.runDate", config.runDateSource);
        assertTrue(config.failOnGaps);
        assertEquals(99L, config.missingOffsetsLimit);
        assertTrue(config.sideTopicConfig.isPresent());
        assertEquals("spark-orders", config.sideTopicConfig.get().sourceTopic);
        assertEquals("spark-broker:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("yaml-orders-canary", config.sideTopicConfig.get().canaryTopic.get());
        assertEquals("spark-orders-dlq", config.sideTopicConfig.get().deadLetterTopic.get());
        assertEquals("earliest", config.sideTopicConfig.get().startingOffsets);
    }

    private static CheckerConfig loadFromYaml(String resourceName) {
        return ConfigLoader.loadConfig(
            new ApplicationYamlLookup(loadProperties(resourceName)),
            fixedDate("2026-07-06")
        );
    }

    private static ReconProperties loadProperties(String resourceName) {
        SpringApplication application = new SpringApplication(PropertiesOnlyConfiguration.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        application.setLogStartupInfo(false);

        ConfigurableApplicationContext context = application.run(
            "--spring.config.location=classpath:/recon-yaml/" + resourceName
        );
        try {
            return context.getBean(ReconProperties.class);
        } finally {
            context.close();
        }
    }

    private static void assertYamlBindingFailure(String resourceName) {
        try {
            loadProperties(resourceName);
            fail("expected YAML binding failure for " + resourceName);
        } catch (RuntimeException error) {
            assertTrue(KafkaOffsetGapChecker.isConfigurationBindingFailure(error));
        }
    }

    private static Supplier<LocalDate> fixedDate(final String date) {
        return new Supplier<LocalDate>() {
            @Override
            public LocalDate get() {
                return LocalDate.parse(date);
            }
        };
    }

    private static final class MapLookup implements ConfLookup {
        private final Map<String, String> values;

        private MapLookup(Map<String, String> values) {
            this.values = values;
        }

        @Override
        public Optional<String> get(String key) {
            String value = values.get(key);
            return value == null || value.trim().isEmpty()
                ? Optional.<String>empty()
                : Optional.of(value.trim());
        }
    }

    @SpringBootConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ReconProperties.class)
    static class PropertiesOnlyConfiguration {
    }
}
