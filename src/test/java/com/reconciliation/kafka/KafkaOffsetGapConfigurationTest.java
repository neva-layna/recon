package com.reconciliation.kafka;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.reconciliation.kafka.config.KafkaConfigsProperties;
import com.reconciliation.kafka.config.LayeredConfLookup;
import com.reconciliation.kafka.config.ReconProperties;
import com.reconciliation.kafka.sidetopic.SideTopicReaderOptions;
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
        assertEquals("main-kafka", config.sideTopicConfig.get().kafkaAlias.get());
        assertEquals("main-a:9092,main-b:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("SASL_SSL", config.sideTopicConfig.get().kafkaConsumerConfig.get("security.protocol"));
        assertEquals("250", config.sideTopicConfig.get().kafkaConsumerConfig.get("max.poll.records"));
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
    public void bindsImportedBrokerYamlByAliasAndBracketedKeys() {
        LoadedProperties loaded = loadProperties("full-application.yml");

        assertTrue(loaded.kafkaConfigs.hasBroker("main-kafka"));
        assertTrue(loaded.kafkaConfigs.hasBroker("reserved-kafka"));
        Map<String, String> main = loaded.kafkaConfigs.normalizedConf("main-kafka");
        Map<String, String> reserved = loaded.kafkaConfigs.normalizedConf("reserved-kafka");

        assertEquals("main-a:9092,main-b:9092", main.get("bootstrap.servers"));
        assertEquals("SASL_SSL", main.get("security.protocol"));
        assertEquals("250", main.get("max.poll.records"));
        assertEquals("reserved-a:9092", reserved.get("bootstrap.servers"));
        assertEquals("PLAINTEXT", reserved.get("security.protocol"));
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
            assertTrue(exit.getMessage().contains("recon.kafkaAlias"));
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
        LoadedProperties yaml = loadProperties("full-application.yml");
        Map<String, String> spark = new HashMap<String, String>();
        spark.put("spark.recon.metadataColumn", "spark_meta");
        spark.put("spark.recon.runDate", "2026-07-04");
        spark.put("spark.recon.failOnGaps", "true");
        spark.put("spark.recon.missingOffsetsLimit", "99");
        spark.put("spark.recon.sideTopic.sourceTopic", "spark-orders");
        spark.put("spark.recon.kafka.alias", "reserved-kafka");
        spark.put("spark.recon.deadLetterTopic", "spark-orders-dlq");
        spark.put("spark.recon.sideTopicStartingOffsets", "earliest");

        CheckerConfig config = ConfigLoader.loadConfig(
            new LayeredConfLookup(new MapLookup(spark), new ApplicationYamlLookup(yaml.recon)),
            yaml.kafkaConfigs,
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
        assertEquals("reserved-kafka", config.sideTopicConfig.get().kafkaAlias.get());
        assertEquals("reserved-a:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("yaml-orders-canary", config.sideTopicConfig.get().canaryTopic.get());
        assertEquals("spark-orders-dlq", config.sideTopicConfig.get().deadLetterTopic.get());
        assertEquals("earliest", config.sideTopicConfig.get().startingOffsets);
    }

    @Test
    public void plainReconKafkaAliasOverrideBeatsYaml() {
        LoadedProperties yaml = loadProperties("full-application.yml");
        Map<String, String> spark = new HashMap<String, String>();
        spark.put("recon.kafkaAlias", "reserved-kafka");
        spark.put("recon.canaryTopic", "spark-orders-canary");

        CheckerConfig config = ConfigLoader.loadConfig(
            new LayeredConfLookup(new MapLookup(spark), new ApplicationYamlLookup(yaml.recon)),
            yaml.kafkaConfigs,
            fixedDate("2026-07-06")
        );

        assertTrue(config.sideTopicConfig.isPresent());
        assertEquals("reserved-kafka", config.sideTopicConfig.get().kafkaAlias.get());
        assertEquals("reserved-a:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("spark-orders-canary", config.sideTopicConfig.get().canaryTopic.get());
        assertEquals("yaml-orders-dlq", config.sideTopicConfig.get().deadLetterTopic.get());
    }

    @Test
    public void rejectsInvalidBrokerAliases() {
        assertConfigFailure(
            sideTopicConf(null),
            brokerConfigs(),
            "Incomplete side-topic config",
            "recon.kafkaAlias"
        );
        assertConfigFailure(
            sideTopicConf("   "),
            brokerConfigs(),
            "recon.kafkaAlias is blank or whitespace"
        );
        assertConfigFailure(
            sideTopicConf("missing-kafka"),
            brokerConfigs(),
            "Unknown recon.kafkaAlias=missing-kafka"
        );
        assertConfigFailure(
            sideTopicConf("empty-kafka"),
            brokerConfigs("empty-kafka", new String[0]),
            "Broker alias empty-kafka has empty kafka-configs.broker.empty-kafka.conf"
        );
        assertConfigFailure(
            sideTopicConf("bootstrapless-kafka"),
            brokerConfigs("bootstrapless-kafka", new String[] {
                "security.protocol", "PLAINTEXT"
            }),
            "Broker alias bootstrapless-kafka missing required",
            "bootstrap.servers"
        );
    }

    @Test
    public void legacySparkConfBootstrapOverrideStillWorks() {
        Map<String, String> spark = new HashMap<String, String>();
        spark.put("recon.inputRoots", "/data/root-a");
        spark.put("spark.recon.sourceTopic", "orders");
        spark.put("spark.recon.kafkaBootstrapServers", "spark-broker:9092");
        spark.put("spark.recon.canaryTopic", "orders-canary");

        CheckerConfig config = ConfigLoader.loadConfig(new MapLookup(spark), new KafkaConfigsProperties(), fixedDate("2026-07-04"));

        assertTrue(config.sideTopicConfig.isPresent());
        assertFalse(config.sideTopicConfig.get().kafkaAlias.isPresent());
        assertEquals("spark-broker:9092", config.sideTopicConfig.get().kafkaBootstrapServers);
        assertEquals("spark-broker:9092", config.sideTopicConfig.get().kafkaConsumerConfig.get("bootstrap.servers"));
    }

    @Test
    public void importedBrokerAliasReachesSparkReaderOptionPath() throws Exception {
        File application = new File("application.yml");
        File brokers = new File("kafka-brokers.yml");
        byte[] previousApplication = application.exists() ? Files.readAllBytes(application.toPath()) : null;
        byte[] previousBrokers = brokers.exists() ? Files.readAllBytes(brokers.toPath()) : null;

        try {
            write(application,
                "spring:\n"
                    + "  config:\n"
                    + "    import: \"file:./kafka-brokers.yml\"\n"
                    + "recon:\n"
                    + "  input-roots:\n"
                    + "    - /yaml/root-a\n"
                    + "  run-date: \"2026-07-05\"\n"
                    + "  source-topic: yaml-orders\n"
                    + "  kafka-alias: main-kafka\n"
                    + "  canary-topic: yaml-orders-canary\n"
                    + "  dead-letter-topic: yaml-orders-dlq\n"
                    + "  side-topic-starting-offsets: earliest\n");
            write(brokers,
                "kafka-configs:\n"
                    + "  broker:\n"
                    + "    main-kafka:\n"
                    + "      conf:\n"
                    + "        \"[bootstrap.servers]\": file-main:9092\n"
                    + "        \"[security.protocol]\": SSL\n"
                    + "        \"[max.poll.records]\": 123\n"
                    + "    reserved-kafka:\n"
                    + "      conf:\n"
                    + "        \"[bootstrap.servers]\": file-reserved:9092\n");
            LoadedProperties loaded = loadPropertiesFromLocation("file:./application.yml");
            CheckerConfig config = ConfigLoader.loadConfig(
                new ApplicationYamlLookup(loaded.recon),
                loaded.kafkaConfigs,
                fixedDate("2026-07-06")
            );

            Map<String, String> options = SideTopicReaderOptions.build(config.sideTopicConfig.get(), "yaml-orders-canary");
            assertEquals("file-main:9092", options.get("kafka.bootstrap.servers"));
            assertEquals("SSL", options.get("kafka.security.protocol"));
            assertEquals("123", options.get("kafka.max.poll.records"));
            assertEquals("yaml-orders-canary", options.get("subscribe"));
            assertEquals("earliest", options.get("startingOffsets"));
            assertEquals("latest", options.get("endingOffsets"));
            assertEquals("true", options.get("failOnDataLoss"));
        } finally {
            restore(application, previousApplication);
            restore(brokers, previousBrokers);
        }
    }

    private static CheckerConfig loadFromYaml(String resourceName) {
        LoadedProperties loaded = loadProperties(resourceName);
        return ConfigLoader.loadConfig(
            new ApplicationYamlLookup(loaded.recon),
            loaded.kafkaConfigs,
            fixedDate("2026-07-06")
        );
    }

    private static LoadedProperties loadProperties(String resourceName) {
        return loadPropertiesFromLocation("classpath:/recon-yaml/" + resourceName);
    }

    private static LoadedProperties loadPropertiesFromLocation(String location) {
        SpringApplication application = new SpringApplication(PropertiesOnlyConfiguration.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        application.setLogStartupInfo(false);

        ConfigurableApplicationContext context = application.run(
            "--spring.config.location=" + location
        );
        try {
            return new LoadedProperties(
                context.getBean(ReconProperties.class),
                context.getBean(KafkaConfigsProperties.class)
            );
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

    private static Map<String, String> sideTopicConf(String alias) {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("recon.inputRoots", "/data/root-a");
        conf.put("recon.sourceTopic", "orders");
        if (alias != null) {
            conf.put("recon.kafkaAlias", alias);
        }
        conf.put("recon.canaryTopic", "orders-canary");
        return conf;
    }

    private static KafkaConfigsProperties brokerConfigs() {
        return brokerConfigs("main-kafka", new String[] {
            "bootstrap.servers", "main:9092",
            "security.protocol", "PLAINTEXT"
        });
    }

    private static KafkaConfigsProperties brokerConfigs(String alias, String[] entries) {
        KafkaConfigsProperties properties = new KafkaConfigsProperties();
        Map<String, KafkaConfigsProperties.BrokerProperties> brokers =
            new LinkedHashMap<String, KafkaConfigsProperties.BrokerProperties>();
        KafkaConfigsProperties.BrokerProperties broker = new KafkaConfigsProperties.BrokerProperties();
        Map<String, String> conf = new LinkedHashMap<String, String>();
        for (int i = 0; i + 1 < entries.length; i += 2) {
            conf.put(entries[i], entries[i + 1]);
        }
        broker.setConf(conf);
        brokers.put(alias, broker);
        properties.setBroker(brokers);
        return properties;
    }

    private static void assertConfigFailure(Map<String, String> conf, KafkaConfigsProperties brokers, String... messages) {
        try {
            ConfigLoader.loadConfig(new MapLookup(conf), brokers, fixedDate("2026-07-04"));
            fail("expected ReconExit");
        } catch (ReconExit exit) {
            assertEquals(2, exit.code);
            for (String message : messages) {
                assertTrue("missing message: " + message + " in " + exit.getMessage(), exit.getMessage().contains(message));
            }
        }
    }

    private static void write(File file, String text) throws IOException {
        FileWriter writer = new FileWriter(file);
        try {
            writer.write(text);
        } finally {
            writer.close();
        }
    }

    private static void restore(File file, byte[] previousContent) throws IOException {
        if (previousContent == null) {
            Files.deleteIfExists(file.toPath());
        } else {
            Files.write(file.toPath(), previousContent);
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
            return value == null
                ? Optional.<String>empty()
                : Optional.of(value);
        }
    }

    private static final class LoadedProperties {
        private final ReconProperties recon;
        private final KafkaConfigsProperties kafkaConfigs;

        private LoadedProperties(ReconProperties recon, KafkaConfigsProperties kafkaConfigs) {
            this.recon = recon;
            this.kafkaConfigs = kafkaConfigs;
        }
    }

    @SpringBootConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties({ReconProperties.class, KafkaConfigsProperties.class})
    static class PropertiesOnlyConfiguration {
    }
}
