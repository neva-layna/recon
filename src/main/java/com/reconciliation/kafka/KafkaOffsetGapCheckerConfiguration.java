package com.reconciliation.kafka;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Supplier;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import com.reconciliation.kafka.config.ApplicationYamlLookup;
import com.reconciliation.kafka.config.ConfLookup;
import com.reconciliation.kafka.config.LayeredConfLookup;
import com.reconciliation.kafka.config.ReconProperties;
import com.reconciliation.kafka.config.SparkConfLookup;

import lombok.extern.slf4j.Slf4j;

/**
 * Spring Boot application context for the Spark offset checker.
 */
@SpringBootConfiguration(proxyBeanMethods = false)
@ComponentScan(basePackages = "com.reconciliation.kafka")
@EnableConfigurationProperties(ReconProperties.class)
@Slf4j
public class KafkaOffsetGapCheckerConfiguration {
    /**
     * Creates or obtains the driver Spark session used by all checker services.
     *
     * @return active Spark session, stopped when the Spring context closes
     */
    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession() {
        log.debug("Creating SparkSession for Kafka offset gap checker");
        return SparkSession.builder()
            .appName("Kafka Offset Gap Checker")
            .getOrCreate();
    }

    /**
     * Creates the Spark-backed configuration lookup.
     *
     * @param spark active Spark session
     * @return configuration lookup used by the loader service
     */
    @Bean
    public ConfLookup confLookup(SparkSession spark, ReconProperties reconProperties) {
        return new LayeredConfLookup(new SparkConfLookup(spark), new ApplicationYamlLookup(reconProperties));
    }

    /**
     * Supplies the driver-local date for default run-date behavior.
     *
     * @return current date supplier
     */
    @Bean
    public Supplier<LocalDate> currentDateSupplier() {
        return new Supplier<LocalDate>() {
            @Override
            public LocalDate get() {
                return LocalDate.now(ZoneId.systemDefault());
            }
        };
    }
}
