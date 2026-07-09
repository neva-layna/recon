package com.reconciliation.synthdata;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Sends one Avro side-topic record with byte-array Kafka serializers.
 */
public final class KafkaSideTopicProducer {
    public KafkaSideTopicDelivery send(KafkaSideTopicProducerOptions options, byte[] payload)
        throws ExecutionException, InterruptedException {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(options.producerProperties());
        try {
            byte[] key = options.getSourceKey().getBytes(StandardCharsets.UTF_8);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
                options.getDestinationTopic(),
                key,
                payload
            );
            RecordMetadata metadata = producer.send(record).get();
            producer.flush();
            return new KafkaSideTopicDelivery(metadata.partition(), metadata.offset());
        } finally {
            producer.close();
        }
    }
}
