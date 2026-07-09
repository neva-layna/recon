package com.reconciliation.synthdata;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Command-line entrypoint for producing checker-compatible Kafka side-topic
 * records.
 */
public final class KafkaSideTopicProducerCli {
    private KafkaSideTopicProducerCli() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            KafkaSideTopicProducerOptions options = KafkaSideTopicProducerOptions.parse(args);
            byte[] payload = SideTopicAvroPayloads.create(options);
            writePayloadFile(options.getPayloadFile(), payload);
            printManifest(out, options, payload.length);
            if (options.isDryRun()) {
                out.println("[synth-data] producer_config " + options.producerConfigSummary());
                out.println("[synth-data] dry_run=true");
                return 0;
            }

            KafkaSideTopicDelivery delivery = new KafkaSideTopicProducer().send(options, payload);
            out.println(
                "[synth-data] delivered"
                    + " destination_topic=" + options.getDestinationTopic()
                    + " kind=" + options.getKind().getCliName()
                    + " source_topic=" + options.getSourceTopic()
                    + " source_partition=" + options.getSourcePartition()
                    + " source_offset=" + options.getSourceOffset()
                    + " kafka_partition=" + delivery.getPartition()
                    + " kafka_offset=" + delivery.getOffset()
                    + " payload_bytes=" + payload.length
            );
            return 0;
        } catch (KafkaSideTopicProducerOptions.HelpRequestedException help) {
            printUsage(out);
            return 0;
        } catch (IllegalArgumentException error) {
            err.println("[synth-data] ERROR: " + error.getMessage());
            printUsage(err);
            return 2;
        } catch (Exception error) {
            err.println("[synth-data] ERROR: " + error.getMessage());
            return 1;
        }
    }

    static boolean isKafkaInvocation(String[] args) {
        if (args.length > 0 && "kafka-side-topic".equals(args[0])) {
            return true;
        }
        for (String arg : args) {
            if ("--bootstrap-server".equals(arg)
                || "--destination-topic".equals(arg)
                || "--kind".equals(arg)
                || "--source-topic".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    static void printUsage(PrintStream stream) {
        stream.println("Usage: synth-data kafka-side-topic --bootstrap-server HOST:PORT --destination-topic TOPIC --kind canary|dead-letter \\");
        stream.println("       --source-topic TOPIC --source-partition N --source-offset N --source-timestamp EPOCH_MS \\");
        stream.println("       --source-key TEXT --source-value TEXT --source-header key=value [--source-header key=value] \\");
        stream.println("       [--conf key=value] [--dry-run] [--payload-file PATH]");
        stream.println("       dead-letter also requires --failure-event-id TEXT --reason-msg TEXT --exception TEXT");
        stream.println("       --source-headers key=value,key2=value2 or --source-headers none may be used instead of repeated --source-header.");
        stream.println("       key.serializer and value.serializer are always ByteArraySerializer for Avro byte payloads.");
    }

    private static void printManifest(PrintStream out, KafkaSideTopicProducerOptions options, int payloadBytes) {
        out.println(
            "[synth-data] manifest"
                + " destination_topic=" + options.getDestinationTopic()
                + " kind=" + options.getKind().getCliName()
                + " source_topic=" + options.getSourceTopic()
                + " source_partition=" + options.getSourcePartition()
                + " source_offset=" + options.getSourceOffset()
                + " payload_bytes=" + payloadBytes
        );
    }

    private static void writePayloadFile(Path payloadFile, byte[] payload) throws IOException {
        if (payloadFile == null) {
            return;
        }
        Path parent = payloadFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(payloadFile, payload);
    }
}
