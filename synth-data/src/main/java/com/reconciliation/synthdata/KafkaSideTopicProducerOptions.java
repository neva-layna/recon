package com.reconciliation.synthdata;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Immutable options for one Kafka side-topic producer invocation.
 */
public final class KafkaSideTopicProducerOptions {
    private final String bootstrapServer;
    private final Map<String, String> producerConf;
    private final String destinationTopic;
    private final KafkaSideTopicKind kind;
    private final String sourceTopic;
    private final int sourcePartition;
    private final long sourceOffset;
    private final long sourceTimestamp;
    private final String sourceKey;
    private final String sourceValue;
    private final Map<String, String> sourceHeaders;
    private final String failureEventId;
    private final String reasonMsg;
    private final String exception;
    private final boolean dryRun;
    private final Path payloadFile;

    private KafkaSideTopicProducerOptions(Builder builder) {
        this.bootstrapServer = requireText(builder.bootstrapServer, "--bootstrap-server");
        this.destinationTopic = requireText(builder.destinationTopic, "--destination-topic");
        this.kind = builder.kind == null ? missingKind() : builder.kind;
        this.sourceTopic = requireText(builder.sourceTopic, "--source-topic");
        this.sourcePartition = requireNonNegativeInt(builder.sourcePartition, "--source-partition");
        this.sourceOffset = requireNonNegativeLong(builder.sourceOffset, "--source-offset");
        this.sourceTimestamp = requireNonNegativeLong(builder.sourceTimestamp, "--source-timestamp");
        this.sourceKey = requireText(builder.sourceKey, "--source-key");
        this.sourceValue = requireText(builder.sourceValue, "--source-value");
        if (!builder.sourceHeadersProvided) {
            throw new IllegalArgumentException("missing required argument: --source-header or --source-headers");
        }
        this.sourceHeaders = Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.sourceHeaders));
        if (this.kind == KafkaSideTopicKind.DEAD_LETTER) {
            this.failureEventId = requireText(builder.failureEventId, "--failure-event-id");
            this.reasonMsg = requireText(builder.reasonMsg, "--reason-msg");
            this.exception = requireText(builder.exception, "--exception");
        } else {
            this.failureEventId = trimToNull(builder.failureEventId);
            this.reasonMsg = trimToNull(builder.reasonMsg);
            this.exception = trimToNull(builder.exception);
        }
        this.producerConf = Collections.unmodifiableMap(new LinkedHashMap<String, String>(builder.producerConf));
        this.dryRun = builder.dryRun;
        this.payloadFile = builder.payloadFile;
    }

    public static KafkaSideTopicProducerOptions parse(String[] rawArgs) {
        String[] args = stripSubcommand(rawArgs);
        Builder builder = new Builder();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if ("--help".equals(arg) || "-h".equals(arg)) {
                builder.help = true;
            } else if ("--dry-run".equals(arg)) {
                builder.dryRun = true;
            } else if ("--bootstrap-server".equals(arg)) {
                builder.bootstrapServer = requireValue(args, ++index, arg);
            } else if ("--conf".equals(arg)) {
                parseKeyValue(builder.producerConf, requireValue(args, ++index, arg), arg);
            } else if ("--destination-topic".equals(arg) || "--topic".equals(arg)) {
                builder.destinationTopic = requireValue(args, ++index, arg);
            } else if ("--kind".equals(arg)) {
                builder.kind = KafkaSideTopicKind.parse(requireValue(args, ++index, arg));
            } else if ("--source-topic".equals(arg)) {
                builder.sourceTopic = requireValue(args, ++index, arg);
            } else if ("--source-partition".equals(arg)) {
                builder.sourcePartition = parseInt(arg, requireValue(args, ++index, arg));
            } else if ("--source-offset".equals(arg)) {
                builder.sourceOffset = parseLong(arg, requireValue(args, ++index, arg));
            } else if ("--source-timestamp".equals(arg)) {
                builder.sourceTimestamp = parseLong(arg, requireValue(args, ++index, arg));
            } else if ("--source-key".equals(arg)) {
                builder.sourceKey = requireValue(args, ++index, arg);
            } else if ("--source-value".equals(arg)) {
                builder.sourceValue = requireValue(args, ++index, arg);
            } else if ("--source-header".equals(arg)) {
                builder.sourceHeadersProvided = true;
                parseKeyValue(builder.sourceHeaders, requireValue(args, ++index, arg), arg);
            } else if ("--source-headers".equals(arg)) {
                builder.sourceHeadersProvided = true;
                parseHeaders(builder.sourceHeaders, requireValue(args, ++index, arg), arg);
            } else if ("--failure-event-id".equals(arg)) {
                builder.failureEventId = requireValue(args, ++index, arg);
            } else if ("--reason-msg".equals(arg)) {
                builder.reasonMsg = requireValue(args, ++index, arg);
            } else if ("--exception".equals(arg)) {
                builder.exception = requireValue(args, ++index, arg);
            } else if ("--payload-file".equals(arg)) {
                builder.payloadFile = Paths.get(requireValue(args, ++index, arg));
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }
        if (builder.help) {
            throw new HelpRequestedException();
        }
        return new KafkaSideTopicProducerOptions(builder);
    }

    public Properties producerProperties() {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : producerConf.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if (!properties.containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "synth-data-side-topic-producer");
        }
        return properties;
    }

    public String producerConfigSummary() {
        StringBuilder builder = new StringBuilder();
        builder.append("bootstrap.servers=").append(bootstrapServer);
        builder.append(" key.serializer=").append(ByteArraySerializer.class.getName());
        builder.append(" value.serializer=").append(ByteArraySerializer.class.getName());
        for (Map.Entry<String, String> entry : producerConf.entrySet()) {
            builder.append(' ').append("conf.").append(entry.getKey()).append('=').append(entry.getValue());
        }
        return builder.toString();
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public Map<String, String> getProducerConf() {
        return producerConf;
    }

    public String getDestinationTopic() {
        return destinationTopic;
    }

    public KafkaSideTopicKind getKind() {
        return kind;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public int getSourcePartition() {
        return sourcePartition;
    }

    public long getSourceOffset() {
        return sourceOffset;
    }

    public long getSourceTimestamp() {
        return sourceTimestamp;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getSourceValue() {
        return sourceValue;
    }

    public Map<String, String> getSourceHeaders() {
        return sourceHeaders;
    }

    public String getFailureEventId() {
        return failureEventId;
    }

    public String getReasonMsg() {
        return reasonMsg;
    }

    public String getException() {
        return exception;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Path getPayloadFile() {
        return payloadFile;
    }

    private static String[] stripSubcommand(String[] args) {
        if (args.length > 0 && "kafka-side-topic".equals(args[0])) {
            String[] stripped = new String[args.length - 1];
            System.arraycopy(args, 1, stripped, 0, stripped.length);
            return stripped;
        }
        return args;
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        String value = args[index];
        if (value == null || value.trim().isEmpty() || value.startsWith("--")) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return value;
    }

    private static void parseHeaders(Map<String, String> headers, String value, String flag) {
        if ("none".equalsIgnoreCase(value) || "-".equals(value)) {
            return;
        }
        String[] entries = value.split(",");
        for (String entry : entries) {
            parseKeyValue(headers, entry, flag);
        }
    }

    private static void parseKeyValue(Map<String, String> target, String value, String flag) {
        int separator = value.indexOf('=');
        if (separator <= 0) {
            throw new IllegalArgumentException(flag + " must be key=value: " + value);
        }
        String key = value.substring(0, separator).trim();
        if (key.isEmpty()) {
            throw new IllegalArgumentException(flag + " key must not be empty: " + value);
        }
        target.put(key, value.substring(separator + 1));
    }

    private static int parseInt(String flag, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(flag + " must be an integer: " + value, error);
        }
    }

    private static long parseLong(String flag, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(flag + " must be an integer: " + value, error);
        }
    }

    private static String requireText(String value, String flag) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            throw new IllegalArgumentException("missing required argument: " + flag);
        }
        return trimmed;
    }

    private static int requireNonNegativeInt(Integer value, String flag) {
        if (value == null) {
            throw new IllegalArgumentException("missing required argument: " + flag);
        }
        if (value.intValue() < 0) {
            throw new IllegalArgumentException(flag + " must be non-negative: " + value);
        }
        return value.intValue();
    }

    private static long requireNonNegativeLong(Long value, String flag) {
        if (value == null) {
            throw new IllegalArgumentException("missing required argument: " + flag);
        }
        if (value.longValue() < 0L) {
            throw new IllegalArgumentException(flag + " must be non-negative: " + value);
        }
        return value.longValue();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static KafkaSideTopicKind missingKind() {
        throw new IllegalArgumentException("missing required argument: --kind");
    }

    static final class HelpRequestedException extends IllegalArgumentException {
        HelpRequestedException() {
            super("help requested");
        }
    }

    private static final class Builder {
        private String bootstrapServer;
        private final Map<String, String> producerConf = new LinkedHashMap<String, String>();
        private String destinationTopic;
        private KafkaSideTopicKind kind;
        private String sourceTopic;
        private Integer sourcePartition;
        private Long sourceOffset;
        private Long sourceTimestamp;
        private String sourceKey;
        private String sourceValue;
        private final Map<String, String> sourceHeaders = new LinkedHashMap<String, String>();
        private boolean sourceHeadersProvided;
        private String failureEventId;
        private String reasonMsg;
        private String exception;
        private boolean dryRun;
        private Path payloadFile;
        private boolean help;
    }
}
