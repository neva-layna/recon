package com.reconciliation.synthdata;

/**
 * Supported Kafka side-topic payload kinds.
 */
public enum KafkaSideTopicKind {
    CANARY("canary"),
    DEAD_LETTER("dead-letter");

    private final String cliName;

    KafkaSideTopicKind(String cliName) {
        this.cliName = cliName;
    }

    public String getCliName() {
        return cliName;
    }

    public static KafkaSideTopicKind parse(String value) {
        if (CANARY.cliName.equals(value)) {
            return CANARY;
        }
        if (DEAD_LETTER.cliName.equals(value)) {
            return DEAD_LETTER;
        }
        throw new IllegalArgumentException("--kind must be canary or dead-letter: " + value);
    }
}
