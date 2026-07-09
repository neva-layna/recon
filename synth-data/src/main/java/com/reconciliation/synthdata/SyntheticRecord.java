package com.reconciliation.synthdata;

import java.util.Map;

/**
 * One checker-compatible synthetic row.
 */
public final class SyntheticRecord {
    private final String metadataJson;
    private final String topic;
    private final String payload;
    private final Map<String, String> extraValues;

    public SyntheticRecord(String metadataJson, String topic, String payload, Map<String, String> extraValues) {
        this.metadataJson = metadataJson;
        this.topic = topic;
        this.payload = payload;
        this.extraValues = extraValues;
    }

    public String getMetadataJson() {
        return metadataJson;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getExtraValues() {
        return extraValues;
    }
}
