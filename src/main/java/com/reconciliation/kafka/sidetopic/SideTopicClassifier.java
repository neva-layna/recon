package com.reconciliation.kafka.sidetopic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.reconciliation.kafka.model.GapAnalysisResult;
import com.reconciliation.kafka.model.MissingOffsetReport;

public final class SideTopicClassifier {
    private SideTopicClassifier() {
    }

    public static SideTopicClassification classify(
        String sourceTopic,
        GapAnalysisResult gaps,
        List<SideTopicRecord> canaryRecords,
        List<SideTopicRecord> deadLetterRecords
    ) {
        Set<MissingOffsetKey> missing = new LinkedHashSet<MissingOffsetKey>();
        boolean truncated = false;
        List<Integer> partitions = new ArrayList<Integer>(gaps.missingOffsetsByPartition.keySet());
        Collections.sort(partitions);
        for (Integer partition : partitions) {
            MissingOffsetReport report = gaps.missingOffsetsByPartition.get(partition);
            truncated = truncated || report.truncated;
            for (Long offset : report.offsets) {
                missing.add(new MissingOffsetKey(sourceTopic, partition, offset));
            }
        }

        Set<MissingOffsetKey> canaryMatched = matchingKeys(missing, canaryRecords);
        Set<MissingOffsetKey> deadLetterMatched = matchingKeys(missing, deadLetterRecords);
        Set<MissingOffsetKey> unresolved = new LinkedHashSet<MissingOffsetKey>(missing);
        unresolved.removeAll(canaryMatched);
        unresolved.removeAll(deadLetterMatched);

        return new SideTopicClassification(
            sourceTopic,
            toPartitionOffsetMap(canaryMatched),
            toPartitionOffsetMap(deadLetterMatched),
            toPartitionOffsetMap(unresolved),
            canaryMatched.size(),
            deadLetterMatched.size(),
            unresolved.size(),
            canaryRecords.size(),
            deadLetterRecords.size(),
            countDeadLetterField(deadLetterRecords, deadLetterMatched, DeadLetterField.FAILURE_EVENT_ID),
            countDeadLetterField(deadLetterRecords, deadLetterMatched, DeadLetterField.REASON_MSG),
            countDeadLetterField(deadLetterRecords, deadLetterMatched, DeadLetterField.EXCEPTION),
            truncated
        );
    }

    private static Set<MissingOffsetKey> matchingKeys(Set<MissingOffsetKey> missing, List<SideTopicRecord> records) {
        Set<MissingOffsetKey> matches = new LinkedHashSet<MissingOffsetKey>();
        for (SideTopicRecord record : records) {
            MissingOffsetKey key = record.key();
            if (missing.contains(key)) {
                matches.add(key);
            }
        }
        return matches;
    }

    private static Map<Integer, List<Long>> toPartitionOffsetMap(Set<MissingOffsetKey> keys) {
        List<MissingOffsetKey> sorted = new ArrayList<MissingOffsetKey>(keys);
        Collections.sort(sorted);
        Map<Integer, List<Long>> byPartition = new LinkedHashMap<Integer, List<Long>>();
        for (MissingOffsetKey key : sorted) {
            if (!byPartition.containsKey(key.sourcePartition)) {
                byPartition.put(key.sourcePartition, new ArrayList<Long>());
            }
            byPartition.get(key.sourcePartition).add(key.sourceOffset);
        }
        return byPartition;
    }

    private static long countDeadLetterField(
        List<SideTopicRecord> records,
        Set<MissingOffsetKey> matchedKeys,
        DeadLetterField field
    ) {
        long count = 0L;
        for (SideTopicRecord record : records) {
            if (!matchedKeys.contains(record.key())) {
                continue;
            }
            boolean present;
            if (field == DeadLetterField.FAILURE_EVENT_ID) {
                present = record.failureEventId.isPresent();
            } else if (field == DeadLetterField.REASON_MSG) {
                present = record.reasonMsg.isPresent();
            } else {
                present = record.exception.isPresent();
            }
            if (present) {
                count++;
            }
        }
        return count;
    }

    private enum DeadLetterField {
        FAILURE_EVENT_ID,
        REASON_MSG,
        EXCEPTION
    }
}
