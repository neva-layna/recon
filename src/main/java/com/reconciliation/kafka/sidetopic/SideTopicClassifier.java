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

import lombok.experimental.UtilityClass;

/**
 * Matches bounded missing offsets against decoded side-topic source records.
 */
@UtilityClass
public final class SideTopicClassifier {
    /**
     * Classifies missing offsets as canary-explained, dead-letter-explained, or
     * unresolved.
     *
     * @param sourceTopic source topic used to build missing-offset keys
     * @param gaps gap analytics result containing bounded missing offsets
     * @param canaryRecords decoded canary side-topic records
     * @param deadLetterRecords decoded dead-letter side-topic records
     * @return bucketed classification and summary counts
     */
    public static SideTopicClassification classify(
        String sourceTopic,
        GapAnalysisResult gaps,
        List<SideTopicRecord> canaryRecords,
        List<SideTopicRecord> deadLetterRecords
    ) {
        Set<MissingOffsetKey> missing = new LinkedHashSet<>();
        boolean truncated = false;
        List<Integer> partitions = new ArrayList<>(gaps.getMissingOffsetsByPartition().keySet());
        Collections.sort(partitions);
        for (Integer partition : partitions) {
            MissingOffsetReport report = gaps.getMissingOffsetsByPartition().get(partition);
            truncated = truncated || report.isTruncated();
            for (Long offset : report.getOffsets()) {
                missing.add(new MissingOffsetKey(sourceTopic, partition, offset));
            }
        }

        Set<MissingOffsetKey> canaryMatched = matchingKeys(missing, canaryRecords);
        Set<MissingOffsetKey> deadLetterMatched = matchingKeys(missing, deadLetterRecords);
        Set<MissingOffsetKey> unresolved = new LinkedHashSet<>(missing);
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
            gaps.getGapPartitionCount(),
            missing.size(),
            canaryRecords.size(),
            deadLetterRecords.size(),
            countDeadLetterField(deadLetterRecords, deadLetterMatched, DeadLetterField.FAILURE_EVENT_ID),
            countDeadLetterField(deadLetterRecords, deadLetterMatched, DeadLetterField.REASON_MSG),
            countDeadLetterField(deadLetterRecords, deadLetterMatched, DeadLetterField.EXCEPTION),
            truncated
        );
    }

    /**
     * Finds source keys from decoded side-topic records that are in the missing
     * offset set.
     *
     * @param missing missing-offset keys from gap analytics
     * @param records decoded side-topic records
     * @return matched missing-offset keys in encounter order
     */
    private static Set<MissingOffsetKey> matchingKeys(Set<MissingOffsetKey> missing, List<SideTopicRecord> records) {
        Set<MissingOffsetKey> matches = new LinkedHashSet<>();
        for (SideTopicRecord record : records) {
            MissingOffsetKey key = record.key();
            if (missing.contains(key)) {
                matches.add(key);
            }
        }
        return matches;
    }

    /**
     * Groups sorted missing-offset keys into partition-to-offset lists.
     *
     * @param keys missing-offset keys to group
     * @return offsets keyed by source partition
     */
    private static Map<Integer, List<Long>> toPartitionOffsetMap(Set<MissingOffsetKey> keys) {
        List<MissingOffsetKey> sorted = new ArrayList<>(keys);
        Collections.sort(sorted);
        Map<Integer, List<Long>> byPartition = new LinkedHashMap<>();
        for (MissingOffsetKey key : sorted) {
            if (!byPartition.containsKey(key.getSourcePartition())) {
                byPartition.put(key.getSourcePartition(), new ArrayList<>());
            }
            byPartition.get(key.getSourcePartition()).add(key.getSourceOffset());
        }
        return byPartition;
    }

    /**
     * Counts matched dead-letter records that contain one diagnostic field.
     *
     * @param records decoded dead-letter records
     * @param matchedKeys source keys that explain missing offsets
     * @param field diagnostic field to count
     * @return matched record count with that field present
     */
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
                present = record.getFailureEventId().isPresent();
            } else if (field == DeadLetterField.REASON_MSG) {
                present = record.getReasonMsg().isPresent();
            } else {
                present = record.getException().isPresent();
            }
            if (present) {
                count++;
            }
        }
        return count;
    }

    /**
     * Dead-letter diagnostic fields included in summary counts.
     */
    private enum DeadLetterField {
        /**
         * failureEventId field on decoded dead-letter records.
         */
        FAILURE_EVENT_ID,
        /**
         * reasonMsg field on decoded dead-letter records.
         */
        REASON_MSG,
        /**
         * exception field on decoded dead-letter records.
         */
        EXCEPTION
    }
}
