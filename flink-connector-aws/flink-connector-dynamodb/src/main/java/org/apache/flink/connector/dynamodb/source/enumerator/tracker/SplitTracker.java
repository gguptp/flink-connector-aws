/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.dynamodb.source.enumerator.tracker;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDBStreamsShardSplitWithAssignmentStatus;
import org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.ShardUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition.LATEST;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON;
import static org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus.ASSIGNED;
import static org.apache.flink.connector.dynamodb.source.enumerator.SplitAssignmentStatus.FINISHED;

/**
 * This class is used to track splits and will be used to assign any unassigned splits. It also
 * ensures that the parent-child shard ordering is maintained.
 */
@Internal
public class SplitTracker {
    private final Map<String, DynamoDbStreamsShardSplit> knownSplits = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> parentChildSplitMap = new ConcurrentHashMap<>();
    private final Set<String> assignedSplits = new HashSet<>();
    private final Set<String> finishedSplits = new HashSet<>();
    private final String streamArn;
    private final InitialPosition initialPosition;
    private final Instant startTimestamp;
    private static final Logger LOG = LoggerFactory.getLogger(SplitTracker.class);

    public SplitTracker(String streamArn, InitialPosition initialPosition, Instant startTimestamp) {
        this(Collections.emptyList(), streamArn, initialPosition, startTimestamp);
    }

    public SplitTracker(
            List<DynamoDBStreamsShardSplitWithAssignmentStatus> initialState,
            String streamArn,
            DynamodbStreamsSourceConfigConstants.InitialPosition initialPosition,
            Instant startTimestamp) {
        this.streamArn = streamArn;
        this.initialPosition = initialPosition;
        this.startTimestamp = startTimestamp;
        initialState.forEach(
                splitWithStatus -> {
                    DynamoDbStreamsShardSplit currentSplit = splitWithStatus.split();
                    knownSplits.put(currentSplit.splitId(), currentSplit);
                    addSplitToMapping(currentSplit);
                    if (ASSIGNED.equals(splitWithStatus.assignmentStatus())) {
                        assignedSplits.add(splitWithStatus.split().splitId());
                    }
                    if (FINISHED.equals(splitWithStatus.assignmentStatus())) {
                        finishedSplits.add(splitWithStatus.split().splitId());
                    }
                });
    }

    /**
     * Add newly discovered splits to tracker.
     *
     * @param shardsToAdd collection of splits to add to tracking
     */
    public void addSplits(Collection<Shard> shardsToAdd) {
        if (TRIM_HORIZON.equals(initialPosition)) {
            addSplitsForTrimHorizon(shardsToAdd);
            return;
        }
        addSplitsForLatest(shardsToAdd);
    }

    public void addChildSplits(Collection<Shard> childShardsToAdd) {
        addSplitsForTrimHorizon(childShardsToAdd);
    }

    private void addSplitsForLatest(Collection<Shard> shardsToAdd) {
        List<Shard> openShards =
                shardsToAdd.stream()
                        .filter(shard -> shard.sequenceNumberRange().endingSequenceNumber() == null)
                        .collect(Collectors.toList());
        Map<String, Shard> shardIdToShardMap =
                shardsToAdd.stream().collect(Collectors.toMap(Shard::shardId, shard -> shard));
        for (Shard shard : openShards) {
            String shardId = shard.shardId();
            if (knownSplits.containsKey(shardId)) {
                continue;
            }
            String firstShardIdBeforeTimestamp =
                    findFirstShardIdBeforeTimestamp(shardId, shardIdToShardMap);
            putAllAncestorShardsTillFirstShardId(
                    shardId, firstShardIdBeforeTimestamp, shardIdToShardMap);
        }
    }

    private void putAllAncestorShardsTillFirstShardId(
            String shardId,
            String firstShardIdBeforeTimestamp,
            Map<String, Shard> shardIdToShardMap) {
        String currentShardId = shardId;
        while (currentShardId != null
                && shardIdToShardMap.containsKey(currentShardId)
                && !knownSplits.containsKey(currentShardId)) {
            Shard currentShard = shardIdToShardMap.get(currentShardId);
            if (Objects.equals(currentShardId, firstShardIdBeforeTimestamp)) {
                DynamoDbStreamsShardSplit newSplit = mapToSplit(currentShard, LATEST);
                knownSplits.putIfAbsent(currentShardId, newSplit);
                break;
            } else {
                DynamoDbStreamsShardSplit newSplit = mapToSplit(currentShard, TRIM_HORIZON);
                knownSplits.putIfAbsent(currentShardId, newSplit);
            }
            currentShardId = currentShard.parentShardId();
        }
    }

    private String findFirstShardIdBeforeTimestamp(
            String shardIdToStartWith, Map<String, Shard> shardIdToShardMap) {
        String shardId = shardIdToStartWith;
        while (shardId != null && shardIdToShardMap.containsKey(shardId)) {
            Shard shard = shardIdToShardMap.get(shardId);
            if (ShardUtils.isShardCreatedBeforeTimestamp(shardId, startTimestamp)) {
                return shardId;
            }
            shardId = shard.parentShardId();
        }
        return null;
    }

    private void addSplitsForTrimHorizon(Collection<Shard> shardsToAdd) {
        for (Shard shard : shardsToAdd) {
            String shardId = shard.shardId();
            if (!knownSplits.containsKey(shardId)) {
                DynamoDbStreamsShardSplit newSplit = mapToSplit(shard, TRIM_HORIZON);
                knownSplits.put(shardId, newSplit);
                addSplitToMapping(newSplit);
            }
        }
    }

    private void addSplitToMapping(DynamoDbStreamsShardSplit split) {
        if (split.getParentShardId() == null) {
            return;
        }
        parentChildSplitMap
                .computeIfAbsent(split.getParentShardId(), k -> new HashSet<>())
                .add(split.splitId());
    }

    private DynamoDbStreamsShardSplit mapToSplit(
            Shard shard, DynamodbStreamsSourceConfigConstants.InitialPosition initialPosition) {
        StartingPosition startingPosition;
        switch (initialPosition) {
            case LATEST:
                startingPosition = StartingPosition.latest();
                break;
            case TRIM_HORIZON:
            default:
                startingPosition = StartingPosition.fromStart();
        }
        return new DynamoDbStreamsShardSplit(
                streamArn, shard.shardId(), startingPosition, shard.parentShardId());
    }

    /**
     * Mark splits as assigned. Assigned splits will no longer be returned as pending splits.
     *
     * @param splitsToAssign collection of splits to mark as assigned
     */
    public void markAsAssigned(Collection<DynamoDbStreamsShardSplit> splitsToAssign) {
        splitsToAssign.forEach(split -> assignedSplits.add(split.splitId()));
    }

    /**
     * Mark splits as finished. Assigned splits will no longer be returned as pending splits.
     *
     * @param splitsToFinish collection of splits to mark as finished
     */
    public void markAsFinished(Collection<String> splitsToFinish) {
        splitsToFinish.forEach(
                splitId -> {
                    finishedSplits.add(splitId);
                    assignedSplits.remove(splitId);
                });
    }

    public boolean isAssigned(String splitId) {
        return assignedSplits.contains(splitId);
    }

    /**
     * Function to get children splits available for given parent ids. This will ensure not to
     * iterate all the values in knownSplits so saving compute
     */
    public List<DynamoDbStreamsShardSplit> getUnassignedChildSplits(Set<String> parentSplitIds) {
        return parentSplitIds
                .parallelStream()
                .filter(
                        splitId -> {
                            if (!parentChildSplitMap.containsKey(splitId)) {
                                LOG.debug(
                                        "splitId: {} is not present in parent-child relationship map. "
                                                + "This indicates that there might be some data loss in the "
                                                + "application or the child shard has not been discovered yet",
                                        splitId);
                            }
                            return parentChildSplitMap.containsKey(splitId);
                        })
                .map(parentChildSplitMap::get)
                .flatMap(Set::stream)
                .filter(knownSplits::containsKey)
                .map(knownSplits::get)
                .filter(this::checkIfSplitCanBeAssigned)
                .collect(Collectors.toList());
    }

    /**
     * Tells whether a split can be assigned or not. Conditions which it checks:
     *
     * <p>- Split should not already be assigned.
     *
     * <p>- Split should not be already finished.
     *
     * <p>- The parent splits should either be finished or no longer be present in knownSplits.
     */
    private boolean checkIfSplitCanBeAssigned(DynamoDbStreamsShardSplit split) {
        boolean splitIsNotAssigned = !isAssigned(split.splitId());
        return splitIsNotAssigned
                && !isFinished(split.splitId())
                && verifyParentIsEitherFinishedOrCleanedUp(split);
    }

    /**
     * Since we never put an inconsistent shard lineage to splitTracker, so if a shard's parent is
     * not there, that means that that should already be cleaned up.
     */
    public List<DynamoDbStreamsShardSplit> splitsAvailableForAssignment() {
        return knownSplits
                .values()
                .parallelStream()
                .filter(this::checkIfSplitCanBeAssigned)
                .collect(Collectors.toList());
    }

    public List<DynamoDBStreamsShardSplitWithAssignmentStatus> snapshotState(long checkpointId) {
        return knownSplits
                .values()
                .parallelStream()
                .map(
                        split -> {
                            SplitAssignmentStatus assignmentStatus =
                                    SplitAssignmentStatus.UNASSIGNED;
                            if (isAssigned(split.splitId())) {
                                assignmentStatus = ASSIGNED;
                            } else if (isFinished(split.splitId())) {
                                assignmentStatus = SplitAssignmentStatus.FINISHED;
                            }
                            return new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                    split, assignmentStatus);
                        })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public Set<String> getKnownSplitIds() {
        return knownSplits.keySet();
    }

    @VisibleForTesting
    public Map<String, DynamoDbStreamsShardSplit> getKnownSplits() {
        return knownSplits;
    }

    /**
     * finishedSplits needs to be cleaned up. The logic applied to cleaning up finished splits is
     * that if any split has been finished reading, its parent has been finished reading and it is
     * no longer present in the describestream response, it means that the split can be cleaned up.
     */
    public void cleanUpOldFinishedSplits(Set<String> discoveredSplitIds) {
        Set<String> finishedSplitsSnapshot = new HashSet<>(finishedSplits);
        for (String finishedSplitId : finishedSplitsSnapshot) {
            DynamoDbStreamsShardSplit finishedSplit = knownSplits.get(finishedSplitId);
            if (isSplitReadyToBeCleanedUp(finishedSplit, discoveredSplitIds)) {
                finishedSplits.remove(finishedSplitId);
                knownSplits.remove(finishedSplitId);
                parentChildSplitMap.remove(finishedSplit.splitId());
            }
        }
    }

    private boolean isSplitReadyToBeCleanedUp(
            DynamoDbStreamsShardSplit finishedSplit, Set<String> discoveredSplitIds) {
        String splitId = finishedSplit.splitId();
        boolean parentIsFinishedOrCleanedUp =
                verifyParentIsEitherFinishedOrCleanedUp(finishedSplit);
        boolean isAnOldSplit = ShardUtils.isShardOlderThanRetentionPeriod(splitId);
        return parentIsFinishedOrCleanedUp && !discoveredSplitIds.contains(splitId) && isAnOldSplit;
    }

    private boolean verifyParentIsEitherFinishedOrCleanedUp(DynamoDbStreamsShardSplit split) {
        if (split.getParentShardId() == null) {
            return true;
        }
        return !knownSplits.containsKey(split.getParentShardId())
                || isFinished(split.getParentShardId());
    }

    /**
     * Provides information whether a split is finished or not.
     *
     * @param splitId
     * @return boolean value indicating if split is finished
     */
    public boolean isFinished(String splitId) {
        return finishedSplits.contains(splitId);
    }
}
