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

package org.apache.flink.connector.dynamodb.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.proxy.StreamProxy;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;
import software.amazon.awssdk.services.dynamodb.model.ShardFilterType;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.CHILD_SHARD_DISCOVERY_MAX_DELAY_MS;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.CHILD_SHARD_DISCOVERY_MIN_DELAY_MS;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.MAX_RETRY_ATTEMPTS_FOR_CHILD_SHARDS;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.NON_BLOCKING_FETCH_MAX_WAIT;

/**
 * An implementation of the SplitReader that periodically polls the DynamoDb stream to retrieve
 * records.
 *
 * <p>By default, the reader makes non-blocking calls. Each {@link #fetch()} starts a GetRecords
 * call (or a child shard discovery call) for every shard that is ready and has no call in progress,
 * and then collects one response. A slow or throttled call on one shard therefore does not delay
 * the other shards. Each shard has at most one call in progress.
 *
 * <p>If {@code blockingGetRecords} is true, the reader calls one shard at a time and waits for each
 * response.
 */
@Internal
public class PollingDynamoDbStreamsShardSplitReader
        implements SplitReader<Record, DynamoDbStreamsShardSplit> {

    private static final RecordsWithSplitIds<Record> INCOMPLETE_SHARD_EMPTY_RECORDS =
            new DynamoDbStreamRecordsWithSplitIds(Collections.emptyIterator(), null, false);

    /** Added to the response queue by {@link #wakeUp()} so that a waiting fetch returns. */
    private static final CompletedCall WAKE_UP =
            new CompletedCall(null, 0, false, null, null, null);

    private final StreamProxy dynamodbStreams;
    private final Duration getRecordsIdlePollingTimeBetweenNonEmptyPolls;
    private final Duration getRecordsIdlePollingTimeBetweenEmptyPolls;
    private final boolean blockingGetRecords;

    private final Deque<DynamoDbStreamsShardSplitWithContext> assignedSplits;
    private final Map<String, List<Shard>> childShardMap;
    private final Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap;
    private final Set<String> pausedSplitIds;

    /**
     * Completed non-blocking calls. AWS SDK threads add to this queue. Only the fetcher thread
     * takes from it.
     */
    private final BlockingQueue<CompletedCall> responseQueue;

    /** Completed calls of paused splits, by split ID. The reader collects them on resume. */
    private final Map<String, CompletedCall> heldCalls;

    private static final Logger LOG =
            LoggerFactory.getLogger(PollingDynamoDbStreamsShardSplitReader.class);

    public PollingDynamoDbStreamsShardSplitReader(
            StreamProxy dynamodbStreamsProxy,
            Duration getRecordsIdlePollingTimeBetweenNonEmptyPolls,
            Duration getRecordsIdlePollingTimeBetweenEmptyPolls,
            Map<String, List<Shard>> childShardMap,
            Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap) {
        this(
                dynamodbStreamsProxy,
                getRecordsIdlePollingTimeBetweenNonEmptyPolls,
                getRecordsIdlePollingTimeBetweenEmptyPolls,
                childShardMap,
                shardMetricGroupMap,
                false);
    }

    public PollingDynamoDbStreamsShardSplitReader(
            StreamProxy dynamodbStreamsProxy,
            Duration getRecordsIdlePollingTimeBetweenNonEmptyPolls,
            Duration getRecordsIdlePollingTimeBetweenEmptyPolls,
            Map<String, List<Shard>> childShardMap,
            Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap,
            boolean blockingGetRecords) {
        this.dynamodbStreams = dynamodbStreamsProxy;
        this.getRecordsIdlePollingTimeBetweenNonEmptyPolls =
                getRecordsIdlePollingTimeBetweenNonEmptyPolls;
        this.getRecordsIdlePollingTimeBetweenEmptyPolls =
                getRecordsIdlePollingTimeBetweenEmptyPolls;
        this.shardMetricGroupMap = shardMetricGroupMap;
        this.childShardMap = childShardMap;
        this.blockingGetRecords = blockingGetRecords;
        this.assignedSplits = new ArrayDeque<>();
        this.pausedSplitIds = new HashSet<>();
        this.responseQueue = new LinkedBlockingQueue<>();
        this.heldCalls = new HashMap<>();
    }

    private long getNextEligibleTime(DynamoDbStreamsShardSplitWithContext splitContext) {
        long requiredDelay =
                splitContext.wasLastPollEmpty
                        ? getRecordsIdlePollingTimeBetweenEmptyPolls.toMillis()
                        : getRecordsIdlePollingTimeBetweenNonEmptyPolls.toMillis();

        return splitContext.lastPollTimeMillis + requiredDelay;
    }

    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        if (assignedSplits.isEmpty()) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }
        return blockingGetRecords ? fetchBlocking() : fetchNonBlocking();
    }

    // ---------------------------------------------------------------------------------------
    // Blocking calls
    // ---------------------------------------------------------------------------------------

    private RecordsWithSplitIds<Record> fetchBlocking() throws IOException {
        DynamoDbStreamsShardSplitWithContext splitContext = assignedSplits.poll();

        if (pausedSplitIds.contains(splitContext.splitState.getSplitId())) {
            assignedSplits.add(splitContext);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        long currentTime = System.currentTimeMillis();

        if (splitContext.splitState.isShardEndReached()) {
            return handleShardEnd(splitContext, currentTime);
        }

        long nextEligibleTime = getNextEligibleTime(splitContext);

        LOG.debug(
                "Polling split: {}, currentTime: {}, eligibleTime: {}, wasEmptyPoll: {}",
                splitContext.splitState.getSplitId(),
                currentTime,
                nextEligibleTime,
                splitContext.wasLastPollEmpty);

        // Check if split is not ready due to empty poll and non-empty poll delay
        if (nextEligibleTime > currentTime) {
            assignedSplits.add(splitContext);
            sleep(1);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }

        GetRecordsResponse getRecordsResponse =
                dynamodbStreams.getRecords(
                        splitContext.splitState.getStreamArn(),
                        splitContext.splitState.getShardId(),
                        splitContext.splitState.getNextStartingPosition());
        assignedSplits.add(splitContext);
        return processGetRecordsResponse(splitContext, getRecordsResponse, currentTime);
    }

    private RecordsWithSplitIds<Record> handleShardEnd(
            DynamoDbStreamsShardSplitWithContext splitContext, long currentTime) {
        if (!splitContext.hasAttemptedChildShardDiscovery) {
            splitContext.hasAttemptedChildShardDiscovery = true;
            splitContext.childShardDiscoveryAttempts = 0;
        }

        if (splitContext.childShardDiscoveryAttempts < MAX_RETRY_ATTEMPTS_FOR_CHILD_SHARDS) {
            long nextChildShardDiscoveryEligibleTime =
                    getNextEligibleTimeForChildDiscovery(splitContext);
            if (currentTime >= nextChildShardDiscoveryEligibleTime) {
                ListShardsResult listShardsResult =
                        dynamodbStreams.listShardsWithFilter(
                                splitContext.splitState.getStreamArn(),
                                childShardFilter(splitContext));
                RecordsWithSplitIds<Record> finishedSplit =
                        processChildShardDiscoveryResult(
                                splitContext, listShardsResult, currentTime);
                if (finishedSplit != null) {
                    return finishedSplit;
                }
            }
            assignedSplits.add(splitContext);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        } else {
            return finishedSplit(splitContext);
        }
    }

    private void sleep(long milliseconds) throws IOException {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new IOException("Split reader was interrupted while sleeping", e);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Non-blocking calls
    // ---------------------------------------------------------------------------------------

    /**
     * Starts a call for every shard that is ready and has no call in progress. Then collects one
     * completed call. If no call is complete, waits until a call completes, a shard is ready, or
     * {@link #wakeUp()} is called, for at most {@code NON_BLOCKING_FETCH_MAX_WAIT}.
     */
    private RecordsWithSplitIds<Record> fetchNonBlocking() throws IOException {
        long currentTime = System.currentTimeMillis();
        long nextEligibleTime = Long.MAX_VALUE;

        Iterator<DynamoDbStreamsShardSplitWithContext> splitIterator = assignedSplits.iterator();
        while (splitIterator.hasNext()) {
            DynamoDbStreamsShardSplitWithContext splitContext = splitIterator.next();
            if (splitContext.callInProgress
                    || pausedSplitIds.contains(splitContext.splitState.getSplitId())) {
                continue;
            }

            long eligibleTime;
            if (splitContext.splitState.isShardEndReached()) {
                if (!splitContext.hasAttemptedChildShardDiscovery) {
                    splitContext.hasAttemptedChildShardDiscovery = true;
                    splitContext.childShardDiscoveryAttempts = 0;
                }
                if (splitContext.childShardDiscoveryAttempts
                        >= MAX_RETRY_ATTEMPTS_FOR_CHILD_SHARDS) {
                    splitIterator.remove();
                    return finishedSplit(splitContext);
                }
                eligibleTime = getNextEligibleTimeForChildDiscovery(splitContext);
                if (currentTime >= eligibleTime) {
                    startChildShardDiscovery(splitContext, currentTime);
                    continue;
                }
            } else {
                eligibleTime = getNextEligibleTime(splitContext);
                if (currentTime >= eligibleTime) {
                    startGetRecords(splitContext, currentTime);
                    continue;
                }
            }
            nextEligibleTime = Math.min(nextEligibleTime, eligibleTime);
        }

        CompletedCall completedCall =
                pollResponseQueue(
                        Math.min(
                                NON_BLOCKING_FETCH_MAX_WAIT.toMillis(),
                                nextEligibleTime - currentTime));
        if (completedCall == null || completedCall == WAKE_UP) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }
        return handleCompletedCall(completedCall);
    }

    private void startGetRecords(
            DynamoDbStreamsShardSplitWithContext splitContext, long startTime) {
        LOG.debug(
                "Starting GetRecords for split: {}, wasEmptyPoll: {}",
                splitContext.splitState.getSplitId(),
                splitContext.wasLastPollEmpty);
        splitContext.callInProgress = true;
        dynamodbStreams
                .getRecordsAsync(
                        splitContext.splitState.getStreamArn(),
                        splitContext.splitState.getShardId(),
                        splitContext.splitState.getNextStartingPosition())
                .whenComplete(
                        (response, error) ->
                                responseQueue.add(
                                        new CompletedCall(
                                                splitContext,
                                                startTime,
                                                false,
                                                response,
                                                null,
                                                error)));
    }

    private void startChildShardDiscovery(
            DynamoDbStreamsShardSplitWithContext splitContext, long startTime) {
        LOG.debug(
                "Starting child shard discovery for split: {}, attempt: {}",
                splitContext.splitState.getSplitId(),
                splitContext.childShardDiscoveryAttempts);
        splitContext.callInProgress = true;
        dynamodbStreams
                .listShardsWithFilterAsync(
                        splitContext.splitState.getStreamArn(), childShardFilter(splitContext))
                .whenComplete(
                        (result, error) ->
                                responseQueue.add(
                                        new CompletedCall(
                                                splitContext,
                                                startTime,
                                                true,
                                                null,
                                                result,
                                                error)));
    }

    @Nullable
    private CompletedCall pollResponseQueue(long waitMillis) throws IOException {
        try {
            return responseQueue.poll(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Split reader was interrupted while waiting for records", e);
        }
    }

    private RecordsWithSplitIds<Record> handleCompletedCall(CompletedCall completedCall)
            throws IOException {
        DynamoDbStreamsShardSplitWithContext splitContext = completedCall.splitContext;
        if (completedCall.error != null) {
            // The call failed after all SDK retries. Fail the fetch, as a blocking call does.
            throw rethrow(completedCall.error);
        }
        if (pausedSplitIds.contains(splitContext.splitState.getSplitId())) {
            // The call stays in progress until the split resumes, so that the reader does not
            // start a second call for the split.
            heldCalls.put(splitContext.splitState.getSplitId(), completedCall);
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }
        splitContext.callInProgress = false;

        if (!completedCall.childShardDiscovery) {
            return processGetRecordsResponse(
                    splitContext, completedCall.getRecordsResponse, completedCall.startTime);
        }
        RecordsWithSplitIds<Record> finishedSplit =
                processChildShardDiscoveryResult(
                        splitContext, completedCall.listShardsResult, completedCall.startTime);
        if (finishedSplit == null) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        }
        assignedSplits.remove(splitContext);
        return finishedSplit;
    }

    private static IOException rethrow(Throwable error) {
        // CompletableFuture wraps a failure in at most one CompletionException.
        Throwable cause =
                error instanceof CompletionException && error.getCause() != null
                        ? error.getCause()
                        : error;
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IOException("DynamoDB Streams call failed", cause);
    }

    // ---------------------------------------------------------------------------------------
    // Shared by blocking and non-blocking calls
    // ---------------------------------------------------------------------------------------

    private RecordsWithSplitIds<Record> processGetRecordsResponse(
            DynamoDbStreamsShardSplitWithContext splitContext,
            GetRecordsResponse getRecordsResponse,
            long callStartTime) {
        boolean isComplete = getRecordsResponse.nextShardIterator() == null;
        boolean isEmptyPoll = hasNoRecords(getRecordsResponse);

        splitContext.lastPollTimeMillis = callStartTime;
        splitContext.wasLastPollEmpty = isEmptyPoll;
        splitContext.splitState.setShardEndReached(isComplete);

        if (isEmptyPoll) {
            return INCOMPLETE_SHARD_EMPTY_RECORDS;
        } else {
            DynamoDbStreamsShardMetrics shardMetrics =
                    shardMetricGroupMap.get(splitContext.splitState.getShardId());
            Record lastRecord =
                    getRecordsResponse.records().get(getRecordsResponse.records().size() - 1);
            shardMetrics.setMillisBehindLatest(
                    Math.max(
                            System.currentTimeMillis()
                                    - lastRecord
                                            .dynamodb()
                                            .approximateCreationDateTime()
                                            .toEpochMilli(),
                            0));
        }

        splitContext.splitState.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(
                        getRecordsResponse
                                .records()
                                .get(getRecordsResponse.records().size() - 1)
                                .dynamodb()
                                .sequenceNumber()));

        return new DynamoDbStreamRecordsWithSplitIds(
                getRecordsResponse.records().iterator(),
                splitContext.splitState.getSplitId(),
                false);
    }

    /**
     * Returns the result that finishes the split, or {@code null} if the stream is enabled and the
     * shard has no child shards yet.
     */
    @Nullable
    private RecordsWithSplitIds<Record> processChildShardDiscoveryResult(
            DynamoDbStreamsShardSplitWithContext splitContext,
            ListShardsResult listShardsResult,
            long callStartTime) {
        if (!StreamStatus.ENABLED.equals(listShardsResult.getStreamStatus())) {
            return finishedSplit(splitContext);
        }
        List<Shard> childShards = listShardsResult.getShards();
        if (!childShards.isEmpty()) {
            this.childShardMap.put(splitContext.splitState.getSplitId(), childShards);
            return finishedSplit(splitContext);
        }
        splitContext.childShardDiscoveryAttempts++;
        splitContext.lastChildShardDiscoveryAttemptTime = callStartTime;
        return null;
    }

    private static RecordsWithSplitIds<Record> finishedSplit(
            DynamoDbStreamsShardSplitWithContext splitContext) {
        return new DynamoDbStreamRecordsWithSplitIds(
                Collections.emptyIterator(), splitContext.splitState.getSplitId(), true);
    }

    private static ShardFilter childShardFilter(DynamoDbStreamsShardSplitWithContext splitContext) {
        return ShardFilter.builder()
                .shardId(splitContext.splitState.getShardId())
                .type(ShardFilterType.CHILD_SHARDS)
                .build();
    }

    private long getNextEligibleTimeForChildDiscovery(
            DynamoDbStreamsShardSplitWithContext splitContext) {
        long exponentialDelay =
                Math.min(
                        CHILD_SHARD_DISCOVERY_MIN_DELAY_MS
                                * (1L << splitContext.childShardDiscoveryAttempts),
                        CHILD_SHARD_DISCOVERY_MAX_DELAY_MS);
        return splitContext.lastChildShardDiscoveryAttemptTime + exponentialDelay;
    }

    private boolean hasNoRecords(GetRecordsResponse getRecordsResponse) {
        return !getRecordsResponse.hasRecords() || getRecordsResponse.records().isEmpty();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DynamoDbStreamsShardSplit> splitsChanges) {
        for (DynamoDbStreamsShardSplit split : splitsChanges.splits()) {
            assignedSplits.add(
                    new DynamoDbStreamsShardSplitWithContext(
                            new DynamoDbStreamsShardSplitState(split)));
        }
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<DynamoDbStreamsShardSplit> splitsToPause,
            Collection<DynamoDbStreamsShardSplit> splitsToResume) {
        splitsToPause.forEach(split -> pausedSplitIds.add(split.splitId()));
        for (DynamoDbStreamsShardSplit split : splitsToResume) {
            pausedSplitIds.remove(split.splitId());
            CompletedCall heldCall = heldCalls.remove(split.splitId());
            if (heldCall != null) {
                responseQueue.add(heldCall);
            }
        }
    }

    @Override
    public void wakeUp() {
        if (!blockingGetRecords) {
            responseQueue.add(WAKE_UP);
        }
    }

    /** Does not close the stream proxy: the split readers of a source reader share it. */
    @Override
    public void close() {}

    /** A completed non-blocking call. {@code error} is set if the call failed. */
    private static final class CompletedCall {
        private final DynamoDbStreamsShardSplitWithContext splitContext;
        private final long startTime;
        private final boolean childShardDiscovery;
        @Nullable private final GetRecordsResponse getRecordsResponse;
        @Nullable private final ListShardsResult listShardsResult;
        @Nullable private final Throwable error;

        private CompletedCall(
                DynamoDbStreamsShardSplitWithContext splitContext,
                long startTime,
                boolean childShardDiscovery,
                @Nullable GetRecordsResponse getRecordsResponse,
                @Nullable ListShardsResult listShardsResult,
                @Nullable Throwable error) {
            this.splitContext = splitContext;
            this.startTime = startTime;
            this.childShardDiscovery = childShardDiscovery;
            this.getRecordsResponse = getRecordsResponse;
            this.listShardsResult = listShardsResult;
            this.error = error;
        }
    }

    private static class DynamoDbStreamRecordsWithSplitIds implements RecordsWithSplitIds<Record> {

        private final Iterator<Record> recordsIterator;
        private final String splitId;
        private final boolean isComplete;

        public DynamoDbStreamRecordsWithSplitIds(
                Iterator<Record> recordsIterator, String splitId, boolean isComplete) {
            this.recordsIterator = recordsIterator;
            this.splitId = splitId;
            this.isComplete = isComplete;
        }

        @Nullable
        @Override
        public String nextSplit() {
            return recordsIterator.hasNext() ? splitId : null;
        }

        @Nullable
        @Override
        public Record nextRecordFromSplit() {
            return recordsIterator.hasNext() ? recordsIterator.next() : null;
        }

        @Override
        public Set<String> finishedSplits() {
            if (splitId == null) {
                return Collections.emptySet();
            }
            if (recordsIterator.hasNext()) {
                return Collections.emptySet();
            }
            return isComplete ? singleton(splitId) : Collections.emptySet();
        }
    }

    @Internal
    private static class DynamoDbStreamsShardSplitWithContext {
        final DynamoDbStreamsShardSplitState splitState;
        long lastPollTimeMillis;
        boolean wasLastPollEmpty;
        boolean hasAttemptedChildShardDiscovery;
        int childShardDiscoveryAttempts;
        long lastChildShardDiscoveryAttemptTime;

        /**
         * True from the time the reader starts a non-blocking call for this split until the reader
         * collects the response of the call.
         */
        boolean callInProgress;

        DynamoDbStreamsShardSplitWithContext(DynamoDbStreamsShardSplitState splitState) {
            this.splitState = splitState;
            this.lastPollTimeMillis = System.currentTimeMillis();
            this.wasLastPollEmpty = false;
            hasAttemptedChildShardDiscovery = false;
        }
    }
}
