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

package org.apache.flink.connector.dynamodb.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumerator;

import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Contains information about the dynamodb stream and shard. Serves as a communication mechanism
 * between the {@link DynamoDbStreamsSourceEnumerator} and the {@link SplitReader}. Information
 * provided here should be immutable. This class is stored in state, so any changes need to be
 * backwards compatible.
 */
@Internal
public final class DynamoDbStreamsShardSplit implements SourceSplit {

    private final String streamArn;
    private final String shardId;
    private final StartingPosition startingPosition;
    private final String parentShardId;
    private final boolean isFinished;
    private final List<Shard> childSplits;

    public DynamoDbStreamsShardSplit(
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            String parentShardId) {
        this(streamArn, shardId, startingPosition, parentShardId, false);
    }

    public DynamoDbStreamsShardSplit(
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            String parentShardId,
            List<Shard> childSplits) {
        this(streamArn, shardId, startingPosition, parentShardId, false, childSplits);
    }

    public DynamoDbStreamsShardSplit(
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            String parentShardId,
            boolean isFinished) {
        this(streamArn, shardId, startingPosition, parentShardId, isFinished, new ArrayList<>());
    }

    public DynamoDbStreamsShardSplit(
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            String parentShardId,
            boolean isFinished,
            List<Shard> childSplits) {
        checkNotNull(streamArn, "streamArn cannot be null");
        checkNotNull(shardId, "shardId cannot be null");
        checkNotNull(startingPosition, "startingPosition cannot be null");

        this.streamArn = streamArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;
        this.parentShardId = parentShardId;
        this.isFinished = isFinished;
        this.childSplits = childSplits;
    }

    @Override
    public String splitId() {
        return shardId;
    }

    public String getStreamArn() {
        return streamArn;
    }

    public String getShardId() {
        return shardId;
    }

    public StartingPosition getStartingPosition() {
        return startingPosition;
    }

    public String getParentShardId() {
        return parentShardId;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public List<Shard> getChildSplits() {
        return childSplits;
    }

    @Override
    public String toString() {
        return "DynamoDbStreamsShardSplit{"
                + "streamArn='"
                + streamArn
                + '\''
                + ", shardId='"
                + shardId
                + '\''
                + ", startingPosition="
                + startingPosition
                + ", parentShardId=["
                + parentShardId
                + "]"
                + ", isFinished="
                + isFinished
                + ", childSplitIds=["
                + childSplits.stream().map(Shard::toString).collect(Collectors.joining(","))
                + "]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbStreamsShardSplit that = (DynamoDbStreamsShardSplit) o;
        return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(shardId, that.shardId)
                && Objects.equals(startingPosition, that.startingPosition)
                && Objects.equals(parentShardId, that.parentShardId)
                && Objects.equals(isFinished, that.isFinished)
                && Objects.equals(childSplits, that.childSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                streamArn, shardId, startingPosition, parentShardId, isFinished, childSplits);
    }
}
