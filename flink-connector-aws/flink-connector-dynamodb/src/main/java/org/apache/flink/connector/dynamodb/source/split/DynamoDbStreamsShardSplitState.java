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

/**
 * Stores the metadata around a given {@link DynamoDbStreamsShardSplit}. This class is stored in
 * state, and any changes should be backwards compatible.
 */
@Internal
public class DynamoDbStreamsShardSplitState {
    private final DynamoDbStreamsShardSplit dynamoDbStreamsShardSplit;
    private StartingPosition nextStartingPosition;
    private String nextShardIterator;
    private boolean hasShardEndReached;

    public DynamoDbStreamsShardSplitState(DynamoDbStreamsShardSplit dynamoDbStreamsShardSplit) {
        this.dynamoDbStreamsShardSplit = dynamoDbStreamsShardSplit;
        this.nextStartingPosition = dynamoDbStreamsShardSplit.getStartingPosition();
        this.hasShardEndReached = false;
    }

    public DynamoDbStreamsShardSplit getDynamoDbStreamsShardSplit() {
        return new DynamoDbStreamsShardSplit(
                dynamoDbStreamsShardSplit.getStreamArn(),
                dynamoDbStreamsShardSplit.getShardId(),
                nextStartingPosition,
                dynamoDbStreamsShardSplit.getParentShardId());
    }

    public String getSplitId() {
        return dynamoDbStreamsShardSplit.splitId();
    }

    public String getStreamArn() {
        return dynamoDbStreamsShardSplit.getStreamArn();
    }

    public String getShardId() {
        return dynamoDbStreamsShardSplit.getShardId();
    }

    public StartingPosition getNextStartingPosition() {
        return nextStartingPosition;
    }

    public void setNextStartingPosition(StartingPosition nextStartingPosition) {
        this.nextStartingPosition = nextStartingPosition;
    }

    public String getNextShardIterator() {
        return nextShardIterator;
    }

    public void setNextShardIterator(String nextShardIterator) {
        this.nextShardIterator = nextShardIterator;
    }

    public boolean isHasShardEndReached() {
        return hasShardEndReached;
    }

    public void setHasShardEndReached(boolean hasShardEndReached) {
        this.hasShardEndReached = hasShardEndReached;
    }
}
