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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Serializes and deserializes the {@link DynamoDbStreamsShardSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class DynamoDbStreamsShardSplitSerializer
        implements SimpleVersionedSerializer<DynamoDbStreamsShardSplit> {

    private static final Set<Integer> COMPATIBLE_VERSIONS = new HashSet<>(Arrays.asList(0, 1, 2));
    private static final int CURRENT_VERSION = 2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(DynamoDbStreamsShardSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeUTF(split.getStreamArn());
            out.writeUTF(split.getShardId());
            out.writeUTF(split.getStartingPosition().getShardIteratorType().toString());
            if (split.getStartingPosition().getStartingMarker() == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                Object startingMarker = split.getStartingPosition().getStartingMarker();
                out.writeBoolean(startingMarker instanceof String);
                if (startingMarker instanceof String) {
                    out.writeUTF((String) startingMarker);
                }
            }
            if (split.getParentShardId() == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(split.getParentShardId());
            }
            out.writeBoolean(split.isFinished());
            out.writeInt(split.getChildSplits().size());
            for (Shard childSplit : split.getChildSplits()) {
                out.writeUTF(childSplit.shardId());
                out.writeUTF(childSplit.parentShardId());
                out.writeUTF(childSplit.sequenceNumberRange().startingSequenceNumber());
                if (childSplit.sequenceNumberRange().endingSequenceNumber() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeUTF(childSplit.sequenceNumberRange().endingSequenceNumber());
                }
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DynamoDbStreamsShardSplit deserialize(int version, byte[] serialized)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            if (!COMPATIBLE_VERSIONS.contains(version)) {
                throw new VersionMismatchException(
                        "Trying to deserialize DynamoDbStreamsShardSplit serialized with unsupported version "
                                + version
                                + ". Version of serializer is "
                                + getVersion());
            }

            final String streamArn = in.readUTF();
            final String shardId = in.readUTF();
            final ShardIteratorType shardIteratorType = ShardIteratorType.fromValue(in.readUTF());
            Object startingMarker = null;

            final boolean hasStartingMarker = in.readBoolean();
            if (hasStartingMarker) {
                if (in.readBoolean()) {
                    startingMarker = in.readUTF();
                }
            }

            final boolean hasParentShardId = in.readBoolean();
            String parentShardId = null;
            if (hasParentShardId) {
                parentShardId = in.readUTF();
            }

            boolean isFinished = false;
            if (version > 0) {
                isFinished = in.readBoolean();
            }

            int childSplitSize = 0;
            List<Shard> childSplits = new ArrayList<>();
            if (version > 1) {
                childSplitSize = in.readInt();
                if (childSplitSize > 0) {
                    for (int i = 0; i < childSplitSize; i++) {
                        String splitId = in.readUTF();
                        String parentSplitId = in.readUTF();
                        String startingSequenceNumber = in.readUTF();
                        String endingSequenceNumber = null;
                        if (in.readBoolean()) {
                            endingSequenceNumber = in.readUTF();
                        }
                        childSplits.add(
                                Shard.builder()
                                        .shardId(splitId)
                                        .parentShardId(parentSplitId)
                                        .sequenceNumberRange(
                                                SequenceNumberRange.builder()
                                                        .startingSequenceNumber(
                                                                startingSequenceNumber)
                                                        .endingSequenceNumber(endingSequenceNumber)
                                                        .build())
                                        .build());
                    }
                }
            }

            return new DynamoDbStreamsShardSplit(
                    streamArn,
                    shardId,
                    new StartingPosition(shardIteratorType, startingMarker),
                    parentShardId,
                    isFinished,
                    childSplits);
        }
    }
}
