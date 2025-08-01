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

package org.apache.flink.connector.dynamodb.source.enumerator.event;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Set;
import java.util.stream.Collectors;

/** Source event used by source reader to communicate that splits are finished to enumerator. */
@Internal
public class SplitsFinishedEvent implements SourceEvent {
    private static final long serialVersionUID = 1;
    private final Set<SplitsFinishedEventContext> finishedSplits;

    public SplitsFinishedEvent(Set<SplitsFinishedEventContext> finishedSplits) {
        this.finishedSplits = finishedSplits;
    }

    public Set<SplitsFinishedEventContext> getFinishedSplits() {
        return finishedSplits;
    }

    @Override
    public String toString() {
        return "SplitsFinishedEvent{"
                + "finishedSplit=["
                + finishedSplits.stream().map(Object::toString).collect(Collectors.joining(","))
                + "]}";
    }
}
