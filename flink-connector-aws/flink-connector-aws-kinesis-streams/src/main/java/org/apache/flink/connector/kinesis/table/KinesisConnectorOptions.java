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

package org.apache.flink.connector.kinesis.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Options for the Kinesis connector. */
@PublicEvolving
public class KinesisConnectorOptions extends AsyncSinkConnectorOptions {

    // -----------------------------------------------------------------------------------------
    // Kinesis specific options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<String> STREAM =
            ConfigOptions.key("stream")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the Kinesis stream backing this table.");

    public static final ConfigOption<String> STREAM_ARN =
            ConfigOptions.key("stream.arn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ARN of the Kinesis stream backing this table.");

    public static final ConfigOption<String> AWS_REGION =
            ConfigOptions.key("aws.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region of used Kinesis stream.");

    // -----------------------------------------------------------------------------------------
    // Source options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<String> SHARD_ASSIGNER =
            ConfigOptions.key("source.shard-assigner")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Shard assigner to use when mapping shards to Flink subtasks.");

    // -----------------------------------------------------------------------------------------
    // Sink options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_PARTITIONER =
            ConfigOptions.key("sink.partitioner")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Optional output partitioning from Flink's partitions into Kinesis shards. "
                                                    + "Sinks that write to tables defined with the %s clause always use a "
                                                    + "field-based partitioner and cannot define this option.",
                                            code("PARTITION BY"))
                                    .linebreak()
                                    .text("Valid enumerations are")
                                    .list(
                                            text("random (use a random partition key)"),
                                            text(
                                                    "fixed (each Flink partition ends up in at most one Kinesis shard)"),
                                            text(
                                                    "custom class name (use a custom %s subclass)",
                                                    text(PartitionKeyGenerator.class.getName())))
                                    .build());

    public static final ConfigOption<String> SINK_PARTITIONER_FIELD_DELIMITER =
            ConfigOptions.key("sink.partitioner-field-delimiter")
                    .stringType()
                    .defaultValue("|")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Optional field delimiter for fields-based partitioner derived from a %s clause",
                                            code("PARTITION BY"))
                                    .build());

    public static final ConfigOption<Boolean> SINK_FAIL_ON_ERROR =
            ConfigOptions.key("sink.fail-on-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Determines whether an exception should fail the job, otherwise failed requests are retried.");
}
