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

package org.apache.flink.connector.dynamodb.source.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

import java.net.URI;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamoDbStreamsAsyncClientUtil}. */
class DynamoDbStreamsAsyncClientUtilTest {

    @Test
    void testCreateAsyncClientUsesRegionAndEndpointOverride() {
        Properties clientProperties = new Properties();
        clientProperties.setProperty(AWSConfigConstants.AWS_REGION, "eu-west-1");
        clientProperties.setProperty(AWSConfigConstants.AWS_ENDPOINT, "https://localhost:4566");

        try (SdkAsyncHttpClient asyncHttpClient =
                        DynamoDbStreamsAsyncClientUtil.createAsyncHttpClient();
                DynamoDbStreamsAsyncClient asyncClient =
                        DynamoDbStreamsAsyncClientUtil.createAsyncClient(
                                clientProperties,
                                asyncHttpClient,
                                SdkDefaultRetryStrategy.defaultRetryStrategy())) {
            assertThat(asyncClient.serviceClientConfiguration().region())
                    .isEqualTo(Region.EU_WEST_1);
            assertThat(asyncClient.serviceClientConfiguration().endpointOverride())
                    .contains(URI.create("https://localhost:4566"));
        }
    }

    @Test
    void testCreateAsyncClientWithoutEndpointOverride() {
        Properties clientProperties = new Properties();
        clientProperties.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");

        try (SdkAsyncHttpClient asyncHttpClient =
                        DynamoDbStreamsAsyncClientUtil.createAsyncHttpClient();
                DynamoDbStreamsAsyncClient asyncClient =
                        DynamoDbStreamsAsyncClientUtil.createAsyncClient(
                                clientProperties,
                                asyncHttpClient,
                                SdkDefaultRetryStrategy.defaultRetryStrategy())) {
            assertThat(asyncClient.serviceClientConfiguration().endpointOverride()).isEmpty();
        }
    }
}
