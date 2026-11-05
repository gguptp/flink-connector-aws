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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClientBuilder;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.util.Properties;

/**
 * Creates the {@link DynamoDbStreamsAsyncClient} that the source readers use for non-blocking
 * GetRecords calls. The client has the same settings as the {@code DynamoDbStreamsClient} that
 * {@link AWSClientUtil#createAwsSyncClient} creates for blocking calls.
 */
@Internal
public final class DynamoDbStreamsAsyncClientUtil {

    /** Same suffix that {@link AWSClientUtil} adds to the default user agent prefix. */
    private static final String V2_USER_AGENT_SUFFIX = " V2";

    private DynamoDbStreamsAsyncClientUtil() {}

    /**
     * Creates a Netty HTTP client with the same settings as the HTTP client for blocking calls.
     *
     * <p>{@link AWSGeneralUtil#createAsyncHttpClient} uses HTTP/2 by default. DynamoDB Streams
     * endpoints use only HTTP/1.1, and an HTTP/2 client fails with "First received frame was not
     * SETTINGS". A protocol set on the builder has precedence over the defaults of the helper, so
     * the client always uses HTTP/1.1.
     */
    public static SdkAsyncHttpClient createAsyncHttpClient() {
        return AWSGeneralUtil.createAsyncHttpClient(
                AttributeMap.empty(), NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1));
    }

    /**
     * Creates the client. The client gets the region, credentials, endpoint override, and user
     * agent prefix from {@code clientProperties}, as {@link AWSClientUtil#createAwsSyncClient}
     * does. This method does not use {@link AWSClientUtil#createAwsAsyncClient}, because that
     * method replaces the client override configuration and drops the retry strategy.
     */
    public static DynamoDbStreamsAsyncClient createAsyncClient(
            Properties clientProperties,
            SdkAsyncHttpClient asyncHttpClient,
            RetryStrategy retryStrategy) {
        String userAgentPrefix =
                clientProperties.getProperty(
                        DynamodbStreamsSourceConfigConstants.DDB_STREAMS_CLIENT_USER_AGENT_PREFIX,
                        AWSClientUtil.formatFlinkUserAgentPrefix(
                                DynamodbStreamsSourceConfigConstants
                                                .BASE_DDB_STREAMS_USER_AGENT_PREFIX_FORMAT
                                        + V2_USER_AGENT_SUFFIX));

        DynamoDbStreamsAsyncClientBuilder clientBuilder =
                DynamoDbStreamsAsyncClient.builder()
                        .httpClient(asyncHttpClient)
                        .overrideConfiguration(
                                ClientOverrideConfiguration.builder()
                                        .retryStrategy(retryStrategy)
                                        .putAdvancedOption(
                                                SdkAdvancedClientOption.USER_AGENT_PREFIX,
                                                userAgentPrefix)
                                        .build())
                        .credentialsProvider(
                                AWSGeneralUtil.getCredentialsProvider(clientProperties))
                        .region(AWSGeneralUtil.getRegion(clientProperties));

        if (clientProperties.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
            clientBuilder.endpointOverride(
                    URI.create(clientProperties.getProperty(AWSConfigConstants.AWS_ENDPOINT)));
        }
        return clientBuilder.build();
    }
}
