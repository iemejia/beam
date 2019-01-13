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
package org.apache.beam.runners.kafkastreams;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/** JUnit Test for the {@link KafkaStreamsRunner}. */
public class KafkaStreamsRunnerTest {

  @ClassRule public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

  private static final String APPLICATION_ID_PREFIX = "APPLICATION_ID_";
  private static final String GROUP_ID_PREFIX = "GROUP_ID_";
  private static final String TOPIC_ONE_PREFIX = "TOPIC_ONE_";
  private static final String TOPIC_TWO_PREFIX = "TOPIC_TWO_";
  private static final int PARTITIONS = 3;
  private static final int REPLICAS = 1;

  private static int testNumber = 0;

  private String topicOne;
  private String topicTwo;
  private Map<String, String> streamsConfiguration;
  private Properties producerConfiguration;
  private Properties consumerConfiguration;

  @Before
  public void setUp() throws InterruptedException {
    testNumber++;
    topicOne = TOPIC_ONE_PREFIX + testNumber;
    CLUSTER.createTopic(topicOne, PARTITIONS, REPLICAS);
    topicTwo = TOPIC_TWO_PREFIX + testNumber;
    CLUSTER.createTopic(topicTwo, PARTITIONS, REPLICAS);

    streamsConfiguration = new HashMap<>();
    streamsConfiguration.put(
        StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_PREFIX + testNumber);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, "true");

    producerConfiguration = new Properties();
    producerConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfiguration.put(
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfiguration.put(
        "value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

    consumerConfiguration = new Properties();
    consumerConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfiguration.put("group.id", GROUP_ID_PREFIX + testNumber);
    consumerConfiguration.put(
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfiguration.put(
        "value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
  }

  @Test
  public void testReadWrite() throws ExecutionException, InterruptedException, IOException {
    LoggerFactory.getLogger(KafkaStreamsRunnerTest.class)
        .error("testReadWrite topicOne: {}", topicOne);
    KafkaStreamsPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    pipelineOptions.setRunner(KafkaStreamsRunner.class);
    pipelineOptions.setNumPartitions(PARTITIONS);
    pipelineOptions.setProperties(streamsConfiguration);
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(
            topicOne,
            KafkaIO.<String, Integer>read()
                .withBootstrapServers(CLUSTER.bootstrapServers())
                .withTopic(topicOne)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(IntegerDeserializer.class)
                .updateConsumerProperties(
                    Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                .withoutMetadata())
        .apply(
            topicTwo,
            KafkaIO.<String, Integer>write()
                .withBootstrapServers(CLUSTER.bootstrapServers())
                .withTopic(topicTwo)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(IntegerSerializer.class));
    KafkaStreamsRunner.fromOptions(pipelineOptions).run(pipeline);

    IntegrationTestUtils.produceKeyValuesSynchronously(
        topicOne, Arrays.asList(KeyValue.pair("KEY", 1)), producerConfiguration, Time.SYSTEM);
    List<KeyValue<String, Integer>> keyValues =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            consumerConfiguration, topicTwo, 1);
    Assert.assertEquals(1, keyValues.size());
    Assert.assertEquals(KeyValue.pair("KEY", 1), keyValues.get(0));
  }

  @Test
  public void testGroupByKey() throws ExecutionException, InterruptedException, IOException {
    LoggerFactory.getLogger(KafkaStreamsRunnerTest.class)
        .error("testGroupByKey topicOne: {}", topicOne);
    KafkaStreamsPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    pipelineOptions.setRunner(KafkaStreamsRunner.class);
    pipelineOptions.setNumPartitions(PARTITIONS);
    pipelineOptions.setProperties(streamsConfiguration);
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(
            topicOne,
            KafkaIO.<String, Integer>read()
                .withBootstrapServers(CLUSTER.bootstrapServers())
                .withTopic(topicOne)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(IntegerDeserializer.class)
                .updateConsumerProperties(
                    Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                .withoutMetadata())
        .apply(
            Window.<KV<String, Integer>>configure()
                .triggering(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(100)))
                .discardingFiredPanes())
        .apply(Sum.integersPerKey())
        .apply(
            topicTwo,
            KafkaIO.<String, Integer>write()
                .withBootstrapServers(CLUSTER.bootstrapServers())
                .withTopic(topicTwo)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(IntegerSerializer.class));
    KafkaStreamsRunner.fromOptions(pipelineOptions).run(pipeline);

    IntegrationTestUtils.produceKeyValuesSynchronously(
        topicOne,
        Arrays.asList(
            KeyValue.pair("KEY_ONE", 1),
            KeyValue.pair("KEY_TWO", 2),
            KeyValue.pair("KEY_ONE", 2),
            KeyValue.pair("KEY_TWO", 3)),
        producerConfiguration,
        Time.SYSTEM);
    List<KeyValue<String, Integer>> keyValues =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            consumerConfiguration, topicTwo, 2);
    Collections.sort(
        keyValues, (keyValueOne, keyValueTwo) -> keyValueOne.key.compareTo(keyValueTwo.key));
    Assert.assertEquals(2, keyValues.size());
    Assert.assertEquals(KeyValue.pair("KEY_ONE", 3), keyValues.get(0));
    Assert.assertEquals(KeyValue.pair("KEY_TWO", 5), keyValues.get(1));
  }
}
