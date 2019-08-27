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
package org.apache.beam.runners.kafkastreams.client;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;

/** Administrative tools for working with a Kafka cluster. */
public class Admin {

  public static String applicationId(KafkaStreamsPipelineOptions pipelineOptions) {
    String applicationId = pipelineOptions.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG);
    if (applicationId == null) {
      throw new RuntimeException(
          "Missing required configuration 'application.id' which has no default value.");
    }
    return applicationId;
  }

  public static boolean createTopicIfNeeded(
      KafkaStreamsPipelineOptions pipelineOptions, String topic) {
    try (AdminClient adminClient = AdminClient.create(properties(pipelineOptions))) {
      if (adminClient.listTopics().names().get().contains(topic)) {
        return false;
      }
      adminClient
          .createTopics(
              Collections.singleton(
                  new NewTopic(
                      topic,
                      pipelineOptions.getNumPartitions(),
                      Short.valueOf(
                          pipelineOptions
                              .getProperties()
                              .getOrDefault(
                                  StreamsConfig.REPLICATION_FACTOR_CONFIG,
                                  StreamsConfig.configDef()
                                      .defaultValues()
                                      .get(StreamsConfig.REPLICATION_FACTOR_CONFIG)
                                      .toString())))))
          .all()
          .get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public static <K, V> Producer<K, V> producer(
      KafkaStreamsPipelineOptions pipelineOptions, Serde<K> keySerde, Serde<V> valueSerde) {
    return new KafkaProducer<K, V>(
        properties(pipelineOptions), keySerde.serializer(), valueSerde.serializer());
  }

  public static String topic(
      KafkaStreamsPipelineOptions pipelineOptions, AppliedPTransform<?, ?, ?> appliedTransform) {
    return applicationId(pipelineOptions) + "-" + uniqueName(pipelineOptions, appliedTransform);
  }

  public static String uniqueName(
      KafkaStreamsPipelineOptions pipelineOptions, AppliedPTransform<?, ?, ?> appliedTransform) {
    return appliedTransform.getFullName().replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
  }

  public static String uniqueName(
      KafkaStreamsPipelineOptions pipelineOptions, PCollectionView<?> view) {
    return view.getPCollection().getName().replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
  }

  private static Properties properties(KafkaStreamsPipelineOptions pipelineOptions) {
    Properties properties = new Properties();
    properties.putAll(pipelineOptions.getProperties());
    return properties;
  }
}
