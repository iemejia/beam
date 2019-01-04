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
package org.apache.beam.runners.kafkastreams.admin;

import java.util.Map;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;

/** Administrative tools for working with a Kafka cluster. */
public class Admin {

  @SuppressWarnings("unchecked")
  public static AdminClient adminClient(KafkaStreamsPipelineOptions pipelineOptions) {
    return AdminClient.create(
        (Map<String, Object>) (Map<String, ?>) pipelineOptions.getProperties());
  }

  @SuppressWarnings("unchecked")
  public static <K, V> Producer<K, V> producer(
      KafkaStreamsPipelineOptions pipelineOptions, Serde<K> keySerde, Serde<V> valueSerde) {
    return new KafkaProducer<K, V>(
        (Map<String, Object>) (Map<String, ?>) pipelineOptions.getProperties(),
        keySerde.serializer(),
        valueSerde.serializer());
  }

  public static String storeName(PCollectionView<?> collectionView) {
    return collectionView.getName().replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
  }

  public static String topic(PCollectionView<?> collectionView) {
    return collectionView.getName().replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
  }

  public static String topic(AppliedPTransform<?, ?, ?> appliedTransform) {
    return appliedTransform.getFullName().replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
  }
}
