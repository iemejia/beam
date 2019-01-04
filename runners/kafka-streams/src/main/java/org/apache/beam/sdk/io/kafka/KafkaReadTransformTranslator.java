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
package org.apache.beam.sdk.io.kafka;

import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaReadTransformTranslator {

  public static boolean isKafkaTopicUnboundedSource(UnboundedSource<?, ?> unboundedSource) {
    try {
      Class.forName("org.apache.beam.sdk.io.kafka.KafkaUnboundedSource");
    } catch (ClassNotFoundException e) {
      return false;
    }
    if (unboundedSource instanceof KafkaUnboundedSource) {
      KafkaIO.Read<?, ?> spec = ((KafkaUnboundedSource<?, ?>) unboundedSource).getSpec();
      return spec.getTopicPartitions().isEmpty();
    } else {
      return false;
    }
  }

  public static <K, V> KStream<K, V> kafkaTopicRead(
      StreamsBuilder streamsBuilder, KafkaUnboundedSource<K, V> kafkaUnboundedSource) {
    KafkaIO.Read<K, V> spec = kafkaUnboundedSource.getSpec();
    List<String> topics = spec.getTopics();
    Serde<K> keySerde = serdeFrom(spec.getKeyDeserializer());
    Serde<V> valueSerde = serdeFrom(spec.getValueDeserializer());

    return streamsBuilder.stream(topics, Consumed.with(keySerde, valueSerde));
  }

  private static <T> Serde<T> serdeFrom(Class<? extends Deserializer<T>> clazz) {
    try {
      Deserializer<T> deserializer = clazz.getConstructor().newInstance();
      // TODO: Possibly call configure on the deserializer.
      return Serdes.serdeFrom(null, deserializer);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
