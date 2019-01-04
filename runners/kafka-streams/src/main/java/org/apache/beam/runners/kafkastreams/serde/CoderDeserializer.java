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
package org.apache.beam.runners.kafkastreams.serde;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.kafka.common.serialization.Deserializer;

/** Kafka {@link Deserializer} that uses a Beam Coder to decode. */
public class CoderDeserializer<T> implements Deserializer<T> {

  public static <T> CoderDeserializer<T> of(Coder<T> coder) {
    return new CoderDeserializer<>(coder);
  }

  private final Coder<T> coder;

  private CoderDeserializer(Coder<T> coder) {
    this.coder = coder;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public T deserialize(String topic, byte[] data) {
    try {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
      return coder.decode(inputStream);
    } catch (IOException e) {
      throw new IllegalStateException("Error decoding bytes for coder: " + coder, e);
    }
  }
}
