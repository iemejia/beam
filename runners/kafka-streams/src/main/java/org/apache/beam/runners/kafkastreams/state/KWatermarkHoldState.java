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
package org.apache.beam.runners.kafkastreams.state;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.Instant;

/** Kafka Streams {@link WatermarkHoldState}. */
public class KWatermarkHoldState<K> extends KAbstractState<K, Instant>
    implements WatermarkHoldState {

  private final TimestampCombiner timestampCombiner;

  protected KWatermarkHoldState(
      K key,
      StateNamespace namespace,
      KeyValueStore<KV<K, String>, Instant> keyValueStore,
      TimestampCombiner timestampCombiner) {
    super(key, namespace, keyValueStore);
    this.timestampCombiner = timestampCombiner;
  }

  @Override
  public void add(Instant value) {
    Instant current = get();
    if (current == null) {
      set(value);
    } else {
      Instant combined = timestampCombiner.combine(current, value);
      set(combined);
    }
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public TimestampCombiner getTimestampCombiner() {
    return timestampCombiner;
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {

      @Override
      public Boolean read() {
        return get() == null;
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  @Override
  public Instant read() {
    return get();
  }

  @Override
  public WatermarkHoldState readLater() {
    return this;
  }
}
