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
package org.apache.beam.runners.kafkastreams.translation.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class KWindowedVToWindowedKeyedWorkItem<K, V>
    implements KeyValueMapper<
        K, WindowedValue<V>, Iterable<KeyValue<Void, WindowedValue<KeyedWorkItem<K, V>>>>> {

  @Override
  public Iterable<KeyValue<Void, WindowedValue<KeyedWorkItem<K, V>>>> apply(
      K key, WindowedValue<V> windowedValues) {
    List<KeyValue<Void, WindowedValue<KeyedWorkItem<K, V>>>> keyedWorkItems = new ArrayList<>();
    for (WindowedValue<V> windowedValue : windowedValues.explodeWindows()) {
      keyedWorkItems.add(
          KeyValue.pair(
              null,
              windowedValue.withValue(
                  KeyedWorkItems.elementsWorkItem(
                      key, Collections.singletonList(windowedValues)))));
    }
    return keyedWorkItems;
  }
}
