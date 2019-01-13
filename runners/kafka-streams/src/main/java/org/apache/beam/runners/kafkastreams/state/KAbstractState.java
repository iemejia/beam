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
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams abstract {@link State}, that queries an underlying {@link KeyValueStore} and
 * handles multiple possible {@link StateNamespaces}.
 */
public abstract class KAbstractState<K, V> {

  private final K key;
  private final String namespace;
  private final KeyValueStore<KV<K, String>, V> keyValueStore;

  protected KAbstractState(
      K key, StateNamespace namespace, KeyValueStore<KV<K, String>, V> keyValueStore) {
    this.key = key;
    this.namespace = namespace.stringKey();
    this.keyValueStore = keyValueStore;
  }

  protected V get() {
    return keyValueStore.get(KV.of(key, namespace));
  }

  protected void set(V value) {
    keyValueStore.put(KV.of(key, namespace), value);
  }

  protected void clear() {
    keyValueStore.delete(KV.of(key, namespace));
  }
}
