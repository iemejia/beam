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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineContextFactory;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.Instant;

/**
 * Kafka Streams {@link StateInternals}, accesses state via the {@link ProcessorContext} of a {@link
 * Transformer}.
 */
public class KStateInternals<K> implements StateInternals {

  public static <K> KStateInternals<K> of(K key, ProcessorContext processorContext) {
    return new KStateInternals<K>(processorContext, key);
  }

  private final ProcessorContext processorContext;
  private final K key;

  private KStateInternals(ProcessorContext processorContext, K key) {
    this.processorContext = processorContext;
    this.key = key;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> stateContext) {
    return address.getSpec().bind(address.getId(), new KStateBinder(namespace, stateContext));
  }

  private class KStateBinder implements StateBinder {

    private StateNamespace namespace;
    private final StateContext<?> stateContext;

    protected KStateBinder(StateNamespace namespace, StateContext<?> stateContext) {
      this.namespace = namespace;
      this.stateContext = stateContext;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> ValueState<V> bindValue(String id, StateSpec<ValueState<V>> spec, Coder<V> coder) {
      return new KValueState<K, V>(
          key, namespace, (KeyValueStore<K, Map<String, V>>) processorContext.getStateStore(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> BagState<V> bindBag(String id, StateSpec<BagState<V>> spec, Coder<V> elemCoder) {
      return new KBagState<K, V>(
          key,
          namespace,
          (KeyValueStore<K, Map<String, List<V>>>) processorContext.getStateStore(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> SetState<V> bindSet(String id, StateSpec<SetState<V>> spec, Coder<V> elemCoder) {
      return new KSetState<K, V>(
          key,
          namespace,
          (KeyValueStore<K, Map<String, Set<V>>>) processorContext.getStateStore(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
        String id,
        StateSpec<MapState<KeyT, ValueT>> spec,
        Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      return new KMapState<K, KeyT, ValueT>(
          key,
          namespace,
          (KeyValueStore<K, Map<String, Map<KeyT, ValueT>>>) processorContext.getStateStore(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new KCombiningState<K, InputT, AccumT, OutputT>(
          key,
          namespace,
          (KeyValueStore<K, Map<String, AccumT>>) processorContext.getStateStore(id),
          combineFn);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <InputT, AccumT, OutputT>
        CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
            String id,
            StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
            Coder<AccumT> accumCoder,
            CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return new KCombiningWithContextState<K, InputT, AccumT, OutputT>(
          key,
          namespace,
          (KeyValueStore<K, Map<String, AccumT>>) processorContext.getStateStore(id),
          combineFn,
          CombineContextFactory.createFromStateContext(stateContext));
    }

    @SuppressWarnings("unchecked")
    @Override
    public WatermarkHoldState bindWatermark(
        String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
      return new KWatermarkHoldState<K>(
          key,
          namespace,
          (KeyValueStore<K, Map<String, Instant>>) processorContext.getStateStore(id),
          timestampCombiner);
    }
  }
}
