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

import java.util.Arrays;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.state.KeyValueStore;

/** Kafka Streams {@link CombiningState} with {@link CombineWithContext.Context Context}. */
public class KCombiningWithContextState<K, InputT, AccumT, OutputT>
    extends KAbstractState<K, AccumT> implements CombiningState<InputT, AccumT, OutputT> {

  private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;
  private final CombineWithContext.Context context;

  protected KCombiningWithContextState(
      K key,
      StateNamespace namespace,
      KeyValueStore<KV<K, String>, AccumT> keyValueStore,
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
      CombineWithContext.Context context) {
    super(key, namespace, keyValueStore);
    this.combineFn = combineFn;
    this.context = context;
  }

  @Override
  public void add(InputT value) {
    AccumT current = get();
    if (current == null) {
      current = combineFn.createAccumulator(context);
    }
    current = combineFn.addInput(current, value, context);
    set(current);
  }

  @Override
  public void addAccum(AccumT accum) {
    AccumT current = get();
    if (current == null) {
      set(accum);
    } else {
      current = combineFn.mergeAccumulators(Arrays.asList(current, accum), context);
      set(current);
    }
  }

  @Override
  public void clear() {
    super.clear();
  }

  @Override
  public AccumT getAccum() {
    AccumT accum = get();
    if (accum == null) {
      return combineFn.createAccumulator(context);
    } else {
      return accum;
    }
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
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    return combineFn.mergeAccumulators(accumulators, context);
  }

  @Override
  public OutputT read() {
    AccumT accum = get();
    if (accum != null) {
      return combineFn.extractOutput(accum, context);
    } else {
      return combineFn.extractOutput(combineFn.createAccumulator(context), context);
    }
  }

  @Override
  public CombiningState<InputT, AccumT, OutputT> readLater() {
    return this;
  }
}
