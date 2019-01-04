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
package org.apache.beam.runners.kafkastreams.sideinput;

import java.util.Map;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams {@link SideInputReader} that will read from a {@link KeyValueStore}. The {@link
 * KeyValueStore} will be populated by writing the {@link PCollection} of a {@link PCollectionView}
 * to a topic, then reading that topic into a {@link GlobalKTable} that is {@link Materialized} with
 * the {@link KeyValueStore}. The key will be the {@link StateNamespace#stringKey()} representation
 * of the window.
 */
public class KSideInputReader implements SideInputReader {

  public static KSideInputReader of(
      ProcessorContext processorContext, Map<PCollectionView<?>, KSideInput> sideInputs) {
    return new KSideInputReader(processorContext, sideInputs);
  }

  private final ProcessorContext processorContext;
  private final Map<PCollectionView<?>, KSideInput> sideInputs;

  private KSideInputReader(
      ProcessorContext processorContext, Map<PCollectionView<?>, KSideInput> sideInputs) {
    this.processorContext = processorContext;
    this.sideInputs = sideInputs;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view);
  }

  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    KSideInput sideInput = sideInputs.get(view);
    @SuppressWarnings("unchecked")
    KeyValueStore<String, T> keyValueStore =
        (KeyValueStore<String, T>) processorContext.getStateStore(sideInput.storeName);
    return keyValueStore.get(StateNamespaces.window(sideInput.coder, window).stringKey());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  /** Holds information for looking up values in the {@link KeyValueStore} backed side input. */
  public static class KSideInput {

    public static KSideInput of(String storeName, Coder<BoundedWindow> coder) {
      return new KSideInput(storeName, coder);
    }

    private String storeName;
    private Coder<BoundedWindow> coder;

    private KSideInput(String storeName, Coder<BoundedWindow> coder) {
      this.storeName = storeName;
      this.coder = coder;
    }
  }
}
