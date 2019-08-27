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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * Kafka Streams {@link SideInputReader} that will read from a {@link KeyValueStore}. The {@link
 * KeyValueStore} will be populated by writing the {@link PCollection} of a {@link PCollectionView}
 * to a topic, then reading that topic into a {@link GlobalKTable} that is {@link Materialized} with
 * the {@link KeyValueStore}. The key will be the {@link StateNamespace#stringKey()} representation
 * of the window.
 */
public class KSideInputReader implements SideInputReader {

  public static KSideInputReader of(
      ProcessorContext processorContext, Map<PCollectionView<?>, String> sideInputs) {
    return new KSideInputReader(processorContext, sideInputs);
  }

  private final ProcessorContext processorContext;
  private final Map<PCollectionView<?>, String> sideInputs;

  private KSideInputReader(
      ProcessorContext processorContext, Map<PCollectionView<?>, String> sideInputs) {
    this.processorContext = processorContext;
    this.sideInputs = sideInputs;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    KeyValueStore<String, T> keyValueStore =
        (KeyValueStore<String, T>) processorContext.getStateStore(sideInputs.get(view));
    return keyValueStore.get(
        StateNamespaces.window(
                (Coder<BoundedWindow>)
                    view.getPCollection().getWindowingStrategy().getWindowFn().windowCoder(),
                view.getWindowMappingFn().getSideInputWindow(window))
            .stringKey());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  @SuppressWarnings("unchecked")
  public static <InputT, ViewT, W extends BoundedWindow> String sideInput(
      KafkaStreamsPipelineOptions pipelineOptions,
      StreamsBuilder streamsBuilder,
      PCollectionView<ViewT> collectionView,
      KStream<Void, WindowedValue<InputT>> stream) {
    PCollection<InputT> collection = (PCollection<InputT>) collectionView.getPCollection();
    Coder<W> windowCoder = (Coder<W>) collection.getWindowingStrategy().getWindowFn().windowCoder();

    KStream<String, InputT> windowStream =
        stream.flatMap(
            (object, windowedValue) -> {
              return ((Collection<W>) windowedValue.getWindows())
                  .stream()
                      .map(
                          window ->
                              KeyValue.pair(
                                  StateNamespaces.window(windowCoder, window).stringKey(),
                                  windowedValue.getValue()))
                      .collect(Collectors.toList());
            });

    String applicationId = Admin.applicationId(pipelineOptions);
    String uniqueName = Admin.uniqueName(pipelineOptions, collectionView);
    String storeName = uniqueName + "-store";
    String topic = applicationId + "-" + uniqueName;
    Serde<InputT> serde = CoderSerde.of(collection.getCoder());
    windowStream.to(topic, Produced.with(Serdes.String(), serde));
    streamsBuilder.addGlobalStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName), Serdes.String(), serde),
        topic,
        Consumed.with(Serdes.String(), serde),
        () ->
            new Processor<String, InputT>() {

              @Override
              public void init(ProcessorContext processorContext) {
                // TODO: Get stateStore for storeName.
              }

              @Override
              public void process(String key, InputT value) {
                // TODO: Use the PCollectionView ViewFunction to update the stateStore.
              }

              @Override
              public void close() {}
            });
    return storeName;
  }
}
