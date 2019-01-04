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
package org.apache.beam.runners.kafkastreams.translation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.kafkastreams.admin.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.runners.kafkastreams.state.KStateInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams translator for the Beam {@link GroupByKey} primitive. Uses {@link
 * KStream#through(String, Produced)} to groupByKeyOnly, then uses the GroupAlsoByWindow {@link
 * Transformer} to groupAlsoByWindow, utilizing a {@link StateStore} for holding aggregated data for
 * the key and a {@link Punctuator} for identifying when triggers are ready.
 */
public class GroupByKeyTransformTranslator<K, V, W extends BoundedWindow>
    implements TransformTranslator<GroupByKey<K, V>> {

  @Override
  public void translate(PipelineTranslator pipelineTranslator, GroupByKey<K, V> transform) {
    LoggerFactory.getLogger(getClass()).error("Translating GroupByKey {}", transform);
    PCollection<KV<K, V>> input = pipelineTranslator.getInput(transform);
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = coder.getKeyCoder();
    Coder<V> valueCoder = coder.getValueCoder();
    @SuppressWarnings("unchecked")
    WindowingStrategy<?, W> windowingStrategy =
        (WindowingStrategy<?, W>) input.getWindowingStrategy();
    TupleTag<KV<K, Iterable<V>>> outputTag = pipelineTranslator.getOutputTag(transform);

    KStream<Object, WindowedValue<KV<K, V>>> stream = pipelineTranslator.getStream(input);

    KStream<K, WindowedValue<V>> keyStream =
        stream.map(
            (key, value) ->
                KeyValue.pair(
                    value.getValue().getKey(),
                    WindowedValue.of(
                        value.getValue().getValue(),
                        value.getTimestamp(),
                        value.getWindows(),
                        value.getPane())));

    // TODO: Create through topic.
    KStream<K, WindowedValue<V>> groupByKeyOnlyStream =
        keyStream.through(
            Admin.topic(pipelineTranslator.getCurrentTransform()),
            Produced.with(
                CoderSerde.of(coder.getKeyCoder()),
                CoderSerde.of(
                    WindowedValue.FullWindowedValueCoder.of(
                        coder.getValueCoder(), windowingStrategy.getWindowFn().windowCoder()))));

    KStream<Object, WindowedValue<KeyedWorkItem<K, V>>> groupByKeyKeyedWorkItemStream =
        groupByKeyOnlyStream.flatMap(
            (key, windowedValues) -> {
              List<KeyValue<Object, WindowedValue<KeyedWorkItem<K, V>>>> keyedWorkItems =
                  new ArrayList<>();
              for (WindowedValue<V> windowedValue : windowedValues.explodeWindows()) {
                keyedWorkItems.add(
                    KeyValue.pair(
                        null,
                        windowedValue.withValue(
                            KeyedWorkItems.elementsWorkItem(
                                key, Collections.singletonList(windowedValues)))));
              }
              return keyedWorkItems;
            });

    // TODO: Create StateStore for use in the GroupAlsoByWindow.
    KStream<Object, WindowedValue<KV<K, Iterable<V>>>> groupAlsoByWindow =
        groupByKeyKeyedWorkItemStream.transform(
            () -> new GroupAlsoByWindow(keyCoder, valueCoder, outputTag, windowingStrategy));

    pipelineTranslator.putStream(pipelineTranslator.getOutput(transform), groupAlsoByWindow);
  }

  private class GroupAlsoByWindow
      implements Transformer<
          Object, WindowedValue<KeyedWorkItem<K, V>>,
          KeyValue<Object, WindowedValue<KV<K, Iterable<V>>>>> {

    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final TupleTag<KV<K, Iterable<V>>> mainOutputTag;
    private final WindowingStrategy<?, W> windowingStrategy;

    private ProcessorContext context;
    private InMemoryTimerInternals timerInternals;
    private DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> doFnRunner;
    private K key;

    private GroupAlsoByWindow(
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        TupleTag<KV<K, Iterable<V>>> mainOutputTag,
        WindowingStrategy<?, W> windowingStrategy) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.mainOutputTag = mainOutputTag;
      this.windowingStrategy = windowingStrategy;
    }

    @Override
    public void init(ProcessorContext context) {
      this.context = context;
      this.timerInternals = new InMemoryTimerInternals();

      DoFnRunners.OutputManager outputManager = new GABWOutputManger();
      SystemReduceFn<K, V, ?, Iterable<V>, W> reduceFn = SystemReduceFn.buffering(valueCoder);
      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> doFn =
          GroupAlsoByWindowViaWindowSetNewDoFn.create(
              windowingStrategy,
              key -> KStateInternals.of(key, context),
              key -> timerInternals,
              NullSideInputReader.empty(),
              reduceFn,
              outputManager,
              mainOutputTag);
      this.doFnRunner =
          DoFnRunners.simpleRunner(
              PipelineOptionsFactory.create(),
              doFn,
              NullSideInputReader.of(Collections.emptyList()),
              outputManager,
              mainOutputTag,
              Collections.emptyList(),
              new GABWStepContext(),
              KeyedWorkItemCoder.of(
                  keyCoder, valueCoder, windowingStrategy.getWindowFn().windowCoder()),
              Collections.emptyMap(),
              windowingStrategy);
    }

    @Override
    public KeyValue<Object, WindowedValue<KV<K, Iterable<V>>>> transform(
        Object object, WindowedValue<KeyedWorkItem<K, V>> keyedWorkItem) {
      key = keyedWorkItem.getValue().key();
      doFnRunner.startBundle();
      doFnRunner.processElement(keyedWorkItem);
      doFnRunner.finishBundle();
      return null;
    }

    @Override
    public KeyValue<Object, WindowedValue<KV<K, Iterable<V>>>> punctuate(long timestamp) {
      return null;
    }

    @Override
    public void close() {}

    private class GABWOutputManger implements DoFnRunners.OutputManager {

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        context.forward(null, output);
      }
    }

    private class GABWStepContext implements StepContext {

      @Override
      public StateInternals stateInternals() {
        return KStateInternals.of(key, context);
      }

      @Override
      public TimerInternals timerInternals() {
        return timerInternals;
      }
    }
  }
}
