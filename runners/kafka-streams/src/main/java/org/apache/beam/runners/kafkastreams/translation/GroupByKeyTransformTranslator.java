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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.runners.kafkastreams.state.KStateInternals;
import org.apache.beam.runners.kafkastreams.state.KTimerInternals;
import org.apache.beam.runners.kafkastreams.translation.mapper.KWindowedVToWindowedKeyedWorkItem;
import org.apache.beam.runners.kafkastreams.translation.mapper.WindowedKVToKWindowedV;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.Instant;

/**
 * Kafka Streams translator for the Beam {@link GroupByKey} primitive. Uses {@link
 * KStream#through(String, Produced)} to groupByKeyOnly, then uses the {@link GroupAlsoByWindow
 * GroupAlsoByWindow Transformer} to groupAlsoByWindow, utilizing {@link StateStore StateStores} for
 * holding aggregated data for the key and a {@link Punctuator} for identifying when triggers are
 * ready.
 */
public class GroupByKeyTransformTranslator<K, V, W extends BoundedWindow>
    implements TransformTranslator<GroupByKey<K, V>> {

  @Override
  @SuppressWarnings("unchecked")
  public void translate(PipelineTranslator pipelineTranslator, GroupByKey<K, V> transform) {
    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    String applicationId = Admin.applicationId(pipelineOptions);
    AppliedPTransform<?, ?, ?> appliedPTransform = pipelineTranslator.getCurrentTransform();
    String uniqueName = Admin.uniqueName(pipelineOptions, appliedPTransform);
    PCollection<KV<K, V>> input = pipelineTranslator.getInput(transform);
    KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = coder.getKeyCoder();
    Coder<V> valueCoder = coder.getValueCoder();
    WindowingStrategy<?, W> windowingStrategy =
        (WindowingStrategy<?, W>) input.getWindowingStrategy();
    TupleTag<KV<K, Iterable<V>>> outputTag = pipelineTranslator.getOutputTag(transform);
    //    DoFnSchemaInformation doFnSchemaInformation =
    //        ParDoTranslation.getSchemaInformation(appliedPTransform);
    Set<String> streamSources = pipelineTranslator.getStreamSources(input);

    KStream<Void, WindowedValue<KV<K, V>>> stream = pipelineTranslator.getStream(input);

    KStream<K, WindowedValue<V>> keyStream = stream.map(new WindowedKVToKWindowedV<>());

    String topic = applicationId + "-" + uniqueName;
    Admin.createTopicIfNeeded(pipelineOptions, topic);
    KStream<K, WindowedValue<V>> groupByKeyOnlyStream =
        keyStream.through(
            topic,
            Produced.with(
                CoderSerde.of(keyCoder),
                CoderSerde.of(
                    WindowedValue.FullWindowedValueCoder.of(
                        valueCoder, windowingStrategy.getWindowFn().windowCoder()))));

    KStream<Void, WindowedValue<KeyedWorkItem<K, V>>> groupByKeyKeyedWorkItemStream =
        groupByKeyOnlyStream.flatMap(new KWindowedVToWindowedKeyedWorkItem<>());

    KStream<Void, WindowedValue<KV<K, Iterable<V>>>> groupAlsoByWindow =
        groupByKeyKeyedWorkItemStream.transform(
            () ->
                new GroupAlsoByWindow(
                    uniqueName,
                    keyCoder,
                    valueCoder,
                    outputTag,
                    windowingStrategy,
                    DoFnSchemaInformation.create(),
                    streamSources),
            stateStores(pipelineTranslator, uniqueName, keyCoder));

    PCollection<KV<K, Iterable<V>>> output = pipelineTranslator.getOutput(transform);
    pipelineTranslator.putStream(output, groupAlsoByWindow);
    pipelineTranslator.putStreamSources(output, Collections.singleton(topic));
  }

  private String[] stateStores(
      PipelineTranslator pipelineTranslator, String unique, Coder<K> keyCoder) {

    String state = unique + KStateInternals.STATE;
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(state),
                    CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                    Serdes.ByteArray())
                .withLoggingDisabled());

    String timer = unique + KTimerInternals.TIMER;
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(timer),
                    CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                    CoderSerde.of(InstantCoder.of()))
                .withLoggingDisabled());

    return new String[] {state, timer};
  }

  private class GroupAlsoByWindow
      implements Transformer<
          Void,
          WindowedValue<KeyedWorkItem<K, V>>,
          KeyValue<Void, WindowedValue<KV<K, Iterable<V>>>>> {

    private final String unique;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final TupleTag<KV<K, Iterable<V>>> mainOutputTag;
    private final WindowingStrategy<?, W> windowingStrategy;
    private final DoFnSchemaInformation doFnSchemaInformation;
    private final Map<String, Instant> streamSourceWatermarks;

    private ProcessorContext processorContext;
    private KStateInternals<K> stateInternals;
    private KTimerInternals<K, W> timerInternals;
    private DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> doFnRunner;
    private K key;

    private GroupAlsoByWindow(
        String unique,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        TupleTag<KV<K, Iterable<V>>> mainOutputTag,
        WindowingStrategy<?, W> windowingStrategy,
        DoFnSchemaInformation doFnSchemaInformation,
        Set<String> streamSources) {
      this.unique = unique;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.mainOutputTag = mainOutputTag;
      this.windowingStrategy = windowingStrategy;
      this.doFnSchemaInformation = doFnSchemaInformation;
      this.streamSourceWatermarks =
          streamSources.stream()
              .collect(
                  Collectors.toMap(string -> string, string -> BoundedWindow.TIMESTAMP_MIN_VALUE));
    }

    @Override
    public void init(ProcessorContext processorContext) {
      this.processorContext = processorContext;
      // TODO: Get punctuateInterval from pipelineOptions.
      this.processorContext.schedule(
          1000, PunctuationType.WALL_CLOCK_TIME, new GroupAlsoByWindowPunctuator(1000));
      stateInternals = KStateInternals.of(unique, processorContext);
      timerInternals =
          KTimerInternals.of(
              unique, processorContext, windowingStrategy.getWindowFn().windowCoder());
      DoFnRunners.OutputManager outputManager = new GroupAlsoByWindowOutputManger();
      SystemReduceFn<K, V, ?, Iterable<V>, W> reduceFn = SystemReduceFn.buffering(valueCoder);
      DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> doFn =
          GroupAlsoByWindowViaWindowSetNewDoFn.create(
              windowingStrategy,
              key -> stateInternals.withKey(key),
              key -> timerInternals.withKey(key),
              NullSideInputReader.empty(),
              reduceFn,
              outputManager,
              mainOutputTag);
      doFnRunner =
          DoFnRunners.simpleRunner(
              PipelineOptionsFactory.create(),
              doFn,
              NullSideInputReader.empty(),
              outputManager,
              mainOutputTag,
              Collections.emptyList(),
              new GroupAlsoByWindowStepContext(),
              KeyedWorkItemCoder.of(
                  keyCoder, valueCoder, windowingStrategy.getWindowFn().windowCoder()),
              Collections.emptyMap(),
              windowingStrategy,
              doFnSchemaInformation);
    }

    @Override
    public KeyValue<Void, WindowedValue<KV<K, Iterable<V>>>> transform(
        Void object, WindowedValue<KeyedWorkItem<K, V>> windowedValue) {
      streamSourceWatermarks.put(
          processorContext.topic(), new Instant(processorContext.timestamp()));
      key = windowedValue.getValue().key();
      doFnRunner.startBundle();
      doFnRunner.processElement(windowedValue);
      doFnRunner.finishBundle();
      key = null;
      return null;
    }

    @Override
    public void close() {}

    private class GroupAlsoByWindowOutputManger implements DoFnRunners.OutputManager {

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        processorContext.forward(null, output);
      }
    }

    private class GroupAlsoByWindowPunctuator implements Punctuator {

      private final long interval;

      private GroupAlsoByWindowPunctuator(long interval) {
        this.interval = interval;
      }

      @Override
      public void punctuate(long timestamp) {
        Instant previousInputWatermarkTime = timerInternals.currentInputWatermarkTime();
        timerInternals.advanceInputWatermarkTime(
            streamSourceWatermarks.values().stream().min(Comparator.naturalOrder()).get());
        timerInternals.advanceOutputWatermarkTime(previousInputWatermarkTime);
        timerInternals.advanceProcessingTime(new Instant(timestamp));
        timerInternals.advanceSynchronizedProcessingTime(new Instant(timestamp - interval));
        Map<K, List<TimerData>> timersWorkItems = new HashMap<>();
        for (KV<K, TimerData> keyedTimerData : timerInternals.getFireableTimers()) {
          K key = keyedTimerData.getKey();
          List<TimerData> timersWorkItem = timersWorkItems.get(key);
          if (timersWorkItem == null) {
            timersWorkItem = new ArrayList<>();
          }
          timersWorkItem.add(keyedTimerData.getValue());
          timersWorkItems.put(key, timersWorkItem);
        }
        for (Map.Entry<K, List<TimerData>> timersWorkItem : timersWorkItems.entrySet()) {
          doFnRunner.processElement(
              WindowedValue.valueInGlobalWindow(
                  KeyedWorkItems.timersWorkItem(
                      timersWorkItem.getKey(), timersWorkItem.getValue())));
          for (TimerData timerData : timersWorkItem.getValue()) {
            timerInternals.deleteTimer(timerData);
          }
        }
      }
    }

    private class GroupAlsoByWindowStepContext implements StepContext {

      @Override
      public StateInternals stateInternals() {
        return stateInternals.withKey(key);
      }

      @Override
      public TimerInternals timerInternals() {
        return timerInternals.withKey(key);
      }
    }
  }
}
