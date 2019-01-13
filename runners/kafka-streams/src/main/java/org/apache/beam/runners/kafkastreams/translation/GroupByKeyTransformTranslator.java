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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import org.apache.beam.runners.kafkastreams.admin.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.runners.kafkastreams.state.KStateInternals;
import org.apache.beam.runners.kafkastreams.state.KTimerInternals;
import org.apache.beam.sdk.coders.BitSetCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
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
 * KStream#through(String, Produced)} to groupByKeyOnly, then uses the GroupAlsoByWindow {@link
 * Transformer} to groupAlsoByWindow, utilizing a {@link StateStore} for holding aggregated data for
 * the key and a {@link Punctuator} for identifying when triggers are ready.
 */
public class GroupByKeyTransformTranslator<K, V, W extends BoundedWindow>
    implements TransformTranslator<GroupByKey<K, V>> {

  private static final String BUF = "buf";
  private static final String CLOSED = "closed";
  private static final String COUNT = "count";
  private static final String DELAYED = "delayed";
  private static final String EXTRA = "extra";
  private static final String HOLD = "hold";
  private static final String PANE = "pane";

  @Override
  public void translate(PipelineTranslator pipelineTranslator, GroupByKey<K, V> transform) {
    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    String topic = Admin.topic(pipelineTranslator.getCurrentTransform());
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

    createTopicIfNeeded(pipelineOptions, topic);
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

    addStateStores(pipelineTranslator, keyCoder, valueCoder);
    KStream<Object, WindowedValue<KV<K, Iterable<V>>>> groupAlsoByWindow =
        groupByKeyKeyedWorkItemStream.transform(
            () -> new GroupAlsoByWindow(keyCoder, valueCoder, outputTag, windowingStrategy),
            BUF,
            CLOSED,
            COUNT,
            DELAYED,
            EXTRA,
            HOLD,
            PANE,
            KTimerInternals.TIMER_INTERNALS);

    pipelineTranslator.putStream(pipelineTranslator.getOutput(transform), groupAlsoByWindow);
  }

  private void createTopicIfNeeded(KafkaStreamsPipelineOptions pipelineOptions, String topic) {
    try (AdminClient adminClient = Admin.adminClient(pipelineOptions)) {
      if (adminClient.listTopics().names().get().contains(topic)) {
        return;
      }
      adminClient
          .createTopics(
              Collections.singleton(
                  new NewTopic(
                      topic,
                      pipelineOptions.getNumPartitions(),
                      new StreamsConfig(pipelineOptions.getProperties())
                          .getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG)
                          .shortValue())))
          .all()
          .get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void addStateStores(
      PipelineTranslator pipelineTranslator, Coder<K> keyCoder, Coder<V> valueCoder) {
    // SystemReduceFn.BUFFER_NAME
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(BUF),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(ListCoder.of(valueCoder))));
    // TriggerStateMachineRunner.FINISHED_BITS_TAG
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(CLOSED),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(BitSetCoder.of())));
    // NonEmptyPanes.GeneralNonEmptyPanes.PANE_ADDITIONS_TAG
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(COUNT),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(
                    Sum.ofLongs()
                        .getAccumulatorCoder(
                            pipelineTranslator.getCoderRegistry(), VarLongCoder.of()))));
    // AfterDelayFromFirstElementStateMachine.DELAYED_UNTIL_TAG
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DELAYED),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(new Combine.HolderCoder<>(InstantCoder.of()))));
    // WatermarkHold.EXTRA_HOLD_TAG
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(EXTRA),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(InstantCoder.of())));
    // WatermarkHold.watermarkHoldTagForTimestampCombiner(TimestampCombiner)
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(HOLD),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(InstantCoder.of())));
    // PaneInfoTracker.PANE_INFO_TAG
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PANE),
                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                CoderSerde.of(PaneInfoCoder.INSTANCE)));
    // KTimerInternals.TIMER_INTERNALS
    pipelineTranslator
        .getStreamsBuilder()
        .addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(KTimerInternals.TIMER_INTERNALS),
                CoderSerde.of(StringUtf8Coder.of()),
                CoderSerde.of(KvCoder.of(keyCoder, InstantCoder.of()))));
  }

  private class GroupAlsoByWindow
      implements Transformer<
          Object, WindowedValue<KeyedWorkItem<K, V>>,
          KeyValue<Object, WindowedValue<KV<K, Iterable<V>>>>> {

    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final TupleTag<KV<K, Iterable<V>>> mainOutputTag;
    private final WindowingStrategy<?, W> windowingStrategy;

    private ProcessorContext processorContext;
    private KStateInternals<K> stateInternals;
    private KTimerInternals<K, W> timerInternals;
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
    public void init(ProcessorContext processorContext) {
      this.processorContext = processorContext;
      // TODO: Get punctuateInterval from pipelineOptions.
      this.processorContext.schedule(1000, PunctuationType.WALL_CLOCK_TIME, new GABWPunctuator());
      stateInternals = KStateInternals.of(processorContext);
      timerInternals =
          KTimerInternals.of(processorContext, windowingStrategy.getWindowFn().windowCoder());
      DoFnRunners.OutputManager outputManager = new GABWOutputManger();
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
        Object object, WindowedValue<KeyedWorkItem<K, V>> windowedValue) {
      key = windowedValue.getValue().key();
      doFnRunner.startBundle();
      doFnRunner.processElement(windowedValue);
      doFnRunner.finishBundle();
      key = null;
      return null;
    }

    @Override
    public void close() {}

    private class GABWOutputManger implements DoFnRunners.OutputManager {

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        processorContext.forward(null, output);
      }
    }

    private class GABWPunctuator implements Punctuator {

      @Override
      public void punctuate(long timestamp) {
        // TODO: Advance other times.
        timerInternals.advanceProcessingTime(new Instant(timestamp));
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

    private class GABWStepContext implements StepContext {

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
