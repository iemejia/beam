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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces.GlobalNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowAndTriggerNamespace;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.runners.kafkastreams.sideinput.KSideInputReader;
import org.apache.beam.runners.kafkastreams.state.KStateInternals;
import org.apache.beam.runners.kafkastreams.state.KTimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Kafka Streams translator for the Beam {@link ParDo} primitive. Uses {@link
 * KStream#transform(TransformerSupplier, String[])} to create {@link TupleTag TupleTagged} outputs,
 * then uses {@link KStream#branch(Predicate[])} to create a {@link KStream} for each output {@link
 * TupleTag}. Creates a {@link StateStore} for each {@link StateSpec} in the {@link DoFn DoFn's}
 * stateDeclarations. Creates a {@link GlobalKTable} for each side input, backed by a {@link
 * StateStore} that is readable by the {@link KSideInputReader}.
 */
public class ParDoTransformTranslator<InputT, OutputT>
    implements TransformTranslator<ParDo.MultiOutput<InputT, OutputT>> {

  @SuppressWarnings("unchecked")
  @Override
  public void translate(
      PipelineTranslator pipelineTranslator, ParDo.MultiOutput<InputT, OutputT> transform) {
    try {
      KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
      PCollection<InputT> input = (PCollection<InputT>) pipelineTranslator.getInput(transform);
      Coder<InputT> inputCoder = input.getCoder();
      Map<TupleTag<?>, PValue> outputs = pipelineTranslator.getOutputs(transform);
      Map<TupleTag<?>, Coder<?>> outputCoders = pipelineTranslator.getOutputCoders();
      AppliedPTransform<?, ?, ?> appliedPTransform = pipelineTranslator.getCurrentTransform();
      DoFn<InputT, OutputT> doFn =
          (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(appliedPTransform);
      TupleTag<OutputT> mainOutputTag =
          (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(appliedPTransform);
      TupleTagList additionalOutputTags =
          ParDoTranslation.getAdditionalOutputTags(appliedPTransform);
      //      DoFnSchemaInformation doFnSchemaInformation =
      //          ParDoTranslation.getSchemaInformation(appliedPTransform);
      Set<String> streamSources = pipelineTranslator.getStreamSources(input);

      KStream<Void, WindowedValue<InputT>> inputStream = pipelineTranslator.getStream(input);

      Map<PCollectionView<?>, String> sideInputs =
          sideInputs(
              pipelineTranslator,
              pipelineOptions,
              ParDoTranslation.getSideInputs(appliedPTransform));
      Collection<String> stateStoreNames = sideInputs.values();
      String unique = null;
      if (ParDoTranslation.usesStateOrTimers(appliedPTransform)) {
        // TODO: Is reshuffle required?
        unique = Admin.uniqueName(pipelineOptions, pipelineTranslator.getCurrentTransform()) + "-";
        stateStoreNames.addAll(
            stateStoreNames(
                pipelineTranslator, unique, ((KvCoder<?, ?>) inputCoder).getKeyCoder()));
      }
      translate(
          pipelineTranslator,
          inputStream,
          pipelineOptions,
          () -> doFn,
          sideInputs,
          mainOutputTag,
          additionalOutputTags,
          inputCoder,
          outputCoders,
          input.getWindowingStrategy(),
          DoFnSchemaInformation.create(),
          streamSources,
          unique,
          stateStoreNames,
          outputs);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Map<PCollectionView<?>, String> sideInputs(
      PipelineTranslator pipelineTranslator,
      KafkaStreamsPipelineOptions pipelineOptions,
      List<PCollectionView<?>> sideInputs) {
    Map<PCollectionView<?>, String> kSideInputs = new HashMap<>();
    for (PCollectionView<?> sideInput : sideInputs) {
      kSideInputs.put(
          sideInput,
          KSideInputReader.sideInput(
              pipelineOptions,
              pipelineTranslator.getStreamsBuilder(),
              sideInput,
              pipelineTranslator.getStream(sideInput.getPCollection())));
    }
    return kSideInputs;
  }

  static Collection<String> stateStoreNames(
      PipelineTranslator pipelineTranslator, String unique, Coder<?> keyCoder) {
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

    return Arrays.asList(state, timer);
  }

  @SuppressWarnings("unchecked")
  static <InputT, OutputT> void translate(
      PipelineTranslator pipelineTranslator,
      KStream<Void, WindowedValue<InputT>> inputStream,
      KafkaStreamsPipelineOptions pipelineOptions,
      Supplier<DoFn<InputT, OutputT>> doFnSupplier,
      Map<PCollectionView<?>, String> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      TupleTagList additionalOutputTags,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Set<String> streamSources,
      String unique,
      Collection<String> stateStoreNames,
      Map<TupleTag<?>, PValue> outputs) {
    KStream<TupleTag<?>, WindowedValue<?>> taggedOutputStream =
        inputStream.transform(
            () ->
                new ParDoTransformer<>(
                    pipelineOptions,
                    doFnSupplier.get(),
                    sideInputs,
                    mainOutputTag,
                    additionalOutputTags.getAll(),
                    inputCoder,
                    outputCoders,
                    windowingStrategy,
                    doFnSchemaInformation,
                    streamSources,
                    unique),
            stateStoreNames.stream().collect(Collectors.toSet()).toArray(new String[0]));

    List<TupleTag<?>> outputTags = additionalOutputTags.and(mainOutputTag).getAll();
    Predicate<TupleTag<?>, WindowedValue<?>>[] predicates =
        outputTags.stream()
            .map(
                outputTag ->
                    new Predicate<TupleTag<?>, WindowedValue<?>>() {
                      @Override
                      public boolean test(TupleTag<?> key, WindowedValue<?> value) {
                        return outputTag.equals(key);
                      }
                    })
            .collect(Collectors.toList())
            .toArray(new Predicate[0]);
    KStream<TupleTag<?>, WindowedValue<?>>[] branches = taggedOutputStream.branch(predicates);
    for (int index = 0; index < outputTags.size(); index++) {
      PCollection<?> output = (PCollection<?>) outputs.get(outputTags.get(index));
      KStream<Void, WindowedValue<?>> branch =
          branches[index].map((key, value) -> KeyValue.pair(null, value));
      pipelineTranslator.putStream(output, branch);
      pipelineTranslator.putStreamSources(output, streamSources);
    }
  }

  static class ParDoTransformer<K, InputT, OutputT, RestrictionT, PositionT>
      implements Transformer<Void, WindowedValue<InputT>, KeyValue<TupleTag<?>, WindowedValue<?>>> {

    private final PipelineOptions pipelineOptions;
    private final DoFn<InputT, OutputT> doFn;
    private final Map<PCollectionView<?>, String> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final List<TupleTag<?>> additionalOutputTags;
    private final Coder<InputT> inputCoder;
    private final Map<TupleTag<?>, Coder<?>> outputCoders;
    private final WindowingStrategy<?, ?> windowingStrategy;
    private final DoFnSchemaInformation doFnSchemaInformation;
    private final Map<String, Instant> streamSourceWatermarks;
    @Nullable private final String unique;

    private ProcessorContext processorContext;
    private KStateInternals<K> stateInternals;
    private KTimerInternals<K, ?> timerInternals;
    private DoFnInvoker<InputT, OutputT> doFnInvoker;
    private DoFnRunner<InputT, OutputT> doFnRunner;
    private K key;

    ParDoTransformer(
        PipelineOptions pipelineOptions,
        DoFn<InputT, OutputT> doFn,
        Map<PCollectionView<?>, String> sideInputs,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        Coder<InputT> inputCoder,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        WindowingStrategy<?, ?> windowingStrategy,
        DoFnSchemaInformation doFnSchemaInformation,
        Set<String> streamSources,
        @Nullable String unique) {
      this.pipelineOptions = pipelineOptions;
      this.doFn = doFn;
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
      this.inputCoder = inputCoder;
      this.outputCoders = outputCoders;
      this.windowingStrategy = windowingStrategy;
      this.doFnSchemaInformation = doFnSchemaInformation;
      this.streamSourceWatermarks =
          streamSources.stream()
              .collect(
                  Collectors.toMap(string -> string, string -> BoundedWindow.TIMESTAMP_MIN_VALUE));
      this.unique = unique;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext processorContext) {
      this.processorContext = processorContext;
      // TODO: Get punctuateInterval from pipelineOptions.
      this.processorContext.schedule(
          java.time.Duration.ofSeconds(1),
          PunctuationType.WALL_CLOCK_TIME,
          new ParDoPunctuator(1000));
      if (unique != null) {
        stateInternals = KStateInternals.of(unique, processorContext);
        timerInternals =
            KTimerInternals.of(
                unique, processorContext, windowingStrategy.getWindowFn().windowCoder());
      }
      if (doFn instanceof ProcessFn) {
        ProcessFn<InputT, OutputT, RestrictionT, PositionT> processFn =
            (ProcessFn<InputT, OutputT, RestrictionT, PositionT>) doFn;
        processFn.setStateInternalsFactory(key -> stateInternals.withKey((K) key));
        processFn.setTimerInternalsFactory(key -> timerInternals.withKey((K) key));
        processFn.setProcessElementInvoker(
            new OutputAndTimeBoundedSplittableProcessElementInvoker<
                InputT, OutputT, RestrictionT, PositionT>(
                doFn,
                pipelineOptions,
                new ParDoOutputWindowedValue(),
                KSideInputReader.of(processorContext, sideInputs),
                Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
                1000,
                Duration.millis(1000)));
      }
      doFnInvoker = DoFnInvokers.invokerFor(doFn);
      doFnInvoker.invokeSetup();
      doFnRunner =
          DoFnRunners.simpleRunner(
              pipelineOptions,
              doFn,
              KSideInputReader.of(processorContext, sideInputs),
              new ParDoOutputManager(),
              mainOutputTag,
              additionalOutputTags,
              new ParDoStepContext(),
              inputCoder,
              outputCoders,
              windowingStrategy,
              doFnSchemaInformation);
      doFnRunner.startBundle();
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyValue<TupleTag<?>, WindowedValue<?>> transform(
        Void object, WindowedValue<InputT> windowedValue) {
      streamSourceWatermarks.put(
          processorContext.topic(), new Instant(processorContext.timestamp()));
      if (unique != null) {
        Object value = windowedValue.getValue();
        if (value instanceof KV) {
          key = ((KV<K, ?>) value).getKey();
        }
        if (value instanceof KeyedWorkItem) {
          key = ((KeyedWorkItem<K, ?>) value).key();
        }
      }
      doFnRunner.processElement(windowedValue);
      key = null;
      return null;
    }

    @Override
    public void close() {
      doFnRunner.finishBundle();
      doFnInvoker.invokeTeardown();
    }

    private class ParDoOutputManager implements DoFnRunners.OutputManager {

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        processorContext.forward(tag, output);
      }
    }

    private class ParDoOutputWindowedValue implements OutputWindowedValue<OutputT> {

      @Override
      public void outputWindowedValue(
          OutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        outputWindowedValue(mainOutputTag, output, timestamp, windows, pane);
      }

      @Override
      public <AdditionalOutputT> void outputWindowedValue(
          TupleTag<AdditionalOutputT> tag,
          AdditionalOutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        processorContext.forward(tag, WindowedValue.of(output, timestamp, windows, pane));
      }
    }

    private class ParDoPunctuator implements Punctuator {

      private final long interval;

      private ParDoPunctuator(long interval) {
        this.interval = interval;
      }

      @Override
      public void punctuate(long timestamp) {
        if (timerInternals != null) {
          Instant previousInputWatermarkTime = timerInternals.currentInputWatermarkTime();
          timerInternals.advanceInputWatermarkTime(
              streamSourceWatermarks.values().stream().min(Comparator.naturalOrder()).get());
          timerInternals.advanceOutputWatermarkTime(previousInputWatermarkTime);
          timerInternals.advanceProcessingTime(new Instant(timestamp));
          timerInternals.advanceSynchronizedProcessingTime(new Instant(timestamp - interval));
          fireTimers();
        }
        doFnRunner.finishBundle();
        doFnRunner.startBundle();
      }

      private void fireTimers() {
        boolean hasFired;
        do {
          hasFired = false;
          for (KV<K, TimerData> timer : timerInternals.getFireableTimers()) {
            hasFired = true;
            fireTimer(timer);
          }
        } while (hasFired);
      }

      private void fireTimer(KV<K, TimerData> timer) {
        if (doFn instanceof ProcessFn) {
          doFnRunner.processElement(
              (WindowedValue<InputT>)
                  WindowedValue.valueInGlobalWindow(
                      KeyedWorkItems.timersWorkItem(
                          timer.getKey(), Collections.singleton(timer.getValue()))));
        } else {
          doFnRunner.onTimer(
              timer.getValue().getTimerId(),
              window(timer.getValue()),
              timer.getValue().getTimestamp(),
              timer.getValue().getDomain());
        }
      }

      private BoundedWindow window(TimerData timerData) {
        StateNamespace namespace = timerData.getNamespace();
        if (namespace instanceof GlobalNamespace) {
          return GlobalWindow.INSTANCE;
        } else if (namespace instanceof WindowNamespace) {
          return ((WindowNamespace<?>) namespace).getWindow();
        } else if (namespace instanceof WindowAndTriggerNamespace) {
          return ((WindowAndTriggerNamespace<?>) namespace).getWindow();
        } else {
          throw new RuntimeException("Invalid namespace: " + namespace);
        }
      }
    }

    private class ParDoStepContext implements StepContext {

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
