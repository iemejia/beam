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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryTimerInternals;
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
import org.apache.beam.runners.kafkastreams.admin.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.runners.kafkastreams.sideinput.KSideInputReader;
import org.apache.beam.runners.kafkastreams.state.KStateInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.Instant;

/**
 * Kafka Streams translator for the Beam {@link ParDo} primitive. Uses {@link
 * KStream#transform(TransformerSupplier, String[])} to create {@link TupleTag TupleTagged} outputs,
 * then uses {@link KStream#branch(Predicate[])} to create a {@link KStream} for each output {@link
 * TupleTag}. Creates a {@link StateStore} for each {@link StateSpec} in the {@link DoFn DoFn's}
 * stateDeclarations. Creates a {@link GlobalKTable} for each side input, backed by a {@link
 * StateStore} that is readable by the {@link KSideInputReader}.
 */
public class ParDoTransformTranslator<InputT, OutputT, W extends BoundedWindow>
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
      Set<String> streamSources = pipelineTranslator.getStreamSources(input);

      KStream<Object, WindowedValue<InputT>> inputStream = pipelineTranslator.getStream(input);

      Map<PCollectionView<?>, String> sideInputs =
          sideInputs(
              pipelineTranslator,
              pipelineOptions,
              ParDoTranslation.getSideInputs(appliedPTransform));
      Collection<String> stateStoreNames = sideInputs.values();
      String statePrefix;
      if (ParDoTranslation.usesStateOrTimers(appliedPTransform)) {
        // TODO: Is reshuffle required?
        statePrefix =
            Admin.uniqueName(pipelineOptions, pipelineTranslator.getCurrentTransform()) + "-";
        stateStoreNames.addAll(stateStoreNames(pipelineTranslator, statePrefix, ((KvCoder<?, ?>) inputCoder).getKeyCoder(), doFn));
      } else {
        statePrefix = null;
      }
      KStream<TupleTag<?>, WindowedValue<?>> taggedOutputStream =
          inputStream.transform(
              () ->
                  new ParDoTransformer<>(
                      pipelineOptions,
                      doFn,
                      sideInputs,
                      mainOutputTag,
                      additionalOutputTags.getAll(),
                      inputCoder,
                      outputCoders,
                      (WindowingStrategy<?, W>) input.getWindowingStrategy(),
                      streamSources,
                      statePrefix),
              stateStoreNames.stream().collect(Collectors.toSet()).toArray(new String[0]));

      List<TupleTag<?>> outputTags = additionalOutputTags.and(mainOutputTag).getAll();
      Predicate<TupleTag<?>, WindowedValue<?>>[] predicates =
          outputTags
              .stream()
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
        PValue output = outputs.get(outputTags.get(index));
        pipelineTranslator.putStream(output, branches[index]);
        pipelineTranslator.putStreamSources(output, streamSources);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<PCollectionView<?>, String> sideInputs(
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

  private Set<String> stateStoreNames(
      PipelineTranslator pipelineTranslator,
      String statePrefix,
      Coder<?> keyCoder,
      DoFn<InputT, OutputT> doFn) {
    try {
      Map<String, StateDeclaration> stateDeclarations =
          DoFnSignatures.signatureForDoFn(doFn).stateDeclarations();
      for (StateDeclaration stateDeclaration : stateDeclarations.values()) {
        StateSpec<?> stateSpec = (StateSpec<?>) stateDeclaration.field().get(doFn);
        pipelineTranslator
            .getStreamsBuilder()
            .addStateStore(
                stateSpec.match(
                    new StateSpec.Cases<StoreBuilder<?>>() {

                      @Override
                      public StoreBuilder<?> dispatchValue(Coder<?> valueCoder) {
                        return Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateDeclaration.id()),
                                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                                CoderSerde.of(valueCoder))
                            .withCachingEnabled();
                      }

                      @Override
                      public StoreBuilder<?> dispatchBag(Coder<?> elementCoder) {
                        return Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateDeclaration.id()),
                                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                                CoderSerde.of(ListCoder.of(elementCoder)))
                            .withCachingEnabled();
                      }

                      @Override
                      public StoreBuilder<?> dispatchSet(Coder<?> elementCoder) {
                        return Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateDeclaration.id()),
                                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                                CoderSerde.of(SetCoder.of(elementCoder)))
                            .withCachingEnabled();
                      }

                      @Override
                      public StoreBuilder<?> dispatchMap(Coder<?> kCoder, Coder<?> vCoder) {
                        return Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateDeclaration.id()),
                                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                                CoderSerde.of(MapCoder.of(kCoder, vCoder)))
                            .withCachingEnabled();
                      }

                      @Override
                      public StoreBuilder<?> dispatchCombining(
                          CombineFn<?, ?, ?> combineFn, Coder<?> accumCoder) {
                        return Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(stateDeclaration.id()),
                                CoderSerde.of(KvCoder.of(keyCoder, StringUtf8Coder.of())),
                                CoderSerde.of(accumCoder))
                            .withCachingEnabled();
                      }
                    }));
      }
      return stateDeclarations.keySet();
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private class ParDoTransformer<K>
      implements Transformer<
          Object, WindowedValue<InputT>, KeyValue<TupleTag<?>, WindowedValue<?>>> {

    private final PipelineOptions pipelineOptions;
    private final DoFn<InputT, OutputT> doFn;
    private final Map<PCollectionView<?>, String> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final List<TupleTag<?>> additionalOutputTags;
    private final Coder<InputT> inputCoder;
    private final Map<TupleTag<?>, Coder<?>> outputCoders;
    private final WindowingStrategy<?, W> windowingStrategy;
    private final Map<String, Instant> streamSourceWatermarks;
    @Nullable private final String statePrefix;

    private ProcessorContext processorContext;
    private KStateInternals<K> stateInternals;
    private InMemoryTimerInternals timerInternals;
    private DoFnInvoker<InputT, OutputT> doFnInvoker;
    private DoFnRunner<InputT, OutputT> doFnRunner;
    private K key;

    private ParDoTransformer(
        PipelineOptions pipelineOptions,
        DoFn<InputT, OutputT> doFn,
        Map<PCollectionView<?>, String> sideInputs,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        Coder<InputT> inputCoder,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        WindowingStrategy<?, W> windowingStrategy,
        Set<String> streamSources,
        @Nullable String statePrefix) {
      this.pipelineOptions = pipelineOptions;
      this.doFn = doFn;
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
      this.inputCoder = inputCoder;
      this.outputCoders = outputCoders;
      this.windowingStrategy = windowingStrategy;
      this.streamSourceWatermarks =
          streamSources
              .stream()
              .collect(
                  Collectors.toMap(string -> string, string -> BoundedWindow.TIMESTAMP_MIN_VALUE));
      this.statePrefix = statePrefix;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      this.processorContext = processorContext;
      // TODO: Get punctuateInterval from pipelineOptions.
      this.processorContext.schedule(
          1000, PunctuationType.WALL_CLOCK_TIME, new ParDoPunctuator(1000));
      stateInternals = KStateInternals.of(statePrefix, processorContext);
      timerInternals = new InMemoryTimerInternals();
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
              windowingStrategy);
      doFnRunner.startBundle();
    }

    @Override
    public KeyValue<TupleTag<?>, WindowedValue<?>> transform(
        Object object, WindowedValue<InputT> windowedValue) {
      streamSourceWatermarks.put(
          processorContext.topic(), new Instant(processorContext.timestamp()));
      if (statePrefix != null) {
        key = ((KV<K, ?>) windowedValue.getValue()).getKey();
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

    private class ParDoPunctuator implements Punctuator {

      private final long interval;

      private ParDoPunctuator(long interval) {
        this.interval = interval;
      }

      @Override
      public void punctuate(long timestamp) {
        Instant previousInputWatermarkTime = timerInternals.currentInputWatermarkTime();
        timerInternals.advanceInputWatermark(
            streamSourceWatermarks.values().stream().min(Comparator.naturalOrder()).get());
        timerInternals.advanceOutputWatermark(previousInputWatermarkTime);
        timerInternals.advanceProcessingTime(new Instant(timestamp));
        timerInternals.advanceSynchronizedProcessingTime(new Instant(timestamp - interval));
        fireTimers();
        doFnRunner.finishBundle();
        doFnRunner.startBundle();
      }

      private void fireTimers() {
        boolean hasFired;
        do {
          hasFired = false;
          TimerInternals.TimerData timer;
          while ((timer = timerInternals.removeNextEventTimer()) != null) {
            hasFired = true;
            fireTimer(timer);
          }
          while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
            hasFired = true;
            fireTimer(timer);
          }
          while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
            hasFired = true;
            fireTimer(timer);
          }
        } while (hasFired);
      }

      private void fireTimer(TimerData timerData) {
        doFnRunner.onTimer(
            timerData.getTimerId(),
            window(timerData),
            timerData.getTimestamp(),
            timerData.getDomain());
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
        return timerInternals;
      }
    }
  }
}
