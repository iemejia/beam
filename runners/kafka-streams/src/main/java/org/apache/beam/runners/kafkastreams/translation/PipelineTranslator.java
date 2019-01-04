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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Translates a {@link Pipeline} into a {@link StreamsBuilder} so it can be run in a {@link
 * KafkaStreams} instance.
 */
@SuppressWarnings("deprecation")
public class PipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

  public static PipelineTranslator of(
      Pipeline pipeline, KafkaStreamsPipelineOptions pipelineOptions) {
    return new PipelineTranslator(pipeline, pipelineOptions);
  }

  private static final Map<String, TransformTranslator<?>> TRANSFORM_TRANSLATORS;

  static {
    TRANSFORM_TRANSLATORS = new HashMap<>();
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, new NoOpTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(
        PTransformTranslation.READ_TRANSFORM_URN, new ReadTransformTranslator<>());
    TRANSFORM_TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new NoOpTransformTranslator<>());
  }

  private final Pipeline pipeline;
  private final KafkaStreamsPipelineOptions pipelineOptions;
  private final StreamsBuilder streamsBuilder;
  private final Map<PValue, KStream<?, ?>> streams;
  private boolean translated;
  private AppliedPTransform<?, ?, ?> currentTransform;

  public PipelineTranslator(Pipeline pipeline, KafkaStreamsPipelineOptions pipelineOptions) {
    this.pipeline = pipeline;
    this.pipelineOptions = pipelineOptions;
    this.streamsBuilder = new StreamsBuilder();
    this.streams = new HashMap<>();
    this.translated = false;
  }

  public StreamsBuilder translate() {
    if (!translated) {
      pipeline.replaceAll(
          ImmutableList.of(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory<>())));
      pipeline.traverseTopologically(this);
      translated = true;
    }
    return streamsBuilder;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      String urn = PTransformTranslation.urnForTransformOrNull(transform);
      if (urn != null && TRANSFORM_TRANSLATORS.containsKey(urn)) {
        visitTransform(node);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    visitTransform(node);
  }

  @SuppressWarnings("unchecked")
  private <TransformT extends PTransform<?, ?>> void visitTransform(TransformHierarchy.Node node) {
    TransformT transform = (TransformT) node.getTransform();
    TransformTranslator<TransformT> transformTranslator =
        (TransformTranslator<TransformT>)
            TRANSFORM_TRANSLATORS.get(PTransformTranslation.urnForTransform(transform));
    currentTransform = node.toAppliedPTransform(pipeline);
    transformTranslator.translate(this, transform);
    currentTransform = null;
  }

  AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  @SuppressWarnings("unchecked")
  <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(currentTransform));
  }

  Map<TupleTag<?>, PValue> getInputs(PTransform<?, ?> transform) {
    return currentTransform.getInputs();
  }

  @SuppressWarnings("unchecked")
  <T extends PValue> T getOutput(PTransform<?, T> transform) {
    return (T) Iterables.getOnlyElement(getOutputs(transform).values());
  }

  @SuppressWarnings("unchecked")
  <OutputT, CollectionT extends PCollection<OutputT>> TupleTag<OutputT> getOutputTag(
      PTransform<?, CollectionT> transform) {
    return (TupleTag<OutputT>) Iterables.getOnlyElement(getOutputs(transform).keySet());
  }

  Map<TupleTag<?>, PValue> getOutputs(PTransform<?, ?> transform) {
    return currentTransform.getOutputs();
  }

  Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return currentTransform
        .getOutputs()
        .entrySet()
        .stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(
            Collectors.toMap(e -> e.getKey(), e -> ((PCollection<?>) e.getValue()).getCoder()));
  }

  KafkaStreamsPipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  StreamsBuilder getStreamsBuilder() {
    return streamsBuilder;
  }

  @SuppressWarnings("unchecked")
  <T> KStream<Object, WindowedValue<T>> getStream(PValue value) {
    return (KStream<Object, WindowedValue<T>>) streams.get(value);
  }

  void putStream(PValue value, KStream<?, ?> stream) {
    streams.put(value, stream);
  }
}
