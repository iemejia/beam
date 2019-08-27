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
package org.apache.beam.runners.core.construction;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.OptionalCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

/**
 * Read from a Java {@link Source} via the {@link Impulse} and {@link ParDo} primitive transforms.
 */
public class JavaReadViaImpulse {
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;
  private static final long DEFAULT_RESUME_DELAY = 1000L;
  private static final int DEFAULT_NUM_SPLITS = 1024;

  public static PTransformOverride override() {
    return PTransformOverride.of(
        PTransformMatchers.urnEqualTo(PTransformTranslation.READ_TRANSFORM_URN),
        new OverrideFactory<>());
  }

  public static PTransformOverride boundedOverride() {
    return PTransformOverride.of(
        PTransformMatchers.urnEqualTo(PTransformTranslation.READ_TRANSFORM_URN)
            .and(
                transform ->
                    ReadTranslation.sourceIsBounded(transform) == PCollection.IsBounded.BOUNDED),
        new BoundedOverrideFactory<>());
  }

  public static <T> PTransform<PBegin, PCollection<T>> bounded(BoundedSource<T> source) {
    return new BoundedReadViaImpulse<>(source);
  }

  public static PTransformOverride unboundedOverride() {
    return PTransformOverride.of(
        PTransformMatchers.urnEqualTo(PTransformTranslation.READ_TRANSFORM_URN)
            .and(
                transform ->
                    ReadTranslation.sourceIsBounded(transform) == PCollection.IsBounded.UNBOUNDED),
        new UnboundedOverrideFactory<>());
  }

  public static <T, CheckpointMarkT extends CheckpointMark>
      PTransform<PBegin, PCollection<T>> unbounded(UnboundedSource<T, CheckpointMarkT> source) {
    return new UnboundedReadViaImpulse<>(source);
  }

  private static class OverrideFactory<T, CheckpointMarkT extends CheckpointMark>
      implements PTransformOverrideFactory<
          PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform) {
      PBegin input = PBegin.in(transform.getPipeline());
      if (ReadTranslation.sourceIsBounded(transform) == PCollection.IsBounded.BOUNDED) {
        BoundedSource<T> source;
        try {
          source = ReadTranslation.boundedSourceFromTransform(transform);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return PTransformReplacement.of(input, new BoundedReadViaImpulse<>(source));
      } else {
        UnboundedSource<T, CheckpointMarkT> source;
        try {
          source = ReadTranslation.unboundedSourceFromTransform(transform);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return PTransformReplacement.of(input, new UnboundedReadViaImpulse<>(source));
      }
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class BoundedOverrideFactory<T>
      implements PTransformOverrideFactory<
          PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform) {
      PBegin input = PBegin.in(transform.getPipeline());
      BoundedSource<T> source;
      try {
        source = ReadTranslation.boundedSourceFromTransform(transform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return PTransformReplacement.of(input, new BoundedReadViaImpulse<>(source));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class BoundedReadViaImpulse<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedSource<T> source;

    private BoundedReadViaImpulse(BoundedSource<T> source) {
      this.source = source;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input
          .apply(Impulse.create())
          .apply(ParDo.of(new SplitBoundedSourceFn<>(source, DEFAULT_BUNDLE_SIZE_BYTES)))
          .setCoder(new BoundedSourceCoder<>())
          .apply(Reshuffle.viaRandomKey())
          .apply(ParDo.of(new ReadFromBoundedSourceFn<>()))
          .setCoder(source.getOutputCoder());
    }
  }

  @VisibleForTesting
  static class SplitBoundedSourceFn<T> extends DoFn<byte[], BoundedSource<T>> {
    private final BoundedSource<T> source;
    private final long bundleSize;

    public SplitBoundedSourceFn(BoundedSource<T> source, long bundleSize) {
      this.source = source;
      this.bundleSize = bundleSize;
    }

    @ProcessElement
    public void splitSource(ProcessContext ctxt) throws Exception {
      for (BoundedSource<T> split : source.split(bundleSize, ctxt.getPipelineOptions())) {
        ctxt.output(split);
      }
    }
  }

  /** Reads elements contained within an input {@link BoundedSource}. */
  // TODO: Extend to be a Splittable DoFn.
  @VisibleForTesting
  static class ReadFromBoundedSourceFn<T> extends DoFn<BoundedSource<T>, T> {
    @ProcessElement
    public void readSource(ProcessContext ctxt) throws IOException {
      try (BoundedSource.BoundedReader<T> reader =
          ctxt.element().createReader(ctxt.getPipelineOptions())) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          ctxt.outputWithTimestamp(reader.getCurrent(), reader.getCurrentTimestamp());
        }
      }
    }
  }

  /**
   * A {@link Coder} for {@link BoundedSource}s that wraps a {@link SerializableCoder}. We cannot
   * safely use an unwrapped SerializableCoder because {@link
   * SerializableCoder#structuralValue(Serializable)} assumes that coded elements support object
   * equality (https://issues.apache.org/jira/browse/BEAM-3807). By default, Coders compare equality
   * by serialized bytes, which we want in this case. It is usually safe to depend on coded
   * representation here because we only compare objects on bundle commit, which compares
   * serializations of the same object instance.
   *
   * <p>BoundedSources are generally not used as PCollection elements, so we do not expose this
   * coder for wider use.
   */
  @VisibleForTesting
  static class BoundedSourceCoder<T> extends CustomCoder<BoundedSource<T>> {
    private final Coder<BoundedSource<T>> coder;

    BoundedSourceCoder() {
      coder = (Coder<BoundedSource<T>>) SerializableCoder.of((Class) BoundedSource.class);
    }

    @Override
    public void encode(BoundedSource<T> value, OutputStream outStream)
        throws CoderException, IOException {
      coder.encode(value, outStream);
    }

    @Override
    public BoundedSource<T> decode(InputStream inStream) throws CoderException, IOException {
      return coder.decode(inStream);
    }
  }

  private static class UnboundedOverrideFactory<T, CheckpointMarkT extends CheckpointMark>
      implements PTransformOverrideFactory<
          PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform) {
      PBegin input = PBegin.in(transform.getPipeline());
      UnboundedSource<T, CheckpointMarkT> source;
      try {
        source = ReadTranslation.unboundedSourceFromTransform(transform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return PTransformReplacement.of(input, new UnboundedReadViaImpulse<>(source));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class UnboundedReadViaImpulse<T, CheckpointMarkT extends CheckpointMark>
      extends PTransform<PBegin, PCollection<T>> {
    private final UnboundedSource<T, CheckpointMarkT> source;

    private UnboundedReadViaImpulse(UnboundedSource<T, CheckpointMarkT> source) {
      this.source = source;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input
          .apply(Impulse.create())
          .apply(ParDo.of(new SplitUnboundedSourceFn<>(source)))
          .setCoder(new UnboundedSourceCoder<>())
          .apply(Reshuffle.viaRandomKey())
          .apply(ParDo.of(new ReadFromUnboundedSourceFn<>(OptionalCoder.of(source.getCheckpointMarkCoder()))))
          .setCoder(source.getOutputCoder());
    }
  }

  private static class SplitUnboundedSourceFn<T, CheckpointMarkT extends CheckpointMark>
      extends DoFn<byte[], UnboundedSource<T, CheckpointMarkT>> {
    private final UnboundedSource<T, CheckpointMarkT> source;

    private SplitUnboundedSourceFn(UnboundedSource<T, CheckpointMarkT> source) {
      this.source = source;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      for (UnboundedSource<T, CheckpointMarkT> unboundedSource :
          source.split(DEFAULT_NUM_SPLITS, processContext.getPipelineOptions())) {
        processContext.output(unboundedSource);
      }
    }
  }

  /** Reads elements contained within an input {@link UnboundedSource}. */
  @UnboundedPerElement
  private static class ReadFromUnboundedSourceFn<T, CheckpointMarkT extends CheckpointMark>
      extends DoFn<UnboundedSource<T, CheckpointMarkT>, T> {
    private final Coder<Optional<CheckpointMarkT>> restrictionCoder;

    private ReadFromUnboundedSourceFn(Coder<Optional<CheckpointMarkT>> restrictionCoder) {
      this.restrictionCoder = restrictionCoder;
    }

    @ProcessElement
    @SuppressWarnings("unchecked")
    public ProcessContinuation processElement(
        ProcessContext context, RestrictionTracker<Optional<CheckpointMarkT>, CheckpointMarkT> tracker)
        throws IOException {
      try (UnboundedReader<T> reader =
          context
              .element()
              .createReader(
                  context.getPipelineOptions(),
                  tracker.currentRestriction().orElse(null))) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          if (tracker.tryClaim((CheckpointMarkT) reader.getCheckpointMark())) {
            context.outputWithTimestamp(reader.getCurrent(), reader.getCurrentTimestamp());
            context.updateWatermark(reader.getWatermark());
          } else {
            ProcessContinuation.stop();
          }
        }
      }
      return ProcessContinuation.resume().withResumeDelay(Duration.millis(DEFAULT_RESUME_DELAY));
    }

    @GetInitialRestriction
    public Optional<CheckpointMarkT> getInitialRestricition(UnboundedSource<T, CheckpointMarkT> element) throws IOException {
      return Optional.empty();
    }

    @NewTracker
    public CheckpointMarkTracker<T, CheckpointMarkT> newTracker(Optional<CheckpointMarkT> restriction) {
      return new CheckpointMarkTracker<>(restriction.orElse(null));
    }

    @GetRestrictionCoder
    public Coder<Optional<CheckpointMarkT>> getRestrictionCoder() {
      return restrictionCoder;
    }
  }

  private static class CheckpointMarkTracker<T, CheckpointMarkT extends CheckpointMark>
      extends RestrictionTracker<Optional<CheckpointMarkT>, CheckpointMarkT> {
    private final CheckpointMarkT initial;

    private CheckpointMarkT position;
    private boolean checkpoint;

    private CheckpointMarkTracker(CheckpointMarkT initial) {
      this.initial = initial;
      this.position = initial;
      this.checkpoint = false;
    }

    @Override
    public boolean tryClaim(CheckpointMarkT position) {
      if (checkpoint) {
        return false;
      } else {
        this.position = position;
        return true;
      }
    }

    @Override
    public Optional<CheckpointMarkT> currentRestriction() {
      if (checkpoint) {
        return Optional.ofNullable(position);
      } else {
        return Optional.ofNullable(initial);
      }
    }

    @Override
    public Optional<CheckpointMarkT> checkpoint() {
      checkpoint = true;
      return Optional.ofNullable(initial);
    }

    @Override
    public void checkDone() throws IllegalStateException {}
  }

  private static class UnboundedSourceCoder<T, CheckpointMarkT extends CheckpointMark>
      extends CustomCoder<UnboundedSource<T, CheckpointMarkT>> {
    private final Coder<UnboundedSource<T, CheckpointMarkT>> coder;

    @SuppressWarnings("unchecked")
    private UnboundedSourceCoder() {
      coder =
          (Coder<UnboundedSource<T, CheckpointMarkT>>)
              SerializableCoder.of((Class) UnboundedSource.class);
    }

    @Override
    public void encode(UnboundedSource<T, CheckpointMarkT> value, OutputStream outStream)
        throws IOException {
      coder.encode(value, outStream);
    }

    @Override
    public UnboundedSource<T, CheckpointMarkT> decode(InputStream inStream) throws IOException {
      return coder.decode(inStream);
    }
  }
}
