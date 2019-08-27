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

import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.WindowIntoTranslation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.joda.time.Instant;

/**
 * Kafka Streams translator for the Beam {@link Window} primitive. Uses {@link
 * KStream#flatMapValues(ValueMapper)} and the {@link WindowFn} to {@link
 * WindowFn#assignWindows(WindowFn.AssignContext)} to the new {@link WindowedValue WindowedValues}.
 */
public class WindowTransformTranslator<T, W extends BoundedWindow>
    implements TransformTranslator<Window.Assign<T>> {

  @Override
  public void translate(PipelineTranslator pipelineTranslator, Window.Assign<T> transform) {
    PCollection<T> input = pipelineTranslator.getInput(transform);
    @SuppressWarnings("unchecked")
    WindowFn<T, W> windowFn =
        (WindowFn<T, W>)
            WindowIntoTranslation.getWindowFn(pipelineTranslator.getCurrentTransform());
    KStream<Void, WindowedValue<T>> stream = pipelineTranslator.getStream(input);
    KStream<Void, WindowedValue<T>> windowedStream =
        stream.flatMapValues(
            value -> {
              try {
                return windowFn.assignWindows(new WindowAssignContext(windowFn, value)).stream()
                    .map(
                        window ->
                            WindowedValue.of(
                                value.getValue(), value.getTimestamp(), window, value.getPane()))
                    .collect(Collectors.toList());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    PCollection<T> output = pipelineTranslator.getOutput(transform);
    pipelineTranslator.putStream(output, windowedStream);
    pipelineTranslator.putStreamSources(output, pipelineTranslator.getStreamSources(input));
  }

  private class WindowAssignContext extends WindowFn<T, W>.AssignContext {

    private final WindowedValue<T> windowedValue;

    private WindowAssignContext(WindowFn<T, W> windowFn, WindowedValue<T> windowedValue) {
      windowFn.super();
      this.windowedValue = windowedValue;
    }

    @Override
    public T element() {
      return windowedValue.getValue();
    }

    @Override
    public Instant timestamp() {
      return windowedValue.getTimestamp();
    }

    @Override
    public BoundedWindow window() {
      return Iterables.getOnlyElement(windowedValue.getWindows());
    }
  }
}
