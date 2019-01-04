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

import java.util.Collection;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Kafka Streams translator for the Beam {@link Flatten} primitive. Uses {@link
 * KStream#merge(KStream)} to combine multiple KStreams into one.
 */
public class FlattenTransformTranslator<T> implements TransformTranslator<Flatten.PCollections<T>> {

  @Override
  public void translate(PipelineTranslator pipelineTranslator, Flatten.PCollections<T> transform) {
    Collection<PValue> values = pipelineTranslator.getInputs(transform).values();
    KStream<Object, WindowedValue<T>> flatten = null;
    for (PValue value : values) {
      if (flatten == null) {
        flatten = pipelineTranslator.getStream(value);
      } else {
        flatten.merge(pipelineTranslator.getStream(value));
      }
    }
    if (flatten == null) {
      throw new IllegalArgumentException("Empty flatten not supported.");
    }
    pipelineTranslator.putStream(pipelineTranslator.getOutput(transform), flatten);
  }
}
