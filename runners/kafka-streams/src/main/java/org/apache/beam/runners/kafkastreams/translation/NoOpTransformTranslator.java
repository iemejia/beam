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

import org.apache.beam.runners.kafkastreams.KafkaStreamsRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Kafka Streams translator that does nothing but add the input stream as the output stream.
 * Currently the {@link KafkaStreamsRunner} does not perform any actions for a CreatePCollectionView
 * or a Reshuffle.
 */
public class NoOpTransformTranslator
    implements TransformTranslator<PTransform<PCollection<?>, PCollection<?>>> {

  @Override
  public void translate(
      PipelineTranslator pipelineTranslator, PTransform<PCollection<?>, PCollection<?>> transform) {
    PCollection<?> input = pipelineTranslator.getInput(transform);
    PCollection<?> output = pipelineTranslator.getOutput(transform);
    pipelineTranslator.putStream(output, pipelineTranslator.getStream(input));
    pipelineTranslator.putStreamSources(output, pipelineTranslator.getStreamSources(input));
  }
}
