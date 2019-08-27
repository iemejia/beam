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

import java.util.Collections;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class ImpulseTransformTranslator implements TransformTranslator<Impulse> {

  private static final String IMPULSE_TOPIC = "impulse";

  @Override
  public void translate(PipelineTranslator pipelineTranslator, Impulse transform) {
    PCollection<byte[]> output = pipelineTranslator.getOutput(transform);
    pipelineTranslator.putStream(output, pipelineTranslator.getImpulse());
    pipelineTranslator.putStreamSources(output, Collections.singleton(IMPULSE_TOPIC));
  }

  public static KStream<Void, WindowedValue<byte[]>> impulse(
      PipelineTranslator pipelineTranslator) {
    Serde<WindowedValue<byte[]>> serde =
        CoderSerde.of(ValueOnlyWindowedValueCoder.of(ByteArrayCoder.of()));
    if (Admin.createTopicIfNeeded(pipelineTranslator.getPipelineOptions(), IMPULSE_TOPIC)) {
      try (Producer<Void, WindowedValue<byte[]>> producer =
          Admin.producer(
              pipelineTranslator.getPipelineOptions(), CoderSerde.of(VoidCoder.of()), serde)) {
        producer
            .send(
                new ProducerRecord<>(IMPULSE_TOPIC, WindowedValue.valueInGlobalWindow(new byte[0])))
            .get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return pipelineTranslator.getStreamsBuilder().stream(
        IMPULSE_TOPIC,
        Consumed.with(CoderSerde.of(VoidCoder.of()), serde)
            .withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
  }
}
