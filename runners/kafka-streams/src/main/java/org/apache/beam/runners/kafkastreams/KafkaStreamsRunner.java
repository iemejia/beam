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
package org.apache.beam.runners.kafkastreams;

import org.apache.beam.runners.kafkastreams.translation.PipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link PipelineRunner}, translates a {@link Pipeline} into a {@link StreamsBuilder}
 * for running in {@link KafkaStreams}.
 */
public class KafkaStreamsRunner extends PipelineRunner<KafkaStreamsPipelineResult> {

  public static KafkaStreamsRunner fromOptions(PipelineOptions pipelineOptions) {
    PipelineOptionsValidator.validate(KafkaStreamsPipelineOptions.class, pipelineOptions);
    return new KafkaStreamsRunner(pipelineOptions);
  }

  private final KafkaStreamsPipelineOptions pipelineOptions;

  private KafkaStreamsRunner(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions.as(KafkaStreamsPipelineOptions.class);
  }

  @Override
  public KafkaStreamsPipelineResult run(Pipeline pipeline) {
    PipelineTranslator pipelineTranslator = PipelineTranslator.of(pipeline, pipelineOptions);
    Topology topology = pipelineTranslator.translate().build();
    LoggerFactory.getLogger(getClass()).error("Topology {}", topology.describe());
    KafkaStreams kafkaStreams =
        new KafkaStreams(topology, new StreamsConfig(pipelineOptions.getProperties()));
    return KafkaStreamsPipelineResult.of(kafkaStreams);
  }
}
