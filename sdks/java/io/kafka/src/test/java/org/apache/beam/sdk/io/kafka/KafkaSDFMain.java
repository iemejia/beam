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
package org.apache.beam.sdk.io.kafka;

import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;

/** KafkaSDFMain. */
public class KafkaSDFMain {
  public static void main(String[] args) {
    final String bootstrapServers = "localhost";
    final String topic = "kafka-sdf-test";

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);
    p.apply("CreateSomeData", Create.of(KafkaQuery.of()))
        .apply("ToRabbitMqMessage", ParDo.of(new KafkaReadSplittableDoFn(null)));

    PCollection<String> data =
        p.apply(
                "ReadFromKafka",
                KafkaIO.<String, String>read()
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withBootstrapServers(bootstrapServers)
                    .withTopics(Collections.singletonList(topic))
                    .withoutMetadata())
            .apply("ExtractPayload", Values.<String>create());

    p.run().waitUntilFinish();
  }
}
