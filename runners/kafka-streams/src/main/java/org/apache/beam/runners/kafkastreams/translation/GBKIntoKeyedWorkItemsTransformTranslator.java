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
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.runners.kafkastreams.translation.mapper.KWindowedVToWindowedKeyedWorkItem;
import org.apache.beam.runners.kafkastreams.translation.mapper.WindowedKVToKWindowedV;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class GBKIntoKeyedWorkItemsTransformTranslator<K, InputT, W extends BoundedWindow>
    implements TransformTranslator<GBKIntoKeyedWorkItems<K, InputT>> {

  @Override
  @SuppressWarnings("unchecked")
  public void translate(
      PipelineTranslator pipelineTranslator, GBKIntoKeyedWorkItems<K, InputT> transform) {
    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    String applicationId = Admin.applicationId(pipelineOptions);
    String uniqueName = Admin.uniqueName(pipelineOptions, pipelineTranslator.getCurrentTransform());
    PCollection<KV<K, InputT>> input = pipelineTranslator.getInput(transform);
    KvCoder<K, InputT> coder = (KvCoder<K, InputT>) input.getCoder();
    Coder<K> keyCoder = coder.getKeyCoder();
    Coder<InputT> valueCoder = coder.getValueCoder();
    WindowingStrategy<?, W> windowingStrategy =
        (WindowingStrategy<?, W>) input.getWindowingStrategy();

    KStream<Void, WindowedValue<KV<K, InputT>>> stream = pipelineTranslator.getStream(input);

    KStream<K, WindowedValue<InputT>> keyStream = stream.map(new WindowedKVToKWindowedV<>());

    String topic = applicationId + "-" + uniqueName;
    Admin.createTopicIfNeeded(pipelineOptions, topic);
    KStream<K, WindowedValue<InputT>> groupByKeyOnlyStream =
        keyStream.through(
            topic,
            Produced.with(
                CoderSerde.of(keyCoder),
                CoderSerde.of(
                    WindowedValue.FullWindowedValueCoder.of(
                        valueCoder, windowingStrategy.getWindowFn().windowCoder()))));

    KStream<Void, WindowedValue<KeyedWorkItem<K, InputT>>> groupByKeyKeyedWorkItemStream =
        groupByKeyOnlyStream.flatMap(new KWindowedVToWindowedKeyedWorkItem<>());

    PCollection<KeyedWorkItem<K, InputT>> output = pipelineTranslator.getOutput(transform);
    pipelineTranslator.putStream(output, groupByKeyKeyedWorkItemStream);
    pipelineTranslator.putStreamSources(output, Collections.singleton(topic));
  }
}
