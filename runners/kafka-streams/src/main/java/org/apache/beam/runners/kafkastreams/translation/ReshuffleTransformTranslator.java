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

import java.util.Set;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.admin.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

@SuppressWarnings({"unchecked", "deprecation"})
public class ReshuffleTransformTranslator<K, V, W extends BoundedWindow>
    implements TransformTranslator<Reshuffle<K, V>> {

  @Override
  public void translate(PipelineTranslator pipelineTranslator, Reshuffle<K, V> transform) {
    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    String applicationId = Admin.applicationId(pipelineOptions);
    String uniqueName = Admin.uniqueName(pipelineOptions, pipelineTranslator.getCurrentTransform());
    PCollection<KV<K, V>> input = pipelineTranslator.getInput(transform);
    KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = coder.getKeyCoder();
    Coder<V> valueCoder = coder.getValueCoder();
    WindowingStrategy<?, W> windowingStrategy =
        (WindowingStrategy<?, W>) input.getWindowingStrategy();
    PCollection<KV<K, V>> output = pipelineTranslator.getOutput(transform);
    Set<String> streamSources = pipelineTranslator.getStreamSources(input);

    KStream<Object, WindowedValue<KV<K, V>>> stream = pipelineTranslator.getStream(input);

    KStream<K, WindowedValue<V>> keyStream =
        stream.map(
            (key, value) ->
                KeyValue.pair(
                    value.getValue().getKey(),
                    WindowedValue.of(
                        value.getValue().getValue(),
                        value.getTimestamp(),
                        value.getWindows(),
                        value.getPane())));

    String topic = applicationId + "-" + uniqueName;
    Admin.createTopicIfNeeded(pipelineOptions, topic);
    KStream<K, WindowedValue<V>> groupByKeyStream =
        keyStream.through(
            topic,
            Produced.with(
                CoderSerde.of(keyCoder),
                CoderSerde.of(
                    WindowedValue.FullWindowedValueCoder.of(
                        valueCoder, windowingStrategy.getWindowFn().windowCoder()))));

    KStream<Object, WindowedValue<KV<K, V>>> reshuffleStream =
        groupByKeyStream.map(
            (key, windowedValue) ->
                KeyValue.pair(
                    null,
                    WindowedValue.of(
                        KV.of(key, windowedValue.getValue()),
                        windowedValue.getTimestamp(),
                        windowedValue.getWindows(),
                        windowedValue.getPane())));

    pipelineTranslator.putStream(output, reshuffleStream);
    pipelineTranslator.putStreamSources(output, streamSources);
  }
}
