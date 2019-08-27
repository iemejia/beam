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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.streams.kstream.KStream;

public class ProcessElementsTransformTranslator<InputT, OutputT, RestrictionT, PositionT>
    implements TransformTranslator<ProcessElements<InputT, OutputT, RestrictionT, PositionT>> {

  @Override
  public void translate(
      PipelineTranslator pipelineTranslator,
      ProcessElements<InputT, OutputT, RestrictionT, PositionT> transform) {

    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    PCollection<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> input =
        pipelineTranslator.getInput(transform);
    Coder<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> inputCoder = input.getCoder();
    Map<TupleTag<?>, PValue> outputs = pipelineTranslator.getOutputs(transform);
    Map<TupleTag<?>, Coder<?>> outputCoders = pipelineTranslator.getOutputCoders();
    TupleTag<OutputT> mainOutputTag = transform.getMainOutputTag();
    TupleTagList additionalOutputTags = transform.getAdditionalOutputTags();
    DoFnSchemaInformation doFnSchemaInformation = DoFnSchemaInformation.create();
    Set<String> streamSources = pipelineTranslator.getStreamSources(input);

    KStream<Void, WindowedValue<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>>> inputStream =
        pipelineTranslator.getStream(input);

    Map<PCollectionView<?>, String> sideInputs =
        ParDoTransformTranslator.sideInputs(
            pipelineTranslator, pipelineOptions, transform.getSideInputs());
    Collection<String> stateStoreNames = new HashSet<>(sideInputs.values());

    String unique =
        Admin.uniqueName(pipelineOptions, pipelineTranslator.getCurrentTransform()) + "-";
    stateStoreNames.addAll(
        ParDoTransformTranslator.stateStoreNames(pipelineTranslator, unique, ByteArrayCoder.of()));

    ParDoTransformTranslator.translate(
        pipelineTranslator,
        inputStream,
        pipelineOptions,
        () -> transform.newProcessFn(transform.getFn()),
        sideInputs,
        mainOutputTag,
        additionalOutputTags,
        inputCoder,
        outputCoders,
        input.getWindowingStrategy(),
        doFnSchemaInformation,
        streamSources,
        unique,
        stateStoreNames,
        outputs);
  }
}
