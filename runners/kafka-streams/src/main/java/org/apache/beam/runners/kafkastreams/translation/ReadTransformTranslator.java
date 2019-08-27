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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.client.Admin;
import org.apache.beam.runners.kafkastreams.serde.CoderSerde;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

/**
 * Kafka Streams translator for the Beam {@link Read} primitive. Creates a topic (if this is the
 * first instance of the Kafka Streams application to run) for UnboundedSources and splits the
 * {@link UnboundedSource} so there is one per partition. Uses {@link StreamsBuilder#stream(String,
 * Consumed)} along with {@link KStream#transform(TransformerSupplier, String...)} to open and read
 * from an {@link UnboundedReader} starting at its {@link UnboundedSource.CheckpointMark}. After
 * reading records the UnboundedSource is written back to the UnboundedSources topic with its latest
 * CheckpointMark.
 *
 * @deprecated Use ImpulseTransformTranslator.
 */
@Deprecated
public class ReadTransformTranslator<
        OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    implements TransformTranslator<PTransform<PBegin, PCollection<OutputT>>> {

  @SuppressWarnings({"unchecked", "serial"})
  @Override
  public void translate(
      PipelineTranslator pipelineTranslator, PTransform<PBegin, PCollection<OutputT>> transform) {
    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    String topic = Admin.topic(pipelineOptions, pipelineTranslator.getCurrentTransform());
    UnboundedSource<OutputT, CheckpointMarkT> unboundedSource =
        getUnboundedSource(pipelineTranslator, transform);
    Coder<KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> unboundedSourceCoder =
        KvCoder.of(
            SerializableCoder.of(
                new TypeDescriptor<UnboundedSource<OutputT, CheckpointMarkT>>() {}),
            NullableCoder.of(unboundedSource.getCheckpointMarkCoder()));
    createTopicIfNeeded(pipelineOptions, topic, unboundedSource, unboundedSourceCoder);

    KStream<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> stream =
        pipelineTranslator.getStreamsBuilder().stream(
            topic,
            Consumed.with(Serdes.Integer(), CoderSerde.of(unboundedSourceCoder))
                .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));

    Predicate<Object, Object>[] predicates =
        new Predicate[] {
          new Predicate<Object, Object>() {
            @Override
            public boolean test(Object key, Object value) {
              return key instanceof Bytes;
            }
          },
          new Predicate<Object, Object>() {
            @Override
            public boolean test(Object key, Object value) {
              return key instanceof Integer;
            }
          }
        };
    KStream<Object, Object>[] readStreams =
        stream.transform(() -> new ReadTransformer(pipelineOptions)).branch(predicates);

    KStream<Void, WindowedValue<OutputT>> outputStream =
        ((KStream<Bytes, WindowedValue<OutputT>>) (KStream<?, ?>) readStreams[0])
            .map((key, value) -> KeyValue.pair(null, value));
    PCollection<OutputT> output = pipelineTranslator.getOutput(transform);
    pipelineTranslator.putStream(output, outputStream);
    pipelineTranslator.putStreamSources(output, Collections.singleton(topic));

    KStream<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
        unboundedSourceStream =
            (KStream<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>)
                (KStream<?, ?>) readStreams[1];
    unboundedSourceStream.to(
        topic,
        Produced.with(Serdes.Integer(), CoderSerde.of(unboundedSourceCoder), (t, k, v, p) -> k));
  }

  @SuppressWarnings("unchecked")
  private UnboundedSource<OutputT, CheckpointMarkT> getUnboundedSource(
      PipelineTranslator pipelineTranslator, PTransform<PBegin, PCollection<OutputT>> transform) {
    AppliedPTransform<PBegin, PCollection<OutputT>, PTransform<PBegin, PCollection<OutputT>>>
        appliedPTransform =
            (AppliedPTransform<
                    PBegin, PCollection<OutputT>, PTransform<PBegin, PCollection<OutputT>>>)
                pipelineTranslator.getCurrentTransform();
    try {
      UnboundedSource<OutputT, CheckpointMarkT> unboundedSource;
      if (pipelineTranslator
          .getOutput(transform)
          .isBounded()
          .equals(PCollection.IsBounded.BOUNDED)) {
        unboundedSource =
            (UnboundedSource<OutputT, CheckpointMarkT>)
                new UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter<OutputT>(
                    ReadTranslation.boundedSourceFromTransform(appliedPTransform));
      } else {
        unboundedSource = ReadTranslation.unboundedSourceFromTransform(appliedPTransform);
      }
      return unboundedSource;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createTopicIfNeeded(
      KafkaStreamsPipelineOptions pipelineOptions,
      String topic,
      UnboundedSource<OutputT, CheckpointMarkT> unboundedSource,
      Coder<KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> unboundedSourceCoder) {
    if (Admin.createTopicIfNeeded(pipelineOptions, topic)) {
      try (Producer<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
          producer =
              Admin.producer(
                  pipelineOptions, Serdes.Integer(), CoderSerde.of(unboundedSourceCoder))) {
        List<? extends UnboundedSource<OutputT, CheckpointMarkT>> unboundedSources =
            unboundedSource.split(pipelineOptions.getNumPartitions(), pipelineOptions);
        for (int i = 0; i < unboundedSources.size(); i++) {
          producer
              .send(new ProducerRecord<>(topic, i, i, KV.of(unboundedSources.get(i), null)))
              .get();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class ReadTransformer
      implements Transformer<
          Integer,
          KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>,
          KeyValue<Object, Object>> {

    private final PipelineOptions pipelineOptions;
    private ProcessorContext processorContext;

    private ReadTransformer(PipelineOptions pipelineOptions) {
      this.pipelineOptions = pipelineOptions;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      this.processorContext = processorContext;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValue<Object, Object> transform(
        Integer key, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> value) {
      UnboundedSource<OutputT, CheckpointMarkT> unboundedSource = value.getKey();
      CheckpointMarkT checkpointMark = value.getValue();
      try (UnboundedReader<OutputT> unboundedReader =
          unboundedSource.createReader(pipelineOptions, checkpointMark)) {
        boolean dataAvailable = unboundedReader.start();
        int batchSize = 0;
        while (dataAvailable && batchSize < 1000) {
          processorContext.forward(
              Bytes.wrap(unboundedReader.getCurrentRecordId()),
              WindowedValue.timestampedValueInGlobalWindow(
                  unboundedReader.getCurrent(), unboundedReader.getCurrentTimestamp()),
              To.all().withTimestamp(unboundedReader.getCurrentTimestamp().getMillis()));
          dataAvailable = unboundedReader.advance();
          batchSize++;
        }
        checkpointMark = (CheckpointMarkT) unboundedReader.getCheckpointMark();
        checkpointMark.finalizeCheckpoint();
        return KeyValue.pair(key, KV.of(unboundedSource, checkpointMark));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {}
  }
}
