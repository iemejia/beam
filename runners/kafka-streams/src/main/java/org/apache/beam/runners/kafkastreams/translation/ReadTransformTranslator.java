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
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.kafkastreams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafkastreams.admin.Admin;
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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams translator for the Beam {@link Read} primitive. Creates a topic (if this is the
 * first instance of the Kafka Streams application to run) for UnboundedSources and splits the
 * {@link UnboundedSource} so there is one per partition. Uses {@link StreamsBuilder#stream(String,
 * Consumed)} along with {@link KStream#transform(TransformerSupplier, String...)} to open and read
 * from an {@link UnboundedReader} starting at its {@link UnboundedSource.CheckpointMark}. After
 * reading records the UnboundedSource is written back to the UnboundedSources topic with its latest
 * CheckpointMark.
 */
public class ReadTransformTranslator<
        OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    implements TransformTranslator<PTransform<PBegin, PCollection<OutputT>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadTransformTranslator.class);

  @SuppressWarnings({"unchecked", "serial"})
  @Override
  public void translate(
      PipelineTranslator pipelineTranslator, PTransform<PBegin, PCollection<OutputT>> transform) {
    LoggerFactory.getLogger(getClass()).error("Translating Read {}", transform);
    KafkaStreamsPipelineOptions pipelineOptions = pipelineTranslator.getPipelineOptions();
    String topic = Admin.topic(pipelineTranslator.getCurrentTransform());
    UnboundedSource<OutputT, CheckpointMarkT> unboundedSource =
        getUnboundedSource(pipelineTranslator, transform);
    Coder<KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> unboundedSourceCoder =
        KvCoder.of(
            SerializableCoder.of(
                new TypeDescriptor<UnboundedSource<OutputT, CheckpointMarkT>>() {}),
            NullableCoder.of(unboundedSource.getCheckpointMarkCoder()));
    createTopicIfNeeded(pipelineOptions, topic, unboundedSource, unboundedSourceCoder);

    KStream<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> stream =
        pipelineTranslator
            .getStreamsBuilder()
            .stream(
                topic,
                Consumed.with(Serdes.Integer(), CoderSerde.of(unboundedSourceCoder))
                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));

    stream.print(Printed.toSysOut());

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

    KStream<Bytes, WindowedValue<OutputT>> outputStream =
        (KStream<Bytes, WindowedValue<OutputT>>) (KStream<?, ?>) readStreams[0];
    pipelineTranslator.putStream(pipelineTranslator.getOutput(transform), outputStream);

    KStream<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
        unboundedSourceStream =
            (KStream<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>)
                (KStream<?, ?>) readStreams[1];
    unboundedSourceStream.to(
        topic,
        Produced.with(
            Serdes.Integer(), CoderSerde.of(unboundedSourceCoder), (key, value, partition) -> key));
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
    LOG.error("createTopicIfNeeded...");
    try (AdminClient adminClient = Admin.adminClient(pipelineOptions)) {
      LOG.error("adminClient...");
      if (adminClient.listTopics().names().get().contains(topic)) {
        return;
      }
      adminClient
          .createTopics(
              Collections.singleton(
                  new NewTopic(
                      topic,
                      pipelineOptions.getNumPartitions(),
                      new StreamsConfig(pipelineOptions.getProperties())
                          .getInt(StreamsConfig.REPLICATION_FACTOR_CONFIG)
                          .shortValue())))
          .all()
          .get();
      LOG.error("adminClientCreateTopics...");
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    try (Producer<Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
        producer =
            Admin.producer(
                pipelineOptions, Serdes.Integer(), CoderSerde.of(unboundedSourceCoder))) {
      LOG.error("producer...");
      List<? extends UnboundedSource<OutputT, CheckpointMarkT>> unboundedSources =
          unboundedSource.split(pipelineOptions.getNumPartitions(), pipelineOptions);
      LOG.error(
          "Split Unbounded Source {} into {} pieces.", unboundedSource, unboundedSources.size());
      for (int i = 0; i < unboundedSources.size(); i++) {
        RecordMetadata metadata =
            producer
                .send(new ProducerRecord<>(topic, i, i, KV.of(unboundedSources.get(i), null)))
                .get();
        LOG.error(
            "Produced {} to topic {}, partition {}, and offset {}.",
            unboundedSources.get(i),
            topic,
            metadata.partition(),
            metadata.offset());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class ReadTransformer
      implements Transformer<
          Integer, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>,
          KeyValue<Object, Object>> {

    private final PipelineOptions pipelineOptions;
    private ProcessorContext processorContext;

    private ReadTransformer(PipelineOptions pipelineOptions) {
      this.pipelineOptions = pipelineOptions;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      LOG.error("Init...");
      this.processorContext = processorContext;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValue<Object, Object> transform(
        Integer key, KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> value) {
      LOG.error("Transform...");
      LOG.error(
          "Transforming Unbounded Source {} with Checkpoint Mark {}",
          value.getKey(),
          value.getValue());
      UnboundedSource<OutputT, CheckpointMarkT> unboundedSource = value.getKey();
      CheckpointMarkT checkpointMark = value.getValue();
      try (UnboundedReader<OutputT> unboundedReader =
          unboundedSource.createReader(pipelineOptions, checkpointMark)) {
        boolean dataAvailable = unboundedReader.start();
        int batchSize = 0;
        while (dataAvailable && batchSize < 1000) {
          LOG.error("Forwarding Current {}", unboundedReader.getCurrent());
          processorContext.forward(
              Bytes.wrap(unboundedReader.getCurrentRecordId()),
              WindowedValue.timestampedValueInGlobalWindow(
                  unboundedReader.getCurrent(), unboundedReader.getCurrentTimestamp()));
          dataAvailable = unboundedReader.advance();
          batchSize++;
        }
        checkpointMark = (CheckpointMarkT) unboundedReader.getCheckpointMark();
        checkpointMark.finalizeCheckpoint();
        LOG.error(
            "Forwarding Unbounded Source {} with Checkpoint Mark {}.",
            unboundedSource,
            checkpointMark);
        return KeyValue.pair(key, KV.of(unboundedSource, checkpointMark));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public KeyValue<Object, Object> punctuate(long timestamp) {
      return null;
    }

    @Override
    public void close() {}
  }
}
