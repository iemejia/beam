package org.apache.beam.sdk.io.kafka;

import java.util.List;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaReadTransformTranslator {

  public static boolean isKafkaTopicUnboundedSource(UnboundedSource<?, ?> unboundedSource) {
    try {
      Class.forName("org.apache.beam.sdk.io.kafka.KafkaUnboundedSource");
    } catch (ClassNotFoundException e) {
      return false;
    }
    if (unboundedSource instanceof KafkaUnboundedSource) {
      KafkaIO.Read<?, ?> spec = ((KafkaUnboundedSource<?, ?>) unboundedSource).getSpec();
      return spec.getTopicPartitions().isEmpty();
    } else {
      return false;
    }
  }

  public static <K, V> KStream<K, V> kafkaTopicRead(StreamsBuilder streamsBuilder, KafkaUnboundedSource<K, V> kafkaUnboundedSource) {
    KafkaIO.Read<K, V> spec = kafkaUnboundedSource.getSpec();
    List<String> topics = spec.getTopics();
    Serde<K> keySerde = serdeFrom(spec.getKeyDeserializer());
    Serde<V> valueSerde = serdeFrom(spec.getValueDeserializer());

    return streamsBuilder.stream(topics, Consumed.with(keySerde, valueSerde));
  }

  private static <T> Serde<T> serdeFrom(Class<? extends Deserializer<T>> clazz) {
    try {
      Deserializer<T> deserializer = clazz.getConstructor().newInstance();
      // TODO: Possibly call configure on the deserializer.
      return Serdes.serdeFrom(null, deserializer);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
