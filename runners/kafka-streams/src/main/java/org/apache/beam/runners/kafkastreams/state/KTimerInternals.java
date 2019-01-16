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
package org.apache.beam.runners.kafkastreams.state;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.Instant;

/**
 * Kafka {@link TimerInternals}, accesses timer state and watermarkTime via the {@link
 * ProcessorContext} of a {@link Transformer}.
 */
public class KTimerInternals<K, W extends BoundedWindow> implements TimerInternals {

  public static final String TIMERS = "timers";

  @SuppressWarnings("unchecked")
  public static <K, W extends BoundedWindow> KTimerInternals<K, W> of(
      String statePrefix, ProcessorContext processorContext, Coder<W> windowCoder) {
    return new KTimerInternals<>(
        (KeyValueStore<String, KV<K, Instant>>)
            processorContext.getStateStore(statePrefix + TIMERS),
        windowCoder);
  }

  private final KeyValueStore<String, KV<K, Instant>> keyValueStore;
  private final Coder<W> windowCoder;
  private final Set<String> eventTimers;
  private final Set<String> processingTimers;
  private final Set<String> synchronizedProcessingTimers;

  private Instant processingTime;
  private Instant synchronizedProcessingTime;
  private Instant inputWatermarkTime;
  private Instant outputWatermarkTime;
  private K key;

  private KTimerInternals(
      KeyValueStore<String, KV<K, Instant>> keyValueStore, Coder<W> windowCoder) {
    this.keyValueStore = keyValueStore;
    this.windowCoder = windowCoder;
    this.eventTimers = new HashSet<>();
    this.processingTimers = new HashSet<>();
    this.synchronizedProcessingTimers = new HashSet<>();
    this.processingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.synchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.outputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  public KTimerInternals<K, W> withKey(K key) {
    this.key = key;
    return this;
  }

  @Override
  public void setTimer(
      StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
    String id = timerId + "+" + namespace.stringKey();
    keyValueStore.put(id, KV.of(key, target));
    switch (timeDomain) {
      case EVENT_TIME:
        eventTimers.add(timerId);
        break;
      case PROCESSING_TIME:
        processingTimers.add(timerId);
        break;
      case SYNCHRONIZED_PROCESSING_TIME:
        synchronizedProcessingTimers.add(timerId);
        break;
      default:
        throw new IllegalStateException("Invalid time domain: " + timeDomain);
    }
  }

  @Override
  public void setTimer(TimerData timerData) {
    setTimer(
        timerData.getNamespace(),
        timerData.getTimerId(),
        timerData.getTimestamp(),
        timerData.getDomain());
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, @Nullable TimeDomain timeDomain) {
    String key = timerId + "+" + namespace.stringKey();
    keyValueStore.delete(key);
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    deleteTimer(namespace, timerId, null);
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    deleteTimer(timerKey.getNamespace(), timerKey.getTimerId(), timerKey.getDomain());
  }

  public void advanceProcessingTime(Instant processingTime) {
    this.processingTime = processingTime;
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  public void advanceSynchronizedProcessingTime(Instant synchronizedProcessingTime) {
    this.synchronizedProcessingTime = synchronizedProcessingTime;
  }

  @Override
  public Instant currentSynchronizedProcessingTime() {
    return synchronizedProcessingTime;
  }

  public void advanceInputWatermarkTime(Instant inputWatermarkTime) {
    this.inputWatermarkTime = inputWatermarkTime;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  public void advanceOutputWatermarkTime(Instant outputWatermarkTime) {
    this.outputWatermarkTime = outputWatermarkTime;
  }

  @Override
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  public List<KV<K, TimerData>> getFireableTimers() {
    Iterator<KeyValue<String, KV<K, Instant>>> keyValueIterator = keyValueStore.all();
    List<KV<K, TimerData>> fireableTimers = new ArrayList<>();
    while (keyValueIterator.hasNext()) {
      KeyValue<String, KV<K, Instant>> keyValue = keyValueIterator.next();

      String id = keyValue.key;
      int lastIndexOfPlus = id.lastIndexOf("+");
      String timerId = id.substring(0, lastIndexOfPlus);
      String namespaceStringKey = id.substring(lastIndexOfPlus + 1);

      TimeDomain domain;
      Instant currentDomainTime;
      if (eventTimers.contains(timerId)) {
        domain = TimeDomain.EVENT_TIME;
        currentDomainTime = inputWatermarkTime;
      } else if (processingTimers.contains(timerId)) {
        domain = TimeDomain.PROCESSING_TIME;
        currentDomainTime = processingTime;
      } else if (synchronizedProcessingTimers.contains(timerId)) {
        domain = TimeDomain.SYNCHRONIZED_PROCESSING_TIME;
        currentDomainTime = synchronizedProcessingTime;
      } else {
        throw new RuntimeException("Invalid timerId: " + timerId);
      }

      Instant timestamp = keyValue.value.getValue();
      if (currentDomainTime.isAfter(timestamp)) {
        StateNamespace namespace = StateNamespaces.fromString(namespaceStringKey, windowCoder);
        TimerData timerData = TimerData.of(timerId, namespace, timestamp, domain);
        fireableTimers.add(KV.of(keyValue.value.getKey(), timerData));
      }
    }
    return fireableTimers;
  }
}
