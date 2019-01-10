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

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.Instant;

/**
 * Kafka {@link TimerInternals}, accesses timer state and watermarkTime via the {@link
 * ProcessorContext} of a {@link Transformer}.
 */
public class KTimerInternals implements TimerInternals {

  @SuppressWarnings("unchecked")
  public static KTimerInternals of(ProcessorContext processorContext) {
    return new KTimerInternals(
        (KeyValueStore<String, Instant>) processorContext.getStateStore(TIMER_INTERNALS));
  }

  public static final String TIMER_INTERNALS = "TIMER_INTERNALS";

  private final KeyValueStore<String, Instant> keyValueStore;
  private final Set<String> eventTimers;
  private final Set<String> processingTimers;
  private final Set<String> synchronizedProcessingTimers;

  private Instant inputWatermarkTime;
  private Instant outputWatermarkTime;

  private KTimerInternals(KeyValueStore<String, Instant> keyValueStore) {
    this.keyValueStore = keyValueStore;
    this.eventTimers = new HashSet<>();
    this.processingTimers = new HashSet<>();
    this.synchronizedProcessingTimers = new HashSet<>();
    this.inputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    this.outputWatermarkTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  @Override
  public void setTimer(
      StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
    String key = timerId + "+" + namespace.stringKey();
    keyValueStore.put(key, target);
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
        throw new IllegalStateException("Unknown time domain.");
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

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Override
  public Instant currentSynchronizedProcessingTime() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  @Override
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  public String getFireableTimers() {
    return null;
  }
}
