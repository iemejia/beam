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

import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link KStateInternals}. */
public class KTimerInternalsTest {

  private static final String UNIQUE = "UNIQUE";
  private static final String KEY = "KEY";
  private static final String ID_ONE = "ID_ONE";
  private static final String ID_TWO = "ID_TWO";
  private static final String ID_THREE = "ID_THREE";
  private static final String ID_FOUR = "ID_FOUR";
  private static final Instant INSTANT = Instant.now();

  private MockKeyValueStore<KV<String, String>, Instant> store;
  private KTimerInternals<String, GlobalWindow> timerInternals;

  @Before
  public void setUp() {
    store = new MockKeyValueStore<>();
    timerInternals =
        KTimerInternals.<String, GlobalWindow>of(
                UNIQUE,
                new MockProcessorContext(
                    Collections.singletonMap(UNIQUE + KTimerInternals.TIMER, store)),
                GlobalWindow.Coder.INSTANCE)
            .withKey(KEY);
  }

  @Test
  public void testSetAndGetFireableTimers() {
    Instant future = INSTANT.plus(1);
    timerInternals.setTimer(StateNamespaces.global(), ID_ONE, INSTANT, TimeDomain.EVENT_TIME);
    timerInternals.setTimer(StateNamespaces.global(), ID_TWO, INSTANT, TimeDomain.PROCESSING_TIME);
    timerInternals.setTimer(
        StateNamespaces.global(), ID_THREE, INSTANT, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    timerInternals.setTimer(
        StateNamespaces.global(), ID_FOUR, future, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    timerInternals.advanceInputWatermarkTime(future);
    timerInternals.advanceOutputWatermarkTime(future);
    timerInternals.advanceProcessingTime(future);
    timerInternals.advanceSynchronizedProcessingTime(future);
    List<KV<String, TimerData>> fireableTimers = timerInternals.getFireableTimers();
    Assert.assertEquals(3, fireableTimers.size());
    for (KV<String, TimerData> fireableTimer : fireableTimers) {
      Assert.assertEquals(KEY, fireableTimer.getKey());
      switch (fireableTimer.getValue().getTimerId()) {
        case ID_ONE:
          Assert.assertEquals(TimeDomain.EVENT_TIME, fireableTimer.getValue().getDomain());
          break;
        case ID_TWO:
          Assert.assertEquals(TimeDomain.PROCESSING_TIME, fireableTimer.getValue().getDomain());
          break;
        case ID_THREE:
          Assert.assertEquals(
              TimeDomain.SYNCHRONIZED_PROCESSING_TIME, fireableTimer.getValue().getDomain());
          break;
        default:
          Assert.fail("Unknown timerId.");
      }
      Assert.assertEquals(StateNamespaces.global(), fireableTimer.getValue().getNamespace());
      Assert.assertEquals(INSTANT, fireableTimer.getValue().getTimestamp());
    }
    Assert.assertEquals(1, store.map.size());
  }

  @Test
  public void testDelete() {
    timerInternals.setTimer(StateNamespaces.global(), ID_ONE, Instant.now(), TimeDomain.EVENT_TIME);
    timerInternals.deleteTimer(StateNamespaces.global(), ID_ONE);
    Assert.assertEquals(0, store.map.size());
  }
}
