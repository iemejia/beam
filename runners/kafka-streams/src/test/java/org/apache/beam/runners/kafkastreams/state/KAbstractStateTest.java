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

import java.io.IOException;
import java.util.Map;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** JUnit Test for {@link KAbstractState}. */
public class KAbstractStateTest {

  private static class MockStateNamespace implements StateNamespace {

    @Override
    public String stringKey() {
      return StateNamespaces.global().stringKey() + "*";
    }

    @Override
    public void appendTo(Appendable sb) throws IOException {}

    @Override
    public Object getCacheKey() {
      return null;
    }
  }

  private static final String KEY = "KEY";
  private static final Integer VALUE_ONE = 1;
  private static final Integer VALUE_TWO = 2;

  private MockKeyValueStore<String, Map<String, Integer>> keyValueStore;
  private KAbstractState<String, Integer> globalState;
  private KAbstractState<String, Integer> windowState;

  @Before
  public void setUp() {
    keyValueStore = new MockKeyValueStore<>();
    globalState =
        new KAbstractState<String, Integer>(KEY, StateNamespaces.global(), keyValueStore) {};
    windowState =
        new KAbstractState<String, Integer>(KEY, new MockStateNamespace(), keyValueStore) {};
  }

  @Test
  public void testSetFirstAndAdditionalThenGet() {
    globalState.set(VALUE_ONE);
    windowState.set(VALUE_TWO);
    Assert.assertEquals(VALUE_ONE, globalState.get());
    Assert.assertEquals(VALUE_TWO, windowState.get());
  }

  @Test
  public void testGetEmpty() {
    Assert.assertNull(globalState.get());
    Assert.assertNull(windowState.get());
  }

  @Test
  public void testClearEmpty() {
    globalState.clear();
    windowState.clear();
    Assert.assertNull(keyValueStore.keyValueStore.get(KEY));
  }

  @Test
  public void testClearValueAndAllNamespaces() {
    globalState.set(VALUE_ONE);
    windowState.set(VALUE_TWO);
    // Remove Value.
    globalState.clear();
    Assert.assertNotNull(keyValueStore.keyValueStore.get(KEY));
    // Remove All Namespaces.
    windowState.clear();
    Assert.assertNull(globalState.get());
    Assert.assertNull(windowState.get());
    Assert.assertNull(keyValueStore.keyValueStore.get(KEY));
  }
}
