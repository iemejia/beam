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
package org.apache.beam.runners.kafkastreams.sideinput;

import java.util.Collections;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.kafkastreams.state.MockKeyValueStore;
import org.apache.beam.runners.kafkastreams.state.MockProcessorContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** JUnit Test for {@link KSideInputReader}. */
public class KSideInputReaderTest {

  @SuppressWarnings("unchecked")
  private static final Coder<BoundedWindow> CODER =
      (Coder<BoundedWindow>) (Coder<?>) GlobalWindow.Coder.INSTANCE;

  private static final String STORE_NAME = "STORE_NAME";
  private static final int VALUE = 1;

  private MockKeyValueStore<String, Integer> keyValueStore;
  private ProcessorContext processorContext;
  private PCollectionView<?> view;
  private KSideInputReader.KSideInput sideInput;
  private KSideInputReader sideInputReader;

  @Before
  public void setUp() {
    keyValueStore = new MockKeyValueStore<>();
    processorContext =
        new MockProcessorContext(Collections.singletonMap(STORE_NAME, keyValueStore));
    view = Mockito.mock(PCollectionView.class);
    sideInput = KSideInputReader.KSideInput.of(STORE_NAME, CODER);
    sideInputReader =
        KSideInputReader.of(processorContext, Collections.singletonMap(view, sideInput));
  }

  @Test
  public void testContains() {
    Assert.assertTrue(sideInputReader.contains(view));
    Assert.assertFalse(
        sideInputReader.contains((PCollectionView<?>) Mockito.mock(PCollectionView.class)));
  }

  @Test
  public void testGet() {
    keyValueStore.put(StateNamespaces.window(CODER, GlobalWindow.INSTANCE).stringKey(), VALUE);
    Assert.assertEquals(VALUE, sideInputReader.get(view, GlobalWindow.INSTANCE));
  }

  @Test
  public void testIsEmpty() {
    Assert.assertFalse(sideInputReader.isEmpty());
  }
}
