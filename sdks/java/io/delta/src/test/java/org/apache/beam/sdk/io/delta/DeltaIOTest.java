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
package org.apache.beam.sdk.io.delta;

import java.io.Serializable;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test on the {@link DeltaIO}. */
@RunWith(JUnit4.class)
public class DeltaIOTest implements Serializable {
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Test
  public void testMatch() throws Exception {
    String path = "/home/ismael/datasets/delta-example/";
    PCollection<Metadata> latestMetadata = readPipeline.apply(DeltaIO.match().path(path));
    PAssert.thatSingleton(latestMetadata.apply("Count", Count.globally())).isEqualTo(10L);
    readPipeline.run().waitUntilFinish();
  }
}
