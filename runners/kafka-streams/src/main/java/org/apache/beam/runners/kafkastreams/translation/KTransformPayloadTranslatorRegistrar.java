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

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

@AutoService(TransformPayloadTranslatorRegistrar.class)
public class KTransformPayloadTranslatorRegistrar implements TransformPayloadTranslatorRegistrar {

  @Override
  @SuppressWarnings("rawtypes")
  public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
      getTransformPayloadTranslators() {
    return ImmutableMap
        .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
        .put(
            SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems.class,
            new SplittableParDoGbkIntoKeyedWorkItemsPayloadTranslator())
        .put(
            SplittableParDoViaKeyedWorkItems.ProcessElements.class,
            new SplittableParDoProcessElementsTranslator())
        .build();
  }

  /**
   * A translator just to vend the URN. This will need to be moved to runners-core-construction-java
   * once SDF is reorganized appropriately.
   */
  private static class SplittableParDoProcessElementsTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
          SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?>> {

    private SplittableParDoProcessElementsTranslator() {}

    @Override
    public String getUrn(SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?> transform) {
      return SplittableParDo.SPLITTABLE_PROCESS_URN;
    }
  }

  /**
   * A translator just to vend the URN. This will need to be moved to runners-core-construction-java
   * once SDF is reorganized appropriately.
   */
  private static class SplittableParDoGbkIntoKeyedWorkItemsPayloadTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
          SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems<?, ?>> {

    private SplittableParDoGbkIntoKeyedWorkItemsPayloadTranslator() {}

    @Override
    public String getUrn(SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems<?, ?> transform) {
      return SplittableParDo.SPLITTABLE_GBKIKWI_URN;
    }
  }
}
