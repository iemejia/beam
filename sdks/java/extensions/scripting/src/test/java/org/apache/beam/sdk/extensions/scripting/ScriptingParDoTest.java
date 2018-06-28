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
package org.apache.beam.sdk.extensions.scripting;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for the ScriptingParDo transform. */
public class ScriptingParDoTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testJavascript() {
    final PCollection<Double> out =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(ParDo.of(new ToJsonFn()))
            .apply(
                new ScriptingParDo<String, Double>() {}.withLanguage("js")
                    .withScript(
                        "var value = JSON.parse(context.element());"
                            + "var x = parseInt(value.id);"
                            + "x * x;"));
    PAssert.that(out).containsInAnyOrder(1d, 4d, 9d);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testJython() {
    final PCollection<Double> out =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(ParDo.of(new ToJsonFn()))
            .apply(
                new ScriptingParDo<String, Double>() {}.withLanguage("jython")
                    .withScript(
                        "import json\n"
                            + "value = json.loads(context.element())\n"
                            + "context.output(float(value['id'] * value['id']))"));
    PAssert.that(out).containsInAnyOrder(1d, 4d, 9d);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testJavascriptInvalidScript() {
    final PCollection<Double> out =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(ParDo.of(new ToJsonFn()))
            .apply(
                new ScriptingParDo<String, Double>() {}.withLanguage("js")
                    .withScript("var x = parseInt(value.id);" + "x * x;"));
    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectCause(Matchers.instanceOf(IllegalStateException.class));
    thrown.expectMessage("ReferenceError: \"value\" is not defined in <eval> at line number 1");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnavailableLanguage() {
    final PCollection<Double> out =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(ParDo.of(new ToJsonFn()))
            .apply(
                new ScriptingParDo<String, Double>() {}.withLanguage("ocaml")
                    .withScript("let square = x * x\n" + "square"));
    //    thrown.expect(Pipeline.PipelineExecutionException.class);
    //    thrown.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
    //    thrown.expectCause(Matchers.instanceOf(UserCodeException.class));
    pipeline.run().waitUntilFinish();
  }

  private static class ToJsonFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(
          String.format("{\"id\": " + c.element() + ", \"name\": \"person" + c.element() + "\"}"));
    }
  }
}
