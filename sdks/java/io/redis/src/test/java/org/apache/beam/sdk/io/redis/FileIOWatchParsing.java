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
package org.apache.beam.sdk.io.redis;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raads .jpg files from a directory (it can be in a distributed filesystem). And extracts its width
 * and height and saves them as an Avro object,
 */
public class FileIOWatchParsing {
  private static final Logger LOG = LoggerFactory.getLogger(FileIOWatchParsing.class);

  /** Specific pipeline options. */
  public interface Options extends PipelineOptions {
    @Description("Input Path")
    String getInput();

    void setInput(String value);

    @Description("Output Path")
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    //    final String inputPath = options.getInput();
    final String outputPath = options.getOutput();

    final String inputPath = "/tmp/beam-test-redis-files/*";
    //    final String outputPath = "~/temp/parsing/example-";

    // .withValidation().as(EventsByLocation.Options.class);
    LOG.info(options.toString());
    Pipeline pipeline = Pipeline.create(options);

    PCollection<MatchResult.Metadata> matches =
        pipeline.apply(
            "MatchAllReadFiles",
            FileIO.match()
                .filepattern(inputPath)
                .continuously(Duration.standardSeconds(10), Watch.Growth.never()));
    matches.apply(
        "ParseFiles",
        ParDo.of(
            new DoFn<MatchResult.Metadata, MatchResult.Metadata>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                System.out.println(c.timestamp() + ": " + c.element());
                //                        c.outputWithTimestamp(c.element(), c.timestamp());
                c.output(c.element());
              }
            }));

    PCollection<String> lines =
        matches.apply("ReadFiles", FileIO.readMatches()).apply("ParseFiles", TextIO.readFiles());

    PCollection<String> windowedOutput =
        lines.apply(
            Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .accumulatingFiredPanes());
    //                        AfterProcessingTime.pastFirstElementInPane()
    //                            .plusDelayOf(Duration.standardSeconds(5)))));

    //    PCollection<String> windowedOutput =
    //        lines.apply(
    //            Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
    //                .triggering(
    //                    Repeatedly.forever(
    //                        AfterProcessingTime.pastFirstElementInPane()
    //                            .plusDelayOf(Duration.standardSeconds(5))))
    //                .withAllowedLateness(Duration.standardSeconds(5))
    //                .accumulatingFiredPanes());

    PCollection<KV<String, String>> kvs =
        windowedOutput
            .apply(
                "ParseFiles",
                ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        System.out.println(c.timestamp() + ": " + c.element());
                        c.output(c.element());
                      }
                    }))
            //                .apply(
            //                        "ParseFiles",
            //                        ParDo.of(
            //                                new DoFn<String, String>() {
            //                                  @ProcessElement
            //                                  public void processElement(DoFn.ProcessContext c)
            // throws Exception {
            //                                    System.out.println("DOS" + c.timestamp() + ": " +
            // c.element());
            //                                    c.output(c.element());
            //                                  }
            //                                }))
            .apply("ToRedis", RedisIO.readAll().withEndpoint("localhost", 6379));

    kvs.apply(
        "ParseFiles",
        ParDo.of(
            new DoFn<KV<String, String>, KV<String, String>>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                System.out.println(c.timestamp() + ": " + c.element());
                c.output(c.element());
              }
            }));

    pipeline.run().waitUntilFinish();
  }
}
