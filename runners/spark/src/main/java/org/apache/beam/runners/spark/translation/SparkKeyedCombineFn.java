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

package org.apache.beam.runners.spark.translation;

import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import com.google.common.base.Optional;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.Arrays;
import java.util.Collections;

import scala.Tuple2;

/**
 * Spark runner implementation of {@link Combine.KeyedCombineFn}.
 *
 * This implementation uses {@link Optional} because, currently, {@link Aggregator}s
 * do not allow creating the buffer (aka accumulator in Beam) using the input, and having zero()
 * return null will skip this computation.
 * //TODO: remove usage of Optional once this is possible.
 */
class SparkKeyedCombineFn<K, InputT, AccumT, OutputT>
    extends Aggregator<KV<WindowedValue<K>, InputT>, Optional<Tuple2<WindowedValue<K>, AccumT>>,
    WindowedValue<OutputT>> {

  private final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> fn;

  public SparkKeyedCombineFn(Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> fn) {
    this.fn = fn;
  }

  @Override
  public Optional<Tuple2<WindowedValue<K>, AccumT>> zero() {
    return Optional.absent();
  }

  @Override
  public Optional<Tuple2<WindowedValue<K>, AccumT>> reduce(
      Optional<Tuple2<WindowedValue<K>, AccumT>> accumOpt, KV<WindowedValue<K>, InputT> in) {
    AccumT accum;
    WindowedValue<K> wk = in.getKey();
    if (!accumOpt.isPresent()) {
      accum = fn.createAccumulator(wk.getValue());
    } else {
      accum = accumOpt.get()._2();
    }
    accum = fn.addInput(wk.getValue(), accum, in.getValue());
    Tuple2<WindowedValue<K>, AccumT> output = new Tuple2<>(WindowedValue.of(wk.getValue(),
        wk.getTimestamp(), wk.getWindows(), wk.getPane()), accum);
    return Optional.of(output);
  }

  @Override
  public Optional<Tuple2<WindowedValue<K>, AccumT>> merge(
      Optional<Tuple2<WindowedValue<K>, AccumT>> accumOpt1,
      Optional<Tuple2<WindowedValue<K>, AccumT>> accumOpt2) {
    if (!accumOpt1.isPresent()) {
      return accumOpt2;
    } else if (!accumOpt2.isPresent()) {
      return accumOpt1;
    } else {
      WindowedValue<K> wk = accumOpt1.get()._1();
      Iterable<AccumT> accums = Collections.unmodifiableCollection(
          Arrays.asList(accumOpt1.get()._2(), accumOpt2.get()._2()));
      AccumT merged = fn.mergeAccumulators(wk.getValue(), accums);
      return Optional.of(new Tuple2<>(wk, merged));
    }
  }

  @Override
  public WindowedValue<OutputT>
  finish(Optional<Tuple2<WindowedValue<K>, AccumT>> reduction) {
    WindowedValue<K> wk = reduction.get()._1();
    AccumT accum = reduction.get()._2();
    return WindowedValue.of(fn.extractOutput(wk.getValue(), accum), wk.getTimestamp(),
        wk.getWindows(), wk.getPane());
  }

  @Override
  public Encoder<Optional<Tuple2<WindowedValue<K>, AccumT>>> bufferEncoder() {
    return EncoderHelpers.encoder();
  }

  @Override
  public Encoder<WindowedValue<OutputT>> outputEncoder() {
    return EncoderHelpers.encoder();
  }
}
