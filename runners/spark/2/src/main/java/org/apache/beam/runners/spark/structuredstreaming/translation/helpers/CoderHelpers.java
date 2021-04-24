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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Serialization utility class. */
public final class CoderHelpers {
  private CoderHelpers() {}

  /**
   * Utility method for serializing an object using the specified coder.
   *
   * @param value Value to serialize.
   * @param coder Coder to serialize with.
   * @param <T> type of value that is serialized
   * @return Byte array representing serialized object.
   */
  public static <T> byte[] toByteArray(T value, Coder<T> coder) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      coder.encode(value, baos);
    } catch (IOException e) {
      throw new IllegalStateException("Error encoding value: " + value, e);
    }
    return baos.toByteArray();
  }

  /**
   * Utility method for deserializing a byte array using the specified coder.
   *
   * @param serialized bytearray to be deserialized.
   * @param coder Coder to deserialize with.
   * @param <T> Type of object to be returned.
   * @return Deserialized object.
   */
  public static <T> T fromByteArray(byte[] serialized, Coder<T> coder) {
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    try {
      return coder.decode(bais);
    } catch (IOException e) {
      throw new IllegalStateException("Error decoding bytes for coder: " + coder, e);
    }
  }

  /**
   * @return a {@link WindowedValueCoder} depending on the given {@link WindowingStrategy} coder, if
   *     it is based on {@link GlobalWindows} it creates a {@link ValueOnlyWindowedValueCoder} to
   *     optimize encoding, otherwise it creates a {@link FullWindowedValueCoder}.
   */
  public static <T> WindowedValueCoder<T> windowedValueCoder(
      Coder<T> valueCoder, WindowingStrategy<?, ?> windowingStrategy) {
    WindowFn<?, ?> windowFn = windowingStrategy.getWindowFn();
    Coder<? extends BoundedWindow> windowCoder = windowFn.windowCoder();
    return (windowFn instanceof GlobalWindows)
        ? ValueOnlyWindowedValueCoder.of(valueCoder)
        : FullWindowedValueCoder.of(valueCoder, windowCoder);
  }
}
