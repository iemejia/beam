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
package org.apache.beam.sdk.io.range;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A restriction represented by a range of integers [from, to). */
public class BigIntegerRange implements Serializable {
  private final BigInteger from;
  private final BigInteger to;

  private BigIntegerRange(BigInteger from, BigInteger to) {
    checkArgument(from.compareTo(to) < 1, "Malformed range [%s, %s)", from, to);
    this.from = from;
    this.to = to;
  }

  public static BigIntegerRange of(BigInteger from, BigInteger to) {
    return new BigIntegerRange(from, to);
  }

  public BigInteger getFrom() {
    return from;
  }

  public BigInteger getTo() {
    return to;
  }

  @Override
  public String toString() {
    return "[" + from + ", " + to + ')';
  }

  @Override
  public boolean equals(Object o) {
    return Objects.equals(this, o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }

  /** A coder for {@link BigIntegerRange}s. */
  public static class Coder extends AtomicCoder<BigIntegerRange> {
    private static final Coder INSTANCE = new Coder();
    private static final TypeDescriptor<BigIntegerRange> TYPE_DESCRIPTOR =
        new TypeDescriptor<BigIntegerRange>() {};

    public static Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(BigIntegerRange value, OutputStream outStream)
        throws CoderException, IOException {
      BigIntegerCoder.of().encode(value.getFrom(), outStream);
      BigIntegerCoder.of().encode(value.getTo(), outStream);
    }

    @Override
    public BigIntegerRange decode(InputStream inStream) throws CoderException, IOException {
      return BigIntegerRange.of(
          BigIntegerCoder.of().decode(inStream), BigIntegerCoder.of().decode(inStream));
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(BigIntegerRange value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(BigIntegerRange value) throws Exception {
      return BigIntegerCoder.of().getEncodedElementByteSize(value.getFrom())
          + BigIntegerCoder.of().getEncodedElementByteSize(value.getTo());
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public TypeDescriptor<BigIntegerRange> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }
  }
}
