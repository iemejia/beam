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
package org.apache.beam.integration.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.integration.nexmark.NexmarkUtils;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * Result of Query6.
 */
public class SellerPrice implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();

  public static final Coder<SellerPrice> CODER = new CustomCoder<SellerPrice>() {
    @Override
    public void encode(SellerPrice value, OutputStream outStream,
        Coder.Context context)
        throws CoderException, IOException {
      LONG_CODER.encode(value.seller, outStream, Context.NESTED);
      LONG_CODER.encode(value.price, outStream, Context.NESTED);
    }

    @Override
    public SellerPrice decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      long seller = LONG_CODER.decode(inStream, Context.NESTED);
      long price = LONG_CODER.decode(inStream, Context.NESTED);
      return new SellerPrice(seller, price);
    }
    @Override public void verifyDeterministic() throws NonDeterministicException {}
  };

  @JsonProperty
  public final long seller;

  /** Price in cents. */
  @JsonProperty
  public final long price;

  // For Avro only.
  @SuppressWarnings("unused")
  private SellerPrice() {
    seller = 0;
    price = 0;
  }

  public SellerPrice(long seller, long price) {
    this.seller = seller;
    this.price = price;
  }

  @Override
  public long sizeInBytes() {
    return 8 + 8;
  }

  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
