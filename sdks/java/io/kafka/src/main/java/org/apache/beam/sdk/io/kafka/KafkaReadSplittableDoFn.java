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
package org.apache.beam.sdk.io.kafka;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;

/** A SplittableDoFn to read from HBase. */
@UnboundedPerElement
class KafkaReadSplittableDoFn<K, V> extends DoFn<KafkaQuery, KafkaRecord<K, V>> {
  //    serializableConfiguration
  public KafkaReadSplittableDoFn(String bootstrapServers) {
    //    withBootstrapServers -> updateConsumerProperties
  }

  @Setup
  public void setup() throws Exception {
    //        connection = ConnectionFactory.createConnection(serializableConfiguration.get());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    // , RestrictionTracker<ByteKeyRange, ByteKey> tracker) {
  }

  //    @GetInitialRestriction
  //    public ByteKeyRange getInitialRestriction(HBaseQuery query) {
  //        return ByteKeyRange.of(
  //                ByteKey.copyFrom(query.getScan().getStartRow()),
  //                ByteKey.copyFrom(query.getScan().getStopRow()));
  //    }
  //
  //    @SplitRestriction
  //    public void splitRestriction(
  //            HBaseQuery query, ByteKeyRange range, Backlog backlog, OutputReceiver<ByteKeyRange>
  // receiver)
  //            throws Exception {
  //        List<HRegionLocation> regionLocations =
  //                HBaseUtils.getRegionLocations(connection, query.getTableId(), query.getScan());
  //        List<ByteKeyRange> splitRanges =
  //                HBaseUtils.getRanges(regionLocations, query.getTableId(), query.getScan());
  //        for (ByteKeyRange splitRange : splitRanges) {
  //            receiver.output(ByteKeyRange.of(splitRange.getStartKey(), splitRange.getEndKey()));
  //        }
  //    }
  //
  //    @NewTracker
  //    public ByteKeyRangeTracker newTracker(ByteKeyRange range) {
  //        return ByteKeyRangeTracker.of(range);
  //    }
  //
  @Teardown
  public void tearDown() throws Exception {
    //        if (connection != null) {
    //            connection.close();
    //            connection = null;
    //        }
  }
}
