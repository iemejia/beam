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
package org.apache.beam.sdk.io.hbase;

import java.util.List;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/** A SplittableDoFn to read from HBase. */
@BoundedPerElement
class HBaseReadSplittableDoFn extends DoFn<Void, Result> {
  private final SerializableConfiguration serializableConfiguration;
  private final String tableId;
  private final SerializableScan scan;

  private Connection connection;

  HBaseReadSplittableDoFn(
      SerializableConfiguration serializableConfiguration, String tableId, SerializableScan scan) {
    this.serializableConfiguration = serializableConfiguration;
    this.tableId = tableId;
    this.scan = scan;
  }

  @Setup
  public void setup() throws Exception {
    if (connection == null) {
      connection = ConnectionFactory.createConnection(serializableConfiguration.get());
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c, ByteKeyRangeTracker tracker) throws Exception {
    TableName tableName = TableName.valueOf(tableId);
    Table table = connection.getTable(tableName);
    final ByteKeyRange range = tracker.currentRestriction();
    Scan newScan =
        new Scan(scan.get())
            .setStartRow(range.getStartKey().getBytes())
            .setStopRow(range.getEndKey().getBytes());
    try (ResultScanner scanner = table.getScanner(newScan)) {
      for (Result result : scanner) {
        ByteKey key = ByteKey.copyFrom(result.getRow());
        if (!tracker.tryClaim(key)) {
          return;
        }
        c.output(result);
      }
      tracker.markDone();
    }
  }

  @GetInitialRestriction
  public ByteKeyRange getInitialRestriction(Void v) {
    return ByteKeyRange.of(
        ByteKey.copyFrom(scan.get().getStartRow()), ByteKey.copyFrom(scan.get().getStopRow()));
  }

  @SplitRestriction
  public void splitRestriction(Void v, ByteKeyRange range, OutputReceiver<ByteKeyRange> receiver)
      throws Exception {
    // TODO remove once BEAM-4016 is fixed (lifecycle ignores calling setup before splitRestriction)
    setup();

    List<ByteKeyRange> splitRanges =
        HBaseUtils.getRanges(
            HBaseUtils.getRegionLocations(connection, tableId, scan.get()), tableId, scan.get());
    if (splitRanges.size() > 1) {
      for (ByteKeyRange splitRange : splitRanges) {
        receiver.output(ByteKeyRange.of(splitRange.getStartKey(), splitRange.getEndKey()));
      }
      return;
    }
    receiver.output(range);
  }

  @NewTracker
  public ByteKeyRangeTracker newTracker(ByteKeyRange range) {
    return ByteKeyRangeTracker.of(range);
  }

  @Teardown
  public void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }
}
