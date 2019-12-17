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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test of the wrapping of Beam Coders as Spark ExpressionEncoders. */
@RunWith(JUnit4.class)
public class EncoderHelpersTest {

  @Test
  public void beamCoderToSparkEncoderTest() {
    SparkSession sparkSession =
        SparkSession.builder()
            .appName("beamCoderToSparkEncoderTest")
            .master("local[4]")
            .getOrCreate();
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> dataset =
        sparkSession.createDataset(data, EncoderHelpers.fromBeamCoder(VarIntCoder.of()));
    assertEquals(data, dataset.collectAsList());
  }

  @Test
  public void schemaCoderToSparkEncoderTest() {
    SparkSession sparkSession =
        SparkSession.builder()
            .appName("beamCoderToSparkEncoderTest")
            .master("local[4]")
            .getOrCreate();
    Schema schema =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();

    Row row1 = Row.withSchema(schema).addValues(1, "row", 1.0).build();
    Row row2 = Row.withSchema(schema).addValues(2, "row", 2.0).build();
    Row row3 = Row.withSchema(schema).addValues(3, "row", 3.0).build();
    List<Row> rows = Arrays.asList(row1, row2, row3);

    SchemaCoder<Row> coder = SchemaCoder.of(schema);
    Dataset<Row> dataset = sparkSession.createDataset(rows, EncoderHelpers.fromBeamCoder(coder));
    List<Row> results = dataset.collectAsList();
    assertEquals(rows, results);
  }
}
