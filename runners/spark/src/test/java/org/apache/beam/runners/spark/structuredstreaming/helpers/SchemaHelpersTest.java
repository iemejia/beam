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
package org.apache.beam.runners.spark.structuredstreaming.helpers;

import static org.junit.Assert.assertNotNull;

import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SchemaHelpers;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.spark.sql.RandomDataGenerator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.Function0;
import scala.Option;
import scala.util.Random;

/** Tests for SchemaHelpers. */
@RunWith(JUnit4.class)
public class SchemaHelpersTest {

  @Test
  public void testToSparkSchema() {
    Schema schema =
        Schema.builder()
            .addByteField("f_byte")
            .addInt16Field("f_int16")
            .addInt32Field("f_int32")
            .addInt64Field("f_int64")
            .addDecimalField("f_decimal")
            .addFloatField("f_float")
            .addDoubleField("f_double")
            .addStringField("f_string")
            .addDateTimeField("f_datetime")
            .addBooleanField("f_boolean")
            .addByteArrayField("f_bytearray")
            .addArrayField("f_array", Schema.FieldType.STRING)
            .addMapField("f_map", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();
    StructType sparkSchema = SchemaHelpers.toSparkSchema(schema);
    Option<Function0<Object>> function0Option =
        RandomDataGenerator.forType(sparkSchema, false, new Random());

    System.out.println(sparkSchema);
  }

  @Test
  public void testToRow() {
    Schema valueSchema =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();
    Row row = Row.withSchema(valueSchema).addValues(1, "row", 1.0).build();
    WindowedValue<Row> wv = WindowedValue.valueInGlobalWindow(row);
    Row windowedValuerow = SchemaHelpers.toWindowedValueRow(wv);
    System.out.println(windowedValuerow);
  }

  @Test
  public void testToInternalRow() {
    Schema valueSchema =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();
    Row row = Row.withSchema(valueSchema).addValues(1, "row", 1.0).build();
    InternalRow internalRow = SchemaHelpers.toInternalRow(row);
    assertNotNull(internalRow);
  }
}
