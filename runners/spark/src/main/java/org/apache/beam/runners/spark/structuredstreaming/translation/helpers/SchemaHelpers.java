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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.Instant;

/** A set of utils to transform between Beam and Spark Schemas/Row representations. */
@Experimental(Experimental.Kind.SCHEMAS)
public class SchemaHelpers {
  static DataType toSparkType(Schema.FieldType fieldType) {
    Schema.TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case BYTE:
        return DataTypes.ByteType;
      case INT16:
        return DataTypes.ShortType;
      case INT32:
        return DataTypes.IntegerType;
      case INT64:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case STRING:
        return DataTypes.StringType;
      case BYTES:
        return DataTypes.BinaryType;
      case DECIMAL:
        // TODO check on trickiness here
        return DataTypes.createDecimalType();
      case DATETIME:
        // TODO check on trickiness here
        return DataTypes.DateType;
      case ARRAY:
      case ITERABLE:
        Schema.FieldType elementType = fieldType.getCollectionElementType();
        DataType elementDataType = toSparkType(elementType);
        // TODO can we check if collection accepts nulls?
        return DataTypes.createArrayType(elementDataType);
      case MAP:
        Schema.FieldType keyType = fieldType.getMapKeyType();
        Schema.FieldType valueType = fieldType.getMapValueType();
        DataType keyDataType = toSparkType(keyType);
        DataType valueDataType = toSparkType(valueType);
        return DataTypes.createMapType(keyDataType, valueDataType);
      case ROW:
        throw new UnsupportedOperationException("ROW not yet supported");
      case LOGICAL_TYPE:
        throw new UnsupportedOperationException("LOGICAL_TYPE not yet supported");
      default:
        throw new AssertionError("Unexpected Beam type: " + typeName);
    }
  }

  static StructType toSparkSchema(Schema schema) {
    List<StructField> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      final Schema.FieldType fieldType = field.getType();
      DataType dataType = toSparkType(fieldType);
      // TODO Add metadata when Beam exposes it
      fields.add(
          new StructField(field.getName(), dataType, fieldType.getNullable(), Metadata.empty()));
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  static <T> Row toRow(WindowedValue<T> wv) {
    T value = wv.getValue();
    if (value instanceof Row) {
      // TODO can we get in with not exploding the values and just make a Row column?
      Row row = (Row) value;
      Schema windowedValueSchema =
          Schema.builder()
              .addFields(row.getSchema().getFields())
              .addDateTimeField("beam.internal.timestamp")
              // TODO These two should have a better representation
              //              .addByteArrayField("beam.internal.windows")
              //              .addByteArrayField("beam.internal.pane")
              .addStringField("beam.internal.windows")
              .addStringField("beam.internal.pane")
              .build();

      List<Object> values = row.getValues();
      Instant timestamp = wv.getTimestamp();
      values.add(timestamp);
      Collection<? extends BoundedWindow> windows = wv.getWindows();
      // TODO add windows
      values.add("");
      // TODO add windows
      PaneInfo pane = wv.getPane();
      values.add("");

      Row windowedValueRow = Row.withSchema(windowedValueSchema).addValues(values).build();
      return windowedValueRow;
    } else {
      throw new UnsupportedOperationException("Only row based PCollections are supported");
    }
  }

  public static void testToSparkSchema() {
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
    StructType sparkSchema = toSparkSchema(schema);
    System.out.println(sparkSchema);
  }

  public static void testToRow() {
    Schema valueSchema =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();
    Row row = Row.withSchema(valueSchema).addValues(1, "row", 1.0).build();
    WindowedValue<Row> wv = WindowedValue.valueInGlobalWindow(row);
    Row windowedValuerow = toRow(wv);
    System.out.println(windowedValuerow);
  }

  // TODO Spark SQL based operations based on WindowedValueRow
  public static void main(String[] args) {
    testToRow();

    SchemaCoder<WindowedValue<T>> x;
  }
}
