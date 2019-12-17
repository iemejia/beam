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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.schema;

import static org.apache.beam.sdk.annotations.Experimental.Kind.SCHEMAS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.schemas.transforms.Select.Fields;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

@Experimental(SCHEMAS)
public class SelectTranslatorBatch<InputT, OutputT>
    implements TransformTranslator<PTransform<PCollection<InputT>, PCollection<OutputT>>> {

  private List<Column> selectColumns(Select.Fields<InputT> fields) {
    FieldAccessDescriptor fieldAccessDescriptor = fields.getFieldAccessDescriptor();
    List<FieldAccessDescriptor.FieldDescriptor> fieldsAccessed =
        fieldAccessDescriptor.getFieldsAccessed();
    List<Column> columns = new ArrayList<>();
    // TODO what happens on errors
    for (FieldAccessDescriptor.FieldDescriptor fieldDescriptor : fieldsAccessed) {
      // TODO can we inject a catalyst expression here udf for more advanced cases
      Column col = functions.col(fieldDescriptor.getFieldName());
      columns.add(col);
      // TODO qualifiers
      // fieldDescriptor.getQualifiers()
    }
    // TODO we should always select the WindowValue columns too and refactor
    columns.add(functions.col("beam.internal.timestamp"));
    columns.add(functions.col("beam.internal.windows"));
    columns.add(functions.col("beam.internal.pane"));
    return columns;
  }

  @Override
  public void translateTransform(
      PTransform<PCollection<InputT>, PCollection<OutputT>> transform, TranslationContext context) {
    PCollection<InputT> input = (PCollection<InputT>) context.getInput();
    checkArgument(
        input.hasSchema(), "This special translation is only for Schema-based PCollections");
    Dataset<WindowedValue<InputT>> dataset = context.getDataset(input);
    DoFn<InputT, OutputT> doFn = getDoFn(context);
    DoFnSchemaInformation doFnSchemaInformation =
        ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
    List<SerializableFunction<?, ?>> elementConverters =
        doFnSchemaInformation.getElementConverters();

    if (dataset != null) {
      dataset.printSchema();
      // We convert dataset to Beam Row
      //      dataset.map(new MapFunction<WindowedValue<InputT>, Row>() {
      //        @Override
      //        public Row call(WindowedValue<InputT> value) {
      //          Row row = SchemaHelpers.toRow(value);
      ////        return
      //          return row;
      //        }
      //      });
      // convert Beam row to spark row
      Fields fields = (Fields) transform;
      List<Column> columns = selectColumns(fields);

      Dataset<org.apache.spark.sql.Row> selectDataset =
          dataset.select(columns.toArray(new Column[0]));
      //      selectDataset.exprEnc().
      Dataset<WindowedValue<OutputT>> output = null;
      context.putDataset(context.getOutput(), output);
    }
    // TODO what to do if not
  }

  @SuppressWarnings("unchecked")
  private DoFn<InputT, OutputT> getDoFn(TranslationContext context) {
    DoFn<InputT, OutputT> doFn;
    try {
      doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(context.getCurrentTransform());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return doFn;
  }
}
