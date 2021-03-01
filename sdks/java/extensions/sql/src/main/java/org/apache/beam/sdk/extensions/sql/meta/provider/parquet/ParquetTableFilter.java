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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.AND;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.COMPARISON;
import static org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind.OR;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;

class ParquetTableFilter implements BeamSqlTableFilter {
  // TODO Check 'IN'
  private static final ImmutableSet<SqlKind> SUPPORTED_OPS =
      ImmutableSet.<SqlKind>builder().add(COMPARISON.toArray(new SqlKind[0])).add(AND, OR).build();

  private final List<RexNode> supported = new ArrayList<>();
  private final List<RexNode> unsupported = new ArrayList<>();
  private final Schema schema;

  ParquetTableFilter(List<RexNode> filter, Schema schema) {
    for (RexNode node : filter) {
      if (!node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
        throw new IllegalArgumentException(
            "Predicate node '"
                + node.getClass().getSimpleName()
                + "' should be a boolean expression, but was: "
                + node.getType().getSqlTypeName());
      }

      if (isSupported(node)) { // TODO .getLeft()
        supported.add(node);
      } else {
        unsupported.add(node);
      }
    }
    this.schema = schema;
  }

  public List<RexNode> getSupported() {
    return supported;
  }

  @Override
  public List<RexNode> getNotSupported() {
    return unsupported;
  }

  @Override
  public int numSupported() {
    return BeamSqlTableFilter.expressionsInFilter(supported);
  }

  /**
   * Check whether a {@code RexNode} is supported. As of right now ParquetTable supports: 1. Complex
   * predicates (both conjunction and disjunction). 2. Comparison between a column and a literal.
   */
  private static boolean isSupported(RexNode node) {
    checkArgument(
        node instanceof RexCall,
        String.format(
            "Encountered an unexpected node type: %s. Should be %s",
            node.getClass().getSimpleName(), RexCall.class.getSimpleName()));
    boolean isSupported = false;
    if (node.getKind().belongsTo(SUPPORTED_OPS)) {
      isSupported = true;
    }
    return isSupported;
    //    long literalsCount = countOperands((RexCall) node, RexLiteral.class);
    //    long fieldsCount = countOperands((RexCall) node, RexInputRef.class);
    //
    //    return literalsCount == 1 && fieldsCount == 1;
  }

  /**
   * Check whether a {@code RexNode} is supported. To keep things simple:<br>
   * 1. Support comparison operations in predicate, which compare a single field to literal values.
   * 2. Support nested Conjunction (AND), Disjunction (OR) as long as child operations are
   * supported.<br>
   * 3. Support boolean fields.
   *
   * @param node A node to check for predicate push-down support.
   * @return A boolean whether an expression is supported.
   */
  private static boolean isSupported2(RexNode node) {
    if (node instanceof RexCall) {
      RexCall compositeNode = (RexCall) node;

      if (node.getKind().belongsTo(COMPARISON) || node.getKind().equals(SqlKind.NOT)) {
        int fields = 0;
        for (RexNode operand : compositeNode.getOperands()) {
          if (operand instanceof RexInputRef) {
            fields++;
          } else if (operand instanceof RexLiteral) {
            // RexLiterals are expected, but no action is needed.
          } else {
            // Complex predicates are not supported. Ex: `field1+5 == 10`.
            return false;
          }
        }
        // All comparison operations should have exactly one field reference.
        // Ex: `field1 == field2` is not supported.
        // TODO: Can be supported via Filters#where.
        if (fields == 1) {
          return true;
        }
      } else if (node.getKind().equals(AND) || node.getKind().equals(OR)) {
        // Nested ANDs and ORs are supported as long as all operands are supported.
        for (RexNode operand : compositeNode.getOperands()) {
          if (!isSupported(operand)) {
            return false;
          }
        }
        return true;
      }
    } else if (node instanceof RexInputRef) {
      // When field is a boolean.
      return true;
    } else {
      throw new RuntimeException(
          "Encountered an unexpected node type: " + node.getClass().getSimpleName());
    }

    return false;
  }

  @Override
  public String toString() {
    String supStr = supported.stream().map(RexNode::toString).collect(Collectors.joining());
    String unsupStr = unsupported.stream().map(RexNode::toString).collect(Collectors.joining());
    return String.format("[supported{%s}, unsupported{%s}]", supStr, unsupStr);
  }

  @VisibleForTesting
  FilterPredicate buildPredicate(List<RexNode> filters) {
    if (filters.size() == 1) {
      return buildPredicate(filters.get(0));
    }
    return FilterApi.and(
        buildPredicate(filters.get(0)), buildPredicate(filters.subList(1, filters.size())));
  }

  private FilterPredicate buildPredicate(RexNode node) {
    if (node.getKind().equals(OR)) {
      List<RexNode> operands = ((RexCall) node).getOperands();
      RexCall left = (RexCall) operands.get(0);
      RexCall right = (RexCall) operands.get(1);
      return FilterApi.or(buildPredicate(left), buildPredicate(right));
    } else if (node.getKind().equals(AND)) {
      List<RexNode> operands = ((RexCall) node).getOperands();
      RexCall left = (RexCall) operands.get(0);
      RexCall right = (RexCall) operands.get(1);
      return FilterApi.and(buildPredicate(left), buildPredicate(right));
      //    } else if (filter.getKind().equals(SqlKind.NOT)) {
      //      return FilterApi.not(buildPredicate(filter));
      //    } else if (filter.getKind().equals(SqlKind.NOT_EQUALS)) {
      //      return FilterApi.not(buildPredicate(filter));
    } else if (node.getKind().equals(SqlKind.GREATER_THAN)) {
      List<RexNode> operands = ((RexCall) node).getOperands();
      RexInputRef inputRef = (RexInputRef) operands.get(0); // 0 = {RexInputRef@10067} "$1"
      RexLiteral rexLiteral = (RexLiteral) operands.get(1); // 1 = {RexLiteral@10068} "25"
      Field field = schema.getField(inputRef.getIndex());
      Long value = rexLiteral.getValueAs(Long.class);
      return FilterApi.gt(FilterApi.longColumn(field.getName()), value);
      // TODO how to deal with invalid types?
    } else if (node.getKind().equals(SqlKind.EQUALS)) {
      List<RexNode> operands = ((RexCall) node).getOperands();
      RexInputRef inputRef = (RexInputRef) operands.get(0); // 0 = {RexInputRef@10067} "$1"
      RexLiteral rexLiteral = (RexLiteral) operands.get(1); // 1 = {RexLiteral@10068} "25"
      Field field = schema.getField(inputRef.getIndex());
      String fieldName = field.getName();
      //      FieldType type = field.getType();
      //        FieldType.INT64
      Integer value = rexLiteral.getValueAs(Integer.class);
      return FilterApi.gt(FilterApi.intColumn(fieldName), value);
    }
    throw new RuntimeException(
        "Cannot create a filter for an unsupported node: " + node.toString());
  }

  private static Column getColumnFor(Field field) {
    String fieldName = field.getName();
    FieldType type = field.getType();
    LongColumn column = FilterApi.longColumn(field.getName());
    //    if (type.equals(FieldType.INT64)) {
    ////      Long value = rexLiteral.getValueAs(Long.class);
    //    }
    return column;
  }

  private static Object getValueFor(Field field, RexLiteral rexLiteral) {
    FieldType type = field.getType();
    Long value = rexLiteral.getValueAs(Long.class);
    //    if (type.equals(FieldType.INT64)) {
    //
    //    }
    return value;
  }
}
