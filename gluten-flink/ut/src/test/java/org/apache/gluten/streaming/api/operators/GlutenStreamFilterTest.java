/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.streaming.api.operators;

import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.gluten.streaming.api.operators.utils.RowDataTestUtils.checkEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamFilterTest extends GlutenStreamOperatorTestBase {

  @Test
  public void testGreaterThanFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(5, null, 30),
            GenericRowData.of(null, StringData.fromString("Frank"), 22));

    testFilter(
        createFilterCondition(SqlTypeName.INTEGER, 2, 18, SqlStdOperatorTable.GREATER_THAN),
        expectedOutput);
  }

  @Test
  public void testLessThanFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(4, StringData.fromString("David"), 15));

    testFilter(
        createFilterCondition(SqlTypeName.INTEGER, 2, 20, SqlStdOperatorTable.LESS_THAN),
        expectedOutput);
  }

  @Test
  public void testEqualToFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(GenericRowData.of(1, StringData.fromString("Alice"), 20));

    testFilter(
        createFilterCondition(SqlTypeName.INTEGER, 2, 20, SqlStdOperatorTable.EQUALS),
        expectedOutput);
  }

  @Test
  @Disabled
  public void testGreaterThanOrEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(5, null, 30),
            GenericRowData.of(null, StringData.fromString("Frank"), 22));

    testFilter(
        createFilterCondition(
            SqlTypeName.INTEGER, 2, 20, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL),
        expectedOutput);
  }

  @Test
  @Disabled
  public void testLessThanOrEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(4, StringData.fromString("David"), 15));

    testFilter(
        createFilterCondition(SqlTypeName.INTEGER, 2, 20, SqlStdOperatorTable.LESS_THAN_OR_EQUAL),
        expectedOutput);
  }

  @Test
  @Disabled
  public void testNotEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(4, StringData.fromString("David"), 15),
            GenericRowData.of(5, null, 30),
            GenericRowData.of(null, StringData.fromString("Frank"), 22));

    testFilter(
        createFilterCondition(SqlTypeName.INTEGER, 2, 20, SqlStdOperatorTable.NOT_EQUALS),
        expectedOutput);
  }

  @Test
  public void testStringEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(GenericRowData.of(1, StringData.fromString("Alice"), 20));

    testFilter(
        createFilterCondition(SqlTypeName.VARCHAR, 1, "Alice", SqlStdOperatorTable.EQUALS),
        expectedOutput);
  }

  @Test
  public void testOpenClose() throws Exception {
    RowType simpleRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

    RexNode filterCondition =
        createFilterCondition(SqlTypeName.INTEGER, 0, 0, SqlStdOperatorTable.GREATER_THAN);
    PlanNode veloxPlan = createFilterPlan(filterCondition, simpleRowType);

    TestableGlutenOneInputOperator operator =
        new TestableGlutenOneInputOperator(veloxPlan, convertToVeloxType(simpleRowType));

    assertThat(operator.isOpened()).isFalse();
    assertThat(operator.isClosed()).isFalse();

    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(
            operator, InternalTypeInfo.of(simpleRowType), InternalTypeInfo.of(simpleRowType));

    harness.processElement(
        new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(
            GenericRowData.of(1), 0L));

    assertThat(harness.getOutput()).isNotEmpty();
    assertThat(operator.isOpened()).isTrue();
    assertThat(operator.isClosed()).isFalse();

    harness.close();

    assertThat(operator.isOpened()).isTrue();
    assertThat(operator.isClosed()).isTrue();
  }

  private RexNode createFilterCondition(
      SqlTypeName typeName, Integer ref, Object value, SqlOperator operator) {
    RexNode idFieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(typeName), ref);
    RexNode idValue = rexBuilder.makeLiteral(value, typeFactory.createSqlType(typeName), false);
    return rexBuilder.makeCall(operator, idFieldRef, idValue);
  }

  private void testFilter(RexNode flinkFilterCondition, List<RowData> expectedOutput)
      throws Exception {
    PlanNode veloxPlan = createFilterPlan(flinkFilterCondition, rowType);
    GlutenOneInputOperator operator = createTestOperator(veloxPlan, typeInfo, typeInfo);

    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(operator, typeInfo, typeInfo);

    processTestData(harness, testData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);
    checkEquals(actualOutput, expectedOutput, rowType.getChildren());

    harness.close();
  }

  private PlanNode createFilterPlan(RexNode filterCondition, RowType rowType) {
    List<String> fieldNames = Utils.getNamesFromRowType(rowType);
    TypedExpr veloxFilterCondition = convertRexToVelox(filterCondition, fieldNames);
    io.github.zhztheplayer.velox4j.type.RowType veloxType = convertToVeloxType(rowType);

    return new FilterNode(
        PlanNodeIdGenerator.newId(), List.of(new EmptyNode(veloxType)), veloxFilterCondition);
  }

  private static class TestableGlutenOneInputOperator extends GlutenOneInputOperator {
    private boolean opened = false;
    private boolean closed = false;

    public TestableGlutenOneInputOperator(
        PlanNode veloxPlan, io.github.zhztheplayer.velox4j.type.RowType veloxType) {
      super(
          new io.github.zhztheplayer.velox4j.plan.StatefulPlanNode(veloxPlan.getId(), veloxPlan),
          PlanNodeIdGenerator.newId(),
          veloxType,
          Map.of(veloxPlan.getId(), veloxType),
          RowData.class,
          RowData.class);
    }

    @Override
    public void open() throws Exception {
      if (closed) {
        throw new IllegalStateException("Close called before open.");
      }
      super.open();
      opened = true;
    }

    @Override
    public void close() throws Exception {
      if (!opened) {
        throw new IllegalStateException("Open was not called before close.");
      }
      super.close();
      closed = true;
    }

    public boolean isOpened() {
      return opened;
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
