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

import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.data.StringData.fromString;

@Disabled("Need to apply for the new interface of gluten operator")
public class GlutenStreamJoinOperatorTest extends GlutenStreamJoinOperatorTestBase {

  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;
  private static List<RowData> leftTestData;
  private static List<RowData> rightTestData;

  @BeforeAll
  public static void setupTestData() {
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);

    leftTestData =
        Arrays.asList(
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#1"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464")),
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#2"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464")),
            GenericRowData.of(
                fromString("Ord#2"),
                fromString("LineOrd#3"),
                fromString("68 Manor Station Street, Honolulu, HI 96815")));

    rightTestData =
        Arrays.asList(
            GenericRowData.of(fromString("LineOrd#2"), fromString("AIR")),
            GenericRowData.of(fromString("LineOrd#3"), fromString("TRUCK")),
            GenericRowData.of(fromString("LineOrd#4"), fromString("SHIP")));
  }

  @Test
  public void testInnerJoin() throws Exception {
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.INNER);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#2"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                fromString("LineOrd#2"),
                fromString("AIR")),
            GenericRowData.of(
                fromString("Ord#2"),
                fromString("LineOrd#3"),
                fromString("68 Manor Station Street, Honolulu, HI 96815"),
                fromString("LineOrd#3"),
                fromString("TRUCK")));
    executeJoinTest(operator, leftTestData, rightTestData, expectedOutput);
  }

  @Test
  @Disabled
  public void testLeftJoin() throws Exception {
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.LEFT);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#1"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                null,
                null),
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#2"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                fromString("LineOrd#2"),
                fromString("AIR")),
            GenericRowData.of(
                fromString("Ord#2"),
                fromString("LineOrd#3"),
                fromString("68 Manor Station Street, Honolulu, HI 96815"),
                fromString("LineOrd#3"),
                fromString("TRUCK")));
    executeJoinTest(operator, leftTestData, rightTestData, expectedOutput);
  }

  @Test
  @Disabled
  public void testRightJoin() throws Exception {
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.RIGHT);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#2"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                fromString("LineOrd#2"),
                fromString("AIR")),
            GenericRowData.of(
                fromString("Ord#2"),
                fromString("LineOrd#3"),
                fromString("68 Manor Station Street, Honolulu, HI 96815"),
                fromString("LineOrd#3"),
                fromString("TRUCK")),
            GenericRowData.of(null, null, null, fromString("LineOrd#4"), fromString("SHIP")));

    executeJoinTest(operator, leftTestData, rightTestData, expectedOutput);
  }

  @Test
  @Disabled
  public void testFullOuterJoin() throws Exception {
    GlutenTwoInputOperator operator = createGlutenJoinOperator(FlinkJoinType.FULL);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#1"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                null,
                null),
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#2"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                fromString("LineOrd#2"),
                fromString("AIR")),
            GenericRowData.of(
                fromString("Ord#2"),
                fromString("LineOrd#3"),
                fromString("68 Manor Station Street, Honolulu, HI 96815"),
                fromString("LineOrd#3"),
                fromString("TRUCK")),
            GenericRowData.of(null, null, null, fromString("LineOrd#4"), fromString("SHIP")));

    executeJoinTest(operator, leftTestData, rightTestData, expectedOutput);
  }

  @Test
  public void testInnerJoinWithNonEquiCondition() throws Exception {
    RexNode nonEquiCondition = createNonEquiCondition();
    GlutenTwoInputOperator operator =
        createGlutenJoinOperator(FlinkJoinType.INNER, nonEquiCondition);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                fromString("Ord#1"),
                fromString("LineOrd#2"),
                fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                fromString("LineOrd#2"),
                fromString("AIR")));

    executeJoinTest(operator, leftTestData, rightTestData, expectedOutput);
  }

  private RexNode createNonEquiCondition() {
    RexNode leftField = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.CHAR, 20), 0);
    RexNode rightField =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.CHAR, 10), 4);
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, leftField, rightField);
  }
}
