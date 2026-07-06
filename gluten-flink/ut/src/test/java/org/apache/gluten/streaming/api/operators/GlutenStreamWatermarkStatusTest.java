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
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamWatermarkStatusTest extends GlutenStreamOperatorTestBase {

  @Test
  public void testWatermarkStatusPropagationThroughNativeProjectOperator() throws Exception {
    // One-input operators should forward explicit upstream IDLE/ACTIVE status changes through
    // native execution and emit the corresponding Flink WatermarkStatus downstream.
    GlutenOneInputOperator operator = createTestOperator(createProjectPlan(), typeInfo, typeInfo);

    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(operator, typeInfo, typeInfo)) {
      // IDLE marks the only input idle, so the operator immediately becomes idle.
      harness.processWatermarkStatus(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      // An explicit ACTIVE status from upstream should reactivate the operator.
      harness.processWatermarkStatus(WatermarkStatus.ACTIVE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE);
    }
  }

  @Test
  public void testWatermarkReactivatesIdleNativeProjectOperator() throws Exception {
    // Flink treats a watermark from an idle input as an implicit reactivation. Native execution
    // must emit ACTIVE before forwarding the watermark so downstream operators see valid ordering.
    GlutenOneInputOperator operator = createTestOperator(createProjectPlan(), typeInfo, typeInfo);

    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(operator, typeInfo, typeInfo)) {
      harness.processWatermarkStatus(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      harness.processWatermark(new Watermark(1L));
      assertThat(harness.getOutput())
          .containsExactly(WatermarkStatus.IDLE, WatermarkStatus.ACTIVE, new Watermark(1L));
    }
  }

  @Test
  public void testRecordDoesNotReactivateIdleNativeProjectOperator() throws Exception {
    // Records are not watermark/status events. They should still be processed while the input is
    // marked idle, but they must not implicitly emit ACTIVE; upstream is responsible for that.
    GlutenOneInputOperator operator = createTestOperator(createProjectPlan(), typeInfo, typeInfo);

    try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(operator, typeInfo, typeInfo)) {
      harness.processWatermarkStatus(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);

      // The projected record is produced, while the watermark status output remains idle only.
      harness.processElement(new StreamRecord<>(testData.get(0), 1L));
      assertThat(harness.getOutput()).contains(WatermarkStatus.IDLE);
      assertThat(harness.getOutput()).doesNotContain(WatermarkStatus.ACTIVE);
      assertThat(extractOutputFromHarness(harness)).containsExactly(testData.get(0));
    }
  }

  private PlanNode createProjectPlan() {
    List<String> inputFieldNames = Utils.getNamesFromRowType(rowType);
    List<TypedExpr> veloxProjections =
        convertRexListToVelox(createProjectionExpressions(), inputFieldNames);

    return new ProjectNode(
        PlanNodeIdGenerator.newId(),
        List.of(new EmptyNode(veloxType)),
        inputFieldNames,
        veloxProjections);
  }

  private List<RexNode> createProjectionExpressions() {
    RexNode idFieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexNode nameFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexNode ageFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);

    return Arrays.asList(idFieldRef, nameFieldRef, ageFieldRef);
  }
}
