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

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.WatermarkAssignerNode;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for WatermarkAssigner idle detection.
 *
 * <p>Native {@code checkWatermarkStatus} is driven by the {@code next()} → {@code advance()} loop
 * inside the {@code GlutenOneInputOperator}'s drain pipeline. Since {@code addInput()} resets the
 * idle baseline on every record, the idle check must happen on an {@code advance()} call that
 * processes no new input. We achieve this by calling {@code processWatermark()} (which triggers a
 * drain cycle without new data) after waiting past the idle timeout.
 */
public class GlutenOneInputWatermarkAssignerIdleTest {

  private static FlinkTypeFactory typeFactory;
  private static FlinkRexBuilder rexBuilder;
  private static RowType inputFlinkRowType;
  private static io.github.zhztheplayer.velox4j.type.RowType inputVeloxType;
  private static TypeInformation<RowData> typeInfo;

  @BeforeAll
  static void setUpClass() {
    Velox4jEnvironment.initializeOnce();

    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);

    inputFlinkRowType =
        RowType.of(new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"id", "ts"});
    inputVeloxType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(inputFlinkRowType);
    typeInfo = InternalTypeInfo.of(inputFlinkRowType);
  }

  @Test
  void testIdleDetectionWithRealTimePassage() throws Exception {
    long idleTimeout = 100L; // 100 ms
    long watermarkInterval = 50L;

    // ── Watermark expression: reference the ts field (index 1) ──
    List<String> fieldNames = Utils.getNamesFromRowType(inputFlinkRowType);
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    TypedExpr watermarkExpr =
        RexNodeConverter.toTypedExpr(tsRef, new RexConversionContext(fieldNames));

    ProjectNode watermarkProject =
        new ProjectNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(inputVeloxType)),
            List.of("TIMESTAMP"),
            List.of(watermarkExpr));

    // ── WatermarkAssignerNode ──
    WatermarkAssignerNode assignerNode =
        new WatermarkAssignerNode(
            PlanNodeIdGenerator.newId(),
            null,
            watermarkProject,
            idleTimeout,
            1, // rowtimeFieldIndex (ts at index 1)
            watermarkInterval);

    // ── GlutenOneInputOperator ──
    GlutenOneInputOperator<RowData, RowData> operator =
        new GlutenOneInputOperator<>(
            new StatefulPlanNode(assignerNode.getId(), assignerNode),
            PlanNodeIdGenerator.newId(),
            inputVeloxType,
            Map.of(assignerNode.getId(), inputVeloxType),
            RowData.class,
            RowData.class,
            "IdleDetectionTest");

    // ── Test harness ──
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(
            operator, typeInfo.createSerializer(new SerializerConfigImpl()));
    harness.setup(typeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    try {
      // ── Phase 1: feed one record ──
      GenericRowData record1 = GenericRowData.of(1, 1000L);
      harness.processElement(new StreamRecord<>(record1, 1000L));
      // output: StreamRecord(record1), Watermark(1000)
      // After drain, checkWatermarkStatus(now1) scheduled timer at now1+100ms.

      // ── Phase 2: wait past idleTimeout, then trigger a drain WITHOUT new input ──
      Thread.sleep(idleTimeout * 2); // 200 ms > 100 ms
      harness.processWatermark(new Watermark(0));
      // Inside drain: advance → next → advanceWithFuture → blocked
      //   → checkWatermarkStatus(now2) → idle detected (now2 - lastRecordTime > 100ms)
      //   → push WatermarkStatus.IDLE to pendings_

      // ── Phase 3: feed a second record — the drain first pops pending IDLE ──
      GenericRowData record2 = GenericRowData.of(2, 2000L);
      harness.processElement(new StreamRecord<>(record2, 2000L));
      // drain: advance → next → pendings_ non-empty (IDLE) → pop IDLE → emit IDLE
      //   → advance → next → process record2 → addInput → onRecord → idle was true
      //     → emit ACTIVE → push ACTIVE → advance → push record2 + watermark
      //   → pop ACTIVE → emit ACTIVE → pop record2 → collect → pop watermark → emit

      // ── Assertions ──
      Queue<Object> output = harness.getOutput();
      assertThat(output)
          .as("Output must contain WatermarkStatus.IDLE after idle timeout")
          .anyMatch(e -> e instanceof WatermarkStatus && ((WatermarkStatus) e).isIdle());
      assertThat(output)
          .as("Output must contain WatermarkStatus.ACTIVE after idle→active transition")
          .anyMatch(e -> e instanceof WatermarkStatus && !((WatermarkStatus) e).isIdle());
      assertThat(output)
          .as("All input records must be preserved")
          .anyMatch(
              e ->
                  e instanceof StreamRecord
                      && ((StreamRecord<RowData>) e).getValue().getInt(0) == 1)
          .anyMatch(
              e ->
                  e instanceof StreamRecord
                      && ((StreamRecord<RowData>) e).getValue().getInt(0) == 2);
    } finally {
      harness.close();
    }
  }

  @Test
  void testNoIdleWithContinuousRecords() throws Exception {
    long idleTimeout = 100L;
    long watermarkInterval = 50L;

    List<String> fieldNames = Utils.getNamesFromRowType(inputFlinkRowType);
    RexNode tsRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    TypedExpr watermarkExpr =
        RexNodeConverter.toTypedExpr(tsRef, new RexConversionContext(fieldNames));

    ProjectNode watermarkProject =
        new ProjectNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(inputVeloxType)),
            List.of("TIMESTAMP"),
            List.of(watermarkExpr));

    WatermarkAssignerNode assignerNode =
        new WatermarkAssignerNode(
            PlanNodeIdGenerator.newId(), null, watermarkProject, idleTimeout, 1, watermarkInterval);

    GlutenOneInputOperator<RowData, RowData> operator =
        new GlutenOneInputOperator<>(
            new StatefulPlanNode(assignerNode.getId(), assignerNode),
            PlanNodeIdGenerator.newId(),
            inputVeloxType,
            Map.of(assignerNode.getId(), inputVeloxType),
            RowData.class,
            RowData.class,
            "NoIdleTest");

    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(
            operator, typeInfo.createSerializer(new SerializerConfigImpl()));
    harness.setup(typeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    try {
      GenericRowData record1 = GenericRowData.of(1, 1000L);
      GenericRowData record2 = GenericRowData.of(2, 2000L);

      harness.processElement(new StreamRecord<>(record1, 1000L));
      // Feed second record immediately (no idle gap) → addInput resets baseline
      harness.processElement(new StreamRecord<>(record2, 2000L));
      // Then drain without new data — idle timeout has NOT elapsed since record2
      harness.processWatermark(new Watermark(0));

      Queue<Object> output = harness.getOutput();
      assertThat(output)
          .as("No WatermarkStatus.IDLE should appear when records arrive continuously")
          .noneMatch(e -> e instanceof WatermarkStatus && ((WatermarkStatus) e).isIdle());
    } finally {
      harness.close();
    }
  }
}
