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
import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.NestedLoopJoinNode;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamJoinNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperatorTestBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.gluten.streaming.api.operators.utils.RowDataTestUtils.checkEquals;

public abstract class GlutenStreamJoinOperatorTestBase extends StreamingJoinOperatorTestBase {

  protected static RowType leftVeloxType;
  protected static RowType rightVeloxType;
  protected static RowType outputVeloxType;
  protected static org.apache.flink.table.types.logical.RowType outputRowType;

  protected MemoryManager sharedMemoryManager;
  protected Session sharedSession;
  protected BufferAllocator sharedAllocator;

  @BeforeAll
  public static void setupGlutenEnvironment() {
    Velox4jEnvironment.initializeOnce();
    setGlutenTypes();
  }

  private static void setGlutenTypes() {

    InternalTypeInfo<RowData> leftTypeInfo =
        InternalTypeInfo.of(
            org.apache.flink.table.types.logical.RowType.of(
                new LogicalType[] {
                  new CharType(false, 20), new CharType(false, 20), VarCharType.STRING_TYPE
                },
                new String[] {"order_id", "line_order_id", "shipping_address"}));
    InternalTypeInfo<RowData> rightTypeInfo =
        InternalTypeInfo.of(
            org.apache.flink.table.types.logical.RowType.of(
                new LogicalType[] {new CharType(false, 20), new CharType(true, 10)},
                new String[] {"line_order_id0", "line_order_ship_mode"}));

    leftVeloxType = (RowType) LogicalTypeConverter.toVLType(leftTypeInfo.toRowType());
    rightVeloxType = (RowType) LogicalTypeConverter.toVLType(rightTypeInfo.toRowType());

    outputRowType =
        org.apache.flink.table.types.logical.RowType.of(
            Stream.concat(
                    leftTypeInfo.toRowType().getChildren().stream(),
                    rightTypeInfo.toRowType().getChildren().stream())
                .toArray(LogicalType[]::new),
            Stream.concat(
                    leftTypeInfo.toRowType().getFieldNames().stream(),
                    rightTypeInfo.toRowType().getFieldNames().stream())
                .toArray(String[]::new));

    outputVeloxType = (RowType) LogicalTypeConverter.toVLType(outputRowType);
  }

  @Override
  @BeforeEach
  public void beforeEach(TestInfo testInfo) {
    sharedMemoryManager = MemoryManager.create(AllocationListener.NOOP);
    sharedSession = Velox4j.newSession(sharedMemoryManager);
    sharedAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Override
  @AfterEach
  public void afterEach() throws Exception {
    if (sharedAllocator != null) {
      sharedAllocator.close();
      sharedAllocator = null;
    }
    if (sharedSession != null) {
      sharedSession.close();
      sharedSession = null;
    }
    if (sharedMemoryManager != null) {
      sharedMemoryManager.close();
      sharedMemoryManager = null;
    }
  }

  @Override
  protected final AbstractStreamingJoinOperator createJoinOperator(TestInfo testInfo) {
    throw new UnsupportedOperationException("Use createGlutenJoinOperator instead");
  }

  @Override
  protected org.apache.flink.table.types.logical.RowType getOutputType() {
    return outputRowType;
  }

  protected GlutenTwoInputOperator createGlutenJoinOperator(FlinkJoinType joinType) {
    return createGlutenJoinOperator(joinType, null);
  }

  protected GlutenTwoInputOperator createGlutenJoinOperator(
      FlinkJoinType joinType, RexNode nonEquiCondition) {
    JoinType veloxJoinType = Utils.toVLJoinType(joinType);

    int[] leftJoinKeys = {1};
    int[] rightJoinKeys = {0};

    List<FieldAccessTypedExpr> leftKeys =
        Utils.analyzeJoinKeys(leftVeloxType, leftJoinKeys, List.of());
    List<FieldAccessTypedExpr> rightKeys =
        Utils.analyzeJoinKeys(rightVeloxType, rightJoinKeys, List.of());

    TypedExpr joinCondition = Utils.generateJoinEqualCondition(leftKeys, rightKeys);

    if (nonEquiCondition != null) {
      RexConversionContext conversionContext = new RexConversionContext(outputVeloxType.getNames());
      TypedExpr nonEqual = RexNodeConverter.toTypedExpr(nonEquiCondition, conversionContext);
      joinCondition = new CallTypedExpr(new BooleanType(), List.of(joinCondition, nonEqual), "and");
    }

    PlanNode leftInput =
        new TableScanNode(
            PlanNodeIdGenerator.newId(),
            leftVeloxType,
            new ExternalStreamTableHandle("connector-external-stream"),
            List.of());

    PlanNode rightInput =
        new TableScanNode(
            PlanNodeIdGenerator.newId(),
            rightVeloxType,
            new ExternalStreamTableHandle("connector-external-stream"),
            List.of());

    List<Integer> leftKeyIndexes = Arrays.stream(leftJoinKeys).boxed().collect(Collectors.toList());
    List<Integer> rightKeyIndexes =
        Arrays.stream(rightJoinKeys).boxed().collect(Collectors.toList());
    PartitionFunctionSpec leftPartFuncSpec =
        new HashPartitionFunctionSpec(leftVeloxType, leftKeyIndexes);
    PartitionFunctionSpec rightPartFuncSpec =
        new HashPartitionFunctionSpec(rightVeloxType, rightKeyIndexes);

    NestedLoopJoinNode probeNode =
        new NestedLoopJoinNode(
            PlanNodeIdGenerator.newId(),
            veloxJoinType,
            joinCondition,
            new EmptyNode(leftVeloxType),
            new EmptyNode(rightVeloxType),
            outputVeloxType);

    PlanNode join =
        new StreamJoinNode(
            PlanNodeIdGenerator.newId(),
            leftInput,
            rightInput,
            leftPartFuncSpec,
            rightPartFuncSpec,
            probeNode,
            outputVeloxType,
            1024);

    return new GlutenTwoInputOperator(
        new StatefulPlanNode(join.getId(), join),
        leftInput.getId(),
        rightInput.getId(),
        leftVeloxType,
        rightVeloxType,
        Map.of(join.getId(), outputVeloxType),
        RowData.class,
        RowData.class);
  }

  protected void processTestData(
      KeyedTwoInputStreamOperatorTestHarness<
              RowData, StatefulRecord, StatefulRecord, StatefulRecord>
          harness,
      List<RowData> leftData,
      List<RowData> rightData)
      throws Exception {
    long timestamp = 0L;
    for (RowData row : leftData) {
      StatefulRecord record = convertToStatefulRecord(row, leftVeloxType);
      harness.processElement1(new StreamRecord<>(record, timestamp++));
    }

    timestamp = 0L;
    for (RowData row : rightData) {
      StatefulRecord record = convertToStatefulRecord(row, rightVeloxType);
      harness.processElement2(new StreamRecord<>(record, timestamp++));
    }
  }

  protected List<RowData> extractOutputFromHarness(
      KeyedTwoInputStreamOperatorTestHarness<
              RowData, StatefulRecord, StatefulRecord, StatefulRecord>
          harness) {
    Queue<Object> outputQueue = harness.getOutput();
    return outputQueue.stream()
        .filter(record -> record instanceof StreamRecord)
        .map(record -> ((StreamRecord<RowData>) record).getValue())
        .collect(Collectors.toList());
  }

  private StatefulRecord convertToStatefulRecord(RowData rowData, RowType rowType) {
    try {
      RowVector rowVector =
          FlinkRowToVLVectorConvertor.fromRowData(rowData, sharedAllocator, sharedSession, rowType);

      StatefulRecord record = new StatefulRecord(null, 0, 0, false, -1);
      record.setRowVector(rowVector);

      return record;
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert RowData to StatefulRecord", e);
    }
  }

  protected void executeJoinTest(
      GlutenTwoInputOperator operator,
      List<RowData> leftData,
      List<RowData> rightData,
      List<RowData> expectedOutput)
      throws Exception {
    KeyedTwoInputStreamOperatorTestHarness<RowData, StatefulRecord, StatefulRecord, StatefulRecord>
        harness =
            new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                new GlutenRowDataKeySelector(leftKeySelector, leftVeloxType),
                new GlutenRowDataKeySelector(rightKeySelector, rightVeloxType),
                joinKeyTypeInfo);

    try {
      harness.setup();
      harness.open();

      processTestData(harness, leftData, rightData);
      List<RowData> actualOutput = extractOutputFromHarness(harness);
      checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    } finally {
      harness.close();
    }
  }

  private static class GlutenRowDataKeySelector
      implements org.apache.flink.api.java.functions.KeySelector<StatefulRecord, RowData> {
    private final RowDataKeySelector delegate;
    private final RowType rowType;

    private transient BufferAllocator allocator;

    public GlutenRowDataKeySelector(RowDataKeySelector delegate, RowType rowType) {
      this.delegate = delegate;
      this.rowType = rowType;
    }

    private BufferAllocator getAllocator() {
      if (allocator == null) {
        allocator = new RootAllocator(Long.MAX_VALUE);
      }
      return allocator;
    }

    @Override
    public RowData getKey(StatefulRecord record) {
      try {
        List<RowData> rowDataList =
            FlinkRowToVLVectorConvertor.toRowData(record.getRowVector(), getAllocator(), rowType);

        return delegate.getKey(rowDataList.get(0));
      } catch (Exception e) {
        throw new RuntimeException("Failed to extract key from StatefulRecord", e);
      }
    }
  }
}
