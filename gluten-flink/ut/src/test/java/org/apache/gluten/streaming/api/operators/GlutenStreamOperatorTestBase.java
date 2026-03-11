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
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public abstract class GlutenStreamOperatorTestBase {

  protected static FlinkTypeFactory typeFactory;
  protected static RexBuilder rexBuilder;
  protected static RowType rowType;
  protected static TypeInformation<RowData> typeInfo;
  protected static List<RowData> testData;
  protected static io.github.zhztheplayer.velox4j.type.RowType veloxType;

  @BeforeAll
  public static void setupBaseEnvironment() {
    Velox4jEnvironment.initializeOnce();

    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);

    rowType =
        RowType.of(
            new LogicalType[] {
              new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new IntType()
            },
            new String[] {"id", "name", "age"});

    typeInfo = InternalTypeInfo.of(rowType);

    testData =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(4, StringData.fromString("David"), 15),
            GenericRowData.of(5, null, 30),
            GenericRowData.of(6, StringData.fromString("Eve"), null),
            GenericRowData.of(7, null, null),
            GenericRowData.of(null, StringData.fromString("Frank"), 22),
            GenericRowData.of(null, null, null));

    veloxType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(rowType);
  }

  protected TypedExpr convertRexToVelox(RexNode rexNode, List<String> fieldNames) {
    RexConversionContext conversionContext = new RexConversionContext(fieldNames);
    return RexNodeConverter.toTypedExpr(rexNode, conversionContext);
  }

  protected List<TypedExpr> convertRexListToVelox(List<RexNode> rexNodes, List<String> fieldNames) {
    RexConversionContext conversionContext = new RexConversionContext(fieldNames);
    return RexNodeConverter.toTypedExpr(rexNodes, conversionContext);
  }

  protected io.github.zhztheplayer.velox4j.type.RowType convertToVeloxType(RowType flinkType) {
    return (io.github.zhztheplayer.velox4j.type.RowType)
        org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkType);
  }

  protected OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
      GlutenOneInputOperator operator,
      TypeInformation<RowData> inputTypeInfo,
      TypeInformation<RowData> outputTypeInfo)
      throws Exception {

    TypeSerializer<RowData> inputSerializer =
        inputTypeInfo.createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);

    harness.setup(outputTypeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    return harness;
  }

  protected void processTestData(
      OneInputStreamOperatorTestHarness<RowData, RowData> harness, List<RowData> inputData)
      throws Exception {
    long timestamp = 0L;
    for (RowData row : inputData) {
      harness.processElement(new StreamRecord<>(row, timestamp++));
    }
  }

  protected List<RowData> extractOutputFromHarness(
      OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
    Queue<Object> outputQueue = harness.getOutput();
    List<RowData> actualOutput = new ArrayList<>();
    while (!outputQueue.isEmpty()) {
      Object record = outputQueue.poll();
      if (record instanceof StreamRecord) {
        actualOutput.add(((StreamRecord<RowData>) record).getValue());
      }
    }
    return actualOutput;
  }

  protected GlutenOneInputOperator createTestOperator(
      PlanNode veloxPlan,
      TypeInformation<RowData> inputTypeInfo,
      TypeInformation<RowData> outputTypeInfo) {

    io.github.zhztheplayer.velox4j.type.RowType inputVeloxType =
        convertToVeloxType(((InternalTypeInfo<RowData>) inputTypeInfo).toRowType());
    io.github.zhztheplayer.velox4j.type.RowType outputVeloxType =
        convertToVeloxType(((InternalTypeInfo<RowData>) outputTypeInfo).toRowType());

    return new GlutenOneInputOperator(
        new StatefulPlanNode(veloxPlan.getId(), veloxPlan),
        PlanNodeIdGenerator.newId(),
        inputVeloxType,
        Map.of(veloxPlan.getId(), outputVeloxType),
        RowData.class,
        RowData.class);
  }
}
