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
package org.apache.gluten.velox;

import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.InputTypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamRecordTimestampInserterNode;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.List;
import java.util.Map;

/**
 * Replaces a native Flink {@link StreamRecordTimestampInserter} (per-row timestamp) with a Gluten
 * columnar inserter (batch-max timestamp) so the columnar chain stays intact when the downstream
 * sink is offloaded to Velox.
 *
 * <p>The native inserter is constructed by {@code CommonExecSink.applyRowtimeTransformation} and
 * sits somewhere on the sink's input chain (position depends on the sink: for a simple sink like
 * Fuzzer/DiscardingSink it is the direct input; for FileSystem it sits below the
 * StreamingFileWriter and PartitionCommitter). Each sink factory's {@code buildVeloxSink} invokes
 * this helper on the inserter-bearing transformation it is about to replace; if no inserter is
 * present (e.g., {@code rowtimeFieldIndex == -1}), the helper is a no-op and returns the input
 * unchanged.
 */
public final class GlutenRowtimeInserterHelper {

  private GlutenRowtimeInserterHelper() {}

  /**
   * Convenience overload that accepts a {@link DataStream} (typical entry point for factories that
   * use {@code sinkTransformation.getInputStream()}). Inspects the underlying transformation; if it
   * is a native inserter, rebuilds it as a Gluten columnar inserter and returns a new DataStream
   * whose terminal node is the Gluten inserter. Otherwise returns the inputStream unchanged.
   */
  public static DataStream<RowData> process(DataStream<RowData> inputStream) {
    Transformation<RowData> inputTrans = inputStream.getTransformation();
    Transformation<RowData> newTrans = processTransformation(inputTrans);
    if (newTrans == inputTrans) {
      return inputStream;
    }
    return new DataStream<>(inputStream.getExecutionEnvironment(), newTrans);
  }

  /**
   * Inspect {@code inputTrans}; if it is a native {@link StreamRecordTimestampInserter}, rebuild it
   * as a Gluten columnar inserter whose input is the native inserter's upstream. Returns the new
   * transformation, or the original inputTrans when no replacement happened.
   */
  public static Transformation<RowData> processTransformation(Transformation<RowData> inputTrans) {
    if (!(inputTrans instanceof OneInputTransformation)) {
      return inputTrans;
    }
    OneInputTransformation<?, ?> oneInput = (OneInputTransformation<?, ?>) inputTrans;
    if (!(oneInput.getOperatorFactory() instanceof SimpleOperatorFactory)) {
      return inputTrans;
    }
    @SuppressWarnings("rawtypes")
    Object op = ((SimpleOperatorFactory) oneInput.getOperatorFactory()).getOperator();
    if (!(op instanceof StreamRecordTimestampInserter)) {
      return inputTrans;
    }
    int rowtimeIndex =
        (int) ReflectUtils.getObjectField(StreamRecordTimestampInserter.class, op, "rowtimeIndex");
    List<Transformation<?>> inputs = oneInput.getInputs();
    if (inputs.isEmpty()) {
      return inputTrans;
    }
    @SuppressWarnings("unchecked")
    Transformation<RowData> aboveInserter = (Transformation<RowData>) inputs.get(0);
    return buildGlutenInserter(aboveInserter, rowtimeIndex, oneInput.getParallelism());
  }

  private static Transformation<RowData> buildGlutenInserter(
      Transformation<RowData> aboveInserter, int rowtimeFieldIndex, int parallelism) {
    @SuppressWarnings("unchecked")
    InternalTypeInfo<RowData> internalTypeInfo =
        (InternalTypeInfo<RowData>) aboveInserter.getOutputType();
    final org.apache.flink.table.types.logical.RowType inputRowType =
        (org.apache.flink.table.types.logical.RowType) internalTypeInfo.toLogicalType();
    final RowType vlInputType = (RowType) LogicalTypeConverter.toVLType(inputRowType);
    final List<String> fieldNames = inputRowType.getFieldNames();
    final String rowtimeFieldName = fieldNames.get(rowtimeFieldIndex);
    final InputTypedExpr inputExpr = new InputTypedExpr(vlInputType);
    final ProjectNode project =
        new ProjectNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(vlInputType)),
            List.of(rowtimeFieldName),
            List.of(FieldAccessTypedExpr.create(inputExpr, rowtimeFieldName)));
    final StreamRecordTimestampInserterNode inserterNode =
        new StreamRecordTimestampInserterNode(
            PlanNodeIdGenerator.newId(), null, project, rowtimeFieldIndex);
    final StatefulPlanNode statefulPlan = new StatefulPlanNode(inserterNode.getId(), inserterNode);

    final GlutenOneInputOperator<StatefulRecord, StatefulRecord> operator =
        new GlutenOneInputOperator<>(
            statefulPlan,
            PlanNodeIdGenerator.newId(),
            vlInputType,
            Map.of(inserterNode.getId(), vlInputType),
            StatefulRecord.class,
            StatefulRecord.class,
            "StreamRecordTimestampInserter");

    @SuppressWarnings({"rawtypes", "unchecked"})
    final OneInputStreamOperator rawOperator = (OneInputStreamOperator) operator;
    return new OneInputTransformation<>(
        aboveInserter,
        "StreamRecordTimestampInserter",
        SimpleOperatorFactory.of(rawOperator),
        aboveInserter.getOutputType(),
        parallelism);
  }
}
