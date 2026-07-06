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
 * Inspects a sink input chain for the native Flink {@link StreamRecordTimestampInserter} (per-row
 * timestamp) added by {@code CommonExecSink.applyRowtimeTransformation}, and either removes or
 * replaces it depending on whether the downstream sink actually consumes the timestamp.
 *
 * <p>The native inserter stamps each row's rowtime onto the surrounding {@code StreamRecord} so
 * that downstream {@code SinkFunction.Context.timestamp()} readers (or sinks that read {@code
 * StreamRecord.timestamp} directly) can access it. Sinks that never read the timestamp therefore
 * don't need the inserter at all.
 *
 * <p>Callers declare this via {@code requiresTimestamp}:
 *
 * <ul>
 *   <li>{@code false}: the sink does not consume {@code StreamRecord.timestamp}. The inserter is
 *       removed from the op chain and its upstream is wired directly to the sink. This is the
 *       correct behavior for every sink currently wired through the helper (Print, Fuzzer/Discard,
 *       FileSystem): Print reads rowtime from RowData via {@code SinkOperator.timestamp()};
 *       SinkV2-based sinks (DiscardingSink, etc.) cannot read the timestamp at all; the velox file
 *       writer doesn't consult StreamRecord.timestamp for partition/roll/commit.
 *   <li>{@code true}: the sink does consume {@code StreamRecord.timestamp}. The inserter is rebuilt
 *       as a Gluten columnar inserter (batch-max timestamp on a {@link StatefulRecord}) so the
 *       columnar chain stays intact end-to-end. No current caller uses this branch; it's kept for
 *       future sinks that read the timestamp directly.
 * </ul>
 *
 * <p>If no inserter is present (e.g., {@code rowtimeFieldIndex == -1}), the helper is a no-op and
 * returns the input unchanged regardless of {@code requiresTimestamp}.
 */
public final class GlutenRowtimeInserterHelper {

  private GlutenRowtimeInserterHelper() {}

  /**
   * Convenience overload that accepts a {@link DataStream} (typical entry point for factories that
   * use {@code sinkTransformation.getInputStream()}). Inspects the underlying transformation and
   * removes or replaces the inserter per {@code requiresTimestamp}; returns a new DataStream whose
   * terminal node reflects the result, or the inputStream unchanged when no replacement happened.
   */
  public static DataStream<RowData> process(
      DataStream<RowData> inputStream, boolean requiresTimestamp) {
    Transformation<RowData> inputTrans = inputStream.getTransformation();
    Transformation<RowData> newTrans = processTransformation(inputTrans, requiresTimestamp);
    if (newTrans == inputTrans) {
      return inputStream;
    }
    return new DataStream<>(inputStream.getExecutionEnvironment(), newTrans);
  }

  /**
   * Inspect {@code inputTrans}. If it is a native {@link StreamRecordTimestampInserter}:
   *
   * <ul>
   *   <li>when {@code requiresTimestamp} is false, return the inserter's upstream, removing the
   *       inserter from the op chain;
   *   <li>when {@code requiresTimestamp} is true, rebuild it as a Gluten columnar inserter whose
   *       input is the native inserter's upstream.
   * </ul>
   *
   * <p>Returns the original inputTrans when no inserter is present.
   */
  public static Transformation<RowData> processTransformation(
      Transformation<RowData> inputTrans, boolean requiresTimestamp) {
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
    List<Transformation<?>> inputs = oneInput.getInputs();
    if (inputs.isEmpty()) {
      return inputTrans;
    }
    @SuppressWarnings("unchecked")
    Transformation<RowData> aboveInserter = (Transformation<RowData>) inputs.get(0);
    if (!requiresTimestamp) {
      return aboveInserter;
    }
    int rowtimeIndex =
        (int) ReflectUtils.getObjectField(StreamRecordTimestampInserter.class, op, "rowtimeIndex");
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
