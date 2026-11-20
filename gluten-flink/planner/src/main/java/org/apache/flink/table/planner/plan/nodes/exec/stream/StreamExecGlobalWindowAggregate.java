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
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamWindowAggregationNode;
import io.github.zhztheplayer.velox4j.plan.StreamWindowPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateSpec;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.gluten.rexnode.AggregateCallConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.rexnode.WindowUtils;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Stream {@link ExecNode} for window table-valued based global aggregate. */
@ExecNodeMetadata(
    name = "stream-exec-global-window-aggregate",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations =
        StreamExecGlobalWindowAggregate.GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecGlobalWindowAggregate extends StreamExecWindowAggregateBase {

  public static final String GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION = "global-window-aggregate";

  private static final long WINDOW_AGG_MEMORY_RATIO = 100;

  public static final String FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE = "localAggInputRowType";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  @JsonProperty(FIELD_NAME_WINDOWING)
  private final WindowingStrategy windowing;

  @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
  private final NamedWindowProperty[] namedWindowProperties;

  /** The input row type of this node's local agg. */
  @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE)
  private final RowType localAggInputRowType;

  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  public StreamExecGlobalWindowAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      WindowingStrategy windowing,
      NamedWindowProperty[] namedWindowProperties,
      Boolean needRetraction,
      InputProperty inputProperty,
      RowType localAggInputRowType,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecGlobalWindowAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecGlobalWindowAggregate.class, tableConfig),
        grouping,
        aggCalls,
        windowing,
        namedWindowProperties,
        needRetraction,
        Collections.singletonList(inputProperty),
        localAggInputRowType,
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecGlobalWindowAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
      @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES) NamedWindowProperty[] namedWindowProperties,
      @Nullable @JsonProperty(FIELD_NAME_NEED_RETRACTION) Boolean needRetraction,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE) RowType localAggInputRowType,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.windowing = checkNotNull(windowing);
    this.namedWindowProperties = checkNotNull(namedWindowProperties);
    this.localAggInputRowType = localAggInputRowType;
    this.needRetraction = Optional.ofNullable(needRetraction).orElse(false);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    if (needRetraction) {
      throw new TableException(
          "GFV window TVF aggregation does not support update-stream input yet (needRetraction=true, L2)");
    }
    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    // The input is the local stage's output: [keys, intermediate
    // accumulator columns..., windowEnd(BIGINT)] (keyless: [intermediate
    // accumulator columns..., windowEnd] with no key column - the
    // exchange routes it SINGLETON and the keyless wrap below applies).
    final RowType partialRowType = (RowType) inputEdge.getOutputType();

    final ZoneId shiftTimeZone =
        TimeWindowUtil.getShiftTimeZone(
            windowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config));
    final AggregateInfoList aggInfoList =
        AggregateUtil.deriveStreamWindowAggregateInfoList(
            planner.getTypeFactory(),
            localAggInputRowType,
            JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
            needRetraction,
            windowing.getWindow(),
            true);

    // --- Begin Gluten-specific code changes (two-stage global side) ---
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(partialRowType);
    // Key channels resolved first (the keyless case reads it).
    List<Integer> keyIndexes =
        Arrays.stream(grouping).boxed().collect(Collectors.toList());
    final boolean keyless = keyIndexes.isEmpty();
    if (keyless) {
      // Keyless: the exchange output has no key column (SINGLETON
      // routing); wrap a constant "$keyless_key" channel at the end (see
      // Utils.wrapKeylessInput) so the C++ wraps the column in addInput
      // and keys on it.
      inputType = Utils.wrapKeylessInput(inputType);
      keyIndexes = List.of(inputType.getChildren().size() - 1);
    }
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(getOutputType());
    // The aggregates reference the ORIGINAL raw input (the local's
    // input): their rawInputTypes drive the C++ intermediate-type
    // resolution; the kFinal branch skips field-name resolution and
    // reads the intermediate columns at fixed positions.
    io.github.zhztheplayer.velox4j.type.RowType rawInputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(localAggInputRowType);
    List<FieldAccessTypedExpr> groupingKeys = Utils.generateFieldAccesses(inputType, grouping);
    List<Aggregate> aggregates = AggregateCallConverter.toAggregates(aggCalls, rawInputType);
    // Window column positions in the Flink output layout (keys,
    // aggregates, window_start/end); the velox output type IS the
    // Flink output type, so the C++ writes window timestamps at these
    // exact positions.
    Tuple2<Integer, Integer> windowStartAndEndIndexes =
        WindowUtils.getWindowStartAndEndIndexes(namedWindowProperties, (RowType) getOutputType());
    List<String> aggNames = new ArrayList<>();
    for (int i = 0; i < aggCalls.length; i++) {
      aggNames.add("a" + i);
    }
    List<io.github.zhztheplayer.velox4j.type.Type> keyTypes = new ArrayList<>();
    for (Integer idx : keyIndexes) {
      keyTypes.add(inputType.getChildren().get(idx));
    }
    KeyedStateSpec keyedStateSpec = new KeyedStateSpec(keyIndexes, keyTypes);
    PartitionFunctionSpec keySelectorSpec = new HashPartitionFunctionSpec(inputType, keyIndexes);

    // The windowing is a SliceAttachedWindowingStrategy: the rowtime
    // index points at the windowEnd column of the partial rows (the
    // planner rule put it at the end of the local output).
    Tuple5<Long, Long, Long, Integer, Integer> windowSpecParams =
        WindowUtils.extractWindowParameters(windowing);
    long size = windowSpecParams.f0;
    long slide = windowSpecParams.f1;
    long offset = windowSpecParams.f2;
    int rowtimeIndex = windowSpecParams.f3;
    int windowType = windowSpecParams.f4;
    PartitionFunctionSpec sliceAssignerSpec =
        new StreamWindowPartitionFunctionSpec(
            inputType, rowtimeIndex, size, slide, offset, windowType);

    // The global's aggregation: FINAL step - consumes the intermediate
    // columns, produces final values.
    PlanNode aggregation =
        new AggregationNode(
            PlanNodeIdGenerator.newId(),
            AggregateStep.FINAL,
            groupingKeys,
            groupingKeys,
            aggNames,
            aggregates,
            false,
            List.of(new EmptyNode(inputType)),
            null,
            List.of());

    PlanNode windowAgg =
        new StreamWindowAggregationNode(
            PlanNodeIdGenerator.newId(),
            aggregation,
            null,
            keySelectorSpec,
            sliceAssignerSpec,
            keyedStateSpec,
            shiftTimeZone.getId(),
            false,
            size,
            slide,
            offset,
            windowType,
            outputType,
            windowing.isRowtime(),
            rowtimeIndex,
            windowStartAndEndIndexes.f0,
            windowStartAndEndIndexes.f1);

    LogicalType[] accTypes =
        Arrays.stream(aggInfoList.getAccTypes())
            .map(x -> x.getLogicalType())
            .collect(Collectors.toList())
            .toArray(new LogicalType[] {});
    final RowDataKeySelector selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            grouping,
            InternalTypeInfo.of(partialRowType));
    final OneInputStreamOperator<RowData, RowData> windowOperator =
        new org.apache.gluten.table.runtime.operators.WindowAggOperator<RowData, RowData, Long>(
            new StatefulPlanNode(windowAgg.getId(), windowAgg),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(windowAgg.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecGlobalWindowAggregate",
            selector.getProducedType(),
            aggInfoList.getAggNames(),
            accTypes);
    // --- End Gluten-specific code changes ---

    return ExecNodeUtil.createOneInputTransformation(
        inputTransform,
        createTransformationMeta(GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION, config),
        SimpleOperatorFactory.of(windowOperator),
        InternalTypeInfo.of(getOutputType()),
        inputTransform.getParallelism(),
        WINDOW_AGG_MEMORY_RATIO,
        false);
  }
}
