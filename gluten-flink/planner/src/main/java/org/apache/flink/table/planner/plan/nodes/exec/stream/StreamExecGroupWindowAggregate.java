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

import org.apache.gluten.rexnode.AggregateCallConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.GroupWindowAggregationNode;
import io.github.zhztheplayer.velox4j.plan.GroupWindowAggsHandlerNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamWindowPartitionFunctionSpec;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasRowIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isRowtimeAttribute;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} for either group window aggregate or group window table aggregate.
 *
 * <p>The differences between {@link StreamExecWindowAggregate} and {@link
 * StreamExecGroupWindowAggregate} is that, this node is translated from window TVF syntax, but the
 * * other is from the legacy GROUP WINDOW FUNCTION syntax. In the long future, {@link
 * StreamExecGroupWindowAggregate} will be dropped.
 */
@ExecNodeMetadata(
    name = "stream-exec-group-window-aggregate",
    version = 1,
    consumedOptions = {
      "table.local-time-zone",
      "table.exec.mini-batch.enabled",
      "table.exec.mini-batch.size"
    },
    producedTransformations = StreamExecGroupWindowAggregate.GROUP_WINDOW_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecGroupWindowAggregate extends StreamExecAggregateBase {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StreamExecGroupWindowAggregate.class);

  public static final String GROUP_WINDOW_AGGREGATE_TRANSFORMATION = "group-window-aggregate";

  public static final String FIELD_NAME_WINDOW = "window";
  public static final String FIELD_NAME_NAMED_WINDOW_PROPERTIES = "namedWindowProperties";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  @JsonProperty(FIELD_NAME_WINDOW)
  private final LogicalWindow window;

  @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
  private final NamedWindowProperty[] namedWindowProperties;

  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  public StreamExecGroupWindowAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      LogicalWindow window,
      NamedWindowProperty[] namedWindowProperties,
      boolean needRetraction,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecGroupWindowAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecGroupWindowAggregate.class, tableConfig),
        grouping,
        aggCalls,
        window,
        namedWindowProperties,
        needRetraction,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecGroupWindowAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_WINDOW) LogicalWindow window,
      @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES) NamedWindowProperty[] namedWindowProperties,
      @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    checkArgument(inputProperties.size() == 1);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.window = checkNotNull(window);
    this.namedWindowProperties = checkNotNull(namedWindowProperties);
    this.needRetraction = needRetraction;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    final boolean isCountWindow;
    if (window instanceof TumblingGroupWindow) {
      isCountWindow = hasRowIntervalType(((TumblingGroupWindow) window).size());
    } else if (window instanceof SlidingGroupWindow) {
      isCountWindow = hasRowIntervalType(((SlidingGroupWindow) window).size());
    } else {
      isCountWindow = false;
    }

    if (isCountWindow && grouping.length > 0 && config.getStateRetentionTime() < 0) {
      LOGGER.warn(
          "No state retention interval configured for a query which accumulates state. "
              + "Please provide a query configuration with valid retention interval to prevent "
              + "excessive state size. You may specify a retention time of 0 to not clean up the state.");
    }

    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    final RowType inputRowType = (RowType) inputEdge.getOutputType();

    final int inputTimeFieldIndex;
    if (isRowtimeAttribute(window.timeAttribute())) {
      inputTimeFieldIndex = window.timeAttribute().getFieldIndex();
      if (inputTimeFieldIndex < 0) {
        throw new TableException(
            "Group window must defined on a time attribute, "
                + "but the time attribute can't be found.\n"
                + "This should never happen. Please file an issue.");
      }
    } else {
      inputTimeFieldIndex = -1;
    }

    WindowEmitStrategy emitStrategy = WindowEmitStrategy.apply(config, window);

    // --- Begin Gluten-specific code changes ---
    // TODO: velox window not equal to flink window.
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    List<FieldAccessTypedExpr> groupingKeys = Utils.generateFieldAccesses(inputType, grouping);
    List<Aggregate> aggregates = AggregateCallConverter.toAggregates(aggCalls, inputType);
    checkArgument(outputType.getNames().size() >= grouping.length + aggCalls.length);
    List<String> aggNames =
        outputType.getNames().stream()
            .skip(grouping.length)
            .limit(aggCalls.length)
            .collect(Collectors.toList());
    List<Integer> keyIndexes = Arrays.stream(grouping).boxed().collect(Collectors.toList());
    PartitionFunctionSpec keySelectorSpec = new HashPartitionFunctionSpec(inputType, keyIndexes);
    // TODO: support more window types.
    PartitionFunctionSpec sliceAssignerSpec =
        new StreamWindowPartitionFunctionSpec(inputType, inputTimeFieldIndex, 0L, 0L, 0L, 1);
    PlanNode aggregation = new GroupWindowAggsHandlerNode(PlanNodeIdGenerator.newId(), outputType);
    PlanNode windowAgg =
        new GroupWindowAggregationNode(
            PlanNodeIdGenerator.newId(),
            aggregation,
            keySelectorSpec,
            sliceAssignerSpec,
            emitStrategy.getAllowLateness(),
            emitStrategy.produceUpdates(),
            inputTimeFieldIndex,
            true, // TODO: get from window attributes
            1, // TODO: get from window attributes
            outputType);
    final OneInputStreamOperator windowOperator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(windowAgg.getId(), windowAgg),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(windowAgg.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecGroupWindowAggregate");
    // --- End Gluten-specific code changes ---

    final OneInputTransformation<RowData, RowData> transform =
        ExecNodeUtil.createOneInputTransformation(
            inputTransform,
            createTransformationMeta(GROUP_WINDOW_AGGREGATE_TRANSFORMATION, config),
            SimpleOperatorFactory.of(windowOperator),
            InternalTypeInfo.of(getOutputType()),
            inputTransform.getParallelism(),
            false);

    // set KeyType and Selector for state
    final RowDataKeySelector selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            grouping,
            InternalTypeInfo.of(inputRowType));
    transform.setStateKeySelector(selector);
    transform.setStateKeyType(selector.getProducedType());

    return transform;
  }
}
