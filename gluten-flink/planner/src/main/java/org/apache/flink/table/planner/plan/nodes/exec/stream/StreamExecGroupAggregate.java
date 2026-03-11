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
import io.github.zhztheplayer.velox4j.plan.GroupAggregationNode;
import io.github.zhztheplayer.velox4j.plan.GroupAggsHandlerNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} for unbounded group aggregate.
 *
 * <p>This node does support un-splittable aggregate function (e.g. STDDEV_POP).
 */
@ExecNodeMetadata(
    name = "stream-exec-group-aggregate",
    version = 1,
    consumedOptions = {"table.exec.mini-batch.enabled", "table.exec.mini-batch.size"},
    producedTransformations = StreamExecGroupAggregate.GROUP_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecGroupAggregate extends StreamExecAggregateBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamExecGroupAggregate.class);

  public static final String GROUP_AGGREGATE_TRANSFORMATION = "group-aggregate";

  public static final String STATE_NAME = "groupAggregateState";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  /** Each element indicates whether the corresponding agg call needs `retract` method. */
  @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS)
  private final boolean[] aggCallNeedRetractions;

  /** Whether this node will generate UPDATE_BEFORE messages. */
  @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
  private final boolean generateUpdateBefore;

  /** Whether this node consumes retraction messages. */
  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  @Nullable
  @JsonProperty(FIELD_NAME_STATE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<StateMetadata> stateMetadataList;

  public StreamExecGroupAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      boolean[] aggCallNeedRetractions,
      boolean generateUpdateBefore,
      boolean needRetraction,
      @Nullable Long stateTtlFromHint,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecGroupAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecGroupAggregate.class, tableConfig),
        grouping,
        aggCalls,
        aggCallNeedRetractions,
        generateUpdateBefore,
        needRetraction,
        StateMetadata.getOneInputOperatorDefaultMeta(stateTtlFromHint, tableConfig, STATE_NAME),
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecGroupAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS) boolean[] aggCallNeedRetractions,
      @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
      @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
      @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.aggCallNeedRetractions = checkNotNull(aggCallNeedRetractions);
    checkArgument(aggCalls.length == aggCallNeedRetractions.length);
    this.generateUpdateBefore = generateUpdateBefore;
    this.needRetraction = needRetraction;
    this.stateMetadataList = stateMetadataList;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {

    final long stateRetentionTime =
        StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList);
    if (grouping.length > 0 && stateRetentionTime < 0) {
      LOG.warn(
          "No state retention interval configured for a query which accumulates state. "
              + "Please provide a query configuration with valid retention interval to prevent excessive "
              + "state size. You may specify a retention time of 0 to not clean up the state.");
    }

    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    final RowType inputRowType = (RowType) inputEdge.getOutputType();

    // --- Begin Gluten-specific code changes ---
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    List<FieldAccessTypedExpr> groupingKeys = Utils.generateFieldAccesses(inputType, grouping);
    List<Aggregate> aggregates = AggregateCallConverter.toAggregates(aggCalls, inputType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    checkArgument(outputType.getNames().size() == grouping.length + aggCalls.length);
    List<String> aggNames =
        outputType.getNames().stream()
            .skip(grouping.length)
            .limit(aggCalls.length)
            .collect(Collectors.toList());
    List<Integer> keyIndexes = Arrays.stream(grouping).boxed().collect(Collectors.toList());
    PartitionFunctionSpec keySelectorSpec = new HashPartitionFunctionSpec(inputType, keyIndexes);
    PlanNode aggsHandlerNode =
        new GroupAggsHandlerNode(
            PlanNodeIdGenerator.newId(), outputType, generateUpdateBefore, needRetraction);
    // TODO: velox agg may not equal to flink
    PlanNode aggregation =
        new GroupAggregationNode(
            PlanNodeIdGenerator.newId(), aggsHandlerNode, keySelectorSpec, outputType);
    final OneInputStreamOperator operator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(aggregation.getId(), aggregation),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(aggregation.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecGroupAggregate");
    // --- End Gluten-specific code changes ---

    // partitioned aggregation
    final OneInputTransformation<RowData, RowData> transform =
        ExecNodeUtil.createOneInputTransformation(
            inputTransform,
            createTransformationMeta(GROUP_AGGREGATE_TRANSFORMATION, config),
            operator,
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
