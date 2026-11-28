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

import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.streaming.api.operators.GlutenTwoInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.HashPartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.NestedLoopJoinNode;
import io.github.zhztheplayer.velox4j.plan.PartitionFunctionSpec;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamWindowJoinNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.window.WindowJoinOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link StreamExecNode} for WindowJoin. */
@ExecNodeMetadata(
    name = "stream-exec-window-join",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations = StreamExecWindowJoin.WINDOW_JOIN_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecWindowJoin extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  public static final String WINDOW_JOIN_TRANSFORMATION = "window-join";

  public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";
  public static final String FIELD_NAME_LEFT_WINDOWING = "leftWindowing";
  public static final String FIELD_NAME_RIGHT_WINDOWING = "rightWindowing";

  @JsonProperty(FIELD_NAME_JOIN_SPEC)
  private final JoinSpec joinSpec;

  @JsonProperty(FIELD_NAME_LEFT_WINDOWING)
  private final WindowingStrategy leftWindowing;

  @JsonProperty(FIELD_NAME_RIGHT_WINDOWING)
  private final WindowingStrategy rightWindowing;

  public StreamExecWindowJoin(
      ReadableConfig tableConfig,
      JoinSpec joinSpec,
      WindowingStrategy leftWindowing,
      WindowingStrategy rightWindowing,
      InputProperty leftInputProperty,
      InputProperty rightInputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecWindowJoin.class),
        ExecNodeContext.newPersistedConfig(StreamExecWindowJoin.class, tableConfig),
        joinSpec,
        leftWindowing,
        rightWindowing,
        Lists.newArrayList(leftInputProperty, rightInputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecWindowJoin(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
      @JsonProperty(FIELD_NAME_LEFT_WINDOWING) WindowingStrategy leftWindowing,
      @JsonProperty(FIELD_NAME_RIGHT_WINDOWING) WindowingStrategy rightWindowing,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    checkArgument(inputProperties.size() == 2);
    this.joinSpec = checkNotNull(joinSpec);
    validate(leftWindowing);
    validate(rightWindowing);
    this.leftWindowing = leftWindowing;
    this.rightWindowing = rightWindowing;
  }

  private void validate(WindowingStrategy windowing) {
    // validate window strategy
    if (!windowing.isRowtime()) {
      throw new TableException("Processing time Window Join is not supported yet.");
    }

    if (!(windowing instanceof WindowAttachedWindowingStrategy)) {
      throw new TableException(windowing.getClass().getName() + " is not supported yet.");
    }
  }

  // Only the shapes the velox window join supports today offload to it:
  // event-time non-LTZ attributes on both sides, any join type whose
  // residual condition is empty (the key equality is enforced by the
  // (key..., window end) state buckets; all six Flink join semantics are
  // handled by the operator's per-type emit paths).
  private boolean isOffloadable() {
    return !(leftWindowing.getTimeAttributeType() instanceof LocalZonedTimestampType)
        && !(rightWindowing.getTimeAttributeType() instanceof LocalZonedTimestampType)
        && !joinSpec.getNonEquiCondition().isPresent();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    int leftWindowEndIndex = ((WindowAttachedWindowingStrategy) leftWindowing).getWindowEnd();
    int rightWindowEndIndex = ((WindowAttachedWindowingStrategy) rightWindowing).getWindowEnd();
    final ExecEdge leftInputEdge = getInputEdges().get(0);
    final ExecEdge rightInputEdge = getInputEdges().get(1);

    final Transformation<RowData> leftTransform =
        (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
    final Transformation<RowData> rightTransform =
        (Transformation<RowData>) rightInputEdge.translateToPlan(planner);

    final RowType leftType = (RowType) leftInputEdge.getOutputType();
    final RowType rightType = (RowType) rightInputEdge.getOutputType();
    JoinUtil.validateJoinSpec(joinSpec, leftType, rightType, true);

    final int[] leftJoinKey = joinSpec.getLeftKeys();
    final int[] rightJoinKey = joinSpec.getRightKeys();

    final InternalTypeInfo<RowData> leftTypeInfo = InternalTypeInfo.of(leftType);
    final InternalTypeInfo<RowData> rightTypeInfo = InternalTypeInfo.of(rightType);

    final TwoInputStreamOperator operator;
    if (!isOffloadable()) {
      // The stock Flink translation for the shapes the velox operator does
      // not cover yet (non-INNER, residual conditions, TIMESTAMP_LTZ).
      GeneratedJoinCondition generatedCondition =
          JoinUtil.generateConditionFunction(
              config, planner.getFlinkContext().getClassLoader(), joinSpec, leftType, rightType);
      ZoneId shiftTimeZone = java.time.ZoneId.of("UTC");
      operator =
          WindowJoinOperatorBuilder.builder()
              .leftSerializer(leftTypeInfo.toRowSerializer())
              .rightSerializer(rightTypeInfo.toRowSerializer())
              .generatedJoinCondition(generatedCondition)
              .leftWindowEndIndex(leftWindowEndIndex)
              .rightWindowEndIndex(rightWindowEndIndex)
              .filterNullKeys(joinSpec.getFilterNulls())
              .joinType(joinSpec.getJoinType())
              .withShiftTimezone(shiftTimeZone)
              .build();
    } else {
      // --- Begin Gluten-specific code changes ---
      io.github.zhztheplayer.velox4j.type.RowType leftInputType =
          (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(leftType);
      io.github.zhztheplayer.velox4j.type.RowType rightInputType =
          (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(rightType);
      io.github.zhztheplayer.velox4j.type.RowType outputType =
          (io.github.zhztheplayer.velox4j.type.RowType)
              LogicalTypeConverter.toVLType(getOutputType());
      rightInputType = Utils.substituteSameName(leftInputType, rightInputType, outputType);

      PlanNode leftInput =
          new TableScanNode(
              PlanNodeIdGenerator.newId(),
              leftInputType,
              new ExternalStreamTableHandle("connector-external-stream"),
              List.of());
      PlanNode rightInput =
          new TableScanNode(
              PlanNodeIdGenerator.newId(),
              rightInputType,
              new ExternalStreamTableHandle("connector-external-stream"),
              List.of());
      // The state key is the composite (equi key..., window end): the
      // window end rides the key row, so the exchange, the state bucketing
      // and the timer key all agree on one key.
      List<Integer> leftKeyIndexes = new ArrayList<>();
      for (int channel : leftJoinKey) {
        leftKeyIndexes.add(channel);
      }
      if (!leftKeyIndexes.contains(leftWindowEndIndex)) {
        leftKeyIndexes.add(leftWindowEndIndex);
      }
      List<Integer> rightKeyIndexes = new ArrayList<>();
      for (int channel : rightJoinKey) {
        rightKeyIndexes.add(channel);
      }
      if (!rightKeyIndexes.contains(rightWindowEndIndex)) {
        rightKeyIndexes.add(rightWindowEndIndex);
      }
      PartitionFunctionSpec leftPartFuncSpec =
          new HashPartitionFunctionSpec(leftInputType, leftKeyIndexes);
      PartitionFunctionSpec rightPartFuncSpec =
          new HashPartitionFunctionSpec(rightInputType, rightKeyIndexes);
      // INNER with no residual condition: the state buckets already enforce
      // every key-equality conjunct, so the probe condition drops and the
      // native fast path (zero-copy dictionary cross product) takes over.
      // The probe always carries INNER: NLJ rejects semi/anti and the
      // operator handles all six semantics from the node's joinType field.
      NestedLoopJoinNode probeNode =
          new NestedLoopJoinNode(
              PlanNodeIdGenerator.newId(),
              Utils.toVLJoinType(FlinkJoinType.INNER),
              null,
              new EmptyNode(leftInputType),
              new EmptyNode(rightInputType),
              outputType);
      // Velox JoinType ordinal: INNER=0, LEFT=1, RIGHT=2, FULL=3,
      // LEFT_SEMI_FILTER=4, ANTI=5. The probe always carries INNER
      // (NLJ rejects semi/anti); the actual semantics ride this field.
      // Map by enum constant, not by toString(): FlinkJoinType.toString()
      // returns descriptions like "LeftOuterJoin", so name comparisons never
      // matched and every join silently degraded to INNER.
      int veloxJoinType;
      switch (joinSpec.getJoinType()) {
        case LEFT:
          veloxJoinType = 1;
          break;
        case RIGHT:
          veloxJoinType = 2;
          break;
        case FULL:
          veloxJoinType = 3;
          break;
        case SEMI:
          veloxJoinType = 4;
          break;
        case ANTI:
          veloxJoinType = 5;
          break;
        default:
          veloxJoinType = 0; // INNER
      }
      PlanNode join =
          new StreamWindowJoinNode(
              PlanNodeIdGenerator.newId(),
              leftInput,
              rightInput,
              leftPartFuncSpec,
              rightPartFuncSpec,
              probeNode,
              outputType,
              1024,
              leftWindowEndIndex,
              rightWindowEndIndex,
              leftKeyIndexes,
              rightKeyIndexes,
              /* shiftTimeZone = */ 0,
              /* joinType = */ veloxJoinType);
      operator =
          new GlutenTwoInputOperator(
              new StatefulPlanNode(join.getId(), join),
              leftInput.getId(),
              rightInput.getId(),
              leftInputType,
              rightInputType,
              Map.of(join.getId(), outputType),
              RowData.class,
              RowData.class,
              "StreamExecWindowJoin");
      // --- End Gluten-specific code changes ---
    }

    final RowType returnType = (RowType) getOutputType();
    final TwoInputTransformation<RowData, RowData, RowData> transform =
        ExecNodeUtil.createTwoInputTransformation(
            leftTransform,
            rightTransform,
            createTransformationMeta(WINDOW_JOIN_TRANSFORMATION, config),
            new GlutenTwoInputOperatorFactory<RowData, RowData, RowData>(
                (StreamOperator<RowData>) operator),
            InternalTypeInfo.of(returnType),
            leftTransform.getParallelism(),
            0L,
            false);

    // set KeyType and Selector for state
    RowDataKeySelector leftSelect =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(), leftJoinKey, leftTypeInfo);
    RowDataKeySelector rightSelect =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(), rightJoinKey, rightTypeInfo);
    transform.setStateKeySelectors(leftSelect, rightSelect);
    transform.setStateKeyType(leftSelect.getProducedType());
    return transform;
  }
}
