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
import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    // --- Begin Gluten-specific code changes ---
    io.github.zhztheplayer.velox4j.type.RowType leftInputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(leftType);
    io.github.zhztheplayer.velox4j.type.RowType rightInputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(rightType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    rightInputType = Utils.substituteSameName(leftInputType, rightInputType, outputType);
    List<FieldAccessTypedExpr> leftKeys =
        Utils.analyzeJoinKeys(leftInputType, leftJoinKey, List.of());
    List<FieldAccessTypedExpr> rightKeys =
        Utils.analyzeJoinKeys(rightInputType, rightJoinKey, List.of());
    TypedExpr joinCondition = Utils.generateJoinEqualCondition(leftKeys, rightKeys);

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
    List<Integer> leftKeyIndexes = Arrays.stream(leftJoinKey).boxed().collect(Collectors.toList());
    List<Integer> rightKeyIndexes =
        Arrays.stream(rightJoinKey).boxed().collect(Collectors.toList());
    PartitionFunctionSpec leftPartFuncSpec =
        new HashPartitionFunctionSpec(leftInputType, leftKeyIndexes);
    PartitionFunctionSpec rightPartFuncSpec =
        new HashPartitionFunctionSpec(rightInputType, rightKeyIndexes);
    NestedLoopJoinNode probeNode =
        new NestedLoopJoinNode(
            PlanNodeIdGenerator.newId(),
            Utils.toVLJoinType(joinSpec.getJoinType()),
            joinCondition,
            new EmptyNode(rightInputType),
            new EmptyNode(leftInputType),
            outputType);
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
            rightWindowEndIndex);
    final TwoInputStreamOperator operator =
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

    final RowType returnType = (RowType) getOutputType();
    final TwoInputTransformation<RowData, RowData, RowData> transform =
        ExecNodeUtil.createTwoInputTransformation(
            leftTransform,
            rightTransform,
            createTransformationMeta(WINDOW_JOIN_TRANSFORMATION, config),
            operator,
            InternalTypeInfo.of(returnType),
            leftTransform.getParallelism(),
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
