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

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.WatermarkAssignerNode;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} which generates watermark based on the input elements. */
@ExecNodeMetadata(
    name = "stream-exec-watermark-assigner",
    version = 1,
    producedTransformations = StreamExecWatermarkAssigner.WATERMARK_ASSIGNER_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecWatermarkAssigner extends ExecNodeBase<RowData>
    implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

  public static final String WATERMARK_ASSIGNER_TRANSFORMATION = "watermark-assigner";

  public static final String FIELD_NAME_WATERMARK_EXPR = "watermarkExpr";
  public static final String FIELD_NAME_ROWTIME_FIELD_INDEX = "rowtimeFieldIndex";

  @JsonProperty(FIELD_NAME_WATERMARK_EXPR)
  private final RexNode watermarkExpr;

  @JsonProperty(FIELD_NAME_ROWTIME_FIELD_INDEX)
  private final int rowtimeFieldIndex;

  public StreamExecWatermarkAssigner(
      ReadableConfig tableConfig,
      RexNode watermarkExpr,
      int rowtimeFieldIndex,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecWatermarkAssigner.class),
        ExecNodeContext.newPersistedConfig(StreamExecWatermarkAssigner.class, tableConfig),
        watermarkExpr,
        rowtimeFieldIndex,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecWatermarkAssigner(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_WATERMARK_EXPR) RexNode watermarkExpr,
      @JsonProperty(FIELD_NAME_ROWTIME_FIELD_INDEX) int rowtimeFieldIndex,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    checkArgument(inputProperties.size() == 1);
    this.watermarkExpr = checkNotNull(watermarkExpr);
    this.rowtimeFieldIndex = rowtimeFieldIndex;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);

    // --- Begin Gluten-specific code changes ---
    final long idleTimeout =
        config.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT).toMillis();
    final long watermarkInterval = config.get(PipelineOptions.AUTO_WATERMARK_INTERVAL).toMillis();

    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(inputEdge.getOutputType());
    List<String> inNames = Utils.getNamesFromRowType(inputEdge.getOutputType());
    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr watermarkExprs = RexNodeConverter.toTypedExpr(watermarkExpr, conversionContext);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    ProjectNode project =
        new ProjectNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(outputType)),
            List.of("TIMESTAMP"),
            List.of(watermarkExprs));
    PlanNode watermark =
        new WatermarkAssignerNode(
            PlanNodeIdGenerator.newId(),
            null,
            project,
            idleTimeout,
            rowtimeFieldIndex,
            watermarkInterval);
    final OneInputStreamOperator watermarkOperator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(watermark.getId(), watermark),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(watermark.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecWatermarkAssigner");

    return ExecNodeUtil.createOneInputTransformation(
        inputTransform,
        createTransformationMeta(WATERMARK_ASSIGNER_TRANSFORMATION, config),
        watermarkOperator,
        InternalTypeInfo.of(getOutputType()),
        inputTransform.getParallelism(),
        false);
    // --- End Gluten-specific code changes ---
  }
}
