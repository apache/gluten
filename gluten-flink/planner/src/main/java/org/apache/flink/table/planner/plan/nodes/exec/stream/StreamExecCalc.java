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
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Gluten Stream {@link ExecNode} for Calc to use {@link GlutenSingleInputOperator}. */
@ExecNodeMetadata(
    name = "stream-exec-calc",
    version = 1,
    producedTransformations = CommonExecCalc.CALC_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecCalc extends CommonExecCalc implements StreamExecNode<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamExecCalc.class);

  public StreamExecCalc(
      ReadableConfig tableConfig,
      List<RexNode> projection,
      @Nullable RexNode condition,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecCalc.class),
        ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
        projection,
        condition,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecCalc(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
      @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(
        id,
        context,
        persistedConfig,
        projection,
        condition,
        TableStreamOperator.class,
        true, // retainHeader
        inputProperties,
        outputType,
        description);
  }

  @Override
  public Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);

    // --- Begin Gluten-specific code changes ---
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(inputEdge.getOutputType());
    List<String> inNames = Utils.getNamesFromRowType(inputEdge.getOutputType());
    RexConversionContext conversionContext = new RexConversionContext(inNames);
    PlanNode filter = null;
    if (condition != null) {
      filter =
          new FilterNode(
              PlanNodeIdGenerator.newId(),
              List.of(new EmptyNode(inputType)),
              RexNodeConverter.toTypedExpr(condition, conversionContext));
    }
    List<TypedExpr> projectExprs = RexNodeConverter.toTypedExpr(projection, conversionContext);
    PlanNode project =
        new ProjectNode(
            PlanNodeIdGenerator.newId(),
            filter == null ? List.of(new EmptyNode(inputType)) : List.of(filter),
            Utils.getNamesFromRowType(getOutputType()),
            projectExprs);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    final OneInputStreamOperator calOperator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(project.getId(), project),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(project.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecCalc");
    return ExecNodeUtil.createOneInputTransformation(
        inputTransform,
        new TransformationMetadata("gluten-calc", "Gluten cal operator"),
        calOperator,
        InternalTypeInfo.of(getOutputType()),
        inputTransform.getParallelism(),
        false);
    // --- End Gluten-specific code changes ---
  }
}
