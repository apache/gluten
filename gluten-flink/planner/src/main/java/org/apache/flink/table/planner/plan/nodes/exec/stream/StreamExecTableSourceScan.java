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
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.velox.VeloxSourceSinkFactory;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Stream {@link ExecNode} to read data from an external source defined by a {@link
 * ScanTableSource}.
 */
@ExecNodeMetadata(
    name = "stream-exec-table-source-scan",
    version = 1,
    producedTransformations = CommonExecTableSourceScan.SOURCE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecTableSourceScan extends CommonExecTableSourceScan
    implements StreamExecNode<RowData> {

  public StreamExecTableSourceScan(
      ReadableConfig tableConfig,
      DynamicTableSourceSpec tableSourceSpec,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecTableSourceScan.class),
        ExecNodeContext.newPersistedConfig(StreamExecTableSourceScan.class, tableConfig),
        tableSourceSpec,
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecTableSourceScan(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_SCAN_TABLE_SOURCE) DynamicTableSourceSpec tableSourceSpec,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(
        id,
        context,
        persistedConfig,
        tableSourceSpec,
        Collections.emptyList(),
        outputType,
        description);
  }

  @Override
  public Transformation<RowData> createInputFormatTransformation(
      StreamExecutionEnvironment env,
      InputFormat<RowData, ?> inputFormat,
      InternalTypeInfo<RowData> outputTypeInfo,
      String operatorName) {
    // It's better to use StreamExecutionEnvironment.createInput()
    // rather than addLegacySource() for streaming, because it take care of checkpoint.
    return env.createInput(inputFormat, outputTypeInfo).name(operatorName).getTransformation();
  }

  private ProjectNode translateWatermarkExpr(
      LogicalType inputType, LogicalType outputType, RexNode watermarkExpr) {
    List<String> inNames = Utils.getNamesFromRowType(inputType);
    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr watermarkExprs = RexNodeConverter.toTypedExpr(watermarkExpr, conversionContext);
    io.github.zhztheplayer.velox4j.type.RowType outputRowType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(outputType);
    return new ProjectNode(
        PlanNodeIdGenerator.newId(),
        List.of(new EmptyNode(outputRowType)),
        List.of("TIMESTAMP"),
        List.of(watermarkExprs));
  }

  private Optional<io.github.zhztheplayer.velox4j.plan.WatermarkPushDownSpec>
      getWatermarkPushDownSpec(Transformation<RowData> transformation, ExecNodeConfig config) {
    io.github.zhztheplayer.velox4j.plan.WatermarkPushDownSpec watermarkPushDownSpecNode = null;
    if (transformation instanceof SourceTransformation) {
      List<SourceAbilitySpec> sourceAbilities = getTableSourceSpec().getSourceAbilities();
      if (sourceAbilities != null) {
        for (SourceAbilitySpec sourceAbility : sourceAbilities) {
          if (sourceAbility instanceof WatermarkPushDownSpec) {
            final long idleTimeout =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT).toMillis();
            final long watermarkInterval =
                config.get(PipelineOptions.AUTO_WATERMARK_INTERVAL).toMillis();
            WatermarkPushDownSpec watermarkPushDownSpec = (WatermarkPushDownSpec) sourceAbility;
            RowField watermarkField = new RowField("watermark", new TimestampType(3));
            ProjectNode project =
                translateWatermarkExpr(
                    getOutputType(),
                    new RowType(List.of(watermarkField)),
                    watermarkPushDownSpec.getWatermarkExpr());
            watermarkPushDownSpecNode =
                new io.github.zhztheplayer.velox4j.plan.WatermarkPushDownSpec(
                    project, idleTimeout, watermarkInterval, -1);
          }
        }
      }
    }
    return watermarkPushDownSpecNode != null
        ? Optional.of(watermarkPushDownSpecNode)
        : Optional.empty();
  }

  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    // --- Begin Gluten-specific code changes ---
    final ScanTableSource tableSource =
        getTableSourceSpec()
            .getScanTableSource(
                planner.getFlinkContext(), ShortcutUtils.unwrapTypeFactory(planner));
    Transformation<RowData> transformation = super.translateToPlanInternal(planner, config);
    Optional<io.github.zhztheplayer.velox4j.plan.WatermarkPushDownSpec> watermarkPushDownSpec =
        getWatermarkPushDownSpec(transformation, config);
    return VeloxSourceSinkFactory.buildSource(
        transformation,
        Map.of(
            ScanTableSource.class.getName(),
            tableSource,
            "checkpoint.enabled",
            planner.getExecEnv().getCheckpointConfig().isCheckpointingEnabled(),
            "watermarkPushDownSpec",
            watermarkPushDownSpec,
            VeloxSourceSinkFactory.FACTORY_CLASS_LOADER_KEY,
            StreamExecTableSourceScan.class.getClassLoader()));
    // --- End Gluten-specific code changes ---
  }
}
