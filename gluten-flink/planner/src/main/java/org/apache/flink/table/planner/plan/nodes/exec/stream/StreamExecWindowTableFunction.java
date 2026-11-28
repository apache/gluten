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
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.IntervalDayTimeType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.DoubleValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.operator.UnalignedWindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.tvf.operator.WindowTableFunctionOperatorBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.plan.utils.WindowTableFunctionUtil.createWindowAssigner;

/**
 * Gluten override of the stream window table-valued function ExecNode.
 *
 * <p>For aligned TUMBLE windows whose size is an exact natural time unit (1s/1m/1h/1d, offset
 * zero), the JVM {@code WindowTableFunctionOperator} is replaced by a velox projection that
 * computes {@code window_start} via {@code date_trunc} and derives {@code window_end} / {@code
 * window_time} by interval arithmetic. This removes the velox→JVM→velox crossing that dominates
 * canonical TVF queries. All other window kinds (HOP, SESSION, CUMULATE, non-unit sizes) fall back
 * to the stock JVM translation.
 */
@ExecNodeMetadata(
    name = "stream-exec-window-table-function",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations = CommonExecWindowTableFunction.WINDOW_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecWindowTableFunction extends CommonExecWindowTableFunction
    implements StreamExecNode<RowData> {

  public StreamExecWindowTableFunction(
      ReadableConfig tableConfig,
      TimeAttributeWindowingStrategy windowingStrategy,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecWindowTableFunction.class),
        ExecNodeContext.newPersistedConfig(StreamExecWindowTableFunction.class, tableConfig),
        windowingStrategy,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecWindowTableFunction(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_WINDOWING) TimeAttributeWindowingStrategy windowingStrategy,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(
        id, context, persistedConfig, windowingStrategy, inputProperties, outputType, description);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    if (isUnitTumble()
        && !(windowingStrategy.getTimeAttributeType() instanceof LocalZonedTimestampType)) {
      // Velox computes the TUMBLE bounds as a projection with the real
      // window_start/window_end/window_time values, so the translation is
      // correct for any consumer of the TVF output.
      return translateWithVeloxProjection(planner, config);
    }
    // HOP/SESSION and TIMESTAMP_LTZ time attributes keep the stock JVM
    // translation; they move to the native stack with their own staged PRs.
    return super.translateToPlanInternal(planner, config);
  }

  /** Whether this TVF is a zero-offset TUMBLE (any size). */
  org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy
      getWindowingStrategy() {
    return windowingStrategy;
  }

  boolean isUnitTumble() {
    WindowSpec window = windowingStrategy.getWindow();
    if (!(window instanceof TumblingWindowSpec)) {
      return false;
    }
    TumblingWindowSpec tumble = (TumblingWindowSpec) window;
    return (tumble.getOffset() == null || Duration.ZERO.equals(tumble.getOffset()))
        && tumble.getSize().toMillis() > 0
        && tumble.getSize().toMillis() % 1000 == 0;
  }

  private boolean isNaturalUnitSize() {
    long millis = sizeMillisOfTumble();
    return millis == 1000L || millis == 60_000L || millis == 3_600_000L || millis == 86_400_000L;
  }

  String unitName() {
    long millis = sizeMillisOfTumble();
    if (millis == 1000L) {
      return "second";
    }
    if (millis == 60_000L) {
      return "minute";
    }
    if (millis == 3_600_000L) {
      return "hour";
    }
    return "day";
  }

  long sizeMillisOfTumble() {
    return ((TumblingWindowSpec) windowingStrategy.getWindow()).getSize().toMillis();
  }

  @SuppressWarnings("unchecked")
  private Transformation<RowData> translateWithVeloxProjection(
      PlannerBase planner, ExecNodeConfig config) {
    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);

    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(inputEdge.getOutputType());
    List<String> inNames = Utils.getNamesFromRowType(inputEdge.getOutputType());
    List<Type> inTypes = inputType.getChildren();

    int timeIdx = windowingStrategy.getTimeAttributeIndex();
    Preconditions.checkArgument(
        timeIdx >= 0 && timeIdx < inNames.size(),
        "Time attribute index %s out of range for input %s",
        timeIdx,
        inNames);

    long sizeMillis = ((TumblingWindowSpec) windowingStrategy.getWindow()).getSize().toMillis();
    TypedExpr tsExpr = FieldAccessTypedExpr.create(inTypes.get(timeIdx), inNames.get(timeIdx));
    // date_trunc expects a TIMESTAMP; cast when the time attribute arrives as another type.
    if (!(tsExpr.getReturnType() instanceof TimestampType)) {
      tsExpr =
          io.github.zhztheplayer.velox4j.expression.CastTypedExpr.create(
              new TimestampType(), tsExpr, false);
    }

    // General aligned-tumble start: floor epoch micros to the window
    // size, then cast back to TIMESTAMP.
    TypedExpr micros =
        io.github.zhztheplayer.velox4j.expression.CastTypedExpr.create(
            new BigIntType(), tsExpr, false);
    TypedExpr microsDouble =
        io.github.zhztheplayer.velox4j.expression.CastTypedExpr.create(
            new DoubleType(), micros, false);
    TypedExpr startExpr;
    if (isNaturalUnitSize()) {
      // date_trunc handles unit-aligned windows natively (1s/1m/1h/1d).
      ConstantTypedExpr unitExpr =
          new ConstantTypedExpr(new VarCharType(), new VarCharValue(unitName()), null);
      startExpr = new CallTypedExpr(new TimestampType(), List.of(unitExpr, tsExpr), "date_trunc");
    } else {
      // cast(TIMESTAMP AS BIGINT) in velox yields epoch seconds; floor to
      // the window boundary via divide+floor+multiply and cast back.
      TypedExpr sizeSecondsDouble =
          new ConstantTypedExpr(new DoubleType(), new DoubleValue(sizeMillis / 1000.0), null);
      TypedExpr ratio =
          new CallTypedExpr(new DoubleType(), List.of(microsDouble, sizeSecondsDouble), "divide");
      TypedExpr floored = new CallTypedExpr(new DoubleType(), List.of(ratio), "floor");
      TypedExpr startDouble =
          new CallTypedExpr(new DoubleType(), List.of(floored, sizeSecondsDouble), "multiply");
      TypedExpr startMicros =
          io.github.zhztheplayer.velox4j.expression.CastTypedExpr.create(
              new BigIntType(), startDouble, false);
      startExpr =
          io.github.zhztheplayer.velox4j.expression.CastTypedExpr.create(
              new TimestampType(), startMicros, false);
    }

    TypedExpr intervalExpr =
        new ConstantTypedExpr(new IntervalDayTimeType(), new BigIntValue(sizeMillis), null);
    TypedExpr endExpr =
        new CallTypedExpr(new TimestampType(), List.of(startExpr, intervalExpr), "plus");

    TypedExpr oneMsExpr =
        new ConstantTypedExpr(new IntervalDayTimeType(), new BigIntValue(1L), null);
    TypedExpr windowTimeExpr =
        new CallTypedExpr(new TimestampType(), List.of(endExpr, oneMsExpr), "minus");

    List<TypedExpr> projectExprs = new ArrayList<>();
    for (int i = 0; i < inNames.size(); i++) {
      projectExprs.add(FieldAccessTypedExpr.create(inTypes.get(i), inNames.get(i)));
    }
    projectExprs.add(startExpr);
    projectExprs.add(endExpr);
    projectExprs.add(windowTimeExpr);

    List<String> outNames = Utils.getNamesFromRowType(getOutputType());
    Preconditions.checkArgument(
        outNames.size() == projectExprs.size(),
        "TVF output arity mismatch: %s names vs %s expressions",
        outNames.size(),
        projectExprs.size());

    PlanNode project =
        new ProjectNode(
            PlanNodeIdGenerator.newId(), List.of(new EmptyNode(inputType)), outNames, projectExprs);

    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());

    final OneInputStreamOperator tvfOperator =
        new GlutenOneInputOperator(
            new StatefulPlanNode(project.getId(), project),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(project.getId(), outputType),
            RowData.class,
            RowData.class,
            "StreamExecWindowTableFunction");

    return ExecNodeUtil.createOneInputTransformation(
        inputTransform,
        new org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata(
            "gluten-tvf", "Gluten window table function (velox projection)"),
        tvfOperator,
        InternalTypeInfo.of(getOutputType()),
        inputTransform.getParallelism(),
        false);
  }

  // ---------------------------------------------------------------------------
  // Stock JVM translation for unsupported window kinds (copied from upstream).
  // ---------------------------------------------------------------------------

  @Override
  protected Transformation<RowData> translateWithUnalignedWindow(
      PlannerBase planner,
      ExecNodeConfig config,
      RowType inputRowType,
      Transformation<RowData> inputTransform) {
    final WindowTableFunctionOperatorBase windowTableFunctionOperator =
        createUnalignedWindowTableFunctionOperator(config, inputRowType);
    final OneInputTransformation<RowData, RowData> transform =
        ExecNodeUtil.createOneInputTransformation(
            inputTransform,
            createTransformationMeta(WINDOW_TRANSFORMATION, config),
            windowTableFunctionOperator,
            InternalTypeInfo.of(getOutputType()),
            inputTransform.getParallelism(),
            false);

    final int[] partitionKeys = extractPartitionKeys(windowingStrategy.getWindow());
    final RowDataKeySelector selector =
        org.apache.flink.table.planner.plan.utils.KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            partitionKeys,
            InternalTypeInfo.of(inputRowType));
    transform.setStateKeySelector(selector);
    transform.setStateKeyType(selector.getProducedType());
    return transform;
  }

  private int[] extractPartitionKeys(WindowSpec window) {
    // The TUMBLE/HOP shapes route to the velox projection above; only the
    // unaligned (session) path reaches here, mirroring upstream.
    if (window instanceof org.apache.flink.table.planner.plan.logical.SessionWindowSpec) {
      return ((org.apache.flink.table.planner.plan.logical.SessionWindowSpec) window)
          .getPartitionKeyIndices();
    }
    return new int[0];
  }

  private WindowTableFunctionOperatorBase createUnalignedWindowTableFunctionOperator(
      ExecNodeConfig config, RowType inputRowType) {
    GroupWindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowingStrategy);
    final ZoneId shiftTimeZone =
        TimeWindowUtil.getShiftTimeZone(
            windowingStrategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config));
    return new UnalignedWindowTableFunctionOperator(
        windowAssigner,
        windowAssigner.getWindowSerializer(new org.apache.flink.api.common.ExecutionConfig()),
        new RowDataSerializer(inputRowType),
        windowingStrategy.getTimeAttributeIndex(),
        shiftTimeZone);
  }
}
