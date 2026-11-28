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
import org.apache.gluten.table.runtime.operators.GlutenWindowOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.plan.GroupWindowAggregationNode;
import io.github.zhztheplayer.velox4j.plan.GroupWindowAggsHandlerNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateSpec;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.AggregateUtil.hasRowIntervalType;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.isRowtimeAttribute;
import static org.apache.flink.table.planner.plan.utils.AggregateUtil.toDuration;
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
  public static final String GROUP_WINDOW_AGGREGATE_TRANSFORMATION = "group-window-aggregate";

  public static final String FIELD_NAME_WINDOW = "window";

  public static final String FIELD_NAME_NAMED_WINDOW_PROPERTIES = "namedWindowProperties";

  /** Values of the velox WindowType enum (experimental/stateful/window/Window.h). */
  private static final int VELOX_WINDOW_TYPE_HOP = 0;

  private static final int VELOX_WINDOW_TYPE_TUMBLE = 1;

  private static final int VELOX_WINDOW_TYPE_SESSION = 2;

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
    // --- Gluten-specific code changes ---
    // Fail-loud faces: row-count windows, non-default EMIT strategies,
    // processing-time windows, TIMESTAMP_LTZ time attributes, and aggregate
    // calls with a FILTER clause are out of scope.
    final boolean isCountWindow;
    if (window instanceof TumblingGroupWindow) {
      isCountWindow = hasRowIntervalType(((TumblingGroupWindow) window).size());
    } else if (window instanceof SlidingGroupWindow) {
      isCountWindow = hasRowIntervalType(((SlidingGroupWindow) window).size());
    } else {
      isCountWindow = false;
    }
    checkArgument(
        !isCountWindow,
        "VELOX_UNSUPPORTED: group window aggregation does not support row-count windows "
            + "(CountTumbling/CountSliding are out of scope)");
    final WindowEmitStrategy emitStrategy = WindowEmitStrategy.apply(config, window);
    checkArgument(
        !emitStrategy.produceUpdates(),
        "VELOX_UNSUPPORTED: group window aggregation does not support the produce-updates "
            + "EMIT strategy (early-fire / late-fire with allowed lateness / update modes)");
    checkArgument(
        emitStrategy.getAllowLateness() <= 0,
        "VELOX_UNSUPPORTED: group window aggregation does not support non-zero allowed "
            + "lateness (table.exec.emit.allow-lateness)");
    checkArgument(
        isRowtimeAttribute(window.timeAttribute()),
        "VELOX_UNSUPPORTED: group window aggregation requires a rowtime time attribute "
            + "(processing-time windows are out of scope)");
    checkArgument(
        !(window.timeAttribute().getOutputDataType().getLogicalType()
            instanceof LocalZonedTimestampType),
        "VELOX_UNSUPPORTED: group window aggregation does not support TIMESTAMP_LTZ time "
            + "attributes yet (the native operator aligns window boundaries on the UTC wall "
            + "clock without the session-time-zone shift)");
    checkArgument(
        !config
            .get(org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND)
            .equalsIgnoreCase("rocksdb"),
        "VELOX_UNSUPPORTED: group window aggregation does not support the RocksDB state "
            + "backend (configure the heap state backend: state.backend: hashmap); the job "
            + "configuration is checked here, a runtime check covers the application-declared "
            + "backends the planner cannot see");
    for (AggregateCall call : aggCalls) {
      checkArgument(
          !call.hasFilter(),
          "VELOX_UNSUPPORTED: group window aggregation does not support aggregate calls with "
              + "a FILTER clause yet (the filter would be silently dropped; AGG(x) FILTER "
              + "(WHERE ...) is rejected instead of aggregating every row)");
    }
    if (!(window instanceof SessionGroupWindow
        || window instanceof TumblingGroupWindow
        || window instanceof SlidingGroupWindow)) {
      throw new UnsupportedOperationException(
          "VELOX_UNSUPPORTED: group window aggregation supports SESSION / TUMBLE / "
              + "HOP windows only, window was: "
              + window.toString());
    }
    // --- End of fail-loud faces ---

    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    final org.apache.flink.table.types.logical.RowType inputRowType =
        (org.apache.flink.table.types.logical.RowType) inputEdge.getOutputType();

    final int inputTimeFieldIndex = window.timeAttribute().getFieldIndex();
    checkArgument(
        inputTimeFieldIndex >= 0,
        "Group window must be defined on a time attribute, but the time attribute can't be "
            + "found.\nThis should never happen. Please file an issue.");

    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    List<Aggregate> aggregates = AggregateCallConverter.toAggregates(aggCalls, inputType);
    PlanNode aggregation =
        new GroupWindowAggsHandlerNode(
            PlanNodeIdGenerator.newId(), outputType, aggregates, countStarIndexOf(aggCalls));
    KeyedStateSpec keyedStateSpec = keyedStateSpec(grouping, inputRowType);
    // Window parameters keyed by window kind, routed through the velox
    // WindowType enum (experimental/stateful/window/Window.h: HOP=0,
    // TUMBLE=1, SESSION=2, CUMULATIVE=3). SESSION carries the gap; TUMBLE
    // the size; HOP the size and slide (the legacy group window planner
    // exposes no user offset — it stays the fixed 0 on the velox side).
    int windowType;
    long gap = 0L;
    long size = 0L;
    long slide = 0L;
    if (window instanceof SessionGroupWindow) {
      windowType = VELOX_WINDOW_TYPE_SESSION;
      gap = toDuration(((SessionGroupWindow) window).gap()).toMillis();
    } else if (window instanceof TumblingGroupWindow) {
      windowType = VELOX_WINDOW_TYPE_TUMBLE;
      size = toDuration(((TumblingGroupWindow) window).size()).toMillis();
    } else {
      windowType = VELOX_WINDOW_TYPE_HOP;
      size = toDuration(((SlidingGroupWindow) window).size()).toMillis();
      slide = toDuration(((SlidingGroupWindow) window).slide()).toMillis();
    }
    PlanNode windowAgg =
        new GroupWindowAggregationNode(
            PlanNodeIdGenerator.newId(),
            aggregation,
            keyedStateSpec,
            inputType,
            // The EMIT fail-loud faces above pin these to the INSERT-only defaults.
            emitStrategy.getAllowLateness(),
            emitStrategy.produceUpdates(),
            inputTimeFieldIndex,
            // Event time is guaranteed by the rowtime check above.
            true,
            windowType,
            gap,
            size,
            slide,
            windowStartIndex(namedWindowProperties, grouping.length, aggCalls.length),
            windowEndIndex(namedWindowProperties, grouping.length, aggCalls.length),
            outputType);
    final OneInputStreamOperator windowOperator =
        new GlutenWindowOneInputOperator(
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

    // The transformation stays non-keyed by design: the user key rides the
    // plan's KeyedStateSpec into the velox keyed state backend, and a
    // Flink-side state key selector would put the transformation into a
    // keyed context the native backend does not share (routing through
    // Flink keyed state fails with "Key group not in KeyGroupRange").

    return transform;
  }

  /** Index of the first no-argument COUNT(*) among the calls; -1 when absent. */
  private static int countStarIndexOf(AggregateCall[] aggCalls) {
    for (int i = 0; i < aggCalls.length; i++) {
      AggregateCall call = aggCalls[i];
      if (call.getAggregation().getKind() == SqlKind.COUNT
          && call.getArgList().isEmpty()
          && !call.hasFilter()) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The user key as pure plan data: grouping channels and their input types, restricted to the
   * fixed-width scalar and varchar/varbinary shapes the velox key equality supports.
   */
  private static KeyedStateSpec keyedStateSpec(
      int[] grouping, org.apache.flink.table.types.logical.RowType inputRowType) {
    List<Integer> keyChannels = Arrays.stream(grouping).boxed().collect(Collectors.toList());
    for (int channel : grouping) {
      LogicalType keyType = inputRowType.getTypeAt(channel);
      switch (keyType.getTypeRoot()) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case VARCHAR:
        case VARBINARY:
          break;
        default:
          throw new IllegalArgumentException(
              "VELOX_UNSUPPORTED: group window aggregation does not support a group key of "
                  + "type "
                  + keyType.asSummaryString());
      }
    }
    List<Type> keyTypes =
        keyChannels.stream()
            .map(channel -> LogicalTypeConverter.toVLType(inputRowType.getTypeAt(channel)))
            .collect(Collectors.toList());
    return new KeyedStateSpec(keyChannels, keyTypes);
  }

  /**
   * Output index of the window-start property: keys, aggregates, then properties.
   *
   * <p>Cross-repo contract: the native emit loop fills the trailing time-attribute columns in the
   * fixed planner order [start, end, rowtime, proctime] (rowtime = window maxTimestamp, proctime =
   * NULL), so any change in the Flink window-property order must be synchronized with the velox
   * emit loop in GroupWindowAggregator.
   */
  private static int windowStartIndex(
      NamedWindowProperty[] namedWindowProperties, int numKeys, int numAggs) {
    for (int i = 0; i < namedWindowProperties.length; i++) {
      if (namedWindowProperties[i].getProperty() instanceof WindowStart) {
        return numKeys + numAggs + i;
      }
    }
    return -1;
  }

  /**
   * Output index of the window-end property: keys, aggregates, then properties.
   *
   * <p>Cross-repo contract: the native emit loop fills the trailing time-attribute columns in the
   * fixed planner order [start, end, rowtime, proctime] (rowtime = window maxTimestamp, proctime =
   * NULL), so any change in the Flink window-property order must be synchronized with the velox
   * emit loop in GroupWindowAggregator.
   */
  private static int windowEndIndex(
      NamedWindowProperty[] namedWindowProperties, int numKeys, int numAggs) {
    for (int i = 0; i < namedWindowProperties.length; i++) {
      if (namedWindowProperties[i].getProperty() instanceof WindowEnd) {
        return numKeys + numAggs + i;
      }
    }
    return -1;
  }
}
