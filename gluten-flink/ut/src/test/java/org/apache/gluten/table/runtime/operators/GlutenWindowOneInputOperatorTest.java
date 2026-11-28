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
package org.apache.gluten.table.runtime.operators;

import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.plan.GroupWindowAggregationNode;
import io.github.zhztheplayer.velox4j.plan.GroupWindowAggsHandlerNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateSpec;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.TimestampType;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link GlutenWindowOneInputOperator}: the RocksDB fail-loud rejection on the two
 * production-reachable declaration routes (job configuration and application backend, both on the
 * non-keyed form the planner actually produces), and the clone-type preservation the offloaded job
 * graph generator relies on. The rejection lives before any native session, so no velox environment
 * is needed.
 */
public class GlutenWindowOneInputOperatorTest {

  private static final RowType INPUT_TYPE =
      new RowType(List.of("k", "ts"), List.of(new IntegerType(), new TimestampType()));
  private static final RowType OUTPUT_TYPE =
      new RowType(
          List.of("k", "cnt", "wstart", "wend"),
          List.of(new IntegerType(), new BigIntType(), new TimestampType(), new TimestampType()));

  private static final org.apache.flink.table.types.logical.RowType FLINK_INPUT_TYPE =
      org.apache.flink.table.types.logical.RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.TimestampType(3)
          },
          new String[] {"k", "ts"});
  private static final org.apache.flink.table.types.logical.RowType FLINK_OUTPUT_TYPE =
      org.apache.flink.table.types.logical.RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.BigIntType(),
            new org.apache.flink.table.types.logical.TimestampType(3),
            new org.apache.flink.table.types.logical.TimestampType(3)
          },
          new String[] {"k", "cnt", "wstart", "wend"});

  /** GROUP BY k, SESSION(ts, gap 10), COUNT(*) - the q11 window form as plan data. */
  private static PlanNode sessionWindowAggPlan() {
    List<Aggregate> aggregates =
        List.of(
            new Aggregate(
                new CallTypedExpr(new BigIntType(), List.of(), "count"),
                List.of(),
                null,
                List.of(),
                List.of(),
                false));
    PlanNode handler =
        new GroupWindowAggsHandlerNode(
            PlanNodeIdGenerator.newId(), OUTPUT_TYPE, aggregates, /* countStarIndex= */ 0);
    // WindowType 2 = SESSION in the velox enum (HOP=0, TUMBLE=1, SESSION=2,
    // CUMULATIVE=3); rowtimeIndex 1 = the ts column.
    return new GroupWindowAggregationNode(
        PlanNodeIdGenerator.newId(),
        handler,
        new KeyedStateSpec(List.of(0), List.of(new IntegerType())),
        INPUT_TYPE,
        /* allowedLateness= */ 0L,
        /* produceUpdates= */ false,
        /* rowtimeIndex= */ 1,
        /* isEventTime= */ true,
        /* windowType= */ 2,
        /* gap= */ 10L,
        /* size= */ 0L,
        /* slide= */ 0L,
        /* windowStartIndex= */ 2,
        /* windowEndIndex= */ 3,
        OUTPUT_TYPE);
  }

  private static GlutenWindowOneInputOperator<RowData, RowData> newOperator(PlanNode plan) {
    return new GlutenWindowOneInputOperator(
        new StatefulPlanNode(plan.getId(), plan),
        PlanNodeIdGenerator.newId(),
        INPUT_TYPE,
        Map.of(plan.getId(), OUTPUT_TYPE),
        RowData.class,
        RowData.class,
        "GlutenWindowOneInputOperatorTest");
  }

  private static OneInputStreamOperatorTestHarness<RowData, RowData> newNonKeyedHarness(
      PlanNode plan) throws Exception {
    // The production form: the group window aggregation transformation is
    // non-keyed (no state key serializer in the config), so no keyed backend
    // is ever materialized and the resolved backend must be inspected.
    TypeSerializer<RowData> inputSerializer =
        InternalTypeInfo.of(FLINK_INPUT_TYPE).createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(newOperator(plan), inputSerializer);
    return harness;
  }

  /**
   * Production form of the negative RocksDB case: RocksDB declared by the job configuration ({@code
   * state.backend: rocksdb} in flink-conf.yaml), on the non-keyed form. A keyed-backend-only check
   * can never fire here; the resolved backend must be inspected.
   */
  @Test
  public void rocksdbJobConfigFailsLoudOnNonKeyedForm() throws Exception {
    PlanNode plan = sessionWindowAggPlan();
    OneInputStreamOperatorTestHarness<RowData, RowData> harness = newNonKeyedHarness(plan);
    harness
        .getEnvironment()
        .getJobConfiguration()
        .setString(
            StateBackendOptions.STATE_BACKEND.key(), StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME);
    harness.setup(
        InternalTypeInfo.of(FLINK_OUTPUT_TYPE).createSerializer(new SerializerConfigImpl()));
    assertThatThrownBy(
            () ->
                harness.initializeState(
                    (org.apache.flink.runtime.checkpoint.OperatorSubtaskState) null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("VELOX_UNSUPPORTED")
        .hasMessageContaining("RocksDB");
    assertThat(harness.getOutput()).isEmpty();
  }

  /**
   * Application-declared backend ({@code env.setStateBackend(..)}), transported to the operator
   * through its StreamConfig, on the non-keyed form: the application backend wins over the job and
   * cluster configuration in the resolution.
   */
  @Test
  public void rocksdbAppBackendFailsLoudOnNonKeyedForm() throws Exception {
    PlanNode plan = sessionWindowAggPlan();
    OneInputStreamOperatorTestHarness<RowData, RowData> harness = newNonKeyedHarness(plan);
    harness.getStreamConfig().setStateBackend(new EmbeddedRocksDBStateBackend(true));
    harness.getStreamConfig().serializeAllConfigs();
    harness.setup(
        InternalTypeInfo.of(FLINK_OUTPUT_TYPE).createSerializer(new SerializerConfigImpl()));
    assertThatThrownBy(
            () ->
                harness.initializeState(
                    (org.apache.flink.runtime.checkpoint.OperatorSubtaskState) null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("VELOX_UNSUPPORTED")
        .hasMessageContaining("RocksDB");
    assertThat(harness.getOutput()).isEmpty();
  }

  /**
   * The offloaded job graph generator rebuilds every offloadable one-input operator through
   * cloneWithInputOutputClasses; the base implementation returns a plain {@link
   * GlutenOneInputOperator}, which would drop the RocksDB rejection and the checkpoint-record
   * persistence on the way to the task managers. The clone must stay a {@link
   * GlutenWindowOneInputOperator} with the plan node and identity preserved.
   */
  @Test
  public void cloneWithInputOutputClassesPreservesSubclass() {
    PlanNode plan = sessionWindowAggPlan();
    // Called through the base type, as the offload generator does: with the
    // override the dynamic dispatch returns the subclass; without it the base
    // implementation would return a plain GlutenOneInputOperator.
    GlutenOneInputOperator<RowData, RowData> operator = newOperator(plan);
    StatefulPlanNode wrapper = new StatefulPlanNode(PlanNodeIdGenerator.newId(), plan);
    GlutenOneInputOperator<StatefulRecord, RowData> clone =
        operator.cloneWithInputOutputClasses(wrapper, StatefulRecord.class, RowData.class);
    assertThat(clone).isInstanceOf(GlutenWindowOneInputOperator.class);
    assertThat(clone.getId()).isEqualTo(operator.getId());
    assertThat(clone.getPlanNode()).isSameAs(wrapper);
    assertThat(clone.getDescription()).isEqualTo(operator.getDescription());
  }
}
