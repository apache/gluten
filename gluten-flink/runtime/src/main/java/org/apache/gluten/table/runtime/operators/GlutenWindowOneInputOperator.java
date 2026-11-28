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

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateBackendParameters;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link GlutenOneInputOperator} for the group-window aggregation: replaces the base class's
 * RocksDB behavior (silently skipping native state initialization) with fail-loud rejection, and
 * persists the native keyed-state checkpoint records into Flink operator state so they survive
 * restarts and rescales.
 *
 * <p>RocksDB rejection: the window aggregation transformation is intentionally non-keyed (the user
 * key rides the plan's KeyedStateSpec and the upstream exchange), so no keyed backend is ever
 * materialized for this operator and inspecting the materialized keyed backend alone can never fire
 * in production. {@code usesRocksDBStateBackend} therefore resolves the effective backend the same
 * way the containing task does (application backend from the operator config, then job
 * configuration, then cluster configuration, via {@link
 * StateBackendLoader#fromApplicationOrConfigOrDefault}) and inspects the resolved factory, in
 * addition to the materialized keyed backend.
 *
 * <p>Checkpoint round-trip: {@code snapshotNativeState} stores the records returned by {@link
 * SerialTask#snapshotState(long)} into a union-list operator state; {@code initializeNativeState}
 * replays them through {@link SerialTask#initializeState(long,
 * io.github.zhztheplayer.velox4j.stateful.KeyedStateBackendParameters, String[])}. The union list
 * hands every restored task the full set of snapshot records; the velox-side restore keeps only the
 * entries overlapping the task's key-group range. (The records ride the velox4j JNI String
 * boundary, which is not binary-safe - the known checkpoint e2e limitation; the native
 * snapshot/restore semantics are pinned by the velox-side unit tests.)
 */
public class GlutenWindowOneInputOperator<IN, OUT> extends GlutenOneInputOperator<IN, OUT>
    implements OneInputStreamOperator<IN, OUT> {

  private static final long serialVersionUID = 1L;

  private static final String NATIVE_STATE_NAME = "group-window-agg-native-state";

  private static final ListStateDescriptor<String> NATIVE_STATE_DESCRIPTOR =
      new ListStateDescriptor<>(NATIVE_STATE_NAME, StringSerializer.INSTANCE);

  /** Union list carrying the native keyed-state records; captured in initializeState. */
  private transient ListState<String> nativeStateStore;

  public GlutenWindowOneInputOperator(
      StatefulPlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass,
      String description) {
    super(plan, id, inputType, outputTypes, inClass, outClass, description);
  }

  public GlutenWindowOneInputOperator(
      StatefulPlanNode plan,
      String id,
      RowType inputType,
      Map<String, RowType> outputTypes,
      Class<IN> inClass,
      Class<OUT> outClass) {
    super(plan, id, inputType, outputTypes, inClass, outClass);
  }

  /**
   * Clones into the same subclass, not the base class: the offloaded job graph generator rebuilds
   * every offloadable one-input operator through this method ({@code
   * OffloadedJobGraphGenerator#createOffloadedOneInputOperator}), and the base implementation
   * constructs a plain {@link GlutenOneInputOperator} - silently dropping this subclass's RocksDB
   * fail-loud rejection and native-checkpoint persistence on the way to the task managers. Same
   * override pattern as {@code WindowAggOperator#cloneWithInputOutputClasses}.
   */
  @Override
  public <NIN, NOUT> GlutenWindowOneInputOperator<NIN, NOUT> cloneWithInputOutputClasses(
      StatefulPlanNode plan, Class<NIN> newInClass, Class<NOUT> newOutClass) {
    return new GlutenWindowOneInputOperator<>(
        plan, getId(), getInputType(), getOutputTypes(), newInClass, newOutClass, getDescription());
  }

  @Override
  protected void initializeNativeState(StateInitializationContext context) throws Exception {
    if (usesRocksDBStateBackend()) {
      // The shared base class silently skips the native state initialization
      // for RocksDB; a keyed native operator must fail loud instead of
      // running without its state. Checked before any native session is
      // created so the rejection needs no native library.
      throw new UnsupportedOperationException(
          "VELOX_UNSUPPORTED: group window aggregation does not support the RocksDB keyed "
              + "state backend; configure the heap state backend (state.backend: hashmap)");
    }
    if (task == null) {
      initSession();
    }
    // The union list is registered and captured here (on every
    // initializeState, fresh or restoring); snapshotNativeState below writes
    // through the same state handle, the standard Flink operator pattern.
    nativeStateStore = context.getOperatorStateStore().getUnionListState(NATIVE_STATE_DESCRIPTOR);
    List<String> restored = new ArrayList<>();
    if (context.isRestored()) {
      for (String record : nativeStateStore.get()) {
        if (record != null && !record.isEmpty()) {
          restored.add(record);
        }
      }
    }
    // The key-group configuration this subtask owns: the native restore picks
    // the subtask's own key groups out of the union-list records, so it
    // needs the max parallelism and this subtask's range (the values
    // KeyGroupRangeAssignment would compute for keyed operators).
    final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
    final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    final int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
    final KeyGroupRange keyGroupRange =
        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism, parallelism, subtaskIndex);
    task.initializeState(
        0,
        new KeyedStateBackendParameters(
            getRuntimeContext().getJobId().toString(),
            getId(),
            0,
            maxParallelism,
            keyGroupRange.getStartKeyGroup(),
            keyGroupRange.getEndKeyGroup() - keyGroupRange.getStartKeyGroup() + 1),
        restored.toArray(new String[0]));
  }

  /**
   * Returns true when the effective state backend of this task is RocksDB, whether it is declared
   * by the application ({@code env.setStateBackend(..)}), transported in the operator config, the
   * job configuration, or the cluster configuration.
   *
   * <p>The resolution mirrors {@code StreamTask#createStateBackend}: the application-declared
   * backend wins, then the job configuration overrides the cluster (task-manager) configuration.
   * The materialized keyed backend is still inspected as defense in depth, but in production the
   * window aggregation transformation is non-keyed, so a keyed backend never exists for this
   * operator - the resolved-factory inspection is what makes the rejection fire in real jobs.
   */
  private boolean usesRocksDBStateBackend() throws Exception {
    ClassLoader userCodeClassLoader = getUserCodeClassloader();
    StateBackend fromApplication = getOperatorConfig().getStateBackend(userCodeClassLoader);
    StateBackend resolved =
        StateBackendLoader.fromApplicationOrConfigOrDefault(
            fromApplication,
            getContainingTask().getEnvironment().getJobConfiguration(),
            getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration(),
            userCodeClassLoader,
            LOG);
    // With changelog delegation enabled the loader wraps the backend (e.g. ChangelogStateBackend
    // around RocksDB); unwrap delegating layers so the wrapped factory is still inspected.
    while (resolved instanceof DelegatingStateBackend) {
      resolved = ((DelegatingStateBackend) resolved).getDelegatedStateBackend();
    }
    // EmbeddedRocksDBStateBackend and the deprecated RocksDBStateBackend are siblings in 1.19
    // (both extend AbstractManagedMemoryStateBackend), so both must be checked.
    return resolved instanceof RocksDBStateBackend
        || resolved instanceof EmbeddedRocksDBStateBackend
        || getKeyedStateBackend() instanceof RocksDBKeyedStateBackend;
  }

  @Override
  protected void snapshotNativeState(StateSnapshotContext context) throws Exception {
    String[] records = task.snapshotState(context.getCheckpointId());
    nativeStateStore.clear();
    for (String record : records) {
      nativeStateStore.add(record);
    }
  }
}
