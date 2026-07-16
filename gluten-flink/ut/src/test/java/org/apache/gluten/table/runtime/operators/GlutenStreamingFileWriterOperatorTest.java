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

import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.stateful.KeyedStateBackendParameters;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GlutenStreamingFileWriterOperatorTest {
  private static final ListStateDescriptor<String> CHECKPOINT_STATE_DESCRIPTOR =
      new ListStateDescriptor<>("gluten-native-file-writer-checkpoint", String.class);

  @Test
  void snapshotNativeStateStoresNativeCheckpointRecords() throws Exception {
    MockOperatorStateStore stateStore = new MockOperatorStateStore();
    TestSerialTask task = new TestSerialTask(new String[] {"bucket-a", "bucket-b"});
    GlutenStreamingFileWriterOperator<RowData> operator = createOperator(task);

    operator.initializeNativeState(createInitializationContext(null, stateStore));
    operator.snapshotNativeState(new StateSnapshotContextSynchronousImpl(42L, 0L));

    ListState<String> checkpointState = stateStore.getListState(CHECKPOINT_STATE_DESCRIPTOR);
    assertThat(task.snapshotCheckpointId).isEqualTo(42L);
    assertThat(checkpointState.get()).containsExactly("bucket-a", "bucket-b");
  }

  @Test
  void initializeNativeStateRestoresCheckpointRecordsToNativeTask() throws Exception {
    MockOperatorStateStore stateStore = new MockOperatorStateStore();
    ListState<String> checkpointState = stateStore.getListState(CHECKPOINT_STATE_DESCRIPTOR);
    checkpointState.add("bucket-a");
    checkpointState.add("bucket-b");
    TestSerialTask task = new TestSerialTask(new String[0]);
    GlutenStreamingFileWriterOperator<RowData> operator = createOperator(task);

    operator.initializeNativeState(createInitializationContext(1L, stateStore));

    assertThat(task.initializeContext).isZero();
    assertThat(task.initializeParameters).isNull();
    assertThat(task.restoredCheckpointRecords).containsExactly("bucket-a", "bucket-b");
  }

  private static StateInitializationContext createInitializationContext(
      Long restoredCheckpointId, MockOperatorStateStore stateStore) {
    return new StateInitializationContextImpl(
        restoredCheckpointId, stateStore, null, Collections.emptyList(), Collections.emptyList());
  }

  private static GlutenStreamingFileWriterOperator<RowData> createOperator(TestSerialTask task) {
    RowType rowType = new RowType(List.of("id"), List.of(new IntegerType()));
    StatefulPlanNode plan = new StatefulPlanNode("scan", new EmptyNode(rowType));
    GlutenStreamingFileWriterOperator<RowData> operator =
        new GlutenStreamingFileWriterOperator<>(
            plan,
            "scan",
            rowType,
            Map.of("scan", rowType),
            RowData.class,
            "test native file writer");
    operator.task = task;
    return operator;
  }

  private static class TestSerialTask extends SerialTask {
    private final String[] checkpointRecords;
    private long snapshotCheckpointId = -1L;
    private long initializeContext = -1L;
    private KeyedStateBackendParameters initializeParameters;
    private String[] restoredCheckpointRecords;

    private TestSerialTask(String[] checkpointRecords) {
      super(null, 0L);
      this.checkpointRecords = checkpointRecords;
    }

    @Override
    public String[] snapshotState(long checkpointId) {
      snapshotCheckpointId = checkpointId;
      return checkpointRecords;
    }

    @Override
    public void initializeState(
        long context, KeyedStateBackendParameters parameters, String[] checkpointRecords) {
      initializeContext = context;
      initializeParameters = parameters;
      restoredCheckpointRecords = checkpointRecords;
    }
  }
}
