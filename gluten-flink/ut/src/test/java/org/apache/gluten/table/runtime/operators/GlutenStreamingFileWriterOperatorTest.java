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
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class GlutenStreamingFileWriterOperatorTest {
  // Hostile checkpoint bytes: 0x00 truncates a modified-UTF-8 string, 0x80 /
  // 0xC0 0x80 / 0xFF are invalid sequences that the decoder drops or
  // rewrites. The record store must carry them untouched.
  private static final byte[] RECORD_A = {
    0x56, 0x53, 0x4b, 0x56, 0x01, 0x03, (byte) 0x80, 0x00, 0x00, 0x00
  };
  private static final byte[] RECORD_B = {
    (byte) 0xC0, (byte) 0x80, (byte) 0xED, (byte) 0xA0, (byte) 0x80, (byte) 0xFF, (byte) 0xFE
  };
  private static final ListStateDescriptor<byte[]> CHECKPOINT_STATE_DESCRIPTOR =
      new ListStateDescriptor<>("gluten-native-file-writer-checkpoint", byte[].class);

  @Test
  void snapshotNativeStateStoresNativeCheckpointRecords() throws Exception {
    UnionMockOperatorStateStore stateStore = new UnionMockOperatorStateStore();
    TestSerialTask task = new TestSerialTask(new byte[][] {RECORD_A, RECORD_B});
    GlutenStreamingFileWriterOperator<RowData> operator = createOperator(task);

    operator.initializeNativeState(createInitializationContext(null, stateStore));
    operator.snapshotNativeState(new StateSnapshotContextSynchronousImpl(42L, 0L));

    ListState<byte[]> checkpointState = stateStore.getListState(CHECKPOINT_STATE_DESCRIPTOR);
    assertThat(task.snapshotCheckpointId).isEqualTo(42L);
    int storedIndex = 0;
    for (byte[] stored : checkpointState.get()) {
      assertArrayEquals(storedIndex == 0 ? RECORD_A : RECORD_B, stored);
      storedIndex++;
    }
    assertThat(storedIndex).isEqualTo(2);
  }

  @Test
  void initializeNativeStateRestoresCheckpointRecordsToNativeTask() throws Exception {
    UnionMockOperatorStateStore stateStore = new UnionMockOperatorStateStore();
    ListState<byte[]> checkpointState = stateStore.getListState(CHECKPOINT_STATE_DESCRIPTOR);
    checkpointState.add(RECORD_A);
    checkpointState.add(RECORD_B);
    TestSerialTask task = new TestSerialTask(new byte[0][]);
    GlutenStreamingFileWriterOperator<RowData> operator = createOperator(task);

    operator.initializeNativeState(createInitializationContext(1L, stateStore));

    assertThat(task.initializeContext).isZero();
    assertThat(task.initializeParameters).isNull();
    assertThat(task.restoredCheckpointRecords.length).isEqualTo(2);
    assertArrayEquals(RECORD_A, task.restoredCheckpointRecords[0]);
    assertArrayEquals(RECORD_B, task.restoredCheckpointRecords[1]);
  }

  private static StateInitializationContext createInitializationContext(
      Long restoredCheckpointId, UnionMockOperatorStateStore stateStore) {
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

  // The upstream MockOperatorStateStore.getUnionListState throws
  // UnsupportedOperationException, so this local store shares one list per
  // state name across the even-split and union access modes - the operator
  // registers its checkpoint state in union mode and the tests read it back
  // through either accessor.
  private static final class UnionMockOperatorStateStore implements OperatorStateStore {
    private final Map<String, List<Object>> states = new HashMap<>();

    @SuppressWarnings("unchecked")
    private <S> ListState<S> state(ListStateDescriptor<S> descriptor) {
      List<Object> values = states.computeIfAbsent(descriptor.getName(), k -> new ArrayList<>());
      return (ListState<S>)
          new ListState<Object>() {
            @Override
            public Iterable<Object> get() {
              return values;
            }

            @Override
            public void add(Object value) {
              values.add(value);
            }

            @Override
            public void update(List<Object> newValues) {
              values.clear();
              values.addAll(newValues);
            }

            @Override
            public void addAll(List<Object> newValues) {
              values.addAll(newValues);
            }

            @Override
            public void clear() {
              values.clear();
            }
          };
    }

    @Override
    public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
      return state(stateDescriptor);
    }

    @Override
    public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) {
      return state(stateDescriptor);
    }

    @Override
    public <K, V> org.apache.flink.api.common.state.BroadcastState<K, V> getBroadcastState(
        org.apache.flink.api.common.state.MapStateDescriptor<K, V> stateDescriptor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Set<String> getRegisteredStateNames() {
      return states.keySet();
    }

    @Override
    public java.util.Set<String> getRegisteredBroadcastStateNames() {
      return java.util.Collections.emptySet();
    }
  }

  private static class TestSerialTask extends SerialTask {
    private final byte[][] checkpointRecords;
    private long snapshotCheckpointId = -1L;
    private long initializeContext = -1L;
    private KeyedStateBackendParameters initializeParameters;
    private byte[][] restoredCheckpointRecords;

    private TestSerialTask(byte[][] checkpointRecords) {
      super(null, 0L);
      this.checkpointRecords = checkpointRecords;
    }

    @Override
    public byte[][] snapshotState(long checkpointId) {
      snapshotCheckpointId = checkpointId;
      return checkpointRecords;
    }

    @Override
    public void initializeState(
        long context, KeyedStateBackendParameters parameters, byte[][] checkpointRecords) {
      initializeContext = context;
      initializeParameters = parameters;
      restoredCheckpointRecords = checkpointRecords;
    }
  }
}
