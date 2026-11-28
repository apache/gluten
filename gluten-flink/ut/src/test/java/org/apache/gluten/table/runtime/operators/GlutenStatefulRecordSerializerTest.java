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

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.table.runtime.typeutils.GlutenStatefulRecordSerializer;
import org.apache.gluten.table.runtime.typeutils.GlutenStatefulRecordSerializer.RowVectorSerializerSnapshot;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GlutenStatefulRecordSerializerTest {
  private static final String OPERATOR_ID = "serializer-test-operator";

  private GlutenSessionResource sourceResource;
  private GlutenSessionResource targetResource;

  @BeforeAll
  public static void initializeVelox() {
    Velox4jEnvironment.initializeOnce();
  }

  @AfterEach
  public void tearDown() {
    GlutenTaskSessionContext.unregisterSessionResource(OPERATOR_ID);
    if (sourceResource != null) {
      sourceResource.close();
      sourceResource = null;
    }
    if (targetResource != null) {
      targetResource.close();
      targetResource = null;
    }
  }

  @Test
  public void testDeserializeUsesTaskLocalSession() throws Exception {
    io.github.zhztheplayer.velox4j.type.RowType veloxRowType = createVeloxRowType();
    GlutenStatefulRecordSerializer serializer =
        new GlutenStatefulRecordSerializer(veloxRowType, new TestGlutenOperator(veloxRowType));

    sourceResource = new GlutenSessionResource();
    RowVector sourceVector =
        FlinkRowToVLVectorConvertor.fromRowData(
            GenericRowData.of(7, StringData.fromString("Alice")),
            sourceResource.getAllocator(),
            sourceResource.getSession(),
            veloxRowType);
    StatefulRecord sourceRecord = new StatefulRecord(OPERATOR_ID, sourceVector.id(), 0, false, 3);
    sourceRecord.setRowVector(sourceVector);

    DataOutputSerializer output = new DataOutputSerializer(128);
    serializer.serialize(sourceRecord, output);

    targetResource = new GlutenSessionResource();
    GlutenTaskSessionContext.addSessionResource(OPERATOR_ID, targetResource);

    StatefulRecord restoredRecord =
        serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    List<RowData> restoredRows =
        FlinkRowToVLVectorConvertor.toRowData(
            restoredRecord.getRowVector(), targetResource.getAllocator(), veloxRowType);

    assertThat(restoredRecord.getNodeId()).isEqualTo(OPERATOR_ID);
    assertThat(restoredRows).hasSize(1);
    RowData restoredRow = restoredRows.get(0);
    assertThat(restoredRow.getInt(0)).isEqualTo(7);
    assertThat(restoredRow.getString(1)).isEqualTo(StringData.fromString("Alice"));

    restoredRecord.close();
    sourceRecord.close();
  }

  @Test
  public void testSerializeMultipleRecordsWithRawFrameBytes() throws Exception {
    io.github.zhztheplayer.velox4j.type.RowType veloxRowType = createVeloxRowType();
    GlutenStatefulRecordSerializer serializer =
        new GlutenStatefulRecordSerializer(veloxRowType, new TestGlutenOperator(veloxRowType));

    sourceResource = new GlutenSessionResource();
    RowVector first =
        FlinkRowToVLVectorConvertor.fromRowData(
            GenericRowData.of(7, StringData.fromString("Alice")),
            sourceResource.getAllocator(),
            sourceResource.getSession(),
            veloxRowType);
    RowVector second =
        FlinkRowToVLVectorConvertor.fromRowData(
            GenericRowData.of(8, StringData.fromString("Bob")),
            sourceResource.getAllocator(),
            sourceResource.getSession(),
            veloxRowType);
    StatefulRecord firstRecord = new StatefulRecord(OPERATOR_ID, first.id(), 0, false, 1);
    firstRecord.setRowVector(first);
    StatefulRecord secondRecord = new StatefulRecord(OPERATOR_ID, second.id(), 0, false, 1);
    secondRecord.setRowVector(second);

    DataOutputSerializer output = new DataOutputSerializer(256);
    serializer.serialize(firstRecord, output);
    serializer.serialize(secondRecord, output);
    byte[] buffer = output.getCopyOfBuffer();
    assertThat(buffer.length).isGreaterThan(4 + first.serialize().length);

    targetResource = new GlutenSessionResource();
    GlutenTaskSessionContext.addSessionResource(OPERATOR_ID, targetResource);
    DataInputDeserializer input = new DataInputDeserializer(buffer);
    StatefulRecord restoredFirst = serializer.deserialize(input);
    StatefulRecord restoredSecond = serializer.deserialize(input);
    List<RowData> firstRows =
        FlinkRowToVLVectorConvertor.toRowData(
            restoredFirst.getRowVector(), targetResource.getAllocator(), veloxRowType);
    List<RowData> secondRows =
        FlinkRowToVLVectorConvertor.toRowData(
            restoredSecond.getRowVector(), targetResource.getAllocator(), veloxRowType);

    assertThat(firstRows).hasSize(1);
    assertThat(firstRows.get(0).getInt(0)).isEqualTo(7);
    assertThat(firstRows.get(0).getString(1)).isEqualTo(StringData.fromString("Alice"));
    assertThat(secondRows).hasSize(1);
    assertThat(secondRows.get(0).getInt(0)).isEqualTo(8);
    assertThat(secondRows.get(0).getString(1)).isEqualTo(StringData.fromString("Bob"));

    restoredFirst.close();
    restoredSecond.close();
    firstRecord.close();
    secondRecord.close();
  }

  @Test
  public void testOldBase64WireFormatFailsLoud() throws Exception {
    io.github.zhztheplayer.velox4j.type.RowType veloxRowType = createVeloxRowType();
    GlutenStatefulRecordSerializer serializer =
        new GlutenStatefulRecordSerializer(veloxRowType, new TestGlutenOperator(veloxRowType));

    sourceResource = new GlutenSessionResource();
    GlutenTaskSessionContext.addSessionResource(OPERATOR_ID, sourceResource);
    RowVector sourceVector =
        FlinkRowToVLVectorConvertor.fromRowData(
            GenericRowData.of(7, StringData.fromString("Alice")),
            sourceResource.getAllocator(),
            sourceResource.getSession(),
            veloxRowType);

    byte[] legacyBytes =
        Base64.getEncoder()
            .encodeToString(sourceVector.serialize())
            .getBytes(StandardCharsets.UTF_8);
    DataOutputSerializer output = new DataOutputSerializer(legacyBytes.length + 8);
    output.writeInt(legacyBytes.length);
    output.write(legacyBytes);

    assertThatThrownBy(
            () -> serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())))
        .isInstanceOf(VeloxException.class);
    sourceVector.close();
  }

  @Test
  public void testSnapshotRejectsV1AndAcceptsV2() throws Exception {
    io.github.zhztheplayer.velox4j.type.RowType veloxRowType = createVeloxRowType();
    GlutenStatefulRecordSerializer serializer =
        new GlutenStatefulRecordSerializer(veloxRowType, new TestGlutenOperator(veloxRowType));

    final RowVectorSerializerSnapshot v1Snapshot = new RowVectorSerializerSnapshot();
    v1Snapshot.readSnapshot(1, new DataInputDeserializer(new byte[0]), getClass().getClassLoader());
    assertThat(
            serializer
                .snapshotConfiguration()
                .resolveSchemaCompatibility(v1Snapshot)
                .isIncompatible())
        .isTrue();
    assertThatThrownBy(v1Snapshot::restoreSerializer).isInstanceOf(IllegalStateException.class);

    final RowVectorSerializerSnapshot v2Snapshot = new RowVectorSerializerSnapshot();
    v2Snapshot.readSnapshot(2, new DataInputDeserializer(new byte[0]), getClass().getClassLoader());
    assertThat(
            serializer
                .snapshotConfiguration()
                .resolveSchemaCompatibility(v2Snapshot)
                .isCompatibleAsIs())
        .isTrue();
  }

  @Test
  public void testTaskLocalSessionContextIsThreadIsolated() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier barrier = new CyclicBarrier(2);
    try {
      Callable<Void> assertion =
          () -> {
            assertTaskLocalSessionIsIsolated(barrier);
            return null;
          };
      Future<?> first = executor.submit(assertion);
      Future<?> second = executor.submit(assertion);

      first.get();
      second.get();
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertTaskLocalSessionIsIsolated(CyclicBarrier barrier) throws Exception {
    GlutenSessionResource resource = new GlutenSessionResource();
    try {
      GlutenTaskSessionContext.addSessionResource(OPERATOR_ID, resource);
      barrier.await();

      assertThat(GlutenTaskSessionContext.getSession(OPERATOR_ID)).isSameAs(resource.getSession());
    } finally {
      GlutenTaskSessionContext.unregisterSessionResource(OPERATOR_ID);
      resource.close();
    }
  }

  private static io.github.zhztheplayer.velox4j.type.RowType createVeloxRowType() {
    RowType flinkRowType =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    return (io.github.zhztheplayer.velox4j.type.RowType)
        LogicalTypeConverter.toVLType(flinkRowType);
  }

  private static class TestGlutenOperator implements GlutenOperator {
    private final io.github.zhztheplayer.velox4j.type.RowType rowType;

    private TestGlutenOperator(io.github.zhztheplayer.velox4j.type.RowType rowType) {
      this.rowType = rowType;
    }

    @Override
    public StatefulPlanNode getPlanNode() {
      return null;
    }

    @Override
    public io.github.zhztheplayer.velox4j.type.RowType getInputType() {
      return rowType;
    }

    @Override
    public Map<String, io.github.zhztheplayer.velox4j.type.RowType> getOutputTypes() {
      return Map.of(OPERATOR_ID, rowType);
    }

    @Override
    public String getId() {
      return OPERATOR_ID;
    }
  }
}
