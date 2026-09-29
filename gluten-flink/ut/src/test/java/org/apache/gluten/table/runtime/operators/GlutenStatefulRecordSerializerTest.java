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
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.data.RowVector;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

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
