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
package org.apache.gluten.vectorized;

import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FlinkRowToVLVectorConvertorTest {
  private static MemoryManager memoryManager;
  private static Session session;
  private static BufferAllocator allocator;
  private static RowType flinkRowType;
  private static io.github.zhztheplayer.velox4j.type.RowType veloxRowType;

  @BeforeAll
  public static void initializeVelox() {
    Velox4jEnvironment.initializeOnce();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
    session = Velox4j.newSession(memoryManager);
    allocator = new RootAllocator(Long.MAX_VALUE);
    flinkRowType =
        RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    veloxRowType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkRowType);
  }

  @AfterAll
  public static void tearDownVelox() {
    if (allocator != null) {
      allocator.close();
      allocator = null;
    }
    if (session != null) {
      session.close();
      session = null;
    }
    if (memoryManager != null) {
      memoryManager.close();
      memoryManager = null;
    }
  }

  @Test
  public void testToRowDataReadsRowKindFromMergedRowVector() {
    RowKind[] kinds = {
      RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE,
    };
    List<FieldVector> arrowVectors = new ArrayList<>(3);
    IntVector idVec = new IntVector("id", allocator);
    idVec.allocateNew(4);
    VarCharVector nameVec = new VarCharVector("name", allocator);
    nameVec.allocateNew(4);
    TinyIntVector rowKindVec =
        new TinyIntVector(
            FlinkRowToVLVectorConvertor.ROW_KIND_COLUMN_NAME,
            new FieldType(false, new ArrowType.Int(8, true), null),
            allocator);
    rowKindVec.allocateNew(4);
    for (int i = 0; i < kinds.length; i++) {
      idVec.setSafe(i, 100 + i);
      nameVec.setSafe(i, ("u" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      rowKindVec.setSafe(i, kinds[i].toByteValue());
    }
    idVec.setValueCount(4);
    nameVec.setValueCount(4);
    rowKindVec.setValueCount(4);
    arrowVectors.add(idVec);
    arrowVectors.add(nameVec);
    arrowVectors.add(rowKindVec);
    RowVector rv = session.arrowOps().fromArrowTable(allocator, new Table(arrowVectors));
    try {
      List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(rv, allocator, veloxRowType);
      assertThat(rows).hasSize(kinds.length);
      for (int i = 0; i < kinds.length; i++) {
        assertThat(rows.get(i).getRowKind()).isEqualTo(kinds[i]);
        assertThat(rows.get(i).getInt(0)).isEqualTo(100 + i);
        assertThat(rows.get(i).getString(1)).isEqualTo(StringData.fromString("u" + i));
      }
    } finally {
      rv.close();
      idVec.close();
      nameVec.close();
      rowKindVec.close();
    }
  }

  @Test
  public void testToRowDataFallsBackToInsertWhenNoRowKindColumn() {
    RowData input = GenericRowData.of(42, StringData.fromString("alice"));
    RowVector rv = FlinkRowToVLVectorConvertor.fromRowData(input, allocator, session, veloxRowType);
    try {
      List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(rv, allocator, veloxRowType);
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).getRowKind()).isEqualTo(RowKind.INSERT);
      assertThat(rows.get(0).getInt(0)).isEqualTo(42);
      assertThat(rows.get(0).getString(1)).isEqualTo(StringData.fromString("alice"));
    } finally {
      rv.close();
    }
  }

  @Test
  public void testFromRowDataAppendsRowKindColumnForAllKinds() {
    RowKind[] kinds = {
      RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE,
    };
    for (RowKind kind : kinds) {
      RowData input =
          GenericRowData.ofKind(kind, 7, StringData.fromString("row-" + kind.shortString()));
      RowVector rv =
          FlinkRowToVLVectorConvertor.fromRowData(input, allocator, session, veloxRowType);
      try {
        List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(rv, allocator, veloxRowType);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRowKind()).isEqualTo(kind);
        assertThat(rows.get(0).getInt(0)).isEqualTo(7);
        assertThat(rows.get(0).getString(1))
            .isEqualTo(StringData.fromString("row-" + kind.shortString()));
      } finally {
        rv.close();
      }
    }
  }
}
