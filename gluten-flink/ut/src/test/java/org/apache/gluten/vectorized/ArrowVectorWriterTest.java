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

import org.apache.gluten.util.LogicalTypeConverter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrowVectorWriterTest {
  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
          new String[] {"x", "y"});

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  // Writes `value` as the only row of a column of `type` and reads it back from the vector.
  private Object writeAndRead(LogicalType type, Object value) {
    ArrowVectorWriter writer =
        ArrowVectorWriter.create("c", LogicalTypeConverter.toVLType(type), allocator);
    writer.write(0, GenericRowData.of(value));
    writer.finish();
    try (FieldVector vector = writer.getVector()) {
      return ArrowVectorAccessor.create(vector).get(0);
    }
  }

  private static RowData row(int x, String y) {
    return GenericRowData.of(x, StringData.fromString(y));
  }

  private static void assertRow(RowData actual, int x, String y) {
    assertThat(actual).isNotNull();
    assertThat(actual.isNullAt(0)).isFalse();
    assertThat(actual.getInt(0)).isEqualTo(x);
    assertThat(actual.isNullAt(1)).isFalse();
    assertThat(actual.getString(1).toString()).isEqualTo(y);
  }

  @Test
  public void testArrayWithNullRowElement() {
    Object[] elements = {row(1, "a"), null, row(2, "b"), row(3, "c")};
    ArrayData array =
        (ArrayData) writeAndRead(new ArrayType(ROW_TYPE), new GenericArrayData(elements));

    assertThat(array.size()).isEqualTo(4);
    assertRow(array.getRow(0, 2), 1, "a");
    assertThat(array.isNullAt(1)).isTrue();
    assertRow(array.getRow(2, 2), 2, "b");
    assertRow(array.getRow(3, 2), 3, "c");
  }

  @Test
  public void testMapWithNullRowValue() {
    Map<Object, Object> entries = new LinkedHashMap<>();
    entries.put(1, row(1, "a"));
    entries.put(2, null);
    entries.put(3, row(3, "c"));
    MapData map =
        (MapData) writeAndRead(new MapType(new IntType(), ROW_TYPE), new GenericMapData(entries));

    ArrayData keys = map.keyArray();
    ArrayData values = map.valueArray();
    assertThat(map.size()).isEqualTo(3);
    assertThat(keys.getInt(2)).isEqualTo(3);
    assertRow(values.getRow(0, 2), 1, "a");
    assertThat(values.isNullAt(1)).isTrue();
    assertRow(values.getRow(2, 2), 3, "c");
  }
}
