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

import io.github.zhztheplayer.velox4j.type.TimestampType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class ArrowTimestampVectorTest {
  @Test
  void preservesTimestampMicros() {
    assertPreservesTimestampMicros(new TimestampType(6, false));
  }

  @Test
  void preservesTimestampLtzMicros() {
    assertPreservesTimestampMicros(new TimestampType(6, true));
  }

  private void assertPreservesTimestampMicros(TimestampType timestampType) {
    TimestampData expected =
        TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 6, 22, 11, 12, 13, 123456000));

    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowVectorWriter writer = ArrowVectorWriter.create("ts", timestampType, allocator);
      try (FieldVector vector = writer.getVector()) {
        writer.write(0, GenericRowData.of(expected));
        writer.finish();

        TimestampData actual = (TimestampData) ArrowVectorAccessor.create(vector).get(0);

        assertThat(actual.getMillisecond()).isEqualTo(expected.getMillisecond());
        assertThat(actual.getNanoOfMillisecond()).isEqualTo(expected.getNanoOfMillisecond());
      }
    }
  }
}
