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

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.task.TaskResources$;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ArrowWritableColumnVectorTest {
  @Test
  public void getBooleansOnStructBackedVectorReturnsExpectedBooleans() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          try (ArrowWritableColumnVector vector =
              new ArrowWritableColumnVector(1, DataTypes.BooleanType)) {
            vector.putBoolean(0, false);
            assertArrayEquals(new boolean[] {false}, vector.getBooleans(0, 1));
          }
          return null;
        });
  }

  @Test
  public void loadsDictionaryEncodedLargeUtf8() {
    IntVector index = new IntVector("index", ArrowBufferAllocators.globalInstance());
    LargeVarCharVector dictionary =
        new LargeVarCharVector("dictionary", ArrowBufferAllocators.globalInstance());
    index.allocateNew(3);
    dictionary.allocateNew();
    index.setSafe(0, 1);
    index.setSafe(1, 0);
    index.setSafe(2, 1);
    index.setValueCount(3);
    dictionary.setSafe(0, "first".getBytes(StandardCharsets.UTF_8));
    dictionary.setSafe(1, "second".getBytes(StandardCharsets.UTF_8));
    dictionary.setValueCount(2);

    try (ArrowWritableColumnVector vector =
        new ArrowWritableColumnVector(index, dictionary, 0, 3, false)) {
      assertEquals("second", vector.getUTF8String(0).toString());
      assertEquals("first", vector.getUTF8String(1).toString());
      assertEquals("second", vector.getUTF8String(2).toString());
    }
  }

  @Test
  public void loadsDictionaryEncodedLargeBinary() {
    IntVector index = new IntVector("index", ArrowBufferAllocators.globalInstance());
    LargeVarBinaryVector dictionary =
        new LargeVarBinaryVector("dictionary", ArrowBufferAllocators.globalInstance());
    index.allocateNew(2);
    dictionary.allocateNew();
    index.setSafe(0, 1);
    index.setSafe(1, 0);
    index.setValueCount(2);
    dictionary.setSafe(0, new byte[] {1, 2});
    dictionary.setSafe(1, new byte[] {3, 4});
    dictionary.setValueCount(2);

    try (ArrowWritableColumnVector vector =
        new ArrowWritableColumnVector(index, dictionary, 0, 2, false)) {
      assertArrayEquals(new byte[] {3, 4}, vector.getBinary(0));
      assertArrayEquals(new byte[] {1, 2}, vector.getBinary(1));
    }
  }
}
