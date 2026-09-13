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

import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.utils.SparkVectorUtil;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResources$;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
  public void arrowViewVectorsSupportSparkAccess() {
    Assume.assumeTrue(ColumnarBatches.supportsArrowStringView());
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          try {
            assertViewStringAccess();
            assertViewBinaryAccess();
            assertReadOnlyViewAccess();
            assertViewRecordBatchBuffers();
          } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
          }
          return null;
        });
  }

  private static void assertViewStringAccess() throws ReflectiveOperationException {
    ValueVector arrowVector = newViewVector("org.apache.arrow.vector.ViewVarCharVector");
    setViewValue(arrowVector, 0, "short".getBytes(StandardCharsets.UTF_8));
    setViewValue(
        arrowVector, 1, "a string longer than twelve bytes".getBytes(StandardCharsets.UTF_8));
    setViewNull(arrowVector, 2);
    arrowVector.setValueCount(3);

    try (ArrowWritableColumnVector vector =
        new ArrowWritableColumnVector(arrowVector, 0, 3, false)) {
      assertEquals(UTF8String.fromString("short"), vector.getUTF8String(0));
      assertEquals(
          UTF8String.fromString("a string longer than twelve bytes"), vector.getUTF8String(1));
      assertNull(vector.getUTF8String(2));
    }
  }

  private static void assertViewBinaryAccess() throws ReflectiveOperationException {
    ValueVector arrowVector = newViewVector("org.apache.arrow.vector.ViewVarBinaryVector");
    setViewValue(arrowVector, 0, new byte[] {1, 2, 3});
    setViewValue(arrowVector, 1, new byte[] {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
    setViewNull(arrowVector, 2);
    arrowVector.setValueCount(3);

    try (ArrowWritableColumnVector vector =
        new ArrowWritableColumnVector(arrowVector, 0, 3, false)) {
      assertArrayEquals(new byte[] {1, 2, 3}, vector.getBinary(0));
      assertArrayEquals(
          new byte[] {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, vector.getBinary(1));
      assertNull(vector.getBinary(2));
    }
  }

  private static void assertReadOnlyViewAccess() throws ReflectiveOperationException {
    ValueVector stringVector = newViewVector("org.apache.arrow.vector.ViewVarCharVector");
    setViewValue(stringVector, 0, "view string".getBytes(StandardCharsets.UTF_8));
    stringVector.setValueCount(1);
    try (ArrowColumnVector vector = new ArrowColumnVector(stringVector)) {
      assertEquals(UTF8String.fromString("view string"), vector.getUTF8String(0));
    }

    ValueVector binaryVector = newViewVector("org.apache.arrow.vector.ViewVarBinaryVector");
    setViewValue(binaryVector, 0, new byte[] {1, 2, 3});
    binaryVector.setValueCount(1);
    try (ArrowColumnVector vector = new ArrowColumnVector(binaryVector)) {
      assertArrayEquals(new byte[] {1, 2, 3}, vector.getBinary(0));
    }
  }

  private static void assertViewRecordBatchBuffers() throws ReflectiveOperationException {
    ValueVector arrowVector = newViewVector("org.apache.arrow.vector.ViewVarCharVector");
    setViewValue(
        arrowVector, 0, "a string longer than twelve bytes".getBytes(StandardCharsets.UTF_8));
    arrowVector.setValueCount(1);

    ArrowWritableColumnVector vector = new ArrowWritableColumnVector(arrowVector, 0, 1, false);
    try (ColumnarBatch batch = new ColumnarBatch(new ColumnVector[] {vector}, 1);
        ArrowRecordBatch recordBatch = SparkVectorUtil.toArrowRecordBatch(batch)) {
      @SuppressWarnings("unchecked")
      java.util.List<Long> variadicBufferCounts =
          (java.util.List<Long>)
              recordBatch.getClass().getMethod("getVariadicBufferCounts").invoke(recordBatch);
      assertEquals(java.util.Collections.singletonList(1L), variadicBufferCounts);
    }
  }

  private static ValueVector newViewVector(String className) throws ReflectiveOperationException {
    Class<?> vectorClass = Class.forName(className);
    Constructor<?> constructor = vectorClass.getConstructor(String.class, BufferAllocator.class);
    ValueVector vector =
        (ValueVector) constructor.newInstance("view", ArrowBufferAllocators.contextInstance());
    vector.allocateNew();
    return vector;
  }

  private static void setViewValue(ValueVector vector, int rowId, byte[] value)
      throws ReflectiveOperationException {
    Method setSafe =
        vector.getClass().getMethod("setSafe", int.class, byte[].class, int.class, int.class);
    setSafe.invoke(vector, rowId, value, 0, value.length);
  }

  private static void setViewNull(ValueVector vector, int rowId)
      throws ReflectiveOperationException {
    vector.getClass().getMethod("setNull", int.class).invoke(vector, rowId);
  }
}
