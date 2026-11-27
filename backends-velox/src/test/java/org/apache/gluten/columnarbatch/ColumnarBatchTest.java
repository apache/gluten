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
package org.apache.gluten.columnarbatch;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.execution.RowToVeloxColumnarExec;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.test.VeloxBackendTestBase;
import org.apache.gluten.vectorized.ArrowColumnVectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.utils.SparkArrowUtil;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntFunction;
import java.util.stream.StreamSupport;

import scala.collection.JavaConverters;

public class ColumnarBatchTest extends VeloxBackendTestBase {

  @Test
  public void testOffloadAndLoad() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch =
              newArrowBatch("a boolean, b int", numRows, j -> new Object[] {j % 2 == 0, j});
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(batch));
          ColumnarBatches.checkLoaded(batch);
          Assert.assertThrows(
              IllegalArgumentException.class, () -> ColumnarBatches.checkOffloaded(batch));
          final ColumnarBatch offloaded =
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertTrue(ColumnarBatches.isLightBatch(offloaded));
          ColumnarBatches.checkOffloaded(offloaded);
          Assert.assertThrows(
              IllegalArgumentException.class, () -> ColumnarBatches.checkLoaded(offloaded));
          final ColumnarBatch loaded =
              ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), offloaded);
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(loaded));
          ColumnarBatches.checkLoaded(loaded);
          Assert.assertThrows(
              IllegalArgumentException.class, () -> ColumnarBatches.checkOffloaded(loaded));
          long cnt =
              StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                          loaded.rowIterator(), Spliterator.ORDERED),
                      false)
                  .count();
          Assert.assertEquals(numRows, cnt);
          loaded.close();
          return null;
        });
  }

  @Test
  public void testZeroColumnBatch() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch = new ColumnarBatch(new ColumnVector[0]);
          batch.setNumRows(numRows);
          Assert.assertTrue(ColumnarBatches.isZeroColumnBatch(batch));
          ColumnarBatches.checkLoaded(batch);
          ColumnarBatches.checkOffloaded(batch);
          final ColumnarBatch offloaded =
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertTrue(ColumnarBatches.isZeroColumnBatch(offloaded));
          ColumnarBatches.checkLoaded(offloaded);
          ColumnarBatches.checkOffloaded(offloaded);
          final ColumnarBatch loaded =
              ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), offloaded);
          Assert.assertTrue(ColumnarBatches.isZeroColumnBatch(loaded));
          ColumnarBatches.checkLoaded(loaded);
          ColumnarBatches.checkOffloaded(loaded);
          long cnt =
              StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                          loaded.rowIterator(), Spliterator.ORDERED),
                      false)
                  .count();
          Assert.assertEquals(numRows, cnt);
          loaded.close();
          return null;
        });
  }

  @Test
  public void testCreateByHandle() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch =
              newArrowBatch("a boolean, b int", numRows, j -> new Object[] {j % 2 == 0, j});
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(batch));
          final ColumnarBatch offloaded =
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(offloaded));
          final long handle =
              ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), offloaded);
          final ColumnarBatch created = ColumnarBatches.create(handle);
          Assert.assertEquals(
              handle,
              ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), created));
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(created));
          ColumnarBatches.retain(created);
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(created));
          ColumnarBatches.retain(offloaded);
          Assert.assertEquals(3, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(3, ColumnarBatches.getRefCnt(created));
          created.close();
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(created));
          offloaded.close();
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(created));
          created.close();
          Assert.assertEquals(0, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(0, ColumnarBatches.getRefCnt(created));
          return null;
        });
  }

  @Test
  public void testReadRow() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 20;
          final ColumnarBatch batch =
              newArrowBatch(
                  "a boolean, b int",
                  numRows,
                  j -> new Object[] {j % 2 == 0, j == numRows - 1 ? null : 15 - j});
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(batch));
          final ColumnarBatch offloaded =
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertTrue(ColumnarBatches.isLightBatch(offloaded));
          final ColumnarBatch loaded =
              ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), offloaded);
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(loaded));
          long cnt =
              StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                          loaded.rowIterator(), Spliterator.ORDERED),
                      false)
                  .count();
          Assert.assertEquals(numRows, cnt);
          Assert.assertEquals(loaded.getRow(0).getInt(1), 15);
          loaded.close();
          return null;
        });
  }

  @Test
  public void testCompose() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 20;
          final ColumnarBatch batch1 =
              newArrowBatch("a boolean, b int", numRows, j -> new Object[] {j % 2 == 0, 15 - j});
          final ColumnarBatch batch2 =
              newArrowBatch("b int, a boolean", numRows, j -> new Object[] {15 - j, j % 2 == 0});
          ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch1);
          ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch2);
          VeloxColumnarBatches.toVeloxBatch(batch1);
          VeloxColumnarBatches.toVeloxBatch(batch2);
          final ColumnarBatch batch3 = VeloxColumnarBatches.compose(batch1, batch2);
          Assert.assertEquals(
              VeloxColumnarBatches.COMPREHENSIVE_TYPE_VELOX,
              ColumnarBatches.getComprehensiveLightBatchType(batch3));

          Assert.assertEquals(numRows, batch3.numRows());
          Assert.assertEquals(4, batch3.numCols());
          Assert.assertEquals(
              "[false,14,14,false]\n[true,13,13,true]", ColumnarBatches.toString(batch3, 1, 2));
          return null;
        });
  }

  @Test
  public void testToString() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 20;
          final ColumnarBatch batch =
              newArrowBatch(
                  "a boolean, b int",
                  numRows,
                  j -> new Object[] {j % 2 == 0, j == numRows - 1 ? null : 15 - j});
          StructType structType = new StructType();
          structType = structType.add("a", DataTypes.BooleanType, true);
          structType = structType.add("b", DataTypes.IntegerType, true);
          ColumnarBatch veloxBatch =
              RowToVeloxColumnarExec.toColumnarBatchIterator(
                      JavaConverters.<InternalRow>asScalaIterator(batch.rowIterator()),
                      structType,
                      numRows,
                      Integer.MAX_VALUE)
                  .next();
          Assert.assertEquals("[true,15]\n[false,14]", ColumnarBatches.toString(veloxBatch, 0, 2));
          Assert.assertEquals(
              "[true,-3]\n[false,null]", ColumnarBatches.toString(veloxBatch, 18, 2));
          veloxBatch.close();
          return null;
        });
  }

  @Test
  public void testTimestampTypesSurviveOffloadAndLoad() {
    // Velox exports TIMESTAMP without a timezone and tags TIMESTAMP_UTC with field metadata, so
    // both arrive in Java as a timestamp with no timezone. Loading must keep the tag only on the
    // columns Velox exported as TIMESTAMP_UTC.
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 10;
          final String schema = "a timestamp, b timestamp_ntz, c struct<x: timestamp_ntz>";
          final ColumnarBatch batch =
              newArrowBatch(
                  schema,
                  numRows,
                  j ->
                      new Object[] {
                        j * 1000L, j * 2000L, new GenericInternalRow(new Object[] {j * 3000L})
                      });
          final ColumnarBatch offloaded =
              ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch);
          // Import into a Velox RowVector so the types come from Velox's Arrow bridge rather than
          // from the original Java schema.
          final ColumnarBatch veloxBatch = VeloxColumnarBatches.toVeloxBatch(offloaded);
          final ColumnarBatch loaded =
              ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), veloxBatch);
          try {
            assertTimestampTags(loaded);
            for (int j = 0; j < numRows; j++) {
              Assert.assertEquals(j * 1000L, loaded.column(0).getLong(j));
              Assert.assertEquals(j * 2000L, loaded.column(1).getLong(j));
              Assert.assertEquals(j * 3000L, loaded.column(2).getChild(0).getLong(j));
            }
          } finally {
            loaded.close();
          }
          return null;
        });
  }

  @Test
  public void testRowToColumnarTagsTimestampNTZ() {
    // The Velox plan declares Spark TimestampNTZType as TIMESTAMP_UTC, so the row-to-columnar
    // converter must produce TIMESTAMP_UTC vectors, including nested ones.
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 10;
          final StructType schema =
              StructType.fromDDL("a timestamp, b timestamp_ntz, c struct<x: timestamp_ntz>");
          final List<InternalRow> rows = new ArrayList<>();
          for (int j = 0; j < numRows; j++) {
            rows.add(
                new GenericInternalRow(
                    new Object[] {
                      j * 1000L, j * 2000L, new GenericInternalRow(new Object[] {j * 3000L})
                    }));
          }
          final ColumnarBatch veloxBatch =
              RowToVeloxColumnarExec.toColumnarBatchIterator(
                      JavaConverters.asScalaIterator(rows.iterator()),
                      schema,
                      numRows,
                      Integer.MAX_VALUE)
                  .next();
          final ColumnarBatch loaded =
              ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), veloxBatch);
          try {
            assertTimestampTags(loaded);
          } finally {
            loaded.close();
          }
          return null;
        });
  }

  private static void assertTimestampTags(ColumnarBatch batch) {
    Assert.assertFalse(isTaggedTimestampUtc(fieldOf(batch.column(0))));
    Assert.assertTrue(isTaggedTimestampUtc(fieldOf(batch.column(1))));
    Assert.assertTrue(isTaggedTimestampUtc(fieldOf(batch.column(2)).getChildren().get(0)));
  }

  private static Field fieldOf(ColumnVector column) {
    return ((ArrowColumnVector) column).getValueVector().getField();
  }

  private static boolean isTaggedTimestampUtc(Field field) {
    return SparkArrowUtil.VeloxTimestampUtcMetadataValue()
        .equals(field.getMetadata().get(SparkArrowUtil.VeloxTimestampTypeMetadataKey()));
  }

  private static ColumnarBatch newArrowBatch(
      String schema, int numRows, IntFunction<Object[]> rowValues) {
    final List<InternalRow> rows = new ArrayList<>();
    for (int j = 0; j < numRows; j++) {
      rows.add(new GenericInternalRow(rowValues.apply(j)));
    }
    return ArrowColumnVectors.fromRows(
        StructType.fromDDL(schema),
        JavaConverters.asScalaIterator(rows.iterator()),
        ArrowBufferAllocators.contextInstance());
  }
}
