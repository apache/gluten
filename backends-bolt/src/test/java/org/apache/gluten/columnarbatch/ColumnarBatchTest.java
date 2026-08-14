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
import org.apache.gluten.execution.RowToBoltColumnarExec;
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.test.BoltBackendTestBase;
import org.apache.gluten.vectorized.ArrowWritableColumnVector;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import scala.collection.JavaConverters;

public class ColumnarBatchTest extends BoltBackendTestBase {

  @Test
  public void testOffloadAndLoad() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
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
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
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
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
          final ArrowWritableColumnVector col0 = (ArrowWritableColumnVector) batch.column(0);
          final ArrowWritableColumnVector col1 = (ArrowWritableColumnVector) batch.column(1);
          for (int j = 0; j < numRows; j++) {
            col0.putBoolean(j, j % 2 == 0);
            col1.putInt(j, 15 - j);
          }
          col1.putNull(numRows - 1);
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
          final ColumnarBatch batch1 = newArrowBatch("a boolean, b int", numRows);
          final ColumnarBatch batch2 = newArrowBatch("b int, a boolean", numRows);
          final ArrowWritableColumnVector col0 = (ArrowWritableColumnVector) batch1.column(0);
          final ArrowWritableColumnVector col1 = (ArrowWritableColumnVector) batch1.column(1);
          final ArrowWritableColumnVector col2 = (ArrowWritableColumnVector) batch2.column(0);
          final ArrowWritableColumnVector col3 = (ArrowWritableColumnVector) batch2.column(1);
          for (int j = 0; j < numRows; j++) {
            col0.putBoolean(j, j % 2 == 0);
            col1.putInt(j, 15 - j);
            col2.putInt(j, 15 - j);
            col3.putBoolean(j, j % 2 == 0);
          }
          ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch1);
          ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), batch2);
          BoltColumnarBatches.toBoltBatch(batch1);
          BoltColumnarBatches.toBoltBatch(batch2);
          final ColumnarBatch batch3 = BoltColumnarBatches.compose(batch1, batch2);
          Assert.assertEquals(
              BoltColumnarBatches.COMPREHENSIVE_TYPE_BOLT,
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
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
          final ArrowWritableColumnVector col0 = (ArrowWritableColumnVector) batch.column(0);
          final ArrowWritableColumnVector col1 = (ArrowWritableColumnVector) batch.column(1);
          for (int j = 0; j < numRows; j++) {
            col0.putBoolean(j, j % 2 == 0);
            col1.putInt(j, 15 - j);
          }
          col1.putNull(numRows - 1);
          StructType structType = new StructType();
          structType = structType.add("a", DataTypes.BooleanType, true);
          structType = structType.add("b", DataTypes.IntegerType, true);
          ColumnarBatch boltBatch =
              RowToBoltColumnarExec.toColumnarBatchIterator(
                      JavaConverters.<InternalRow>asScalaIterator(batch.rowIterator()),
                      structType,
                      numRows,
                      Integer.MAX_VALUE)
                  .next();
          Assert.assertEquals("[true,15]\n[false,14]", ColumnarBatches.toString(boltBatch, 0, 2));
          Assert.assertEquals(
              "[true,-3]\n[false,null]", ColumnarBatches.toString(boltBatch, 18, 2));
          boltBatch.close();
          return null;
        });
  }

  private static ColumnarBatch newArrowBatch(String schema, int numRows) {
    final ArrowWritableColumnVector[] columns =
        ArrowWritableColumnVector.allocateColumns(numRows, StructType.fromDDL(schema));
    for (ArrowWritableColumnVector col : columns) {
      col.setValueCount(numRows);
    }
    final ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(numRows);
    return batch;
  }
}
