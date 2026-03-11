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

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;
import org.apache.gluten.utils.ArrowAbiUtil;
import org.apache.gluten.utils.ArrowUtil;
import org.apache.gluten.utils.InternalRowUtil;
import org.apache.gluten.vectorized.ArrowColumnarBatch;
import org.apache.gluten.vectorized.ArrowWritableColumnVector;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.utils.SparkArrowUtil;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.SparkColumnarBatchUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import scala.collection.JavaConverters;

public final class ColumnarBatches {
  private static final String INTERNAL_BACKEND_KIND = "internal";

  private ColumnarBatches() {}

  private enum BatchType {
    LIGHT,
    HEAVY,
    ZERO_COLUMN
  }

  private static BatchType identifyBatchType(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      return BatchType.ZERO_COLUMN;
    }

    final ColumnVector col0 = batch.column(0);
    if (col0 instanceof IndicatorVector) {
      // it's likely a light batch
      for (int i = 1; i < batch.numCols(); i++) {
        ColumnVector col = batch.column(i);
        if (!(col instanceof PlaceholderVector)) {
          throw new IllegalStateException(
              "Light batch should consist of one indicator vector "
                  + "and (numCols - 1) placeholder vectors");
        }
      }
      return BatchType.LIGHT;
    }

    // it's likely a heavy batch
    for (int i = 0; i < batch.numCols(); i++) {
      ColumnVector col = batch.column(i);
      if (!(col instanceof ArrowWritableColumnVector)) {
        throw new IllegalStateException("Heavy batch should consist of arrow vectors");
      }
    }
    return BatchType.HEAVY;
  }

  /** Heavy batch: Data is readable from JVM and formatted as Arrow data. */
  @VisibleForTesting
  static boolean isHeavyBatch(ColumnarBatch batch) {
    return identifyBatchType(batch) == BatchType.HEAVY;
  }

  /**
   * Light batch: Data is not readable from JVM, a long int handle (which is a pointer usually) is
   * used to bind the batch to a native side implementation.
   */
  @VisibleForTesting
  static boolean isLightBatch(ColumnarBatch batch) {
    return identifyBatchType(batch) == BatchType.LIGHT;
  }

  /** Zero-column batch: The batch doesn't have columns. Though it could have a fixed row count. */
  @VisibleForTesting
  static boolean isZeroColumnBatch(ColumnarBatch batch) {
    return identifyBatchType(batch) == BatchType.ZERO_COLUMN;
  }

  /** This method will always return a velox based ColumnarBatch. */
  public static ColumnarBatch select(String backendName, ColumnarBatch batch, int[] columnIndices) {
    final Runtime runtime = Runtimes.contextInstance(backendName, "ColumnarBatches#select");
    switch (identifyBatchType(batch)) {
      case LIGHT:
        final IndicatorVector iv = getIndicatorVector(batch);
        long outputBatchHandle =
            ColumnarBatchJniWrapper.create(runtime).select(iv.handle(), columnIndices);
        return create(outputBatchHandle);
      case HEAVY:
        return new ColumnarBatch(
            Arrays.stream(columnIndices).mapToObj(batch::column).toArray(ColumnVector[]::new),
            batch.numRows());
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Ensure the input batch is offloaded as native-based columnar batch (See {@link IndicatorVector}
   * and {@link PlaceholderVector}). This method will close the input column batch after offloaded.
   */
  static ColumnarBatch ensureOffloaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (ColumnarBatches.isLightBatch(batch)) {
      return batch;
    }
    return offload(allocator, batch);
  }

  /**
   * Ensure the input batch is loaded as Arrow-based Java columnar batch. ABI-based sharing will
   * take place if loading is required, which means when the input batch is not loaded yet. This
   * method will close the input column batch after loaded.
   */
  static ColumnarBatch ensureLoaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (isHeavyBatch(batch)) {
      return batch;
    }
    return load(allocator, batch);
  }

  public static void checkLoaded(ColumnarBatch batch) {
    final BatchType type = identifyBatchType(batch);
    switch (type) {
      case HEAVY:
      case ZERO_COLUMN:
        break;
      default:
        throw new IllegalArgumentException("Input batch is not loaded");
    }
  }

  public static void checkOffloaded(ColumnarBatch batch) {
    final BatchType type = identifyBatchType(batch);
    switch (type) {
      case LIGHT:
      case ZERO_COLUMN:
        break;
      default:
        throw new IllegalArgumentException("Input batch is not offloaded");
    }
  }

  public static ArrowColumnarBatch convertToArrowColumnarBatch(ColumnarBatch sparkColumnarBatch) {
    int numCols = sparkColumnarBatch.numCols();
    ArrowWritableColumnVector[] writableColumns = new ArrowWritableColumnVector[numCols];
    for (int i = 0; i < numCols; i++) {
      writableColumns[i] = (ArrowWritableColumnVector) sparkColumnarBatch.column(i);
    }
    return new ArrowColumnarBatch(writableColumns, sparkColumnarBatch.numRows());
  }

  public static ColumnarBatch load(BufferAllocator allocator, ColumnarBatch input) {
    if (isZeroColumnBatch(input)) {
      return input;
    }
    if (!ColumnarBatches.isLightBatch(input)) {
      throw new IllegalArgumentException(
          "Input is not light columnar batch. "
              + "Please consider to use vanilla spark's row based input by setting one of the below"
              + " configs: \n"
              + "spark.sql.parquet.enableVectorizedReader=false\n"
              + "spark.sql.inMemoryColumnarStorage.enableVectorizedReader=false\n"
              + "spark.sql.orc.enableVectorizedReader=false\n");
    }
    IndicatorVector iv = (IndicatorVector) input.column(0);
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      ColumnarBatchJniWrapper.exportToArrow(
          iv.handle(), cSchema.memoryAddress(), cArray.memoryAddress());

      Data.exportSchema(
          allocator, ArrowUtil.toArrowSchema(cSchema, allocator, provider), provider, arrowSchema);

      ColumnarBatch output =
          ArrowAbiUtil.importToSparkColumnarBatch(allocator, arrowSchema, cArray);

      // Follow input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched.
      long refCnt = getRefCntLight(input);
      for (long i = 0; i < (refCnt - 1); i++) {
        for (int j = 0; j < output.numCols(); j++) {
          final ArrowWritableColumnVector col = (ArrowWritableColumnVector) output.column(j);
          col.retain();
        }
      }

      // Close the input one.
      for (long i = 0; i < refCnt; i++) {
        input.close();
      }

      // Populate new vectors to input.
      SparkColumnarBatchUtil.transferVectors(output, input);

      return output;
    }
  }

  public static ColumnarBatch offload(BufferAllocator allocator, ColumnarBatch input) {
    if (isZeroColumnBatch(input)) {
      return input;
    }
    if (!isHeavyBatch(input)) {
      throw new IllegalArgumentException("batch is not Arrow columnar batch");
    }
    if (input.numCols() == 0) {
      throw new IllegalArgumentException("batch with zero columns cannot be offloaded");
    }
    // Batch-offloading doesn't involve any backend-specific native code. Use the
    // internal
    // backend to store native batch references only.
    final Runtime runtime =
        Runtimes.contextInstance(INTERNAL_BACKEND_KIND, "ColumnarBatches#offload");
    try (ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)) {
      ArrowAbiUtil.exportFromSparkColumnarBatch(allocator, input, cSchema, cArray);
      long handle =
          ColumnarBatchJniWrapper.create(runtime)
              .createWithArrowArray(cSchema.memoryAddress(), cArray.memoryAddress());
      ColumnarBatch output = ColumnarBatches.create(handle);

      // Follow input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched.
      long refCnt = getRefCntHeavy(input);
      final IndicatorVector giv = (IndicatorVector) output.column(0);
      for (long i = 0; i < (refCnt - 1); i++) {
        giv.retain();
      }

      // Close the input one.
      for (long i = 0; i < refCnt; i++) {
        input.close();
      }

      // Populate new vectors to input.
      SparkColumnarBatchUtil.transferVectors(output, input);
      return input;
    }
  }

  public static Iterator<InternalRow> emptyRowIterator(int numRows) {
    final int maxRows = numRows;
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        rowId++;
        return new UnsafeRow(0);
      }
    };
  }

  static long getRefCntLight(ColumnarBatch input) {
    if (!isLightBatch(input)) {
      throw new UnsupportedOperationException("Input batch is not light batch");
    }
    IndicatorVector iv = (IndicatorVector) input.column(0);
    return iv.refCnt();
  }

  static long getRefCntHeavy(ColumnarBatch input) {
    if (!isHeavyBatch(input)) {
      throw new UnsupportedOperationException("Input batch is not heavy batch");
    }
    if (input.numCols() == 0) {
      throw new IllegalArgumentException(
          "batch with zero columns doesn't have meaningful " + "reference count");
    }
    long refCnt = -1L;
    for (int i = 0; i < input.numCols(); i++) {
      ArrowWritableColumnVector col = ((ArrowWritableColumnVector) input.column(i));
      long colRefCnt = col.refCnt();
      if (refCnt == -1L) {
        refCnt = colRefCnt;
      } else {
        if (colRefCnt != refCnt) {
          throw new IllegalStateException();
        }
      }
    }
    if (refCnt == -1L) {
      throw new IllegalStateException();
    }
    return refCnt;
  }

  @VisibleForTesting
  static long getRefCnt(ColumnarBatch input) {
    switch (identifyBatchType(input)) {
      case LIGHT:
        return getRefCntLight(input);
      case HEAVY:
        return getRefCntHeavy(input);
      default:
        throw new IllegalStateException();
    }
  }

  public static void forceClose(ColumnarBatch input) {
    if (isZeroColumnBatch(input)) {
      return;
    }
    for (long i = 0; i < getRefCnt(input); i++) {
      input.close();
    }
  }

  private static ColumnarBatch create(IndicatorVector iv) {
    int numColumns = Math.toIntExact(iv.getNumColumns());
    int numRows = Math.toIntExact(iv.getNumRows());
    if (numColumns == 0) {
      return new ColumnarBatch(new ColumnVector[0], numRows);
    }
    final ColumnVector[] columnVectors = new ColumnVector[numColumns];
    columnVectors[0] = iv;
    long numPlaceholders = numColumns - 1;
    for (int i = 0; i < numPlaceholders; i++) {
      final PlaceholderVector pv = PlaceholderVector.INSTANCE;
      columnVectors[i + 1] = pv;
    }
    return new ColumnarBatch(columnVectors, numRows);
  }

  public static ColumnarBatch create(long nativeHandle) {
    return create(IndicatorVector.obtain(nativeHandle));
  }

  public static void retain(ColumnarBatch b) {
    switch (identifyBatchType(b)) {
      case LIGHT:
        IndicatorVector iv = (IndicatorVector) b.column(0);
        iv.retain();
        break;
      case HEAVY:
        for (int i = 0; i < b.numCols(); i++) {
          ArrowWritableColumnVector col = ((ArrowWritableColumnVector) b.column(i));
          col.retain();
        }
        break;
      case ZERO_COLUMN:
        break;
      default:
        throw new IllegalStateException();
    }
  }

  public static void release(ColumnarBatch b) {
    b.close();
  }

  private static IndicatorVector getIndicatorVector(ColumnarBatch input) {
    if (!isLightBatch(input)) {
      throw new UnsupportedOperationException("Input batch is not light batch");
    }
    return (IndicatorVector) input.column(0);
  }

  public static long getNativeHandle(String backendName, ColumnarBatch batch) {
    if (isZeroColumnBatch(batch)) {
      final ColumnarBatchJniWrapper jniWrapper =
          ColumnarBatchJniWrapper.create(
              Runtimes.contextInstance(backendName, "ColumnarBatches#getNativeHandle"));
      return jniWrapper.getForEmptySchema(batch.numRows());
    }
    return getIndicatorVector(batch).handle();
  }

  static String getComprehensiveLightBatchType(ColumnarBatch batch) {
    return getIndicatorVector(batch).getType();
  }

  public static String toString(ColumnarBatch batch, int start, int length) {
    ColumnarBatch loadedBatch = ensureLoaded(ArrowBufferAllocators.contextInstance(), batch);
    StructType type = SparkArrowUtil.fromArrowSchema(ArrowUtil.toSchema(loadedBatch));
    return InternalRowUtil.toString(
        type,
        JavaConverters.<InternalRow>asScalaIterator(loadedBatch.rowIterator()),
        start,
        length);
  }
}
