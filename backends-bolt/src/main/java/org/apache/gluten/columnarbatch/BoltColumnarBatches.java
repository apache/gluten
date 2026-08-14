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
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.SparkColumnarBatchUtil;

import java.util.Arrays;
import java.util.Objects;

public final class BoltColumnarBatches {
  public static final String COMPREHENSIVE_TYPE_BOLT = "bolt";

  private static boolean isBoltBatch(ColumnarBatch batch) {
    final String comprehensiveType = ColumnarBatches.getComprehensiveLightBatchType(batch);
    return Objects.equals(comprehensiveType, COMPREHENSIVE_TYPE_BOLT);
  }

  public static void checkBoltBatch(ColumnarBatch batch) {
    if (ColumnarBatches.isZeroColumnBatch(batch)) {
      return;
    }
    Preconditions.checkArgument(
        isBoltBatch(batch),
        String.format(
            "Expected comprehensive batch type %s, but got %s",
            COMPREHENSIVE_TYPE_BOLT, ColumnarBatches.getComprehensiveLightBatchType(batch)));
  }

  public static ColumnarBatch toBoltBatch(ColumnarBatch input) {
    ColumnarBatches.checkOffloaded(input);
    if (ColumnarBatches.isZeroColumnBatch(input)) {
      return input;
    }
    Preconditions.checkArgument(!isBoltBatch(input));
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "BoltColumnarBatches#toBoltBatch");
    final long handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), input);
    final long outHandle = BoltColumnarBatchJniWrapper.create(runtime).from(handle);
    final ColumnarBatch output = ColumnarBatches.create(outHandle);

    // Follow input's reference count. This might be optimized using
    // automatic clean-up or once the extensibility of ColumnarBatch is enriched
    final long refCnt = ColumnarBatches.getRefCntLight(input);
    final IndicatorVector giv = (IndicatorVector) output.column(0);
    for (long i = 0; i < (refCnt - 1); i++) {
      giv.retain();
    }

    // close the input one
    for (long i = 0; i < refCnt; i++) {
      input.close();
    }

    // Populate new vectors to input.
    SparkColumnarBatchUtil.transferVectors(output, input);

    return input;
  }

  /**
   * Check if a columnar batch is in Bolt format. If not, convert it to Bolt format then return. If
   * already in Bolt format, return the batch directly.
   *
   * <p>Should only be used for certain conditions when unable to insert explicit to-Bolt
   * transitions through query planner.
   *
   * <p>For example, used by {@link org.apache.spark.sql.execution.ColumnarCachedBatchSerializer} as
   * Spark directly calls API ColumnarCachedBatchSerializer#convertColumnarBatchToCachedBatch for
   * query plan that returns supportsColumnar=true without generating a cache-write query plan node.
   */
  public static ColumnarBatch ensureBoltBatch(ColumnarBatch input) {
    final ColumnarBatch light =
        ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), input);
    if (isBoltBatch(light)) {
      return light;
    }
    return toBoltBatch(light);
  }

  /**
   * Combine multiple columnar batches horizontally, assuming each of them is already offloaded.
   * Otherwise {@link UnsupportedOperationException} will be thrown.
   */
  public static ColumnarBatch compose(ColumnarBatch... batches) {
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "BoltColumnarBatches#compose");
    final long[] handles =
        Arrays.stream(batches)
            .mapToLong(b -> ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), b))
            .toArray();
    final long handle = BoltColumnarBatchJniWrapper.create(runtime).compose(handles);
    return ColumnarBatches.create(handle);
  }

  /**
   * Returns a new ColumnarBatch that contains at most `limit` rows from the given batch.
   *
   * <p>If `limit >= batch.numRows()`, returns the original batch. Otherwise, copies up to `limit`
   * rows into new column vectors.
   *
   * @param batch the original batch
   * @param limit the maximum number of rows to include
   * @return a new pruned [[ColumnarBatch]] with row count = `limit`, or the original batch if no
   *     pruning is required
   */
  public static ColumnarBatch slice(ColumnarBatch batch, int offset, int limit) {
    int totalRows = batch.numRows();
    if (limit >= totalRows) {
      // No need to prune
      return batch;
    } else {
      Runtime runtime =
          Runtimes.contextInstance(
              BackendsApiManager.getBackendName(), "BoltColumnarBatches#sliceBatch");
      long nativeHandle =
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), batch);
      long handle = BoltColumnarBatchJniWrapper.create(runtime).slice(nativeHandle, offset, limit);
      return ColumnarBatches.create(handle);
    }
  }

  /**
   * repeat batch1 using the array `rowId2RowNums` passed in and then compose with batch2.
   * rowId2RowNums records the number of each row after repeated.
   */
  public static ColumnarBatch repeatedThenCompose(
      ColumnarBatch batch1, ColumnarBatch batch2, int[] rowId2RowNums) {
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "BoltColumnarBatches#repeatedThenCompose");
    final long handle =
        BoltColumnarBatchJniWrapper.create(runtime)
            .repeatedThenCompose(
                ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), batch1),
                ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), batch2),
                rowId2RowNums);
    return ColumnarBatches.create(handle);
  }
}
