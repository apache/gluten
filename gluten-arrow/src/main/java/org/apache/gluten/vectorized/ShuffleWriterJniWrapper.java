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

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import java.io.IOException;

public class ShuffleWriterJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ShuffleWriterJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ShuffleWriterJniWrapper create(Runtime runtime) {
    return new ShuffleWriterJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public native long createHashShuffleWriter(
      int numPartitions,
      String partitioningName,
      int startPartitionId,
      int splitBufferSize,
      double splitBufferReallocThreshold,
      int partitionBufferEvictThreshold,
      long partitionWriterHandle,
      boolean rowBasedChecksumEnabled);

  public native long createSortShuffleWriter(
      int numPartitions,
      String partitioningName,
      int startPartitionId,
      int diskWriteBufferSize,
      int initialSortBufferSize,
      boolean useRadixSort,
      long partitionWriterHandle);

  public native long createRssSortShuffleWriter(
      int numPartitions,
      String partitioningName,
      int startPartitionId,
      int splitBufferSize,
      long sortBufferMaxSize,
      String codec,
      long partitionWriterHandle);

  /**
   * Reclaim memory from the shuffle writer instance. It will first try to shrink allocated memory,
   * and may trigger a spill if needed.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @param size expected size to reclaim (in bytes)
   * @return actual spilled size
   */
  public native long reclaim(long shuffleWriterHandle, long size) throws RuntimeException;

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @param numRows Rows per batch
   * @param columnarBatchHandle handle of Velox Vector
   * @param memLimit memory usage limit for the split operation FIXME setting a cap to pool /
   *     allocator instead
   * @return batch bytes.
   */
  public native long write(
      long shuffleWriterHandle, int numRows, long columnarBatchHandle, long memLimit);

  /**
   * Write the data remained in the buffers hold by native shuffle writer to each partition's
   * temporary file. And stop processing splitting
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @return GlutenSplitResult
   */
  public native GlutenSplitResult stop(long shuffleWriterHandle) throws IOException;

  /**
   * Release resources associated with designated shuffle writer instance.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   */
  public native void close(long shuffleWriterHandle);
}
