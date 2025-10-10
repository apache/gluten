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
package org.apache.gluten.shuffle;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import java.io.IOException;

public class BoltShuffleWriterJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private BoltShuffleWriterJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static BoltShuffleWriterJniWrapper create(Runtime runtime) {
    return new BoltShuffleWriterJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  /**
   * Add shuffle writer as a Bolt operator into inner WholeStageResultIterator
   *
   * @param iterHandle a WholeStageResultIterator object
   * @param shuffleWriterInfo shuffle writer information to create shuffle writer operator
   * @param celebornPusher a celeborn pusher object when use celeborn, otherwise should be null
   */
  public native void addShuffleWriter(
      long iterHandle, byte[] shuffleWriterInfo, Object celebornPusher);
  /**
   * Get shuffle writer result from Runtime, including metrics and partition information
   *
   * @return a serialized ShuffleWriterResult data
   */
  public native byte[] getShuffleWriterResult();

  /**
   * Create a shuffle writer instance.
   *
   * @param shuffleWriterInfo shuffle writer info
   * @param columnarBatchHandler columnar batch handler
   * @param partitionPusher partition pusher
   * @return shuffle writer instance handle
   */
  public native long createShuffleWriter(
      byte[] shuffleWriterInfo, long columnarBatchHandler, Object partitionPusher);

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
   * @param columnarBatchHandle handle of Bolt Vector
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
   * @return BoltSplitResult
   */
  public native BoltSplitResult stop(long shuffleWriterHandle) throws IOException;

  /**
   * Release resources associated with designated shuffle writer instance.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   */
  public native void close(long shuffleWriterHandle);
}
