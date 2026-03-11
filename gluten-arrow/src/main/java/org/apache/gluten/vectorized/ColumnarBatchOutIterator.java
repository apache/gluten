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
import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class ColumnarBatchOutIterator extends ClosableIterator<ColumnarBatch>
    implements RuntimeAware {
  private final Runtime runtime;
  private final long iterHandle;

  public ColumnarBatchOutIterator(Runtime runtime, long iterHandle) {
    super();
    this.runtime = runtime;
    this.iterHandle = iterHandle;
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public long itrHandle() {
    return iterHandle;
  }

  private native boolean nativeHasNext(long iterHandle);

  private native long nativeNext(long iterHandle);

  private native long nativeSpill(long iterHandle, long size);

  private native void nativeClose(long iterHandle);

  private native boolean nativeAddIteratorSplits(
      long iterHandle, ColumnarBatchInIterator[] batchItr);

  private native void nativeNoMoreSplits(long iterHandle);

  private native void nativeRequestBarrier(long iterHandle);

  @Override
  public boolean hasNext0() throws IOException {
    return nativeHasNext(iterHandle);
  }

  @Override
  public ColumnarBatch next0() throws IOException {
    long batchHandle = nativeNext(iterHandle);
    if (batchHandle == -1L) {
      return null; // stream ended
    }
    return ColumnarBatches.create(batchHandle);
  }

  public long spill(long size) {
    if (!closed.get()) {
      return nativeSpill(iterHandle, size);
    } else {
      return 0L;
    }
  }

  /**
   * Add new iterator splits to the iterator as new inputs for processing. Note: File-based splits
   * are not supported.
   *
   * @param batchItr Array of iterators to add as splits
   * @return true if splits were added successfully, false otherwise
   * @throws IllegalStateException if the iterator is closed
   */
  public boolean addIteratorSplits(ColumnarBatchInIterator[] batchItr) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot add splits to a closed iterator");
    }
    return nativeAddIteratorSplits(iterHandle, batchItr);
  }

  /**
   * Signal that no more splits will be added to the iterator. This is required for proper task
   * completion.
   *
   * @throws IllegalStateException if the iterator is closed
   */
  public void noMoreSplits() {
    if (closed.get()) {
      throw new IllegalStateException("Cannot call noMoreSplits on a closed iterator");
    }
    nativeNoMoreSplits(iterHandle);
  }

  /**
   * Request a barrier in the task execution. This signals the task to finish processing all
   * currently queued splits and drain all stateful operators before continuing. After calling this
   * method, continue calling next() to fetch results. When next() returns null and hasNext()
   * returns false, the barrier has been reached.
   *
   * <p>This enables task reuse and deterministic execution for workloads like AI training data
   * loading and real-time streaming processing.
   *
   * @throws IllegalStateException if the iterator is closed
   * @see <a href="https://facebookincubator.github.io/velox/develop/task-barrier.html">Velox Task
   *     Barrier Documentation</a>
   */
  public void requestBarrier() {
    if (closed.get()) {
      throw new IllegalStateException("Cannot call requestBarrier on a closed iterator");
    }
    nativeRequestBarrier(iterHandle);
  }

  /**
   * Translates a Velox type conversion error into a SchemaColumnConvertNotSupportedException.
   * Returns null if the message does not indicate a type conversion error.
   */
  private static RuntimeException translateToSchemaException(String msg) {
    if (msg.contains("not allowed for requested type") || msg.contains("Not a valid type for")) {
      return new SchemaColumnConvertNotSupportedException("unknown", msg, "unknown");
    }
    return null;
  }

  @Override
  protected RuntimeException translateException(Exception e) {
    String msg = findFirstNonNullMessage(e);
    if (msg != null) {
      RuntimeException schemaEx = translateToSchemaException(msg);
      if (schemaEx != null) {
        schemaEx.initCause(e);
        return schemaEx;
      }
    }
    return new GlutenException(e);
  }

  private static String findFirstNonNullMessage(Throwable t) {
    while (t != null) {
      String msg = t.getMessage();
      if (msg != null) {
        return msg;
      }
      t = t.getCause();
    }
    return null;
  }

  @Override
  public void close0() {
    // To make sure the outputted batches are still accessible after the iterator is closed.
    // TODO: Remove this API if we have other choice, e.g., hold the pools in native code.
    runtime.memoryManager().hold();
    nativeClose(iterHandle);
  }
}
