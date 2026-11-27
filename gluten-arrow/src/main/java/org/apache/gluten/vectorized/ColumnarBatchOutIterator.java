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
import org.apache.gluten.exception.NativeCastException;
import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.SparkThrowable;
import org.apache.spark.sql.errors.GlutenCastErrors;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SparkVersionUtil;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.collection.immutable.Map;

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

  // Velox's message when a char or varchar value is longer than its declared length.
  private static final Pattern CHAR_VARCHAR_LENGTH_EXCEEDED =
      Pattern.compile("Exceeds allowed length limitation: (\\d+)");

  // Velox's message when assert_not_null finds a null in a non-nullable field.
  private static final Pattern NOT_NULL_ASSERT_VIOLATED =
      Pattern.compile("^Reason: Null value appeared in non-nullable field$", Pattern.MULTILINE);

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

  /**
   * Translates a Velox char/varchar length check failure into the exception Spark throws for a
   * value longer than its declared length. Returns null if the message does not indicate one.
   */
  private static RuntimeException translateToCharVarcharLengthException(String msg, Exception e) {
    Matcher matcher = CHAR_VARCHAR_LENGTH_EXCEEDED.matcher(msg);
    if (!matcher.find()) {
      return null;
    }
    String limit = matcher.group(1);
    if (!SparkVersionUtil.gteSpark35()) {
      // Spark 3.4 has no EXCEED_LIMIT_LENGTH error class and throws this plain RuntimeException.
      return new RuntimeException("Exceeds char/varchar type length limitation: " + limit, e);
    }
    // Builds the exception QueryExecutionErrors.exceedMaxLimit builds, with the Velox error as its
    // cause. SparkRuntimeException's cause can only be set through its constructor.
    return new SparkRuntimeException(
        "EXCEED_LIMIT_LENGTH", new Map.Map1<>("limit", limit), e, new QueryContext[0], "");
  }

  /**
   * Translates a Velox assert_not_null failure into the exception Spark throws for a null in a
   * non-nullable field. Returns null if the message does not indicate one.
   */
  private static RuntimeException translateToNotNullAssertViolation(String msg, Exception e) {
    // Spark 3.x throws a NullPointerException whose message names the field instead.
    if (!SparkVersionUtil.gteSpark40() || !NOT_NULL_ASSERT_VIOLATED.matcher(msg).find()) {
      return null;
    }
    // Builds the exception QueryExecutionErrors.notNullAssertViolation builds. Velox's message
    // does not name the field, so the path is "\n\n", the value Spark uses when it has none.
    return new SparkRuntimeException(
        "NOT_NULL_ASSERT_VIOLATION",
        new Map.Map1<>("walkedTypePath", "\n\n"),
        e,
        new QueryContext[0],
        "");
  }

  @Override
  protected RuntimeException translateException(Exception e) {
    // Spark 4 shows a task's top-level SparkThrowable to the user and wraps anything else in
    // "Job aborted". Keep Spark exceptions from nested iterators, such as a cast error raised
    // before a shuffle, unchanged.
    if (e instanceof SparkThrowable && e instanceof RuntimeException) {
      return (RuntimeException) e;
    }
    if (e instanceof NativeCastException) {
      // The native side attributed this error to a cast. The message-based translations below
      // target other errors, and a cast reason can contain user data.
      NativeCastException nativeCast = (NativeCastException) e;
      RuntimeException castEx =
          GlutenCastErrors.fromNativeReason(nativeCast.getReason(), nativeCast);
      return castEx != null ? castEx : nativeCast;
    }
    String msg = findFirstNonNullMessage(e);
    if (msg != null) {
      RuntimeException schemaEx = translateToSchemaException(msg);
      if (schemaEx != null) {
        schemaEx.initCause(e);
        return schemaEx;
      }
      RuntimeException lengthEx = translateToCharVarcharLengthException(msg, e);
      if (lengthEx != null) {
        return lengthEx;
      }
      RuntimeException notNullEx = translateToNotNullAssertViolation(msg, e);
      if (notNullEx != null) {
        return notNullEx;
      }
    }
    // A GlutenException raised by native/nested code is already translated; return it
    // as-is rather than nesting it in another GlutenException. Keep this after every more
    // specific translation: a GlutenException subclass that needs its own translation must
    // be matched above, or this check returns it untranslated.
    if (e instanceof GlutenException) {
      return (GlutenException) e;
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
