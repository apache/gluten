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

import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Spark's {@link ArrowColumnVector} with a reference count, used by Gluten's Java Arrow batches
 * (see {@link org.apache.gluten.columnarbatch.ColumnarBatches}). The count starts at 1; {@link
 * #retain()} increments it and {@link #close()} decrements it. The Arrow vector is released when
 * the count reaches 0.
 *
 * <p>Reading goes through Spark's accessors. To write, fill the Arrow vectors (e.g. with Spark's
 * {@code ArrowWriter}) and wrap them, see {@link ArrowColumnVectors}.
 */
public final class RefCountedArrowColumnVector extends ArrowColumnVector {
  private final AtomicLong refCnt = new AtomicLong(1L);

  public RefCountedArrowColumnVector(ValueVector vector) {
    super(vector);
  }

  public void retain() {
    refCnt.getAndIncrement();
  }

  public long refCnt() {
    return refCnt.get();
  }

  @Override
  public void close() {
    if (refCnt.get() == 0) {
      return;
    }
    if (refCnt.decrementAndGet() == 0) {
      super.close();
    }
  }
}
