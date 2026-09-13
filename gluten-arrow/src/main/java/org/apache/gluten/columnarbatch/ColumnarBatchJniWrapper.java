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

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

public class ColumnarBatchJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ColumnarBatchJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ColumnarBatchJniWrapper create(Runtime runtime) {
    return new ColumnarBatchJniWrapper(runtime);
  }

  // Static methods.
  public static native String getType(long batch);

  public static native long numColumns(long batch);

  public static native long numRows(long batch);

  public static native long numBytes(long batch);

  public static native void exportToArrow(
      long batch, long cSchema, long cArray, boolean useStringView);

  public static native void close(long batch);

  // Member methods in which native code relies on the backend's runtime API implementation.
  public native long createWithArrowArray(long cSchema, long cArray);

  public native long getForEmptySchema(int numRows);

  public native long select(long batch, int[] columnIndices);

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }
}
