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

/** Test-only prepared plan pipeline; optional local file shuffle without Spark jobs. */
public final class ExpressionBenchmarkJni implements RuntimeAware {
  private final Runtime runtime;

  public ExpressionBenchmarkJni(Runtime runtime) {
    this.runtime = runtime;
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  // One stage executes directly; two stages exchange through local files.
  public native long compile(
      byte[][] stagePlans,
      int batchSize,
      boolean hashPartitioned,
      boolean resizeShuffleInput,
      boolean resizeShuffleOutput,
      int minRows,
      int maxRows,
      long preferredBytes,
      boolean copyRanges,
      String shuffleDir);

  // Returns a caller-owned result iterator; metrics contain encoded bytes.
  public native long execute(long compiled, long[][] inputs, long[] metrics);

  public native void close(long compiled);

  public native long usedBytes();
}
