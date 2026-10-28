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

/**
 * Direct scalar expression evaluation using the creating runtime's Spark session configuration.
 *
 * <p>Query settings include the session timezone, ANSI/legacy behavior, duplicate-map-key policy,
 * and dynamic Velox overrides. The runtime must provide a valid session timezone.
 *
 * <p>Evaluators are reusable but not thread-safe. Keep the evaluator and every input/output batch
 * within their task resource scope, with the creating runtime alive. Inputs are borrowed and must
 * already be Velox batches. Returned batches are caller-owned.
 */
public final class VeloxExpressionEvaluatorJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private VeloxExpressionEvaluatorJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static VeloxExpressionEvaluatorJniWrapper create(Runtime runtime) {
    return new VeloxExpressionEvaluatorJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  /** Compiles an ExtendedExpression with exactly one scalar result. */
  public native long compile(byte[] extendedExpression);

  /** Evaluates one batch without running a query pipeline or converting its input. */
  public native long evaluate(long evaluatorHandle, long inputBatchHandle);

  /** Releases the evaluator without invalidating its previously returned batches. */
  public native void close(long evaluatorHandle);
}
