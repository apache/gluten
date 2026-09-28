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
package org.apache.gluten.execution;

import org.apache.gluten.metrics.BatchWriteMetrics;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

public class IcebergWriteJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  public IcebergWriteJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  // Return the native IcebergWriteJniWrapper handle
  public native long init(long cSchema, int format,
                          String directory,
                          String codec,
                          int partitionId,
                          long taskId,
                          String operationId,
                          byte[] partitionSpec,
                          byte[] field);

  public native void write(long writerHandle, long batch);

  // Returns the json iceberg Datafile represent
  public native String[] commit(long writerHandle);

  public native BatchWriteMetrics metrics(long writerHandle);

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }
}
