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

public class UnifflePartitionWriterJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private UnifflePartitionWriterJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static UnifflePartitionWriterJniWrapper create(Runtime runtime) {
    return new UnifflePartitionWriterJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public native long createPartitionWriter(
      int numPartitions,
      String codec,
      String codecBackend,
      int compressionLevel,
      int compressionBufferSize,
      int pushBufferMaxSize,
      long sortBufferMaxSize,
      Object pusher);
}
