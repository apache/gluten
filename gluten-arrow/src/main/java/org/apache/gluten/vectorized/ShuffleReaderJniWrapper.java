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

import org.apache.gluten.execution.CPUStageMode;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

public class ShuffleReaderJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ShuffleReaderJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ShuffleReaderJniWrapper create(Runtime runtime) {
    return new ShuffleReaderJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public long make(
      String shuffleWriterType,
      long cSchema,
      String compressionType,
      String compressionCodecBackend,
      int batchSize,
      long readerBufferSize,
      long deserializerBufferSize,
      boolean enableHashShuffleReaderStreamMerge) {
    return make(
        shuffleWriterType,
        cSchema,
        compressionType,
        compressionCodecBackend,
        batchSize,
        readerBufferSize,
        deserializerBufferSize,
        enableHashShuffleReaderStreamMerge,
        false,
        0L);
  }

  public native long make(
      String shuffleWriterType,
      long cSchema,
      String compressionType,
      String compressionCodecBackend,
      int batchSize,
      long readerBufferSize,
      long deserializerBufferSize,
      boolean enableHashShuffleReaderStreamMerge,
      boolean enableGpuAsyncReader,
      long gpuAsyncReaderMaxPrefetchBytes);

  public native long read(
      long shuffleReaderHandle, ShuffleStreamReader streamReader, int executionMode);

  public long read(long shuffleReaderHandle, ShuffleStreamReader streamReader) {
    return read(shuffleReaderHandle, streamReader, CPUStageMode.id());
  }

  public native void populateMetrics(long shuffleReaderHandle, ShuffleReaderMetrics metrics);

  // Stop the native shuffle reader from reading and deserializing streams
  // when the deserializer is closed.
  public native void stop(long shuffleReaderHandle);

  public native void close(long shuffleReaderHandle);
}
