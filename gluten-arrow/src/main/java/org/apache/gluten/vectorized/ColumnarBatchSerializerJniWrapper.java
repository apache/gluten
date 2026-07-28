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

import org.apache.spark.sql.execution.unsafe.JniUnsafeByteBuffer;

public class ColumnarBatchSerializerJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ColumnarBatchSerializerJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ColumnarBatchSerializerJniWrapper create(Runtime runtime) {
    return new ColumnarBatchSerializerJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public native JniUnsafeByteBuffer serialize(long handle);

  public native JniUnsafeByteBuffer serializeAll(long[] handles);

  // Framed [magic=0x02 | statsLen | statsBlob | bytesLen | bytesBlob] payload (V2) produced by
  // VeloxColumnarBatchSerializer::framedSerializeWithStats. Returns byte[] (not
  // JniUnsafeByteBuffer) because the framed wire is small enough that the simpler return type
  // avoids ByteBuffer lifetime concerns.
  public native byte[] serializeWithStats(long handle);

  // V3 per-column framed payload [magic=0x03 | statsLen=0 | numRows | numCols | per-col].
  // Returns null when the backend does not support V3 (callers should fall back).
  public native byte[] serializeV3(long handle);

  // V3 per-column framed payload [magic=0x03 | statsLen | statsBlob | numRows | numCols | per-col].
  // Returns null when the backend does not support V3 (callers should fall back to V2).
  public native byte[] serializeWithStatsV3(long handle);

  // Return the native ColumnarBatchSerializer handle
  public native long init(long cSchema);

  public native long deserialize(long serializerHandle, byte[] data);

  // Return the native ColumnarBatch handle using memory address and length
  public native long deserializeDirect(long serializerHandle, long offset, int len);

  public native void close(long serializerHandle);

  // V3 deserialize with column projection. Returns M-column native batch handle.
  // requestedColumnIndices: null = all columns; int[0] = zero columns; int[m] = M specified cols.
  public native long deserializeWithProjection(
      long serializerHandle, byte[] data, int[] requestedColumnIndices);
}
