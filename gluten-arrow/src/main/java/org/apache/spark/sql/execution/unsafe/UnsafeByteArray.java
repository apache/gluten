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
package org.apache.spark.sql.execution.unsafe;

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.arrow.memory.ArrowBuf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/** A serializable unsafe byte array. */
public class UnsafeByteArray implements Externalizable, KryoSerializable {
  private static final int CHUNK_SIZE = 8 * 1024;

  private ArrowBuf buffer;
  private long size;
  private transient byte[] chunkBuf;

  UnsafeByteArray(ArrowBuf buffer, long size) {
    this.buffer = buffer;
    this.buffer.getReferenceManager().retain();
    this.size = size;
  }

  public UnsafeByteArray() {}

  private byte[] chunkBuf() {
    if (chunkBuf == null) {
      chunkBuf = new byte[CHUNK_SIZE];
    }
    return chunkBuf;
  }

  public long address() {
    return buffer.memoryAddress();
  }

  public long size() {
    return size;
  }

  public void release() {
    if (buffer != null) {
      buffer.close();
      buffer = null;
      size = 0;
    }
  }

  // ------------ KryoSerializable ------------

  @Override
  public void write(Kryo kryo, Output output) {
    // write length first
    output.writeLong(size);

    // stream bytes out of ArrowBuf
    byte[] tmp = chunkBuf();

    long remaining = size;
    int index = 0;
    while (remaining > 0) {
      int chunk = (int) Math.min(CHUNK_SIZE, remaining);
      buffer.getBytes(index, tmp, 0, chunk);
      output.write(tmp, 0, chunk);
      index += chunk;
      remaining -= chunk;
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    // read length
    this.size = input.readLong();

    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("UnsafeByteArray size too large: " + size);
    }

    // allocate ArrowBuf
    this.buffer = ArrowBufferAllocators.globalInstance().buffer((int) size);

    // stream bytes into ArrowBuf
    byte[] tmp = chunkBuf();

    long remaining = size;
    int index = 0;
    while (remaining > 0) {
      int chunk = (int) Math.min(CHUNK_SIZE, remaining);
      input.readBytes(tmp, 0, chunk);
      buffer.setBytes(index, tmp, 0, chunk);
      index += chunk;
      remaining -= chunk;
    }
  }

  // ------------ Externalizable ------------

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // write length first
    out.writeLong(size);

    byte[] tmp = chunkBuf();

    long remaining = size;
    int index = 0;
    while (remaining > 0) {
      int chunk = (int) Math.min(CHUNK_SIZE, remaining);
      buffer.getBytes(index, tmp, 0, chunk);
      out.write(tmp, 0, chunk);
      index += chunk;
      remaining -= chunk;
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    this.size = in.readLong();

    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("UnsafeByteArray size too large: " + size);
    }

    this.buffer = ArrowBufferAllocators.globalInstance().buffer((int) size);

    byte[] tmp = chunkBuf();

    long remaining = size;
    int index = 0;
    while (remaining > 0) {
      int chunk = (int) Math.min(CHUNK_SIZE, remaining);
      // ObjectInput extends DataInput, so we can use readFully
      in.readFully(tmp, 0, chunk);
      buffer.setBytes(index, tmp, 0, chunk);
      index += chunk;
      remaining -= chunk;
    }
  }

  /**
   * It's needed once the broadcast variable is garbage collected. Since now, we don't have an
   * elegant way to free the underlying memory in off-heap.
   *
   * <p>Since: https://github.com/apache/gluten/pull/8127.
   */
  public void finalize() throws Throwable {
    release();
    super.finalize();
  }
}
