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
package org.apache.gluten.utils;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.runtime.Runtimes;

import io.netty.util.internal.PlatformDependent;
import org.apache.commons.io.IOUtils;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class VeloxBloomFilter extends BloomFilter {
  private static final int SERIALIZED_HEADER_SIZE = 5;

  private final VeloxBloomFilterJniWrapper jni =
      VeloxBloomFilterJniWrapper.create(
          Runtimes.contextInstance(BackendsApiManager.getBackendName(), "VeloxBloomFilter"));
  private final long handle;

  private VeloxBloomFilter(byte[] data) {
    handle = jni.init(data);
  }

  private VeloxBloomFilter(int capacity) {
    handle = jni.empty(capacity);
  }

  public static VeloxBloomFilter empty(int capacity) {
    return new VeloxBloomFilter(capacity);
  }

  public static VeloxBloomFilter readFrom(InputStream in) {
    try {
      byte[] data = IOUtils.toByteArray(in);
      return new VeloxBloomFilter(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static VeloxBloomFilter readFrom(byte[] data) {
    return new VeloxBloomFilter(data);
  }

  public byte[] serialize() {
    try (ByteArrayOutputStream o = new ByteArrayOutputStream()) {
      writeTo(o);
      return o.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double expectedFpp() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public long bitSize() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public long cardinality() {
    byte[] data = jni.serialize(handle);
    long bitsSet = 0L;
    for (int i = SERIALIZED_HEADER_SIZE; i < data.length; i++) {
      bitsSet += Integer.bitCount(data[i] & 0xFF);
    }
    return bitsSet;
  }

  @Override
  public boolean put(Object item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean putString(String item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean putLong(long item) {
    jni.insertLong(handle, item);
    return true;
  }

  @Override
  public boolean putBinary(byte[] item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    if (!(other instanceof VeloxBloomFilter)) {
      throw new IncompatibleMergeException(
          "Cannot merge Velox bloom-filter with non-Velox bloom-filter");
    }
    final VeloxBloomFilter from = (VeloxBloomFilter) other;

    if (!jni.isCompatibleWith(from.jni)) {
      throw new IncompatibleMergeException(
          "Cannot merge Velox bloom-filters with different Velox runtimes");
    }
    jni.mergeFrom(handle, from.handle);
    return this;
  }

  @Override
  public BloomFilter intersectInPlace(BloomFilter other) throws IncompatibleMergeException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean mightContain(Object item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean mightContainString(String item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean mightContainLong(long item) {
    return jni.mightContainLong(handle, item);
  }

  /**
   * GLUTEN-9849: We have to use this API for static may-contain evaluation over {@link
   * #mightContainLong} in Spark because if we are on Spark driver, there is no task context
   * available for managing the releasing of the native bloom-filter handles. In the case, it's
   * practical to serialize the bloom-filter into a Java direct buffer, then invoke this API for
   * zero-copy may-contain evaluation. JVM should manage the releasing correctly for the direct
   * buffer that stores the serialized bloom-filter data.
   */
  public static boolean mightContainLongOnSerializedBloom(ByteBuffer serializedBloom, long item) {
    return mightContainLongOnSerializedBloom(
        PlatformDependent.directBufferAddress(serializedBloom), item);
  }

  /**
   * Similar to the previous method, but accepts the exact memory address of the bloom-filter data
   * as input.
   */
  public static boolean mightContainLongOnSerializedBloom(long address, long item) {
    return VeloxBloomFilterJniWrapper.mightContainLongOnSerializedBloom(address, item);
  }

  /** Serializes the current bloom-filter into a direct byte buffer. */
  public ByteBuffer serializeToDirectBuffer() {
    final byte[] serialized = serialize();
    final ByteBuffer bb = ByteBuffer.allocateDirect(serialized.length);
    bb.put(serialized);
    return bb;
  }

  @Override
  public boolean mightContainBinary(byte[] item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    byte[] data = jni.serialize(handle);
    out.write(data);
  }
}
