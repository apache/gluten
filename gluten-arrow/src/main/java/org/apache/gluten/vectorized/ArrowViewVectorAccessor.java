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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

/**
 * Reads Arrow BinaryView and Utf8View descriptors without sign-extending their uint32 offsets.
 *
 * <p>Arrow Java 18.1 reads the data-buffer offset as a signed int. An offset above 2 GiB therefore
 * becomes negative even though the Arrow View layout defines this field as uint32.
 */
final class ArrowViewVectorAccessor {
  private static final int VIEW_SIZE = 16;
  private static final int LENGTH_OFFSET = 0;
  private static final int INLINE_DATA_OFFSET = 4;
  private static final int BUFFER_INDEX_OFFSET = 8;
  private static final int BUFFER_OFFSET_OFFSET = 12;
  private static final int INLINE_SIZE = 12;

  private final ArrowBuf viewBuffer;
  private final List<ArrowBuf> dataBuffers;

  ArrowViewVectorAccessor(ValueVector vector) {
    List<ArrowBuf> buffers = ((FieldVector) vector).getFieldBuffers();
    if (buffers.size() < 2) {
      throw new IllegalArgumentException(
          "Arrow View vector must contain validity and view buffers: " + vector.getClass());
    }
    viewBuffer = buffers.get(1);
    dataBuffers = buffers.subList(2, buffers.size());
  }

  UTF8String getUTF8String(int rowId) {
    ValueLocation location = getValueLocation(rowId);
    return UTF8String.fromAddress(
        null, location.buffer.memoryAddress() + location.offset, location.length);
  }

  byte[] getBinary(int rowId) {
    ValueLocation location = getValueLocation(rowId);
    byte[] result = new byte[location.length];
    location.buffer.getBytes(location.offset, result, 0, location.length);
    return result;
  }

  private ValueLocation getValueLocation(int rowId) {
    long viewOffset = (long) rowId * VIEW_SIZE;
    int length = viewBuffer.getInt(viewOffset + LENGTH_OFFSET);
    if (length < 0) {
      throw new IllegalStateException("Arrow View value has negative length: " + length);
    }
    if (length <= INLINE_SIZE) {
      return new ValueLocation(viewBuffer, viewOffset + INLINE_DATA_OFFSET, length);
    }

    int bufferIndex = viewBuffer.getInt(viewOffset + BUFFER_INDEX_OFFSET);
    long bufferOffset =
        Integer.toUnsignedLong(viewBuffer.getInt(viewOffset + BUFFER_OFFSET_OFFSET));
    if (bufferIndex < 0 || bufferIndex >= dataBuffers.size()) {
      throw new IndexOutOfBoundsException(
          "Arrow View data buffer index "
              + bufferIndex
              + " is outside [0, "
              + dataBuffers.size()
              + ")");
    }

    ArrowBuf dataBuffer = dataBuffers.get(bufferIndex);
    if (bufferOffset + length > dataBuffer.capacity()) {
      throw new IndexOutOfBoundsException(
          "Arrow View value range ["
              + bufferOffset
              + ", "
              + (bufferOffset + length)
              + ") exceeds data buffer capacity "
              + dataBuffer.capacity());
    }
    return new ValueLocation(dataBuffer, bufferOffset, length);
  }

  private static final class ValueLocation {
    private final ArrowBuf buffer;
    private final long offset;
    private final int length;

    private ValueLocation(ArrowBuf buffer, long offset, int length) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
    }
  }
}
