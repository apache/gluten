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

import org.apache.gluten.exception.GlutenException;

import org.apache.spark.storage.BufferReleasingInputStream;
import org.apache.spark.storage.GlutenBufferReleasingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.zip.CheckedInputStream;

/** Create optimal {@link JniByteInputStream} implementation from Java {@link InputStream}. */
public final class JniByteInputStreams {
  private static final Logger LOG = LoggerFactory.getLogger(JniByteInputStreams.class);

  private static final Field FIELD_FilterInputStream_in;

  static {
    try {
      FIELD_FilterInputStream_in = FilterInputStream.class.getDeclaredField("in");
      FIELD_FilterInputStream_in.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private JniByteInputStreams() {}

  public static JniByteInputStream create(InputStream in) {
    // Unwrap BufferReleasingInputStream
    final InputStream unwrapped = unwrapSparkInputStream(in);
    if (LowCopyNettyJniByteInputStream.isSupported(unwrapped)) {
      return new LowCopyNettyJniByteInputStream(in);
    }
    if (LowCopyFileSegmentJniByteInputStream.isSupported(unwrapped)) {
      return new LowCopyFileSegmentJniByteInputStream(in);
    }
    return new OnHeapJniByteInputStream(in);
  }

  static InputStream unwrapSparkInputStream(InputStream in) {
    InputStream unwrapped = in;
    if (unwrapped instanceof GlutenBufferReleasingInputStream) {
      final GlutenBufferReleasingInputStream brin = (GlutenBufferReleasingInputStream) unwrapped;
      unwrapped = brin.delegate();
    }
    if (unwrapped instanceof BufferReleasingInputStream) {
      final BufferReleasingInputStream brin = (BufferReleasingInputStream) unwrapped;
      unwrapped =
          org.apache.spark.storage.SparkInputStreamUtil.unwrapBufferReleasingInputStream(brin);
    }
    if (unwrapped instanceof CheckedInputStream) {
      final CheckedInputStream cin = (CheckedInputStream) unwrapped;
      try {
        unwrapped = ((InputStream) FIELD_FilterInputStream_in.get(cin));
      } catch (IllegalAccessException e) {
        throw new GlutenException(e);
      }
    }
    return unwrapped;
  }
}
