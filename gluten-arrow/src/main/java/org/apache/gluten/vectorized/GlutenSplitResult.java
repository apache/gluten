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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class GlutenSplitResult {
  private final long totalComputePidTime;
  private final long totalWriteTime;
  private final long totalEvictTime;
  private final long totalCompressTime; // overlaps with totalEvictTime and totalWriteTime
  private final long bytesWritten;
  private final long totalBytesEvicted;
  private final long[] partitionLengths;
  private final long[] rawPartitionLengths;
  private final long bytesToEvict;
  private final long peakBytes;
  private final long sortTime;
  private final long c2rTime;
  private final double avgDictionaryFields;
  private final long dictionarySize;

  // Backend-specific shuffle writer metrics. Marshalled across JNI as two
  // parallel arrays (keys + values) to keep the JNI call cheap; reassembled
  // here into a Map on first access. See `ShuffleWriterMetrics::customMetrics`
  // in `cpp/core/shuffle/Options.h` for the key-naming convention
  // (`<Backend>.<Family>.<Stat>`).
  private final String[] customMetricsKeys;
  private final long[] customMetricsValues;
  private volatile Map<String, Long> customMetricsCache;

  public GlutenSplitResult(
      long totalComputePidTime,
      long totalWriteTime,
      long totalEvictTime,
      long totalCompressTime,
      long totalSortTime,
      long totalC2RTime,
      long bytesWritten,
      long totalBytesEvicted,
      long totalBytesToEvict, // In-memory bytes(uncompressed) before spill.
      long peakBytes,
      double avgDictionaryFields,
      long dictionarySize,
      long[] partitionLengths,
      long[] rawPartitionLengths,
      String[] customMetricsKeys,
      long[] customMetricsValues) {
    this.totalComputePidTime = totalComputePidTime;
    this.totalWriteTime = totalWriteTime;
    this.totalEvictTime = totalEvictTime;
    this.totalCompressTime = totalCompressTime;
    this.bytesWritten = bytesWritten;
    this.totalBytesEvicted = totalBytesEvicted;
    this.partitionLengths = partitionLengths;
    this.rawPartitionLengths = rawPartitionLengths;
    this.bytesToEvict = totalBytesToEvict;
    this.peakBytes = peakBytes;
    this.sortTime = totalSortTime;
    this.c2rTime = totalC2RTime;
    this.avgDictionaryFields = avgDictionaryFields;
    this.dictionarySize = dictionarySize;
    this.customMetricsKeys = customMetricsKeys;
    this.customMetricsValues = customMetricsValues;
  }

  public long getTotalComputePidTime() {
    return totalComputePidTime;
  }

  public long getTotalWriteTime() {
    return totalWriteTime;
  }

  public long getTotalSpillTime() {
    return totalEvictTime;
  }

  public long getTotalCompressTime() {
    return totalCompressTime;
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  public long getTotalBytesSpilled() {
    return totalBytesEvicted;
  }

  public long getTotalPushTime() {
    return totalEvictTime;
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public long[] getRawPartitionLengths() {
    return rawPartitionLengths;
  }

  public long getBytesToEvict() {
    return bytesToEvict;
  }

  public long getPeakBytes() {
    return peakBytes;
  }

  public long getSortTime() {
    return sortTime;
  }

  public long getC2RTime() {
    return c2rTime;
  }

  public double getAvgDictionaryFields() {
    return avgDictionaryFields;
  }

  public long getDictionarySize() {
    return dictionarySize;
  }

  /**
   * Backend-specific shuffle writer metrics, keyed by `<Backend>.<Family>.<Stat>`. The map
   * preserves the iteration order JNI marshalled, but callers should treat the map as unordered.
   * Returns an empty map if the native side did not populate any custom metrics. The returned map
   * is unmodifiable.
   */
  public Map<String, Long> getCustomMetrics() {
    Map<String, Long> cached = customMetricsCache;
    if (cached != null) {
      return cached;
    }
    synchronized (this) {
      if (customMetricsCache != null) {
        return customMetricsCache;
      }
      if (customMetricsKeys == null || customMetricsKeys.length == 0) {
        customMetricsCache = Collections.emptyMap();
      } else {
        // Defensive check: if a future native-side producer ever ships
        // mismatched arrays, fail loudly here (before the cache field is
        // assigned to a partial result). Without this, the AIOOBE inside the
        // loop would leave `customMetricsCache` null and every subsequent
        // `getCustomMetrics()` call would re-enter the synchronized block
        // and re-throw — a confusing failure mode.
        if (customMetricsValues == null || customMetricsValues.length != customMetricsKeys.length) {
          throw new IllegalStateException(
              "customMetricsKeys / customMetricsValues length mismatch: "
                  + customMetricsKeys.length
                  + " vs "
                  + (customMetricsValues == null ? "null" : customMetricsValues.length));
        }
        LinkedHashMap<String, Long> map = new LinkedHashMap<>(customMetricsKeys.length);
        for (int i = 0; i < customMetricsKeys.length; i++) {
          map.put(customMetricsKeys[i], customMetricsValues[i]);
        }
        customMetricsCache = Collections.unmodifiableMap(map);
      }
      return customMetricsCache;
    }
  }
}
