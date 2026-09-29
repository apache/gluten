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

public class GlutenSplitResult {
  private final long totalComputePidTime;
  private final long totalWriteTime;
  private final long totalEvictTime;
  private final long totalCompressTime; // overlaps with totalEvictTime and totalWriteTime
  private final long bytesWritten;
  private final long totalBytesEvicted;
  private final long[] partitionLengths;
  private final long[] rawPartitionLengths;
  private final long[] rowBasedChecksums;
  private final long bytesToEvict;
  private final long peakBytes;
  private final long sortTime;
  private final long c2rTime;
  private final double avgDictionaryFields;
  private final long dictionarySize;

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
      long[] rowBasedChecksums) {
    this.totalComputePidTime = totalComputePidTime;
    this.totalWriteTime = totalWriteTime;
    this.totalEvictTime = totalEvictTime;
    this.totalCompressTime = totalCompressTime;
    this.bytesWritten = bytesWritten;
    this.totalBytesEvicted = totalBytesEvicted;
    this.partitionLengths = partitionLengths;
    this.rawPartitionLengths = rawPartitionLengths;
    this.rowBasedChecksums = rowBasedChecksums;
    this.bytesToEvict = totalBytesToEvict;
    this.peakBytes = peakBytes;
    this.sortTime = totalSortTime;
    this.c2rTime = totalC2RTime;
    this.avgDictionaryFields = avgDictionaryFields;
    this.dictionarySize = dictionarySize;
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

  public long[] getRowBasedChecksums() {
    return rowBasedChecksums;
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
}
