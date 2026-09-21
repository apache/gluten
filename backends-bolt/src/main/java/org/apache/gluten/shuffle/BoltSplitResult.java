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
package org.apache.gluten.shuffle;

public class BoltSplitResult {
  private final long totalComputePidTime;
  private final long totalWriteTime;
  private final long totalEvictTime;
  private final long totalCompressTime; // overlaps with totalEvictTime and totalWriteTime
  private final long totalBytesWritten;
  private final long totalBytesEvicted;
  private final long splitBufferSize;
  private final long preAllocSize;
  private final long useV2Count;
  private final long rowVectorModeCompress;
  private final long combinedVectorNumber;
  private final long combineVectorTimes;
  private final long combineVectorCost;
  private final long useRowBased;
  private final long convertTime;
  private final long flattenTime;
  private final long computePidTime;
  private final long[] partitionLengths;
  private final long[] rawPartitionLengths;

  public BoltSplitResult(
      long totalComputePidTime,
      long totalWriteTime,
      long totalEvictTime,
      long totalCompressTime,
      long totalBytesWritten,
      long totalBytesEvicted,
      long splitBufferSize,
      long preAllocSize,
      long useV2Count,
      long rowVectorModeCompress,
      long combinedVectorNumber,
      long combineVectorTimes,
      long combineVectorCost,
      long useRowBased,
      long convertTime,
      long flattenTime,
      long computePidTime,
      long[] partitionLengths,
      long[] rawPartitionLengths) {
    this.totalComputePidTime = totalComputePidTime;
    this.totalWriteTime = totalWriteTime;
    this.totalEvictTime = totalEvictTime;
    this.totalCompressTime = totalCompressTime;
    this.totalBytesWritten = totalBytesWritten;
    this.totalBytesEvicted = totalBytesEvicted;
    this.splitBufferSize = splitBufferSize;
    this.preAllocSize = preAllocSize;
    this.useV2Count = useV2Count;
    this.rowVectorModeCompress = rowVectorModeCompress;
    this.combinedVectorNumber = combinedVectorNumber;
    this.combineVectorTimes = combineVectorTimes;
    this.combineVectorCost = combineVectorCost;
    this.useRowBased = useRowBased;
    this.convertTime = convertTime;
    this.flattenTime = flattenTime;
    this.computePidTime = computePidTime;
    this.partitionLengths = partitionLengths;
    this.rawPartitionLengths = rawPartitionLengths;
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

  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }

  public long getTotalBytesSpilled() {
    return totalBytesEvicted;
  }

  public long getSplitBufferSize() {
    return splitBufferSize;
  }

  public long getPreAllocSize() {
    return preAllocSize;
  }

  public long getUseV2Count() {
    return useV2Count;
  }

  public long rowVectorModeCompress() {
    return rowVectorModeCompress;
  }

  public long combinedVectorNumber() {
    return combinedVectorNumber;
  }

  public long combineVectorTimes() {
    return combineVectorTimes;
  }

  public long combineVectorCost() {
    return combineVectorCost;
  }

  public long getUseRowBased() {
    return useRowBased;
  }

  public long getConvertTime() {
    return convertTime;
  }

  public long getFlattenTime() {
    return flattenTime;
  }

  public long getComputePidTime() {
    return computePidTime;
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public long[] getRawPartitionLengths() {
    return rawPartitionLengths;
  }

  public long getTotalPushTime() {
    return totalEvictTime;
  }
}
