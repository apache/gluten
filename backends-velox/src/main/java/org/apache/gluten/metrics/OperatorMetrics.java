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
package org.apache.gluten.metrics;

public class OperatorMetrics implements IOperatorMetrics {
  public long inputRows;
  public long inputVectors;
  public long inputBytes;
  public long rawInputRows;
  public long rawInputBytes;
  public long outputRows;
  public long outputVectors;
  public long outputBytes;
  public long cpuCount;
  public long wallNanos;
  public long scanTime;
  public long peakMemoryBytes;
  public long numMemoryAllocations;
  public long spilledInputBytes;
  public long spilledBytes;
  public long spilledRows;
  public long spilledPartitions;
  public long spilledFiles;
  public long numDynamicFiltersProduced;
  public long numDynamicFiltersAccepted;
  public long numReplacedWithDynamicFilterRows;
  public long flushRowCount;
  public long loadedToValueHook;
  public long bloomFilterBlocksByteSize;
  public long skippedSplits;
  public long processedSplits;
  public long skippedStrides;
  public long processedStrides;
  public long remainingFilterTime;
  public long ioWaitTime;
  public long storageReadBytes;
  public long localReadBytes;
  public long ramReadBytes;
  public long preloadSplits;
  public long pageLoadTime;
  public long dataSourceAddSplitTime;
  public long dataSourceReadTime;

  public long physicalWrittenBytes;
  public long writeIOTime;
  public long numWrittenFiles;

  public long loadLazyVectorTime;

  /** Create an instance for operator metrics. */
  public OperatorMetrics(
      long inputRows,
      long inputVectors,
      long inputBytes,
      long rawInputRows,
      long rawInputBytes,
      long outputRows,
      long outputVectors,
      long outputBytes,
      long cpuCount,
      long wallNanos,
      long peakMemoryBytes,
      long numMemoryAllocations,
      long spilledInputBytes,
      long spilledBytes,
      long spilledRows,
      long spilledPartitions,
      long spilledFiles,
      long numDynamicFiltersProduced,
      long numDynamicFiltersAccepted,
      long numReplacedWithDynamicFilterRows,
      long flushRowCount,
      long loadedToValueHook,
      long bloomFilterBlocksByteSize,
      long scanTime,
      long skippedSplits,
      long processedSplits,
      long skippedStrides,
      long processedStrides,
      long remainingFilterTime,
      long ioWaitTime,
      long storageReadBytes,
      long localReadBytes,
      long ramReadBytes,
      long preloadSplits,
      long pageLoadTime,
      long dataSourceAddSplitTime,
      long dataSourceReadTime,
      long physicalWrittenBytes,
      long writeIOTime,
      long numWrittenFiles,
      long loadLazyVectorTime) {
    this.inputRows = inputRows;
    this.inputVectors = inputVectors;
    this.inputBytes = inputBytes;
    this.rawInputRows = rawInputRows;
    this.rawInputBytes = rawInputBytes;
    this.outputRows = outputRows;
    this.outputVectors = outputVectors;
    this.outputBytes = outputBytes;
    this.cpuCount = cpuCount;
    this.wallNanos = wallNanos;
    this.scanTime = scanTime;
    this.peakMemoryBytes = peakMemoryBytes;
    this.numMemoryAllocations = numMemoryAllocations;
    this.spilledInputBytes = spilledInputBytes;
    this.spilledBytes = spilledBytes;
    this.spilledRows = spilledRows;
    this.spilledPartitions = spilledPartitions;
    this.spilledFiles = spilledFiles;
    this.numDynamicFiltersProduced = numDynamicFiltersProduced;
    this.numDynamicFiltersAccepted = numDynamicFiltersAccepted;
    this.numReplacedWithDynamicFilterRows = numReplacedWithDynamicFilterRows;
    this.flushRowCount = flushRowCount;
    this.loadedToValueHook = loadedToValueHook;
    this.bloomFilterBlocksByteSize = bloomFilterBlocksByteSize;
    this.skippedSplits = skippedSplits;
    this.processedSplits = processedSplits;
    this.skippedStrides = skippedStrides;
    this.processedStrides = processedStrides;
    this.remainingFilterTime = remainingFilterTime;
    this.ioWaitTime = ioWaitTime;
    this.storageReadBytes = storageReadBytes;
    this.localReadBytes = localReadBytes;
    this.ramReadBytes = ramReadBytes;
    this.preloadSplits = preloadSplits;
    this.pageLoadTime = pageLoadTime;
    this.dataSourceAddSplitTime = dataSourceAddSplitTime;
    this.dataSourceReadTime = dataSourceReadTime;
    this.physicalWrittenBytes = physicalWrittenBytes;
    this.writeIOTime = writeIOTime;
    this.numWrittenFiles = numWrittenFiles;
    this.loadLazyVectorTime = loadLazyVectorTime;
  }
}
