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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class MetricsStep {

  protected String name;
  protected String description;

  @JsonProperty("parquet_reader_version")
  protected long parquetReaderVersion;

  protected List<MetricsProcessor> processors;

  @JsonProperty("total_marks_pk")
  protected long totalMarksPk;

  @JsonProperty("selected_marks_pk")
  protected long selectedMarksPk;

  @JsonProperty("selected_marks")
  protected long selectedMarks;

  @JsonProperty("read_cache_hits")
  protected long readCacheHits;

  @JsonProperty("miss_cache_hits")
  protected long missCacheHits;

  @JsonProperty("read_cache_bytes")
  protected long readCacheBytes;

  @JsonProperty("read_miss_bytes")
  protected long readMissBytes;

  @JsonProperty("read_cache_millisecond")
  protected long readCacheMillisecond;

  @JsonProperty("miss_cache_millisecond")
  protected long missCacheMillisecond;

  @JsonProperty("parquet_metadata_cache_hits")
  protected long parquetMetadataCacheHits;

  @JsonProperty("parquet_metadata_cache_misses")
  protected long parquetMetadataCacheMisses;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<MetricsProcessor> getProcessors() {
    return processors;
  }

  public void setProcessors(List<MetricsProcessor> processors) {
    this.processors = processors;
  }

  public void setTotalMarksPk(long totalMarksPk) {
    this.totalMarksPk = totalMarksPk;
  }

  public void setSelectedMarksPk(long selectedMarksPk) {
    this.selectedMarksPk = selectedMarksPk;
  }

  public long getSelectedMarks() {
    return selectedMarks;
  }

  public void setSelectedMarks(long selectedMarks) {
    this.selectedMarks = selectedMarks;
  }

  public long getTotalMarksPk() {
    return totalMarksPk;
  }

  public long getSelectedMarksPk() {
    return selectedMarksPk;
  }

  public long getReadCacheHits() {
    return readCacheHits;
  }

  public void setReadCacheHits(long readCacheHits) {
    this.readCacheHits = readCacheHits;
  }

  public long getMissCacheHits() {
    return missCacheHits;
  }

  public void setMissCacheHits(long missCacheHits) {
    this.missCacheHits = missCacheHits;
  }

  public long getReadCacheBytes() {
    return readCacheBytes;
  }

  public void setReadCacheBytes(long readCacheBytes) {
    this.readCacheBytes = readCacheBytes;
  }

  public long getReadMissBytes() {
    return readMissBytes;
  }

  public void setReadMissBytes(long readMissBytes) {
    this.readMissBytes = readMissBytes;
  }

  public long getReadCacheMillisecond() {
    return readCacheMillisecond;
  }

  public void setReadCacheMillisecond(long readCacheMillisecond) {
    this.readCacheMillisecond = readCacheMillisecond;
  }

  public long getMissCacheMillisecond() {
    return missCacheMillisecond;
  }

  public void setMissCacheMillisecond(long missCacheMillisecond) {
    this.missCacheMillisecond = missCacheMillisecond;
  }

  public long getParquetMetadataCacheHits() {
    return parquetMetadataCacheHits;
  }

  public void setParquetMetadataCacheHits(long parquetMetadataCacheHits) {
    this.parquetMetadataCacheHits = parquetMetadataCacheHits;
  }

  public long getParquetMetadataCacheMisses() {
    return parquetMetadataCacheMisses;
  }

  public void setParquetMetadataCacheMisses(long parquetMetadataCacheMisses) {
    this.parquetMetadataCacheMisses = parquetMetadataCacheMisses;
  }

  public long getParquetReaderVersion() {
    return parquetReaderVersion;
  }

  public void setParquetReaderVersion(long parquetReaderVersion) {
    this.parquetReaderVersion = parquetReaderVersion;
  }
}
