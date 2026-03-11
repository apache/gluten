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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.MetricsApi
import org.apache.gluten.config.{GpuHashShuffleWriterType, HashShuffleWriterType, RssSortShuffleWriterType, ShuffleWriterType, SortShuffleWriterType}
import org.apache.gluten.metrics._
import org.apache.gluten.substrait.{AggregationParams, JoinParams}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{ColumnarInputAdapter, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import java.lang.{Long => JLong}
import java.util.{List => JList, Map => JMap}

class VeloxMetricsApi extends MetricsApi with Logging {
  override def metricsUpdatingFunction(
      child: SparkPlan,
      relMap: JMap[JLong, JList[JLong]],
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    MetricsUtil.genMetricsUpdatingFunction(child, relMap, joinParamsMap, aggParamsMap)
  }

  override def genInputIteratorTransformerMetrics(
      child: SparkPlan,
      sparkContext: SparkContext,
      forBroadcast: Boolean,
      forShuffle: Boolean): Map[String, SQLMetric] = {
    def metricsPlan(plan: SparkPlan): SparkPlan = {
      plan match {
        case ColumnarInputAdapter(child) => metricsPlan(child)
        case q: QueryStageExec => metricsPlan(q.plan)
        case _ => plan
      }
    }

    val outputMetrics = if (forBroadcast) {
      metricsPlan(child).metrics
        .filterKeys(key => key.equals("numOutputRows") || key.equals("outputVectors"))
    } else {
      Map(
        "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
        "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors")
      )
    }

    val wallNanosMetric = if (forShuffle) {
      // For input from shuffle, the time of shuffle read is inclusive to the metrics.
      SQLMetrics.createNanoTimingMetric(sparkContext, "time of reducer input")
    } else if (forBroadcast) {
      // For input from broadcast, the time of broadcasting is exclusive.
      SQLMetrics.createNanoTimingMetric(sparkContext, "time of broadcast input")
    } else {
      // For other occasions, e.g. fallback, union, the time of the previous pipeline is inclusive.
      SQLMetrics.createNanoTimingMetric(sparkContext, "time of operator input")
    }

    Map(
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "wallNanos" -> wallNanosMetric
    ) ++ outputMetrics
  }

  override def genInputIteratorTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      forBroadcast: Boolean): MetricsUpdater = {
    InputIteratorMetricsUpdater(metrics, forBroadcast)
  }

  override def genBatchScanTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
      "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan and filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "number of dynamic filters accepted"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "number of skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "number of processed splits"),
      "preloadSplits" -> SQLMetrics.createMetric(sparkContext, "number of preloaded splits"),
      "pageLoadTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "page load time"),
      "dataSourceAddSplitTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "data source add split time"),
      "dataSourceReadTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "data source read time"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "number of skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "number of processed row groups"),
      "remainingFilterTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "remaining filter time"),
      "ioWaitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "io wait time"),
      "storageReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "storage read bytes"),
      "localReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "local ssd read bytes"),
      "ramReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "ram read bytes"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genBatchScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new BatchScanMetricsUpdater(metrics)

  override def genHiveTableScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan and filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "number of dynamic filters accepted"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "number of skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "number of processed splits"),
      "preloadSplits" -> SQLMetrics.createMetric(sparkContext, "number of preloaded splits"),
      "pageLoadTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "page load time"),
      "dataSourceAddSplitTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "data source add split time"),
      "dataSourceReadTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "data source read time"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "number of skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "number of processed row groups"),
      "remainingFilterTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "remaining filter time"),
      "ioWaitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "io wait time"),
      "storageReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "storage read bytes"),
      "localReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "local ssd read bytes"),
      "ramReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "ram read bytes"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genHiveTableScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HiveTableScanMetricsUpdater(metrics)

  override def genFileSourceScanTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
      "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "scanTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of scan and filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
      "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
      "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
      "pruningTime" ->
        SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "numDynamicFiltersAccepted" -> SQLMetrics.createMetric(
        sparkContext,
        "number of dynamic filters accepted"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "skippedSplits" -> SQLMetrics.createMetric(sparkContext, "number of skipped splits"),
      "processedSplits" -> SQLMetrics.createMetric(sparkContext, "number of processed splits"),
      "preloadSplits" -> SQLMetrics.createMetric(sparkContext, "number of preloaded splits"),
      "pageLoadTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "page load time"),
      "dataSourceAddSplitTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "data source add split time"),
      "dataSourceReadTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "data source read time"),
      "skippedStrides" -> SQLMetrics.createMetric(sparkContext, "number of skipped row groups"),
      "processedStrides" -> SQLMetrics.createMetric(sparkContext, "number of processed row groups"),
      "remainingFilterTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "remaining filter time"),
      "ioWaitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "io wait time"),
      "storageReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "storage read bytes"),
      "localReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "local ssd read bytes"),
      "ramReadBytes" -> SQLMetrics.createSizeMetric(sparkContext, "ram read bytes"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genFileSourceScanTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new FileSourceScanMetricsUpdater(metrics)

  override def genFilterTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of filter"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genFilterTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      extraMetrics: Seq[(String, SQLMetric)] = Seq.empty): MetricsUpdater =
    new FilterMetricsUpdater(metrics, extraMetrics)

  override def genProjectTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of project"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genProjectTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric],
      extraMetrics: Seq[(String, SQLMetric)] = Seq.empty): MetricsUpdater =
    new ProjectMetricsUpdater(metrics, extraMetrics)

  override def genHashAggregateTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "aggOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "aggOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "aggOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "aggCpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "aggWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of aggregation"),
      "aggPeakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "aggNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "aggSpilledBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of spilled bytes"),
      "aggSpilledRows" -> SQLMetrics.createMetric(sparkContext, "number of spilled rows"),
      "aggSpilledPartitions" -> SQLMetrics.createMetric(
        sparkContext,
        "number of spilled partitions"),
      "aggSpilledFiles" -> SQLMetrics.createMetric(sparkContext, "number of spilled files"),
      "flushRowCount" -> SQLMetrics.createMetric(sparkContext, "number of flushed rows"),
      "loadedToValueHook" -> SQLMetrics.createMetric(
        sparkContext,
        "number of pushdown aggregations"),
      "bloomFilterBlocksByteSize" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "bloom filter blocks byte size"),
      "rowConstructionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "rowConstruction cpu wall time count"),
      "rowConstructionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of rowConstruction"),
      "extractionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "extraction cpu wall time count"),
      "extractionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of extraction"),
      "finalOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of final output rows"),
      "finalOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "number of final output vectors"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genHashAggregateTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater =
    new HashAggregateMetricsUpdaterImpl(metrics)

  override def genExpandTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of expand"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genExpandTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new ExpandMetricsUpdater(metrics)

  override def genCustomExpandMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def genColumnarShuffleExchangeMetrics(
      sparkContext: SparkContext,
      shuffleWriterType: ShuffleWriterType): Map[String, SQLMetric] = {
    val baseMetrics = Map(
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"),
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
      "avgReadBatchNumRows" -> SQLMetrics
        .createAverageMetric(sparkContext, "avg read batch num rows"),
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputRows" -> SQLMetrics
        .createMetric(sparkContext, "number of output rows"),
      "inputBatches" -> SQLMetrics
        .createMetric(sparkContext, "number of input batches"),
      "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to spill"),
      "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to compress"),
      "decompressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to decompress"),
      "deserializeTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to deserialize"),
      "shuffleWallTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle wall time"),
      // For hash shuffle writer, the peak bytes represents the maximum split buffer size.
      // For sort shuffle writer, the peak bytes represents the maximum
      // row buffer + sort buffer size.
      "peakBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak bytes allocated")
    )
    shuffleWriterType match {
      case HashShuffleWriterType | GpuHashShuffleWriterType =>
        baseMetrics ++ Map(
          "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to split"),
          "avgDictionaryFields" -> SQLMetrics
            .createAverageMetric(sparkContext, "avg dictionary fields"),
          "dictionarySize" -> SQLMetrics.createSizeMetric(sparkContext, "dictionary size")
        )
      case SortShuffleWriterType =>
        baseMetrics ++ Map(
          "sortTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to shuffle sort"),
          "c2rTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to shuffle c2r")
        )
      case RssSortShuffleWriterType =>
        baseMetrics ++ Map(
          "sortTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to shuffle sort")
        )
      case _ =>
        baseMetrics
    }
  }

  override def genWindowTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of window"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "spilledBytes" -> SQLMetrics.createSizeMetric(sparkContext, "bytes written for spilling"),
      "spilledRows" -> SQLMetrics.createMetric(sparkContext, "total rows written for spilling"),
      "spilledPartitions" -> SQLMetrics.createMetric(sparkContext, "total spilled partitions"),
      "spilledFiles" -> SQLMetrics.createMetric(sparkContext, "total spilled files"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genWindowTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new WindowMetricsUpdater(metrics)

  override def genColumnarToRowMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
    )

  override def genRowToColumnarMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "convertTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert")
    )

  override def genLimitTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of limit"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genLimitTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new LimitMetricsUpdater(metrics)

  def genWriteFilesTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "physicalWrittenBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "number of written bytes"),
      "writeIONanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of write IO"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of write"),
      "numWrittenFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  def genWriteFilesTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new WriteFilesMetricsUpdater(metrics)

  def genBatchWriteMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numWrittenBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of written bytes"),
      "writeIOTimeNs" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of write IO"),
      "writeWallNs" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of write"),
      "numWrittenFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files")
    )

  override def genSortTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of sort"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "spilledBytes" -> SQLMetrics.createSizeMetric(sparkContext, "bytes written for spilling"),
      "spilledRows" -> SQLMetrics.createMetric(sparkContext, "total rows written for spilling"),
      "spilledPartitions" -> SQLMetrics.createMetric(sparkContext, "total spilled partitions"),
      "spilledFiles" -> SQLMetrics.createMetric(sparkContext, "total spilled files"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genSortTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new SortMetricsUpdater(metrics)

  override def genSortMergeJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "numOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of merge join"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "streamPreProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "stream preProject cpu wall time count"),
      "streamPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of stream preProjection"),
      "bufferPreProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "buffer preProject cpu wall time count"),
      "bufferPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of buffer preProjection"),
      "postProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "postProject cpu wall time count"),
      "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of postProjection"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genSortMergeJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new SortMergeJoinMetricsUpdater(metrics)

  override def genColumnarBroadcastExchangeMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
      "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")
    )

  override def genColumnarSubqueryBroadcastMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)")
    )

  override def genHashJoinTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "hashBuildInputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash build input rows"),
      "hashBuildOutputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash build output rows"),
      "hashBuildOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash build output vectors"),
      "hashBuildOutputBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "number of hash build output bytes"),
      "hashBuildCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "hash build cpu wall time count"),
      "hashBuildWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of hash build"),
      "hashBuildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "hash build peak memory bytes"),
      "hashBuildNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash build memory allocations"),
      "hashBuildSpilledBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "bytes written for spilling of hash build"),
      "hashBuildSpilledRows" -> SQLMetrics.createMetric(
        sparkContext,
        "total rows written for spilling of hash build"),
      "hashBuildSpilledPartitions" -> SQLMetrics.createMetric(
        sparkContext,
        "total spilled partitions of hash build"),
      "hashBuildSpilledFiles" -> SQLMetrics.createMetric(
        sparkContext,
        "total spilled files of hash build"),
      "hashProbeInputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash probe input rows"),
      "hashProbeOutputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash probe output rows"),
      "hashProbeOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash probe output vectors"),
      "hashProbeOutputBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "number of hash probe output bytes"),
      "hashProbeCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "hash probe cpu wall time count"),
      "hashProbeWallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of hash probe"),
      "hashProbePeakMemoryBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "hash probe peak memory bytes"),
      "hashProbeNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash probe memory allocations"),
      "hashProbeSpilledBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "bytes written for spilling of hash probe"),
      "hashProbeSpilledRows" -> SQLMetrics.createMetric(
        sparkContext,
        "total rows written for spilling of hash probe"),
      "hashProbeSpilledPartitions" -> SQLMetrics.createMetric(
        sparkContext,
        "total spilled partitions of hash probe"),
      "hashProbeSpilledFiles" -> SQLMetrics.createMetric(
        sparkContext,
        "total spilled files of hash probe"),
      "hashProbeReplacedWithDynamicFilterRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash probe replaced with dynamic filter rows"),
      "hashProbeDynamicFiltersProduced" -> SQLMetrics.createMetric(
        sparkContext,
        "number of hash probe dynamic filters produced"),
      "bloomFilterBlocksByteSize" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "bloom filter blocks byte size"),
      "streamPreProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "stream preProject cpu wall time count"),
      "streamPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of stream preProjection"),
      "buildPreProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "preProject cpu wall time count"),
      "buildPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time to build preProjection"),
      "postProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "postProject cpu wall time count"),
      "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of postProjection"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "numOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genHashJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new HashJoinMetricsUpdater(metrics)

  override def genNestedLoopJoinTransformerMetrics(
      sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "nestedLoopJoinBuildInputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join build input rows"),
      "nestedLoopJoinBuildOutputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join build output rows"),
      "nestedLoopJoinBuildOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join build output vectors"),
      "nestedLoopJoinBuildOutputBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "number of nested loop join build output bytes"),
      "nestedLoopJoinBuildCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "nested loop join build cpu wall time count"),
      "nestedLoopJoinBuildWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of nested loop join build"),
      "nestedLoopJoinBuildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "nested loop join build peak memory bytes"),
      "nestedLoopJoinBuildNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join build memory allocations"),
      "nestedLoopJoinProbeInputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join probe input rows"),
      "nestedLoopJoinProbeOutputRows" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join probe output rows"),
      "nestedLoopJoinProbeOutputVectors" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join probe output vectors"),
      "nestedLoopJoinProbeOutputBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "number of nested loop join probe output bytes"),
      "nestedLoopJoinProbeCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "nested loop join probe cpu wall time count"),
      "nestedLoopJoinProbeWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of nested loop join probe"),
      "nestedLoopJoinProbePeakMemoryBytes" -> SQLMetrics.createSizeMetric(
        sparkContext,
        "nested loop join probe peak memory bytes"),
      "nestedLoopJoinProbeNumMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of nested loop join probe memory allocations"),
      "postProjectionCpuCount" -> SQLMetrics.createMetric(
        sparkContext,
        "postProject cpu wall time count"),
      "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of postProjection"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "numOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genNestedLoopJoinTransformerMetricsUpdater(
      metrics: Map[String, SQLMetric]): MetricsUpdater = new NestedLoopJoinMetricsUpdater(metrics)

  override def genSampleTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
      "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
      "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of sample"),
      "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
      "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
      "numMemoryAllocations" -> SQLMetrics.createMetric(
        sparkContext,
        "number of memory allocations"),
      "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
        sparkContext,
        "time of loading lazy vectors")
    )

  override def genSampleTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new SampleMetricsUpdater(metrics)

  override def genUnionTransformerMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
    "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time of union"),
    "cpuCount" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
    "loadLazyVectorTime" -> SQLMetrics.createNanoTimingMetric(
      sparkContext,
      "time of loading lazy vectors")
  )

  override def genUnionTransformerMetricsUpdater(metrics: Map[String, SQLMetric]): MetricsUpdater =
    new UnionMetricsUpdater(metrics)
}
