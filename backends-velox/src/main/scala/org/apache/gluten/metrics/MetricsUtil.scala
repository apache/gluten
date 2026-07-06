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
package org.apache.gluten.metrics

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution._
import org.apache.gluten.substrait.{AggregationParams, JoinParams}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.TaskStatsAccumulator
import org.apache.spark.sql.execution.SparkPlan

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._

object MetricsUtil extends Logging {
  private val objectMapper = new ObjectMapper()

  private def value(node: JsonNode, fieldName: String): Long =
    Option(node.get(fieldName)).map(_.asLong()).getOrElse(0L)

  private def customMetricSum(node: JsonNode, metricName: String): Long =
    Option(node.get("customStats"))
      .flatMap(customStats => Option(customStats.get(metricName)))
      .flatMap(metric => Option(metric.get("sum")))
      .map(_.asLong())
      .getOrElse(0L)

  private def customMetricCount(node: JsonNode, metricName: String): Long =
    Option(node.get("customStats"))
      .flatMap(customStats => Option(customStats.get(metricName)))
      .flatMap(metric => Option(metric.get("count")))
      .map(_.asLong())
      .getOrElse(0L)

  private def operatorMetricFromJson(node: JsonNode): OperatorMetrics = {
    val metrics = new OperatorMetrics()
    metrics.inputRows = value(node, "inputRows")
    metrics.inputVectors = value(node, "inputVectors")
    metrics.inputBytes = value(node, "inputBytes")
    metrics.rawInputRows = value(node, "rawInputRows")
    metrics.rawInputBytes = value(node, "rawInputBytes")
    metrics.outputRows = value(node, "outputRows")
    metrics.outputVectors = value(node, "outputVectors")
    metrics.outputBytes = value(node, "outputBytes")
    metrics.cpuCount = value(node, "cpuCount")
    metrics.wallNanos = value(node, "wallNanos")
    metrics.peakMemoryBytes = value(node, "peakMemoryBytes")
    metrics.numMemoryAllocations = value(node, "numMemoryAllocations")
    metrics.spilledInputBytes = value(node, "spilledInputBytes")
    metrics.spilledBytes = value(node, "spilledBytes")
    metrics.spilledRows = value(node, "spilledRows")
    metrics.spilledPartitions = value(node, "spilledPartitions")
    metrics.spilledFiles = value(node, "spilledFiles")
    metrics.numDynamicFiltersProduced = customMetricSum(node, "dynamicFiltersProduced")
    metrics.numDynamicFiltersAccepted = customMetricSum(node, "dynamicFiltersAccepted")
    metrics.numReplacedWithDynamicFilterRows =
      customMetricSum(node, "replacedWithDynamicFilterRows")
    metrics.numDynamicFilterInputRows = customMetricSum(node, "dynamicFilterInputRows")
    metrics.flushRowCount = customMetricSum(node, "flushRowCount")
    metrics.abandonedPartialAggregationRows =
      customMetricSum(node, "abandonedPartialAggregationRows")
    metrics.loadedToValueHook = customMetricSum(node, "loadedToValueHook")
    metrics.bloomFilterBlocksByteSize = customMetricSum(node, "bloomFilterSize")
    metrics.scanTime = customMetricSum(node, "totalScanTime")
    metrics.skippedSplits = customMetricSum(node, "skippedSplits")
    metrics.processedSplits = customMetricSum(node, "processedSplits")
    metrics.skippedStrides = customMetricSum(node, "skippedStrides")
    metrics.processedStrides = customMetricSum(node, "processedStrides")
    metrics.remainingFilterTime = customMetricSum(node, "totalRemainingFilterWallNanos")
    metrics.ioWaitTime = customMetricSum(node, "ioWaitWallNanos")
    metrics.storageReadBytes = customMetricSum(node, "storageReadBytes")
    metrics.storageReads = customMetricCount(node, "storageReadBytes")
    metrics.localReadBytes = customMetricSum(node, "localReadBytes")
    metrics.ramReadBytes = customMetricSum(node, "ramReadBytes")
    metrics.preloadSplits = customMetricSum(node, "readyPreloadedSplits")
    metrics.pageLoadTime = customMetricSum(node, "pageLoadTimeNs")
    metrics.dataSourceAddSplitTime = customMetricSum(node, "dataSourceAddSplitWallNanos") +
      customMetricSum(node, "waitForPreloadSplitNanos")
    metrics.dataSourceReadTime = customMetricSum(node, "dataSourceReadWallNanos")
    metrics.physicalWrittenBytes = value(node, "physicalWrittenBytes")
    metrics.writeIOTime = customMetricSum(node, "writeIOWallNanos")
    metrics.numWrittenFiles = customMetricSum(node, "numWrittenFiles")
    metrics
  }

  private def parseNativeOperatorMetrics(metrics: Metrics): JArrayList[OperatorMetrics] = {
    val operatorMetrics = new JArrayList[OperatorMetrics]()
    if (metrics.numMetrics == 0 || metrics.metricsJson == null || metrics.metricsJson.isEmpty) {
      return operatorMetrics
    }

    val root = objectMapper.readTree(metrics.metricsJson)
    val omittedNodeIds = root.path("omittedNodeIds").elements().asScala.map(_.asText()).toSet
    val nodeStats = root.path("nodeStats")
    val orderedNodeIds = root.path("orderedNodeIds")
    orderedNodeIds.elements().asScala.foreach {
      nodeIdNode =>
        val nodeId = nodeIdNode.asText()
        val node = nodeStats.get(nodeId)
        if (node == null || node.isNull || node.isMissingNode) {
          if (omittedNodeIds.contains(nodeId)) {
            operatorMetrics.add(new OperatorMetrics())
          } else {
            throw new GlutenException(s"Node id cannot be found in native metrics: $nodeId.")
          }
        } else {
          node.path("operatorStats").elements().asScala.foreach {
            opStats => operatorMetrics.add(operatorMetricFromJson(opStats))
          }
        }
    }

    val loadLazyVectorMetricsIdx = operatorMetrics.size() - 1
    if (loadLazyVectorMetricsIdx >= 0) {
      operatorMetrics.get(loadLazyVectorMetricsIdx).loadLazyVectorTime =
        root.path("loadLazyVectorTime").asLong(0L)
    }

    if (operatorMetrics.size() != metrics.numMetrics) {
      throw new GlutenException(
        s"Unexpected native metrics size. Expected ${metrics.numMetrics}, " +
          s"got ${operatorMetrics.size()}.")
    }
    operatorMetrics
  }

  /**
   * Generate the function which updates metrics fetched from certain iterator to transformers.
   *
   * @param child
   *   the child spark plan
   * @param relMap
   *   the map between operator index and its rels
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def genMetricsUpdatingFunction(
      child: SparkPlan,
      relMap: JMap[JLong, JList[JLong]],
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): IMetrics => Unit = {
    def treeifyMetricsUpdaters(plan: SparkPlan): MetricsUpdaterTree = {
      plan match {
        case j: HashJoinLikeExecTransformer =>
          MetricsUpdaterTree(
            j.metricsUpdater(),
            Seq(treeifyMetricsUpdaters(j.buildPlan), treeifyMetricsUpdaters(j.streamedPlan)))
        case smj: SortMergeJoinExecTransformer =>
          MetricsUpdaterTree(
            smj.metricsUpdater(),
            Seq(treeifyMetricsUpdaters(smj.bufferedPlan), treeifyMetricsUpdaters(smj.streamedPlan)))
        case t: TransformSupport if t.metricsUpdater() == MetricsUpdater.None =>
          assert(t.children.size == 1, "MetricsUpdater.None can only be used on unary operator")
          treeifyMetricsUpdaters(t.children.head)
        case t: TransformSupport =>
          // Reversed children order to match the traversal code.
          MetricsUpdaterTree(t.metricsUpdater(), t.children.reverse.map(treeifyMetricsUpdaters))
        case _ =>
          MetricsUpdaterTree(MetricsUpdater.Terminate, Seq())
      }
    }

    val accumulator = new TaskStatsAccumulator()
    child.session.sparkContext.register(accumulator, "velox task stats")

    val mut: MetricsUpdaterTree = treeifyMetricsUpdaters(child)

    genMetricsUpdatingFunction(
      mut,
      relMap,
      JLong.valueOf(relMap.size() - 1),
      joinParamsMap,
      aggParamsMap,
      accumulator)
  }

  /**
   * Merge several suites of metrics together.
   *
   * @param operatorMetrics
   *   : a list of metrics to merge
   * @return
   *   the merged metrics
   */
  private def mergeMetrics(operatorMetrics: JList[OperatorMetrics]): OperatorMetrics = {
    if (operatorMetrics.size() == 0) {
      return null
    }

    // We are accessing the metrics from end to start. So the input metrics are got from the
    // last suite of metrics, and the output metrics are got from the first suite.
    val inputRows = operatorMetrics.get(operatorMetrics.size() - 1).inputRows
    val inputVectors = operatorMetrics.get(operatorMetrics.size() - 1).inputVectors
    val inputBytes = operatorMetrics.get(operatorMetrics.size() - 1).inputBytes
    val rawInputRows = operatorMetrics.get(operatorMetrics.size() - 1).rawInputRows
    val rawInputBytes = operatorMetrics.get(operatorMetrics.size() - 1).rawInputBytes

    val outputRows = operatorMetrics.get(0).outputRows
    val outputVectors = operatorMetrics.get(0).outputVectors
    val outputBytes = operatorMetrics.get(0).outputBytes

    val physicalWrittenBytes = operatorMetrics.get(0).physicalWrittenBytes
    val writeIOTime = operatorMetrics.get(0).writeIOTime

    var cpuCount: Long = 0
    var wallNanos: Long = 0
    var peakMemoryBytes: Long = 0
    var numMemoryAllocations: Long = 0
    var spilledInputBytes: Long = 0
    var spilledBytes: Long = 0
    var spilledRows: Long = 0
    var spilledPartitions: Long = 0
    var spilledFiles: Long = 0
    var numDynamicFiltersProduced: Long = 0
    var numDynamicFiltersAccepted: Long = 0
    var numReplacedWithDynamicFilterRows: Long = 0
    var numDynamicFilterInputRows: Long = 0
    var flushRowCount: Long = 0
    var abandonedPartialAggregationRows: Long = 0
    var loadedToValueHook: Long = 0
    var bloomFilterBlocksByteSize: Long = 0
    var scanTime: Long = 0
    var skippedSplits: Long = 0
    var processedSplits: Long = 0
    var skippedStrides: Long = 0
    var processedStrides: Long = 0
    var remainingFilterTime: Long = 0
    var ioWaitTime: Long = 0
    var storageReadBytes: Long = 0
    var storageReads: Long = 0
    var localReadBytes: Long = 0
    var ramReadBytes: Long = 0
    var preloadSplits: Long = 0
    var pageLoadTime: Long = 0
    var dataSourceAddSplitTime: Long = 0
    var dataSourceReadTime: Long = 0
    var numWrittenFiles: Long = 0
    var loadLazyVectorTime: Long = 0

    val metricsIterator = operatorMetrics.iterator()
    while (metricsIterator.hasNext) {
      val metrics = metricsIterator.next()
      cpuCount += metrics.cpuCount
      wallNanos += metrics.wallNanos
      peakMemoryBytes = peakMemoryBytes.max(metrics.peakMemoryBytes)
      numMemoryAllocations += metrics.numMemoryAllocations
      spilledInputBytes += metrics.spilledInputBytes
      spilledBytes += metrics.spilledBytes
      spilledRows += metrics.spilledRows
      spilledPartitions += metrics.spilledPartitions
      spilledFiles += metrics.spilledFiles
      numDynamicFiltersProduced += metrics.numDynamicFiltersProduced
      numDynamicFiltersAccepted += metrics.numDynamicFiltersAccepted
      numReplacedWithDynamicFilterRows += metrics.numReplacedWithDynamicFilterRows
      numDynamicFilterInputRows += metrics.numDynamicFilterInputRows
      flushRowCount += metrics.flushRowCount
      abandonedPartialAggregationRows += metrics.abandonedPartialAggregationRows
      loadedToValueHook += metrics.loadedToValueHook
      bloomFilterBlocksByteSize += metrics.bloomFilterBlocksByteSize
      scanTime += metrics.scanTime
      skippedSplits += metrics.skippedSplits
      processedSplits += metrics.processedSplits
      skippedStrides += metrics.skippedStrides
      processedStrides += metrics.processedStrides
      remainingFilterTime += metrics.remainingFilterTime
      ioWaitTime += metrics.ioWaitTime
      storageReadBytes += metrics.storageReadBytes
      storageReads += metrics.storageReads
      localReadBytes += metrics.localReadBytes
      ramReadBytes += metrics.ramReadBytes
      preloadSplits += metrics.preloadSplits
      pageLoadTime += metrics.pageLoadTime
      dataSourceAddSplitTime += metrics.dataSourceAddSplitTime
      dataSourceReadTime += metrics.dataSourceReadTime
      numWrittenFiles += metrics.numWrittenFiles
      loadLazyVectorTime += metrics.loadLazyVectorTime
    }

    new OperatorMetrics(
      inputRows,
      inputVectors,
      inputBytes,
      rawInputRows,
      rawInputBytes,
      outputRows,
      outputVectors,
      outputBytes,
      cpuCount,
      wallNanos,
      peakMemoryBytes,
      numMemoryAllocations,
      spilledInputBytes,
      spilledBytes,
      spilledRows,
      spilledPartitions,
      spilledFiles,
      numDynamicFiltersProduced,
      numDynamicFiltersAccepted,
      numReplacedWithDynamicFilterRows,
      numDynamicFilterInputRows,
      flushRowCount,
      abandonedPartialAggregationRows,
      loadedToValueHook,
      bloomFilterBlocksByteSize,
      scanTime,
      skippedSplits,
      processedSplits,
      skippedStrides,
      processedStrides,
      remainingFilterTime,
      ioWaitTime,
      storageReadBytes,
      storageReads,
      localReadBytes,
      ramReadBytes,
      preloadSplits,
      pageLoadTime,
      dataSourceAddSplitTime,
      dataSourceReadTime,
      physicalWrittenBytes,
      writeIOTime,
      numWrittenFiles,
      loadLazyVectorTime
    )
  }

  // FIXME: Metrics updating code is too magical to maintain. Tree-walking algorithm should be made
  //  more declarative than by counting down these counters that don't have fixed definition.
  /**
   * @return
   *   operator index and metrics index
   */
  def updateTransformerMetricsInternal(
      mutNode: MetricsUpdaterTree,
      relMap: JMap[JLong, JList[JLong]],
      operatorIdx: JLong,
      nativeMetrics: JList[OperatorMetrics],
      singleMetrics: Metrics.SingleMetric,
      metricsIdx: Int,
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams]): (JLong, Int) = {
    if (mutNode.updater == MetricsUpdater.Terminate) {
      return (operatorIdx, metricsIdx)
    }
    val operatorMetrics = new JArrayList[OperatorMetrics]()
    var curMetricsIdx = metricsIdx
    relMap
      .get(operatorIdx)
      .forEach(
        _ => {
          operatorMetrics.add(nativeMetrics.get(curMetricsIdx))
          curMetricsIdx -= 1
        })

    mutNode.updater match {
      case smj: SortMergeJoinMetricsUpdater =>
        val joinParams = Option(joinParamsMap.get(operatorIdx)).getOrElse {
          val p = JoinParams()
          p.postProjectionNeeded = false
          p
        }
        smj.updateJoinMetrics(operatorMetrics, singleMetrics, joinParams)
      case ju: JoinMetricsUpdaterBase =>
        // JoinRel and CrossRel output two suites of metrics respectively for build and probe.
        // Therefore, fetch one more suite of metrics here.
        operatorMetrics.add(nativeMetrics.get(curMetricsIdx))
        curMetricsIdx -= 1
        val joinParams = Option(joinParamsMap.get(operatorIdx)).getOrElse {
          val p = JoinParams()
          p.postProjectionNeeded = false
          p
        }
        ju.updateJoinMetrics(operatorMetrics, singleMetrics, joinParams)
      case u: UnionMetricsUpdater =>
        // Union outputs two suites of metrics respectively.
        // Therefore, fetch one more suite of metrics here.
        operatorMetrics.add(nativeMetrics.get(curMetricsIdx))
        curMetricsIdx -= 1
        u.updateUnionMetrics(operatorMetrics)
      case hau: HashAggregateMetricsUpdater =>
        val aggParams = Option(aggParamsMap.get(operatorIdx)).getOrElse(AggregationParams())
        hau.updateAggregationMetrics(operatorMetrics, aggParams)
      case lu: LimitMetricsUpdater =>
        // Limit over Sort is converted to TopN node in Velox, so there is only one suite of metrics
        // for the two transformers. We do not update metrics for limit and leave it for sort.
        if (!mutNode.children.head.updater.isInstanceOf[SortMetricsUpdater]) {
          val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
          lu.updateNativeMetrics(opMetrics)
        }
      case u =>
        val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
        u.updateNativeMetrics(opMetrics)
    }

    var newOperatorIdx: JLong = operatorIdx - 1
    var newMetricsIdx: Int =
      if (
        mutNode.updater.isInstanceOf[LimitMetricsUpdater] &&
        mutNode.children.head.updater.isInstanceOf[SortMetricsUpdater]
      ) {
        // This suite of metrics is not consumed.
        metricsIdx
      } else {
        curMetricsIdx
      }

    mutNode.children.foreach {
      child =>
        val result = updateTransformerMetricsInternal(
          child,
          relMap,
          newOperatorIdx,
          nativeMetrics,
          singleMetrics,
          newMetricsIdx,
          joinParamsMap,
          aggParamsMap)
        newOperatorIdx = result._1
        newMetricsIdx = result._2
    }

    (newOperatorIdx, newMetricsIdx)
  }

  /**
   * Get a function which would update the metrics of transformers.
   *
   * @param mutNode
   *   the metrics updater tree built from the original plan
   * @param relMap
   *   the map between operator index and its rels
   * @param operatorIdx
   *   the index of operator
   * @param metricsIdx
   *   the index of metrics
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   *
   * @return
   *   A recursive function updating the metrics of operator(transformer) and its children.
   */
  def genMetricsUpdatingFunction(
      mutNode: MetricsUpdaterTree,
      relMap: JMap[JLong, JList[JLong]],
      operatorIdx: JLong,
      joinParamsMap: JMap[JLong, JoinParams],
      aggParamsMap: JMap[JLong, AggregationParams],
      taskStatsAccumulator: TaskStatsAccumulator): IMetrics => Unit = {
    imetrics =>
      val metrics = imetrics.asInstanceOf[Metrics]
      val nativeMetrics = parseNativeOperatorMetrics(metrics)
      val numNativeMetrics = nativeMetrics.size()
      if (numNativeMetrics == 0) {
        ()
      } else {
        updateTransformerMetricsInternal(
          mutNode,
          relMap,
          operatorIdx,
          nativeMetrics,
          metrics.getSingleMetrics,
          numNativeMetrics - 1,
          joinParamsMap,
          aggParamsMap)

        // Update the task stats accumulator with the metrics.
        if (metrics.taskStats != null) {
          taskStatsAccumulator.add(metrics.taskStats)
        }
      }
  }
}
