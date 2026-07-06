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
package org.apache.gluten.extension

import org.apache.gluten.execution.WholeStageTransformContext
import org.apache.gluten.substrait.rel.{InputIteratorRelNode, ReadRelNode, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{CoalescedMapperPartitionSpec, CoalescedPartitionSpec, PartialMapperPartitionSpec, PartialReducerPartitionSpec, ShufflePartitionSpec}
import org.apache.spark.sql.execution.adaptive.{BroadcastStats, InputStats, ScanInputStats, ShuffleStats}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.{mutable, Seq}

object ApplyStageInputStatsRule extends Logging {
  private def estimateInputIteratorRowCount(stats: InputStats, index: Int): Option[Long] = {
    if (stats.inputStatsKind == BroadcastStats) {
      Some(stats.rowCount.toLong)
    } else {
      val totalBytes = Math.max(stats.bytesByPartitionId.sum, 1L)
      if (index >= 0 && index < stats.bytesByPartitionId.length) {
        val partitionBytes = stats.bytesByPartitionId(index)
        Some((1.0d * stats.rowCount.toLong / totalBytes * partitionBytes).toLong)
      } else {
        logWarning(
          s"Skip invalid input stats index $index, bytesByPartitionId length: " +
            s"${stats.bytesByPartitionId.length}")
        None
      }
    }
  }

  private def estimateScanRowCount(stats: InputStats, partitionLength: Int): Long = {
    val safePartitionLength = Math.max(partitionLength, 1)
    (1.0d * stats.rowCount.toLong / safePartitionLength).toLong
  }

  def setStageInputStatsToInputNode(
      ws: WholeStageTransformContext,
      index: Int,
      partitionLength: Int): Unit = {
    val relNodesQueue: mutable.Queue[RelNode] = mutable.Queue()
    val nodes = ws.root.getRelNodes
    if (nodes != null) {
      nodes.forEach(relNodesQueue.enqueue(_))
    }
    while (relNodesQueue.nonEmpty) {
      val node = relNodesQueue.dequeue()
      node match {
        case input: InputIteratorRelNode =>
          val stats = input.getInputStats
          if (null != stats) {
            estimateInputIteratorRowCount(stats, index).foreach {
              estimatedRowCount =>
                logDebug(
                  s"pass input stats idx: $index with estimated row count $estimatedRowCount")
                input.setRowCount(estimatedRowCount)
            }
          }
        case scan: ReadRelNode =>
          val stats = scan.getInputStats
          if (null != stats) {
            val estimatedRowCount = estimateScanRowCount(stats, partitionLength)
            logDebug(s"pass scan stats idx: $index with estimated row count $estimatedRowCount")
            scan.setRowCount(estimatedRowCount)
          }
        case _ =>
      }
      node.childNodes().forEach(relNodesQueue.enqueue(_))
    }
  }

  /** Compute task stats for each partition */
  def recomputeInputStatsForAQEShuffleReadExec(
      original: InputStats,
      aqeShufflePartitions: Seq[ShufflePartitionSpec]): InputStats = {
    val originalBytesByPartition = original.bytesByPartitionId
    val totalBytes = originalBytesByPartition.sum
    val rowCount = original.rowCount
    val aqeReadPartitionsStats = new mutable.ArrayBuffer[Long]()
    aqeShufflePartitions.foreach {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        var newPartitionDataSize = 0L
        for (i <- startReducerIndex until endReducerIndex) {
          if (i < originalBytesByPartition.length) {
            newPartitionDataSize += originalBytesByPartition(i)
          } else {
            newPartitionDataSize += 0L
          }
        }
        aqeReadPartitionsStats += newPartitionDataSize
      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        // over estimated stats
        if (reducerIndex < originalBytesByPartition.length) {
          aqeReadPartitionsStats += originalBytesByPartition(reducerIndex)
        } else {
          aqeReadPartitionsStats += 0L
        }
      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        // unknown stats
        aqeReadPartitionsStats += 0L
      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        // unknown stats
        aqeReadPartitionsStats += 0L
    }
    InputStats(known = true, totalBytes, rowCount, aqeReadPartitionsStats.toArray, ShuffleStats)
  }
  def createInputStats(logicalLink: Option[LogicalPlan]): Option[InputStats] = {
    if (logicalLink.isEmpty) {
      logInfo(s"no logicalLink associated")
      return None
    }
    val logicalPlan = logicalLink.get
    logInfo("scan logical link:" + logicalPlan)
    logInfo("FileSourceScanTransformer logical plan " + logicalPlan)
    val maybeScanStats = logicalPlan.collectFirst {
      case relation: LogicalRelation => relation.catalogTable
    }.flatten.flatMap(_.stats)
    if (maybeScanStats.isDefined) {
      val scanStats = maybeScanStats.get
      logInfo("catalogTable " + scanStats)
      Some(
        InputStats(
          known = true,
          scanStats.sizeInBytes,
          scanStats.rowCount.getOrElse(0),
          Array(),
          ScanInputStats))
    } else {
      None
    }
  }
}
