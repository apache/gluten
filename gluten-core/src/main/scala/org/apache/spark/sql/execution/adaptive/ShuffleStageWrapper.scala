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
package org.apache.spark.sql.execution.adaptive

import org.apache.spark.MapOutputStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS

/**
 * Represents stage input stats, eg scan or shuffle
 *
 * @param known
 * @param sizeInBytes
 * @param rowCount
 * @param bytesByPartitionId
 */
sealed trait InputStatsKind
case object ScanInputStats extends InputStatsKind
case object BroadcastStats extends InputStatsKind
case object ShuffleStats extends InputStatsKind
case object UnknownStats extends InputStatsKind
case class InputStats(
    known: Boolean,
    sizeInBytes: BigInt,
    rowCount: BigInt,
    bytesByPartitionId: Array[Long],
    inputStatsKind: InputStatsKind)
  extends Serializable {
  override def toString: String = {
    s"known : ($known) sizeInBytes:($sizeInBytes), rowCount: ($rowCount), " +
      s"bytesByPartitionId:  [${bytesByPartitionId.mkString(",")}], statsKind: $inputStatsKind"
  }
}
object ShuffleStageWrapper extends Logging {
  val INPUT_STATS: TreeNodeTag[java.util.List[InputStats]] = TreeNodeTag("InputStats")
  def unapply(plan: SparkPlan): Option[ShuffleQueryStageRuntimeStats] = plan match {
    case s: ShuffleQueryStageExec
        if s.isMaterialized && s.mapStats.isDefined &&
          s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
      logInfo("hit InputIteratorTransformer ShuffleQueryStageExec with stats")
      Some(
        ShuffleQueryStageRuntimeStats(
          s.getRuntimeStatistics.sizeInBytes,
          s.getRuntimeStatistics.rowCount,
          s.mapStats))
    case a: AQEShuffleReadExec if a.child.isInstanceOf[ShuffleQueryStageExec] =>
      logInfo("hit InputIteratorTransformer AQEShuffleReadExec with stats")
      val queryStageExec = a.child.asInstanceOf[ShuffleQueryStageExec]
      Some(
        ShuffleQueryStageRuntimeStats(
          queryStageExec.getRuntimeStatistics.sizeInBytes,
          queryStageExec.getRuntimeStatistics.rowCount,
          queryStageExec.mapStats))
    case b: BroadcastQueryStageExec if b.isMaterialized =>
      logInfo("hit InputIteratorTransformer BroadcastQueryStageExec with stats")
      Some(
        ShuffleQueryStageRuntimeStats(
          b.getRuntimeStatistics.sizeInBytes,
          b.getRuntimeStatistics.rowCount,
          None))
    case _ => None
  }
}
case class ShuffleQueryStageRuntimeStats(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    mapOutputStatistics: Option[MapOutputStatistics] = None)
