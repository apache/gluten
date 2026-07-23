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
package org.apache.gluten.extension.columnar.util

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec

object ShuffleSkewDetector {

  case class SkewJudgement(
      factor: Double,
      partitionThresholdBytes: Long,
      minTotalBytes: Long)

  def totalBytes(side: SparkPlan): Option[Long] = {
    val stages = side.collect { case s: ShuffleQueryStageExec => s }
    if (stages.isEmpty) return None
    var total = 0L
    var found = false
    stages.foreach {
      stage =>
        if (stage.isMaterialized) {
          stage.mapStats.foreach {
            ms =>
              total += ms.bytesByPartitionId.foldLeft(0L)(_ + _)
              found = true
          }
        }
    }
    if (found) Some(total) else None
  }

  def analyze(side: SparkPlan, judgement: SkewJudgement): Result = {
    val stages = side.collect { case s: ShuffleQueryStageExec => s }
    if (stages.isEmpty) {
      return Result(hasStage = false, materialized = false, isSkewed = false)
    }
    val materializedWithStats = stages.flatMap {
      stage => if (stage.isMaterialized) stage.mapStats.map(ms => ms.bytesByPartitionId) else None
    }
    if (materializedWithStats.isEmpty) {
      val anyMaterialized = stages.exists(_.isMaterialized)
      return Result(hasStage = true, materialized = anyMaterialized, isSkewed = false)
    }
    val bytes = materializedWithStats.maxBy(_.foldLeft(0L)(_ + _))
    if (bytes.isEmpty) {
      return Result(hasStage = true, materialized = true, isSkewed = false)
    }
    val nonEmpty = bytes.filter(_ > 0)
    val total = bytes.foldLeft(0L)(_ + _)
    if (nonEmpty.isEmpty) {
      return Result(
        hasStage = true,
        materialized = true,
        isSkewed = false,
        totalBytes = total,
        totalPartitions = bytes.length)
    }
    val sorted = nonEmpty.sorted
    val median = sorted(sorted.length / 2)
    val max = bytes.max

    val bigEnoughTotal = total >= judgement.minTotalBytes
    val partitionSkewed = median > 0 &&
      max.toDouble > judgement.factor * median &&
      max > judgement.partitionThresholdBytes

    Result(
      hasStage = true,
      materialized = true,
      isSkewed = bigEnoughTotal && partitionSkewed,
      totalBytes = total,
      nonEmptyPartitions = nonEmpty.length,
      totalPartitions = bytes.length,
      medianBytes = median,
      maxBytes = max
    )
  }

  case class Result(
      hasStage: Boolean,
      materialized: Boolean,
      isSkewed: Boolean,
      totalBytes: Long = 0L,
      nonEmptyPartitions: Int = 0,
      totalPartitions: Int = 0,
      medianBytes: Long = 0L,
      maxBytes: Long = 0L) {

    def skewRatio: Double =
      if (medianBytes > 0) maxBytes.toDouble / medianBytes.toDouble else Double.NaN
  }
}
