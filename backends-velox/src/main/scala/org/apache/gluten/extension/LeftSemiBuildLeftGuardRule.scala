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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar.rewrite.RewriteJoin
import org.apache.gluten.extension.columnar.util.ShuffleSkewDetector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

case class LeftSemiBuildLeftGuardRule(session: SparkSession)
  extends Rule[SparkPlan]
  with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    val glutenConf = GlutenConfig.get
    if (!glutenConf.shjLeftSemiBuildLeftEnabled) {
      return plan
    }
    val skewJudgement = buildSkewJudgement()
    val minRatio = glutenConf.shjLeftSemiBuildLeftMinRightToLeftRatio
    val minRightBytes = glutenConf.shjLeftSemiBuildLeftMinRightBytes
    plan.foreachUp {
      case smj: SortMergeJoinExec if smj.joinType == LeftSemi =>
        maybeTag(smj, smj.left, smj.right, "SMJ", skewJudgement, minRatio, minRightBytes)
      case shj: ShuffledHashJoinExec if shj.joinType == LeftSemi =>
        maybeTag(shj, shj.left, shj.right, "SHJ", skewJudgement, minRatio, minRightBytes)
      case _ =>
    }
    plan
  }

  private def maybeTag(
      join: SparkPlan,
      leftSide: SparkPlan,
      rightSide: SparkPlan,
      kind: String,
      skewJudgement: ShuffleSkewDetector.SkewJudgement,
      minRatio: Double,
      minRightBytes: Long): Unit = {
    if (join.getTagValue(RewriteJoin.ForceShjBuildLeftTag).getOrElse(false)) {
      return
    }

    val rightTotalBytes = ShuffleSkewDetector.totalBytes(rightSide) match {
      case Some(bytes) => bytes
      case None => return
    }
    if (rightTotalBytes < minRightBytes) {
      logDebug(
        s"LeftSemiBuildLeftGuardRule: right-side too small on LeftSemi $kind " +
          s"(rightBytes=$rightTotalBytes < minRightBytes=$minRightBytes); skipping BuildLeft.")
      return
    }

    val leftTotalBytes = ShuffleSkewDetector.totalBytes(leftSide) match {
      case Some(bytes) => bytes
      case None => return
    }
    if (leftTotalBytes > 0) {
      val ratio = rightTotalBytes.toDouble / leftTotalBytes.toDouble
      if (ratio < minRatio) {
        logDebug(
          f"LeftSemiBuildLeftGuardRule: insufficient right/left ratio on LeftSemi $kind " +
            f"(rightBytes=$rightTotalBytes / leftBytes=$leftTotalBytes " +
            f"= $ratio%.1f < minRatio=$minRatio%.1f); skipping BuildLeft.")
        return
      }
    }

    val rightStats = ShuffleSkewDetector.analyze(rightSide, skewJudgement)
    if (rightStats.isSkewed) {
      logDebug(
        s"LeftSemiBuildLeftGuardRule: right-side partition skew on LeftSemi $kind " +
          s"(totalBytes=${rightStats.totalBytes}, max=${rightStats.maxBytes}, " +
          s"median=${rightStats.medianBytes}); skipping BuildLeft.")
      return
    }

    join.setTagValue(RewriteJoin.ForceShjBuildLeftTag, true)
  }

  private def buildSkewJudgement(): ShuffleSkewDetector.SkewJudgement = {
    val sqlConf = SQLConf.get
    ShuffleSkewDetector.SkewJudgement(
      factor = sqlConf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR),
      partitionThresholdBytes = sqlConf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD),
      minTotalBytes = 0L
    )
  }
}
