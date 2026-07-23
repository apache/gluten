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

import org.apache.gluten.extension.columnar.rewrite.RewriteJoin
import org.apache.gluten.extension.columnar.util.ShuffleSkewDetector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * QueryStagePrepRule that fires after each AQE shuffle stage completes, i.e. once `mapStats` are
 * available.
 *
 * For a LeftSemi join (either [[SortMergeJoinExec]] or already-rewritten [[ShuffledHashJoinExec]])
 * it decides whether BuildLeft is safe and, if so, sets [[RewriteJoin.ForceShjBuildLeftTag]] on the
 * join node. The tag is later consumed by:
 *   - [[RewriteJoin.getSmjBuildSide]] (SMJ path).
 *   - [[org.apache.gluten.extension.columnar.offload.OffloadJoin.getShjBuildSide]] (SHJ path).
 *
 * AQE `OptimizeSkewedJoin` cannot split the probe side of a ShuffledHashJoin, so a skewed probe
 * would materialize as extreme straggler tasks. When the right (would-be probe) side is skewed,
 * this rule refuses to tag the join for BuildLeft so that Velox builds the (skewed) side per
 * partition.
 *
 * Skew thresholds reuse Spark's `spark.sql.adaptive.skewJoin.*` so we don't fight AQE's own
 * definition of skew.
 */
case class LeftSemiBuildLeftGuardRule(session: SparkSession)
  extends Rule[SparkPlan]
  with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    val skewJudgement = buildSkewJudgement()
    plan.foreachUp {
      case smj: SortMergeJoinExec if smj.joinType == LeftSemi =>
        maybeTag(smj, smj.right, "SMJ", skewJudgement)
      case shj: ShuffledHashJoinExec if shj.joinType == LeftSemi =>
        maybeTag(shj, shj.right, "SHJ", skewJudgement)
      case _ =>
    }
    plan
  }

  private def maybeTag(
      join: SparkPlan,
      rightSide: SparkPlan,
      kind: String,
      skewJudgement: ShuffleSkewDetector.SkewJudgement): Unit = {
    if (join.getTagValue(RewriteJoin.ForceShjBuildLeftTag).getOrElse(false)) {
      return
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
