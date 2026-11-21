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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.velox.VeloxSparkPlanExecApi
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.SortMergeJoinExecTransformerBase

import org.apache.spark.sql.GlutenQueryTest
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualTo, SortOrder}
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LeafNode, LocalRelation, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, LongType}

class SortMergeJoinTranslationSuite extends GlutenQueryTest with SharedSparkSession {
  private val optimizeKey = VeloxConfig.SORT_MERGE_JOIN_OPTIMIZE_STREAMED_SIDE.key

  private case class SizedRelation(
      override val output: Seq[Attribute],
      bytes: BigInt,
      rows: Option[BigInt] = None)
    extends LeafNode {
    override def computeStats(): Statistics = Statistics(sizeInBytes = bytes, rowCount = rows)
  }

  private def localScan(attribute: Attribute): SparkPlan =
    spark.sessionState.planner.plan(LocalRelation(Seq(attribute))).next()

  private def mergeJoin(
      leftBytes: BigInt = 16,
      rightBytes: BigInt = 160,
      joinType: JoinType = Inner,
      skew: Boolean = false,
      leftRows: Option[BigInt] = None,
      rightRows: Option[BigInt] = None): SortMergeJoinExec = {
    val l = AttributeReference("key", LongType, nullable = false)()
    val r = AttributeReference("key", LongType, nullable = false)()
    val left = SizedRelation(Seq(l), leftBytes, leftRows)
    val right = SizedRelation(Seq(r), rightBytes, rightRows)
    val condition = Some(EqualTo(l, r))
    val logical = Join(left, right, joinType, condition, JoinHint.NONE)
    val smj = SortMergeJoinExec(
      Seq(l),
      Seq(r),
      joinType,
      condition,
      SortExec(
        Seq(SortOrder(l, Ascending)),
        global = false,
        ShuffleExchangeExec(HashPartitioning(Seq(l), 2), localScan(l))),
      SortExec(
        Seq(SortOrder(r, Ascending)),
        global = false,
        ShuffleExchangeExec(HashPartitioning(Seq(r), 2), localScan(r))),
      skew
    )
    smj.setLogicalLink(logical)
    smj
  }

  private def withOptimization(f: => Unit): Unit = {
    withSQLConf(
      optimizeKey -> "true",
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false")(f)
  }

  private def convert(smj: SortMergeJoinExec): SortMergeJoinExecTransformerBase =
    new VeloxSparkPlanExecApi().genSortMergeJoinExecTransformer(smj)

  private def assertOriginalLayout(
      result: SortMergeJoinExecTransformerBase,
      smj: SortMergeJoinExec): Unit = {
    assert(result.left eq smj.left)
    assert(result.right eq smj.right)
    assert(result.leftKeys == smj.leftKeys)
    assert(result.rightKeys == smj.rightKeys)
    assert(result.joinType == smj.joinType)
    assert(result.condition == smj.condition)
    assert(result.output == smj.output)
    assert(result.requiredChildOrdering == smj.requiredChildOrdering)
    assert(result.outputPartitioning == smj.outputPartitioning)
    assert(SortOrder.orderingSatisfies(result.outputOrdering, smj.outputOrdering))
  }

  test("merge join streamed side optimization is disabled by default") {
    assert(!VeloxConfig.get.optimizeSortMergeJoinStreamedSide)
    val smj = mergeJoin()
    assert(convert(smj).streamedPlan eq smj.left)
  }

  test("swap native inputs while preserving the Spark plan layout") {
    withOptimization {
      val smj = mergeJoin()
      val result = convert(smj)
      assertOriginalLayout(result, smj)
      assert(result.streamedPlan eq smj.right)
      assert(result.bufferedPlan eq smj.left)
      assert(result.streamedKeys == smj.rightKeys)
      assert(result.bufferedKeys == smj.leftKeys)
      val copied = result.withNewChildren(result.children)
        .asInstanceOf[SortMergeJoinExecTransformerBase]
      assert(copied.streamedPlan eq smj.right)
    }
  }

  test("swap inner-like equijoins but preserve joins without equality keys") {
    withOptimization {
      val smj = mergeJoin(joinType = Cross)
      assertOriginalLayout(convert(smj), smj)
      assert(convert(smj).streamedPlan eq smj.right)
      val withoutKeys = smj.copy(leftKeys = Nil, rightKeys = Nil)
      withoutKeys.setLogicalLink(smj.logicalLink.get)
      assert(convert(withoutKeys).streamedPlan eq smj.left)
    }
  }

  test("preserve a larger left input and ties without row counts") {
    withOptimization {
      Seq(mergeJoin(160, 16), mergeJoin(16, 16)).foreach {
        smj => assert(convert(smj).streamedPlan eq smj.left)
      }
    }
  }

  test("reuse hash join row count tie breaking") {
    withOptimization {
      val smj = mergeJoin(160, 160, leftRows = Some(BigInt(2)), rightRows = Some(BigInt(20)))
      assert(convert(smj).streamedPlan eq smj.right)
    }
  }

  test("preserve joins with missing or unknown statistics") {
    withOptimization {
      val noLink = mergeJoin()
      noLink.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
      val unknown = spark.sessionState.conf.defaultSizeInBytes
      Seq(noLink, mergeJoin(unknown, 160), mergeJoin(16, unknown), mergeJoin(-1, 160))
        .foreach(smj => assert(convert(smj).streamedPlan eq smj.left))
    }
  }

  Seq(LeftOuter, RightOuter, FullOuter).foreach {
    joinType =>
      test(s"swap native $joinType inputs while preserving Spark output properties") {
        withOptimization {
          val smj = mergeJoin(joinType = joinType)
          val result = convert(smj)
          assertOriginalLayout(result, smj)
          assert(result.output.map(_.nullable) == smj.output.map(_.nullable))
          assert(result.streamedPlan eq smj.right)
          assert(result.bufferedPlan eq smj.left)
          val largerLeft = mergeJoin(160, 16, joinType = joinType)
          assert(convert(largerLeft).streamedPlan eq largerLeft.left)
        }
      }
  }

  test("preserve semi, anti and existence join orientations") {
    withOptimization {
      Seq(LeftSemi, LeftAnti, ExistenceJoin(AttributeReference("exists", BooleanType)())).foreach {
        joinType =>
          val smj = mergeJoin(joinType = joinType)
          assert(convert(smj).streamedPlan eq smj.left)
      }
    }
  }

  test("preserve skew joins") {
    withOptimization {
      val smj = mergeJoin(skew = true)
      assert(convert(smj).streamedPlan eq smj.left)
    }
  }

  test("preserve the forced shuffled hash join path") {
    withOptimization {
      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "true") {
        val smj = mergeJoin()
        assert(convert(smj).streamedPlan eq smj.left)
      }
    }
  }
}
