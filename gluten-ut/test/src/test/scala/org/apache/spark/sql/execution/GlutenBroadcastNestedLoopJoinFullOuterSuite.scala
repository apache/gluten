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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{BroadcastNestedLoopJoinExecTransformer, SortMergeJoinExecTransformer}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, GlutenQueryTest, Row}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

import scala.reflect.ClassTag

/**
 * Tests for the Velox full outer `BroadcastNestedLoopJoinExec` rewrite.
 *
 * The full outer BNLJ rewrite is a Velox backend feature, hence each test is guarded with
 * `assumeVeloxBackend()` so the ClickHouse backend skips them.
 */
class GlutenBroadcastNestedLoopJoinFullOuterSuite
  extends GlutenQueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  // Disable the forced shuffled hash join rewrite so explicit join hints retain their semantics.
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")
  }

  private def assumeVeloxBackend(): Unit = assume(BackendTestUtils.isVeloxBackendLoaded())

  private def materializePlan(df: Dataset[_]): SparkPlan = {
    val materializedDf = df.toDF()
    val executedPlan = materializedDf.queryExecution.executedPlan
    executedPlan.execute()
    stripAQEPlan(executedPlan match {
      case adaptivePlan: AdaptiveSparkPlanExec => adaptivePlan.executedPlan
      case otherPlan => otherPlan
    })
  }

  private def assertPlanCount[T <: SparkPlan: ClassTag](
      df: Dataset[_],
      expectedCount: Int): Unit = {
    val targetClass = implicitly[ClassTag[T]].runtimeClass
    val plan = materializePlan(df)
    val matchedNodes = plan.collect {
      case node if targetClass.isInstance(node) => node
    }
    assert(
      matchedNodes.size === expectedCount,
      s"Expected $expectedCount ${targetClass.getSimpleName} node(s), but found " +
        s"${matchedNodes.size}:\n" + plan.treeString
    )
  }

  private def assertNoSparkFullOuterBNLJ(df: Dataset[_]): SparkPlan = {
    val plan = materializePlan(df)
    val rawFullOuterBnljs = plan.collect {
      case bnlj: BroadcastNestedLoopJoinExec if bnlj.joinType == FullOuter => bnlj
    }
    assert(
      rawFullOuterBnljs.isEmpty,
      s"Expected rewritten/supported final plan without raw Spark FullOuter " +
        s"BroadcastNestedLoopJoinExec, but found ${rawFullOuterBnljs.size}:\n" +
        plan.treeString
    )
    plan
  }

  private def assertSupportedFullOuterPlan(df: Dataset[_]): Unit = {
    val plan = assertNoSparkFullOuterBNLJ(df)
    val nativeBnljCount = plan.collect { case _: BroadcastNestedLoopJoinExecTransformer => 1 }.size
    val nativeSmjCount = plan.collect { case _: SortMergeJoinExecTransformer => 1 }.size
    assert(
      nativeBnljCount + nativeSmjCount > 0,
      s"Expected a supported native full outer plan after rewrite/planning, but found neither " +
        s"${classOf[BroadcastNestedLoopJoinExecTransformer].getSimpleName} nor " +
        s"${classOf[SortMergeJoinExecTransformer].getSimpleName}:\n" +
        plan.treeString
    )
  }

  private def assertNativeExistenceJoin(df: Dataset[_]): Unit = {
    val plan = materializePlan(df)
    val existenceJoinCount = plan.collect {
      case bnlj: BroadcastNestedLoopJoinExecTransformer =>
        bnlj.joinType match {
          case ExistenceJoin(_) => 1
          case _ => 0
        }
    }.sum
    assert(
      existenceJoinCount === 1,
      s"Expected exactly one native ExistenceJoin in the rewritten plan, but found " +
        s"$existenceJoinCount:\n${plan.treeString}"
    )
  }

  test("Full outer BroadcastNestedLoopJoinExec should be rewritten into supported stages") {
    assumeVeloxBackend()
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))

    Seq(true, false).foreach {
      aqeEnabled =>
        Seq(true, false).foreach {
          codegenEnabled =>
            withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString,
              SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString,
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
              SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
              SQLConf.ANSI_ENABLED.key -> "false"
            ) {
              val fullOuterJoin = df1.hint("broadcast").join(df2, $"k1" < $"k2", "full_outer")
              assertNoSparkFullOuterBNLJ(fullOuterJoin)
              assertPlanCount[BroadcastNestedLoopJoinExecTransformer](
                fullOuterJoin,
                expectedCount = 2)
              assertNativeExistenceJoin(fullOuterJoin)
              checkAnswer(
                fullOuterJoin,
                Seq(
                  Row(0, 1),
                  Row(0, 2),
                  Row(1, 2),
                  Row(2, null),
                  Row(3, null),
                  Row(null, 0)))
            }
        }
    }
  }

  test(
    "Full outer BNLJ rewrite should use existence join for high-cardinality matches") {
    assumeVeloxBackend()
    val left = (Seq.fill(100)(1) :+ 2).toDF("k1")
    val right = (Seq.fill(100)(1) :+ 0).toDF("k2")

    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false"
    ) {
      val fullOuterJoin = left.join(right.hint("broadcast"), $"k1" <= $"k2", "full_outer")
      assertNoSparkFullOuterBNLJ(fullOuterJoin)
      assertNativeExistenceJoin(fullOuterJoin)
      assert(fullOuterJoin.count() === 10002)
    }
  }

  test("Full outer BNLJ rewrite should be disabled by a negative threshold") {
    assumeVeloxBackend()
    val left = spark.range(4).select($"id".as("k1"))
    val right = spark.range(3).select($"id".as("k2"))

    withSQLConf(
      VeloxConfig.VELOX_BROADCAST_NESTED_LOOP_JOIN_FULL_OUTER_REWRITE_THRESHOLD.key -> "-1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString
    ) {
      val fullOuterJoin = left.hint("broadcast").join(right, $"k1" < $"k2", "full_outer")
      val plan = materializePlan(fullOuterJoin)
      assert(
        plan.exists {
          case bnlj: BroadcastNestedLoopJoinExec if bnlj.joinType == FullOuter => true
          case _ => false
        },
        s"Expected the original full outer BNLJ when the rewrite is disabled:\n${plan.treeString}"
      )
    }
  }

  test(
    "Full outer BroadcastNestedLoopJoin rewrite should preserve null semantics for equals") {
    assumeVeloxBackend()
    val df1 = Seq[java.lang.Integer](null, 1, 2, null).toDF("k1")
    val df2 = Seq[java.lang.Integer](null, 1, 3, null).toDF("k2")

    Seq(true, false).foreach {
      codegenEnabled =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val fullOuterJoin = df1.hint("broadcast").join(df2, $"k1" === $"k2", "full_outer")
          assertSupportedFullOuterPlan(fullOuterJoin)
          checkAnswer(
            fullOuterJoin,
            Seq(
              Row(null, null),
              Row(null, null),
              Row(null, null),
              Row(null, null),
              Row(1, 1),
              Row(2, null),
              Row(null, 3)))
        }
    }
  }

  test(
    "Full outer BNLJ rewrite should preserve null semantics for null-safe equals") {
    assumeVeloxBackend()
    val df1 = Seq[java.lang.Integer](null, 1, 2, null).toDF("k1")
    val df2 = Seq[java.lang.Integer](null, 1, 3, null).toDF("k2")

    Seq(true, false).foreach {
      codegenEnabled =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val fullOuterJoin = df1.hint("broadcast").join(df2, $"k1" <=> $"k2", "full_outer")
          assertSupportedFullOuterPlan(fullOuterJoin)
          checkAnswer(
            fullOuterJoin,
            Seq(
              Row(null, null),
              Row(null, null),
              Row(null, null),
              Row(null, null),
              Row(1, 1),
              Row(2, null),
              Row(null, 3)))
        }
    }
  }
}
