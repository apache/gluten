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
package org.apache.gluten.execution

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.extension.columnar.{EliminateLocalSort, EnsureLocalSortRequirements}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Literal, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.optimizer.EliminateSorts
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{LeafExecNode, SortExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

class VeloxStreamingAggregateOrderingSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .range(0, 20000)
      .selectExpr(
        "CASE WHEN id % 17 = 0 THEN NULL ELSE id % 1000 END AS k",
        "id % 7 AS k2",
        "id AS v")
      .write
      .saveAsTable("agg_t1")
    spark
      .range(0, 5000)
      .selectExpr("CASE WHEN id % 19 = 0 THEN NULL ELSE id % 1000 END AS k", "id AS w")
      .write
      .saveAsTable("agg_t2")
    spark.range(0, 3000).selectExpr("id % 1000 AS k", "id AS u").write.saveAsTable("agg_t3")
  }

  override def afterAll(): Unit = {
    Seq("agg_t1", "agg_t2", "agg_t3").foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))
    super.afterAll()
  }

  private val reportOrderingKey = GlutenConfig.COLUMNAR_STREAMING_AGGREGATE_REPORT_ORDERING.key

  // Tiny batches make groups span batch boundaries in the streaming aggregate.
  private def withOrderingConf(reportOrdering: Boolean, adaptive: Boolean)(f: => Unit): Unit =
    withSQLConf(
      reportOrderingKey -> reportOrdering.toString,
      GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE.key -> "true",
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "7",
      SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "3",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString
    )(f)

  private def aggregates(df: DataFrame): Seq[HashAggregateExecBaseTransformer] =
    collect(df.queryExecution.executedPlan) { case agg: HashAggregateExecBaseTransformer => agg }

  private def isStreaming(agg: HashAggregateExecBaseTransformer): Boolean =
    agg.simpleString(Int.MaxValue).contains("isStreamingAgg=true")

  // Sorts placed on top of an aggregate, possibly through projects.
  private def sortsOnAggregates(df: DataFrame): Seq[SortExecTransformer] =
    collect(df.queryExecution.executedPlan) {
      case sort: SortExecTransformer if onAggregate(sort.child) => sort
    }

  private def onAggregate(plan: SparkPlan): Boolean = plan match {
    case _: HashAggregateExecBaseTransformer => true
    case project: ProjectExecTransformer => onAggregate(project.child)
    case _ => false
  }

  private val joinOnAggregate =
    """
      |SELECT a.k, a.cnt, a.total, t3.u FROM (
      |  SELECT t1.k, count(*) AS cnt, sum(t2.w) AS total
      |  FROM agg_t1 t1 JOIN agg_t2 t2 ON t1.k = t2.k
      |  GROUP BY t1.k
      |) a JOIN agg_t3 t3 ON a.k = t3.k
      |""".stripMargin

  test("sort merge join on the keys of a streaming aggregate adds no sort") {
    for (adaptive <- Seq(false, true)) {
      withOrderingConf(reportOrdering = true, adaptive) {
        runQueryAndCompare(joinOnAggregate) {
          df =>
            assert(aggregates(df).nonEmpty)
            assert(aggregates(df).forall(isStreaming), df.queryExecution.executedPlan)
            assert(sortsOnAggregates(df).isEmpty, df.queryExecution.executedPlan)
        }
      }
    }
  }

  test("sort merge join on a streaming aggregate keeps the sort when reporting is disabled") {
    for (adaptive <- Seq(false, true)) {
      withOrderingConf(reportOrdering = false, adaptive) {
        runQueryAndCompare(joinOnAggregate) {
          df => assert(sortsOnAggregates(df).nonEmpty, df.queryExecution.executedPlan)
        }
      }
    }
  }

  private val aggregateOnKeyPrefix =
    """
      |SELECT k, count(*) AS groups, sum(s) AS total FROM (
      |  SELECT k, k2, sum(rn) AS s FROM (
      |    SELECT k, k2, row_number() OVER (PARTITION BY k ORDER BY k2, v) AS rn FROM agg_t1
      |  ) GROUP BY k, k2
      |) GROUP BY k
      |""".stripMargin

  test("aggregate on a key prefix of a streaming aggregate also streams") {
    for (reportOrdering <- Seq(false, true); adaptive <- Seq(false, true)) {
      withOrderingConf(reportOrdering, adaptive) {
        runQueryAndCompare(aggregateOnKeyPrefix) {
          df =>
            val outer = aggregates(df).filter(_.groupingExpressions.size == 1)
            assert(outer.nonEmpty, df.queryExecution.executedPlan)
            assert(outer.forall(isStreaming) == reportOrdering, df.queryExecution.executedPlan)
        }
      }
    }
  }

  test("sort merge join keeps its sort when EliminateLocalSort removes the aggregate input sort") {
    val query =
      """
        |SELECT a.k, a.cnt, t3.u FROM (
        |  SELECT k, count(*) AS cnt FROM (SELECT * FROM agg_t1 DISTRIBUTE BY k SORT BY k)
        |  GROUP BY k
        |) a JOIN agg_t3 t3 ON a.k = t3.k
        |""".stripMargin
    for (adaptive <- Seq(false, true)) {
      withOrderingConf(reportOrdering = true, adaptive) {
        // Keeps SORT BY under the aggregate, so vanilla Spark plans a SortAggregate on it.
        withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> EliminateSorts.ruleName) {
          runQueryAndCompare(query) {
            df =>
              val plan = df.queryExecution.executedPlan
              assert(collect(plan) { case agg: SortAggregateExecTransformer => agg }.nonEmpty, plan)
              assert(sortsOnAggregates(df).nonEmpty, plan)
          }
        }
      }
    }
  }

  test("sort merge join on a distinct aggregate with flushable partial aggregation") {
    val query =
      """
        |SELECT a.k, a.d, a.total, t3.u FROM (
        |  SELECT k, count(DISTINCT k2) AS d, sum(v) AS total FROM agg_t1 GROUP BY k
        |) a JOIN agg_t3 t3 ON a.k = t3.k
        |""".stripMargin
    for (adaptive <- Seq(false, true)) {
      withOrderingConf(reportOrdering = true, adaptive) {
        withSQLConf(VeloxConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key -> "true") {
          runQueryAndCompare(query) {
            df =>
              val plan = df.queryExecution.executedPlan
              val flushable = aggregates(df).collect {
                case agg: FlushableHashAggregateExecTransformer => agg
              }
              assert(flushable.nonEmpty, plan)
              assert(flushable.forall(_.outputOrdering.isEmpty), plan)
          }
        }
      }
    }
  }

  private val key = AttributeReference("key", LongType)()
  private val otherKey = AttributeReference("other_key", LongType)()
  private val keyOrdering = Seq(SortOrder(key, Ascending))

  private case class StubLeaf(output: Seq[Attribute], override val outputOrdering: Seq[SortOrder])
    extends LeafExecNode {
    override protected def doExecute() = throw new UnsupportedOperationException
  }

  private val count = AggregateExpression(Count(Literal(1)), Complete, isDistinct = false)
  private val countAttribute = AttributeReference("cnt", LongType)()

  private def regularAggregateOver(child: SparkPlan): HashAggregateExecTransformer =
    RegularHashAggregateExecTransformer(
      Some(Seq(key)),
      Seq(key),
      Seq(count),
      Seq(countAttribute),
      0,
      Seq(key, countAttribute),
      child)

  private def sortAggregateOver(child: SparkPlan): HashAggregateExecTransformer =
    SortHashAggregateExecTransformer(
      Some(Seq(key)),
      Seq(key),
      Seq(count),
      Seq(countAttribute),
      0,
      Seq(key, countAttribute),
      child)

  private def flushableAggregateOver(child: SparkPlan): HashAggregateExecTransformer =
    FlushableHashAggregateExecTransformer(
      Some(Seq(key)),
      Seq(key),
      Seq(count),
      Seq(countAttribute),
      0,
      Seq(key, countAttribute),
      child)

  private def reportsKeyOrdering(plan: SparkPlan): Boolean =
    plan.outputOrdering.nonEmpty && SortOrder.orderingSatisfies(plan.outputOrdering, keyOrdering)

  test("only streaming aggregates that keep the input order report an ordering") {
    val sorted = StubLeaf(Seq(key), keyOrdering)
    val unsorted = StubLeaf(Seq(key), Nil)
    withSQLConf(reportOrderingKey -> "true") {
      assert(reportsKeyOrdering(regularAggregateOver(sorted)))
      assert(reportsKeyOrdering(sortAggregateOver(sorted)))
      // A flushable aggregate may emit pre-aggregated rows early.
      assert(flushableAggregateOver(sorted).outputOrdering.isEmpty)
      // A hash aggregate over unsorted input emits groups in no particular order.
      assert(regularAggregateOver(unsorted).outputOrdering.isEmpty)
    }
    withSQLConf(reportOrderingKey -> "false") {
      assert(regularAggregateOver(sorted).outputOrdering.isEmpty)
    }
  }

  // A sort merge join on `key` whose right side is already sorted.
  private def joinOn(left: SparkPlan): SortMergeJoinExecTransformer = {
    val right = StubLeaf(Seq(otherKey), Seq(SortOrder(otherKey, Ascending)))
    SortMergeJoinExecTransformer(Seq(key), Seq(otherKey), Inner, None, left, right)
  }

  private def isSort(plan: SparkPlan): Boolean =
    plan.isInstanceOf[SortExec] || plan.isInstanceOf[SortExecTransformer]

  test("EliminateLocalSort before EnsureLocalSortRequirements keeps the sort a join needs") {
    withSQLConf(reportOrderingKey -> "true") {
      val localSort = SortExecTransformer(keyOrdering, global = false, StubLeaf(Seq(key), Nil))
      Seq(
        sortAggregateOver(localSort),
        sortAggregateOver(ProjectExecTransformer(Seq(key), localSort)),
        sortAggregateOver(sortAggregateOver(localSort))
      ).foreach {
        aggregate =>
          // The aggregate streams over the local sort, which EliminateLocalSort removes.
          assert(reportsKeyOrdering(aggregate))
          val planned =
            EliminateLocalSort(EnsureLocalSortRequirements(EliminateLocalSort(joinOn(aggregate))))
          assert(isSort(planned.children.head), planned)
          assert(planned.children.head.collect { case sort if isSort(sort) => sort }.size == 1)
          // In the opposite order the join would read the unsorted aggregate output.
          val reversed = EliminateLocalSort(EnsureLocalSortRequirements(joinOn(aggregate)))
          assert(reversed.children.head.find(isSort).isEmpty, reversed)
      }
    }
  }
}
