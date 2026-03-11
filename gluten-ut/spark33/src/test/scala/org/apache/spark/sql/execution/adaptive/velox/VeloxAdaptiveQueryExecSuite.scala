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
package org.apache.spark.sql.execution.adaptive.velox

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BroadcastHashJoinExecTransformerBase, ColumnarToCarrierRowExecBase, ShuffledHashJoinExecTransformerBase, SortExecTransformer, SortMergeJoinExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{Dataset, GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.noop.NoopDataSource
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.logging.log4j.Level

class VeloxAdaptiveQueryExecSuite extends AdaptiveQueryExecSuite with GlutenSQLTestsTrait {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
  }

  private def runAdaptiveAndVerifyResult(query: String): (SparkPlan, SparkPlan) = {
    var finalPlanCnt = 0
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
            if (sparkPlanInfo.simpleString.startsWith("AdaptiveSparkPlan isFinalPlan=true")) {
              finalPlanCnt += 1
            }
          case _ => // ignore other events
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    val dfAdaptive = sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, result)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    spark.sparkContext.listenerBus.waitUntilEmpty()
    // AQE will post `SparkListenerSQLAdaptiveExecutionUpdate` twice in case of subqueries that
    // exist out of query stages.
    val expectedFinalPlanCnt = adaptivePlan.find(_.subqueries.nonEmpty).map(_ => 2).getOrElse(1)
    assert(finalPlanCnt == expectedFinalPlanCnt)
    spark.sparkContext.removeSparkListener(listener)

    val exchanges = adaptivePlan.collect { case e: Exchange => e }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def broadcastHashJoinSize(plan: SparkPlan): Int = {
    findTopLevelBroadcastHashJoinTransform(plan).size + findTopLevelBroadcastHashJoin(plan).size
  }

  private def findTopLevelBroadcastHashJoinTransform(
      plan: SparkPlan): Seq[BroadcastHashJoinExecTransformerBase] = {
    collect(plan) { case j: BroadcastHashJoinExecTransformerBase => j }
  }

  private def findTopLevelBroadcastHashJoin(plan: SparkPlan): Seq[BroadcastHashJoinExec] = {
    collect(plan) { case j: BroadcastHashJoinExec => j }
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] = {
    collect(plan) { case j: SortMergeJoinExec => j }
  }

  private def findTopLevelSortMergeJoinTransform(
      plan: SparkPlan): Seq[SortMergeJoinExecTransformer] = {
    collect(plan) { case j: SortMergeJoinExecTransformer => j }
  }

  private def sortMergeJoinSize(plan: SparkPlan): Int = {
    findTopLevelSortMergeJoinTransform(plan).size + findTopLevelSortMergeJoin(plan).size
  }

  private def findTopLevelShuffledHashJoin(plan: SparkPlan): Seq[ShuffledHashJoinExec] = {
    collect(plan) { case j: ShuffledHashJoinExec => j }
  }

  private def findTopLevelShuffledHashJoinTransform(
      plan: SparkPlan): Seq[ShuffledHashJoinExecTransformerBase] = {
    collect(plan) { case j: ShuffledHashJoinExecTransformerBase => j }
  }

  private def findTopLevelBaseJoin(plan: SparkPlan): Seq[BaseJoinExec] = {
    collect(plan) { case j: BaseJoinExec => j }
  }

  private def findTopLevelSort(plan: SparkPlan): Seq[SortExec] = {
    collect(plan) { case s: SortExec => s }
  }

  private def findTopLevelSortTransform(plan: SparkPlan): Seq[SortExecTransformer] = {
    collect(plan) { case s: SortExecTransformer => s }
  }

  private def findReusedExchange(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    collectWithSubqueries(plan) {
      case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _) => e
      case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
    }
  }

  private def findReusedSubquery(plan: SparkPlan): Seq[ReusedSubqueryExec] = {
    collectWithSubqueries(plan) { case e: ReusedSubqueryExec => e }
  }

  private def checkNumLocalShuffleReads(
      plan: SparkPlan,
      numShufflesWithoutLocalRead: Int = 0): Unit = {
    val numShuffles = collect(plan) { case s: ShuffleQueryStageExec => s }.length

    val numLocalReads = collect(plan) {
      case r: AQEShuffleReadExec if r.isLocalRead => r
    }
    // because columnar local reads cannot execute
    numLocalReads.foreach {
      r =>
        val rdd = r.executeColumnar()
        val parts = rdd.partitions
        assert(parts.forall(rdd.preferredLocations(_).nonEmpty))
    }
    assert(numShuffles === (numLocalReads.length + numShufflesWithoutLocalRead))
  }

  testGluten("Change merge join to broadcast join") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300"
    ) {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
      assert(bhj.size == 1)
      checkNumLocalShuffleReads(adaptivePlan)
    }
  }

  testGluten("Change broadcast join to merge join") {
    withTable("t1", "t2") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        sql("CREATE TABLE t1 USING PARQUET AS SELECT 1 c1")
        sql("CREATE TABLE t2 USING PARQUET AS SELECT 1 c1")
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult("""
                                                                |SELECT * FROM (
                                                                | SELECT distinct c1 from t1
                                                                | ) tmp1 JOIN (
                                                                |  SELECT distinct c1 from t2
                                                                | ) tmp2 ON tmp1.c1 = tmp2.c1
                                                                |""".stripMargin)
        assert(broadcastHashJoinSize(plan) == 1)
        assert(broadcastHashJoinSize(adaptivePlan) == 0)
        assert(findTopLevelSortMergeJoinTransform(adaptivePlan).size == 1)
      }
    }
  }

  testGluten("Reuse the parallelism of coalesced shuffle in local shuffle read") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")

      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)
      val localReads = collect(adaptivePlan) {
        case read: AQEShuffleReadExec if read.isLocalRead => read
      }
      assert(localReads.length == 2)
    }
  }

  testGluten("Reuse the default parallelism in local shuffle read") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = '1'")
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)
      val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
      assert(bhj.size == 1)
      val localReads = collect(adaptivePlan) {
        case read: AQEShuffleReadExec if read.isLocalRead => read
      }
      assert(localReads.length == 2)
      val localShuffleRDD0 = localReads(0)
        .executeColumnar()
        .asInstanceOf[ShuffledColumnarBatchRDD]
      val localShuffleRDD1 = localReads(1)
        .executeColumnar()
        .asInstanceOf[ShuffledColumnarBatchRDD]
      // the final parallelism is math.max(1, numReduces / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD0.getPartitions.length == 4)
      // the final parallelism is math.max(1, numReduces / numMappers): math.max(1, 5/2) = 2
      // and the partitions length is 2 * numMappers = 4
      assert(localShuffleRDD1.getPartitions.length == 4)
    }
  }

  testGluten("Empty stage coalesced to 1-partition RDD") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName
    ) {
      val df1 = spark.range(10).withColumn("a", 'id)
      val df2 = spark.range(10).withColumn("b", 'id)
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val testDf = df1
          .where('a > 10)
          .join(df2.where('b > 10), Seq("id"), "left_outer")
          .groupBy('a)
          .count()
        checkAnswer(testDf, Seq())
        val plan = testDf.queryExecution.executedPlan
        assert(find(plan)(_.isInstanceOf[SortMergeJoinExecTransformer]).isDefined)
      }

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
        val testDf = df1
          .where('a > 10)
          .join(df2.where('b > 10), Seq("id"), "left_outer")
          .groupBy('a)
          .count()
        checkAnswer(testDf, Seq())
        val plan = testDf.queryExecution.executedPlan
        assert(find(plan)(_.isInstanceOf[BroadcastHashJoinExecTransformerBase]).isDefined)
        val coalescedReads = collect(plan) { case r: AQEShuffleReadExec => r }
        assert(coalescedReads.length == 3, s"$plan")
        coalescedReads.foreach(r => assert(r.isLocalRead || r.partitionSpecs.length == 1))
      }
    }
  }

  testGluten("Scalar subquery") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a " +
          "where value = (SELECT max(a) from testData3)")
      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)
      checkNumLocalShuffleReads(adaptivePlan)
    }
  }

  testGluten("Scalar subquery in later stages") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a " +
          "where (value + a) = (SELECT max(a) from testData3)")
      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)

      checkNumLocalShuffleReads(adaptivePlan)
    }
  }

  testGluten("multiple joins") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |WITH t4 AS (
          |  SELECT * FROM lowercaseData t2 JOIN testData3 t3 ON t2.n = t3.a where t2.n = '1'
          |)
          |SELECT * FROM testData
          |JOIN testData2 t2 ON key = t2.a
          |JOIN t4 ON t2.b = t4.a
          |WHERE value = 1
        """.stripMargin)
      assert(sortMergeJoinSize(plan) == 3)
      assert(broadcastHashJoinSize(adaptivePlan) == 3)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- BroadcastExchange
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //          +- BroadcastHashJoin
      //             +- BroadcastExchange
      //                +- LocalShuffleReader*
      //                   +- ShuffleExchange
      //             +- LocalShuffleReader*
      //                +- ShuffleExchange
      // +- BroadcastHashJoin
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //    +- BroadcastExchange
      //       +-LocalShuffleReader*
      //             +- ShuffleExchange

      // After applied the 'OptimizeShuffleWithLocalRead' rule, we can convert all the four
      // shuffle read to local shuffle read in the bottom two 'BroadcastHashJoin'.
      // For the top level 'BroadcastHashJoin', the probe side is not shuffle query stage
      // and the build side shuffle query stage is also converted to local shuffle read.
      checkNumLocalShuffleReads(adaptivePlan, 0)
    }
  }

  testGluten("multiple joins with aggregate") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) =
        runAdaptiveAndVerifyResult("""
                                     |WITH t4 AS (
                                     |  SELECT * FROM lowercaseData t2 JOIN (
                                     |    select a, sum(b) from testData3 group by a
                                     |  ) t3 ON t2.n = t3.a where t2.n = '1'
                                     |)
                                     |SELECT * FROM testData
                                     |JOIN testData2 t2 ON key = t2.a
                                     |JOIN t4 ON t2.b = t4.a
                                     |WHERE value = 1
        """.stripMargin)
      assert(sortMergeJoinSize(plan) == 3)
      assert(broadcastHashJoinSize(adaptivePlan) == 3)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- BroadcastExchange
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //          +- BroadcastHashJoin
      //             +- BroadcastExchange
      //                +- LocalShuffleReader*
      //                   +- ShuffleExchange
      //             +- LocalShuffleReader*
      //                +- ShuffleExchange
      // +- BroadcastHashJoin
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //    +- BroadcastExchange
      //       +-HashAggregate
      //          +- CoalescedShuffleReader
      //             +- ShuffleExchange

      // The shuffle added by Aggregate can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 1)
    }
  }

  testGluten("multiple joins with aggregate 2") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
      val (plan, adaptivePlan) =
        runAdaptiveAndVerifyResult("""
                                     |WITH t4 AS (
                                     |  SELECT * FROM lowercaseData t2 JOIN (
                                     |    select a, max(b) b from testData2 group by a
                                     |  ) t3 ON t2.n = t3.b
                                     |)
                                     |SELECT * FROM testData
                                     |JOIN testData2 t2 ON key = t2.a
                                     |JOIN t4 ON value = t4.a
                                     |WHERE value = 1
        """.stripMargin)
      assert(sortMergeJoinSize(plan) == 3)
      assert(broadcastHashJoinSize(adaptivePlan) == 3)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- BroadcastExchange
      //    +- LocalShuffleReader*
      //       +- ShuffleExchange
      //          +- BroadcastHashJoin
      //             +- BroadcastExchange
      //                +- LocalShuffleReader*
      //                   +- ShuffleExchange
      //             +- LocalShuffleReader*
      //                +- ShuffleExchange
      // +- BroadcastHashJoin
      //    +- Filter
      //       +- HashAggregate
      //          +- CoalescedShuffleReader
      //             +- ShuffleExchange
      //    +- BroadcastExchange
      //       +-LocalShuffleReader*
      //           +- ShuffleExchange

      // The shuffle added by Aggregate can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 1)
    }
  }

  testGluten("Exchange reuse") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "20"
    ) {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT value FROM testData join testData2 ON key = a " +
          "join (SELECT value v from testData join testData3 ON key = a) on value = v")
      assert(sortMergeJoinSize(plan) == 3)
      assert(broadcastHashJoinSize(adaptivePlan) == 2)
      // There is still a SMJ, and its two shuffles can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 2)
      // Even with local shuffle read, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.size == 1)
    }
  }

  testGluten("Exchange reuse with subqueries") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
          "where value = (SELECT max(a) from testData join testData2 ON key = a)")
      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)
      checkNumLocalShuffleReads(adaptivePlan)
      //      // Even with local shuffle read, the query stage reuse can also work.
      // gluten change the smj to bhj, stage is changed, so we cannot find the stage with old
      // ReuseExchange from stageCache, then the reuse is removed
      // https://github.com/apache/spark/pull/24706/
      // files#diff-ec42cd27662f3f528832c298a60fffa1d341feb04aa1d8c80044b70cbe0ebbfcR224
      // maybe vanilla spark should checkReuse rile again
      //      val ex = findReusedExchange(adaptivePlan)
      //      assert(ex.size == 1)
    }
  }

  testGluten("Exchange reuse across subqueries") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
          "where value >= (SELECT max(a) from testData join testData2 ON key = a) " +
          "and a <= (SELECT max(a) from testData join testData2 ON key = a)")
      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.nonEmpty)
      val sub = findReusedSubquery(adaptivePlan)
      assert(sub.isEmpty)
    }
  }

  testGluten("Subquery reuse") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
          "where value >= (SELECT max(a) from testData join testData2 ON key = a) " +
          "and a <= (SELECT max(a) from testData join testData2 ON key = a)")
      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.isEmpty)
      val sub = findReusedSubquery(adaptivePlan)
      assert(sub.nonEmpty)
    }
  }

  testGluten("Broadcast exchange reuse across subqueries") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "20000000",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT a FROM testData join testData2 ON key = a " +
          "where value >= (" +
          "SELECT /*+ broadcast(testData2) */ max(key) from testData join testData2 ON key = a) " +
          "and a <= (" +
          "SELECT /*+ broadcast(testData2) */ max(value) from testData join testData2 ON key = a)")
      assert(sortMergeJoinSize(plan) == 1)
      assert(broadcastHashJoinSize(adaptivePlan) == 1)
      checkNumLocalShuffleReads(adaptivePlan)
      // Even with local shuffle read, the query stage reuse can also work.
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.nonEmpty)
      assert(ex.head.child.isInstanceOf[ColumnarBroadcastExchangeExec])
      val sub = findReusedSubquery(adaptivePlan)
      assert(sub.isEmpty)
    }
  }

  // Cost is equal, not test cost is greater, need new test, but other test may contain cost change,
  // so it maybe not essential
  testGluten("Avoid plan change if cost is greater") {}

  testGluten("Change merge join to broadcast join without local shuffle read") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        """
          |SELECT * FROM testData t1 join testData2 t2
          |ON t1.key = t2.a join testData3 t3 on t2.a = t3.a
          |where t1.value = 1
        """.stripMargin
      )
      assert(sortMergeJoinSize(plan) == 2)
      val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
      assert(bhj.size == 2)
      // There is still a SMJ, and its two shuffles can't apply local read.
      checkNumLocalShuffleReads(adaptivePlan, 0)
    }
  }

  testGluten(
    "Avoid changing merge join to broadcast join if too many empty partitions " +
      "on build plan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key -> "0.5",
      // this config will make some empty partitions
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    ) {
      // `testData` is small enough to be broadcast but has empty partition ratio over the config.
      // because testData2 in gluten sizeInBytes(from ColumnarShuffleExchangeExec plan stats)
      // is 24B sometimes, so change the threshold from 80 to 20
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "20") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM testData join testData2 ON key = a where value = '1'")
        assert(sortMergeJoinSize(plan) == 1)
        val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
        assert(bhj.isEmpty)
      }
      // It is still possible to broadcast `testData2`.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM testData join testData2 ON key = a where value = '1'")
        assert(sortMergeJoinSize(plan) == 1)
        val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
        assert(bhj.size == 1)
        assert(bhj.head.joinBuildSide == BuildRight)
      }
    }
  }

  testGluten("SPARK-30524: Do not optimize skew join if introduce additional shuffle") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "100",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100"
    ) {
      withTempView("skewData1", "skewData2") {
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 3 as key1", "id as value1")
          .createOrReplaceTempView("skewData1")
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id % 1 as key2", "id as value2")
          .createOrReplaceTempView("skewData2")

        def checkSkewJoin(query: String, optimizeSkewJoin: Boolean): Unit = {
          val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(query)
          val innerSmj = findTopLevelSortMergeJoinTransform(innerAdaptivePlan)
          assert(innerSmj.size == 1 && innerSmj.head.isSkewJoin == optimizeSkewJoin)
        }

        // OptimizeSkewedJoin check the map status, because the
        checkSkewJoin("SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2", true)
        // Additional shuffle introduced, so disable the "OptimizeSkewedJoin" optimization
        checkSkewJoin(
          "SELECT key1 FROM skewData1 JOIN skewData2 ON key1 = key2 GROUP BY key1",
          false)
      }
    }
  }

  testGluten("SPARK-29544: adaptive skew join with different join types") {
    Seq("SHUFFLE_MERGE", "SHUFFLE_HASH").foreach {
      joinHint =>
        def getJoinNode(plan: SparkPlan): Seq[BinaryExecNode] = if (joinHint == "SHUFFLE_MERGE") {
          findTopLevelSortMergeJoinTransform(plan)
        } else {
          findTopLevelShuffledHashJoinTransform(plan)
        }
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "100",
          SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "800",
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "800"
        ) {
          withTempView("skewData1", "skewData2") {
            spark
              .range(0, 1000, 1, 10)
              .select(
                when('id < 250, 249)
                  .when('id >= 750, 1000)
                  .otherwise('id)
                  .as("key1"),
                'id.as("value1"))
              .createOrReplaceTempView("skewData1")
            spark
              .range(0, 1000, 1, 10)
              .select(
                when('id < 250, 249)
                  .otherwise('id)
                  .as("key2"),
                'id.as("value2"))
              .createOrReplaceTempView("skewData2")

            def checkSkewJoin(
                joins: Seq[BinaryExecNode],
                leftSkewNum: Int,
                rightSkewNum: Int): Unit = {
              assert(joins.size == 1)
              joins.head match {
                case s: SortMergeJoinExecTransformer => assert(s.isSkewJoin)
                case g: ShuffledHashJoinExecTransformerBase => assert(g.isSkewJoin)
                case _ => assert(false)
              }
              assert(
                joins.head.left
                  .collect { case r: AQEShuffleReadExec => r }
                  .head
                  .partitionSpecs
                  .collect { case p: PartialReducerPartitionSpec => p.reducerIndex }
                  .distinct
                  .length == leftSkewNum)
              assert(
                joins.head.right
                  .collect { case r: AQEShuffleReadExec => r }
                  .head
                  .partitionSpecs
                  .collect { case p: PartialReducerPartitionSpec => p.reducerIndex }
                  .distinct
                  .length == rightSkewNum)
            }

            // skewed inner join optimization
            val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
              s"SELECT /*+ $joinHint(skewData1) */ * FROM skewData1 " +
                "JOIN skewData2 ON key1 = key2")
            val inner = getJoinNode(innerAdaptivePlan)
            //          checkSkewJoin(inner, 2, 1)

            // skewed left outer join optimization
            val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
              s"SELECT /*+ $joinHint(skewData2) */ * FROM skewData1 " +
                "LEFT OUTER JOIN skewData2 ON key1 = key2")
            val leftJoin = getJoinNode(leftAdaptivePlan)
            //          checkSkewJoin(leftJoin, 2, 0)

            // skewed right outer join optimization
            val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
              s"SELECT /*+ $joinHint(skewData1) */ * FROM skewData1 " +
                "RIGHT OUTER JOIN skewData2 ON key1 = key2")
            val rightJoin = getJoinNode(rightAdaptivePlan)
            //          checkSkewJoin(rightJoin, 0, 1)
          }
        }
    }
  }

  testGluten("SPARK-34682: AQEShuffleReadExec operating on canonicalized plan") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult("SELECT key FROM testData GROUP BY key")
      val reads = collect(adaptivePlan) { case r: AQEShuffleReadExec => r }
      assert(reads.length == 1)
      val read = reads.head
      val c = read.canonicalized.asInstanceOf[AQEShuffleReadExec]
      // we can't just call execute() because that has separate checks for canonicalized plans
      val ex = intercept[IllegalStateException] {
        val doExecute = PrivateMethod[Unit](Symbol("doExecuteColumnar"))
        c.invokePrivate(doExecute())
      }
      assert(ex.getMessage === "operating on canonicalized plan")
    }
  }

  testGluten("metrics of the shuffle read") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult("SELECT key FROM testData GROUP BY key")
      val reads = collect(adaptivePlan) { case r: AQEShuffleReadExec => r }
      assert(reads.length == 1)
      val read = reads.head
      assert(!read.isLocalRead)
      assert(!read.hasSkewedPartition)
      assert(read.hasCoalescedPartition)
      assert(
        read.metrics.keys.toSeq.sorted == Seq(
          "numCoalescedPartitions",
          "numPartitions",
          "partitionDataSize"))
      assert(read.metrics("numCoalescedPartitions").value == 1)
      assert(read.metrics("numPartitions").value == read.partitionSpecs.length)
      assert(read.metrics("partitionDataSize").value > 0)

      // Gluten has smaller shuffle data size, and the right side is materialized before the left
      // side. Need to lower the threshold to avoid the planner broadcasting the right side first.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "40") {
        val (_, adaptivePlan) = runAdaptiveAndVerifyResult(
          "SELECT * FROM testData join testData2 ON key = a where value = '1'")
        val join = collect(adaptivePlan) { case j: BroadcastHashJoinExecTransformerBase => j }.head
        assert(join.joinBuildSide == BuildLeft)

        val reads = collect(join.right) { case r: AQEShuffleReadExec => r }
        assert(reads.length == 1)
        val read = reads.head
        assert(read.isLocalRead)
        assert(read.metrics.keys.toSeq == Seq("numPartitions"))
        assert(read.metrics("numPartitions").value == read.partitionSpecs.length)
      }

      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "100",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "800",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1000"
      ) {
        withTempView("skewData1", "skewData2") {
          spark
            .range(0, 1000, 1, 10)
            .select(
              when('id < 250, 249)
                .when('id >= 750, 1000)
                .otherwise('id)
                .as("key1"),
              'id.as("value1"))
            .createOrReplaceTempView("skewData1")
          spark
            .range(0, 1000, 1, 10)
            .select(
              when('id < 250, 249)
                .otherwise('id)
                .as("key2"),
              'id.as("value2"))
            .createOrReplaceTempView("skewData2")
          val (_, adaptivePlan) =
            runAdaptiveAndVerifyResult("SELECT * FROM skewData1 join skewData2 ON key1 = key2")
        }
      }
    }
  }

  // because gluten use columnar format, which cannot execute to get rowIterator, then get the key
  // null status
  ignore(
    GLUTEN_TEST + "SPARK-32573: Eliminate NAAJ when BuildSide is HashedRelationWithAllNullKeys") {}

  // EmptyRelation case
  ignore(
    GLUTEN_TEST + "SPARK-35455: Unify empty relation optimization " +
      "between normal and AQE optimizer - single join") {}

  testGluten("SPARK-32753: Only copy tags to node with no tags") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("v1") {
        spark.range(10).union(spark.range(10)).createOrReplaceTempView("v1")

        val (_, adaptivePlan) =
          runAdaptiveAndVerifyResult("SELECT id FROM v1 GROUP BY id DISTRIBUTE BY id")
        assert(collect(adaptivePlan) { case s: ColumnarShuffleExchangeExec => s }.length == 1)
      }
    }
  }

  testGluten("Logging plan changes for AQE") {
    val testAppender = new LogAppender("plan changes")
    withLogAppender(testAppender) {
      withSQLConf(
        // this test default level is WARN, so we should check warn level
        SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "WARN",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80"
      ) {
        sql(
          "SELECT * FROM testData JOIN testData2 ON key = a " +
            "WHERE value = (SELECT max(a) FROM testData3)").collect()
      }
      Seq(
        "=== Result of Batch AQE Preparations ===",
        "=== Result of Batch AQE Post Stage Creation ===",
        "=== Result of Batch AQE Replanning ===",
        "=== Result of Batch AQE Query Stage Optimization ==="
      ).foreach {
        expectedMsg =>
          assert(
            testAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains(expectedMsg)))
      }
    }
  }

  testGluten("SPARK-33551: Do not use AQE shuffle read for repartition") {
    def hasRepartitionShuffle(plan: SparkPlan): Boolean = {
      find(plan) {
        case s: ShuffleExchangeLike =>
          s.shuffleOrigin == REPARTITION_BY_COL || s.shuffleOrigin == REPARTITION_BY_NUM
        case _ => false
      }.isDefined
    }

    def checkBHJ(
        df: Dataset[Row],
        optimizeOutRepartition: Boolean,
        probeSideLocalRead: Boolean,
        probeSideCoalescedRead: Boolean): Unit = {
      df.collect()
      val plan = df.queryExecution.executedPlan
      // There should be only one shuffle that can't do local read, which is either the top shuffle
      // from repartition, or BHJ probe side shuffle.
      checkNumLocalShuffleReads(plan, 1)
      assert(hasRepartitionShuffle(plan) == !optimizeOutRepartition)
      val bhj = findTopLevelBroadcastHashJoinTransform(plan)
      assert(bhj.length == 1)

      // Build side should do local read.
      val buildSide = find(bhj.head.left)(_.isInstanceOf[AQEShuffleReadExec])
      assert(buildSide.isDefined)
      assert(buildSide.get.asInstanceOf[AQEShuffleReadExec].isLocalRead)

      val probeSide = find(bhj.head.right)(_.isInstanceOf[AQEShuffleReadExec])
      if (probeSideLocalRead || probeSideCoalescedRead) {
        assert(probeSide.isDefined)
        if (probeSideLocalRead) {
          assert(probeSide.get.asInstanceOf[AQEShuffleReadExec].isLocalRead)
        } else {
          assert(probeSide.get.asInstanceOf[AQEShuffleReadExec].hasCoalescedPartition)
        }
      } else {
        assert(probeSide.isEmpty)
      }
    }

    def checkSMJ(
        df: Dataset[Row],
        optimizeOutRepartition: Boolean,
        optimizeSkewJoin: Boolean,
        coalescedRead: Boolean): Unit = {
      df.collect()
      val plan = df.queryExecution.executedPlan
      assert(hasRepartitionShuffle(plan) == !optimizeOutRepartition)
      val smj = findTopLevelSortMergeJoinTransform(plan)
      assert(smj.length == 1)
      assert(smj.head.isSkewJoin == optimizeSkewJoin)
      val aqeReads = collect(smj.head) { case c: AQEShuffleReadExec => c }
      if (coalescedRead || optimizeSkewJoin) {
        assert(aqeReads.length == 2)
        if (coalescedRead) assert(aqeReads.forall(_.hasCoalescedPartition))
      } else {
        assert(aqeReads.isEmpty)
      }
    }

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      val df = sql("""
                     |SELECT * FROM (
                     |  SELECT * FROM testData WHERE key = 1
                     |)
                     |RIGHT OUTER JOIN testData2
                     |ON value = b
        """.stripMargin)

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
        // Repartition with no partition num specified.
        checkBHJ(
          df.repartition('b),
          // The top shuffle from repartition is optimized out.
          optimizeOutRepartition = true,
          probeSideLocalRead = false,
          probeSideCoalescedRead = true
        )

        // Repartition with default partition num (5 in test env) specified.
        checkBHJ(
          df.repartition(5, 'b),
          // The top shuffle from repartition is optimized out
          // The final plan must have 5 partitions, no optimization can be made to the probe side.
          optimizeOutRepartition = true,
          probeSideLocalRead = false,
          probeSideCoalescedRead = false
        )

        // Repartition with non-default partition num specified.
        checkBHJ(
          df.repartition(4, 'b),
          // The top shuffle from repartition is not optimized out
          optimizeOutRepartition = false,
          probeSideLocalRead = true,
          probeSideCoalescedRead = true
        )

        // Repartition by col and project away the partition cols
        checkBHJ(
          df.repartition('b).select('key),
          // The top shuffle from repartition is not optimized out
          optimizeOutRepartition = false,
          probeSideLocalRead = true,
          probeSideCoalescedRead = true
        )
      }

      // Force skew join
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "1",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR.key -> "0",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10"
      ) {
        // Repartition with no partition num specified.
        checkSMJ(
          df.repartition('b),
          // The top shuffle from repartition is optimized out.
          optimizeOutRepartition = true,
          optimizeSkewJoin = false,
          coalescedRead = true)

        // Repartition with default partition num (5 in test env) specified.
        checkSMJ(
          df.repartition(5, 'b),
          // The top shuffle from repartition is optimized out.
          // The final plan must have 5 partitions, can't do coalesced read.
          optimizeOutRepartition = true,
          optimizeSkewJoin = false,
          coalescedRead = false
        )

        // Repartition with non-default partition num specified.
        checkSMJ(
          df.repartition(4, 'b),
          // The top shuffle from repartition is not optimized out.
          optimizeOutRepartition = false,
          optimizeSkewJoin = true,
          coalescedRead = false)

        // Repartition by col and project away the partition cols
        checkSMJ(
          df.repartition('b).select('key),
          // The top shuffle from repartition is not optimized out.
          optimizeOutRepartition = false,
          optimizeSkewJoin = true,
          coalescedRead = false
        )
      }
    }
  }

  testGluten("SPARK-34091: Batch shuffle fetch in AQE partition coalescing") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10",
      SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key -> "true") {
      withTable("t1") {
        spark.range(100).selectExpr("id + 1 as a").write.format("parquet").saveAsTable("t1")
        val query = "SELECT SUM(a) FROM t1 GROUP BY a"
        val (_, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        val metricName = SQLShuffleReadMetricsReporter.LOCAL_BLOCKS_FETCHED
        val blocksFetchedMetric = collectFirst(adaptivePlan) {
          case p if p.metrics.contains(metricName) => p.metrics(metricName)
        }
        assert(blocksFetchedMetric.isDefined)
        val blocksFetched = blocksFetchedMetric.get.value
        withSQLConf(SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key -> "false") {
          val (_, adaptivePlan2) = runAdaptiveAndVerifyResult(query)
          val blocksFetchedMetric2 = collectFirst(adaptivePlan2) {
            case p if p.metrics.contains(metricName) => p.metrics(metricName)
          }
          assert(blocksFetchedMetric2.isDefined)
          val blocksFetched2 = blocksFetchedMetric2.get.value
          assert(blocksFetched == blocksFetched2)
        }
      }
    }
  }

  testGluten("SPARK-34899: Use origin plan if we can not coalesce shuffle partition") {
    def checkNoCoalescePartitions(ds: Dataset[Row], origin: ShuffleOrigin): Unit = {
      assert(collect(ds.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec if s.shuffleOrigin == origin && s.numPartitions == 2 => s
      }.size == 1)
      ds.collect()
      val plan = ds.queryExecution.executedPlan
      assert(collect(plan) {
        case s: ColumnarShuffleExchangeExec if s.shuffleOrigin == origin && s.numPartitions == 2 =>
          s
      }.size == 1)
      checkAnswer(ds, testData)
    }

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      // Pick a small value so that no coalesce can happen.
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2"
    ) {
      val df =
        spark.sparkContext.parallelize((1 to 100).map(i => TestData(i, i.toString)), 10).toDF()

      // partition size [1420, 1420]
      checkNoCoalescePartitions(df.repartition($"key"), REPARTITION_BY_COL)
      // partition size [1140, 1119]
      checkNoCoalescePartitions(df.sort($"key"), ENSURE_REQUIREMENTS)
    }
  }

  testGluten("SPARK-35239: Coalesce shuffle partition should handle empty input RDD") {
    withTable("t") {
      withSQLConf(
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName
      ) {
        spark.sql("CREATE TABLE t (c1 int) USING PARQUET")
        val (_, adaptive) = runAdaptiveAndVerifyResult("SELECT c1, count(*) FROM t GROUP BY c1")
        assert(
          collect(adaptive) {
            case c @ AQEShuffleReadExec(_, partitionSpecs) if partitionSpecs.length == 1 =>
              assert(c.hasCoalescedPartition)
              c
          }.length == 1
        )
      }
    }
  }

  testGluten("SPARK-35264: Support AQE side broadcastJoin threshold") {
    withTempView("t1", "t2") {
      def checkJoinStrategy(shouldBroadcast: Boolean): Unit = {
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val (origin, adaptive) =
            runAdaptiveAndVerifyResult("SELECT t1.c1, t2.c1 FROM t1 JOIN t2 ON t1.c1 = t2.c1")
          assert(findTopLevelSortMergeJoin(origin).size == 1)
          if (shouldBroadcast) {
            assert(findTopLevelBroadcastHashJoinTransform(adaptive).size == 1)
          } else {
            assert(findTopLevelSortMergeJoinTransform(adaptive).size == 1)
          }
        }
      }

      // t1: 1600 bytes
      // t2: 160 bytes
      spark.sparkContext
        .parallelize((1 to 100).map(i => TestData(i, i.toString)), 10)
        .toDF("c1", "c2")
        .createOrReplaceTempView("t1")
      spark.sparkContext
        .parallelize((1 to 10).map(i => TestData(i, i.toString)), 5)
        .toDF("c1", "c2")
        .createOrReplaceTempView("t2")

      checkJoinStrategy(false)
      withSQLConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        checkJoinStrategy(false)
      }

      withSQLConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "400") {
        checkJoinStrategy(true)
      }
    }
  }

  // table partition size is different with spark
  testGluten("SPARK-35264: Support AQE side shuffled hash join formula") {
    withTempView("t1", "t2") {
      def checkJoinStrategy(shouldShuffleHashJoin: Boolean): Unit = {
        Seq("100", "100000").foreach {
          size =>
            withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> size) {
              val (origin1, adaptive1) =
                runAdaptiveAndVerifyResult("SELECT t1.c1, t2.c1 FROM t1 JOIN t2 ON t1.c1 = t2.c1")
              assert(findTopLevelSortMergeJoin(origin1).size === 1)
              if (shouldShuffleHashJoin && size.toInt < 100000) {
                val shj = findTopLevelShuffledHashJoinTransform(adaptive1)
                assert(shj.size === 1)
                assert(shj.head.joinBuildSide == BuildRight)
              } else {
                assert(findTopLevelSortMergeJoinTransform(adaptive1).size === 1)
              }
            }
        }
        // respect user specified join hint
        val (origin2, adaptive2) = runAdaptiveAndVerifyResult(
          "SELECT /*+ MERGE(t1) */ t1.c1, t2.c1 FROM t1 JOIN t2 ON t1.c1 = t2.c1")
        assert(findTopLevelSortMergeJoin(origin2).size === 1)
        assert(findTopLevelSortMergeJoinTransform(adaptive2).size === 1)
      }

      spark.sparkContext
        .parallelize((1 to 100).map(i => TestData(i, i.toString)), 10)
        .toDF("c1", "c2")
        .createOrReplaceTempView("t1")
      spark.sparkContext
        .parallelize((1 to 10).map(i => TestData(i, i.toString)), 5)
        .toDF("c1", "c2")
        .createOrReplaceTempView("t2")

      // t1 partition size: [395, 316, 313]
      // t2 partition size: [140, 50, 0]
      withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true") {
        // check default value
        checkJoinStrategy(false)
        // t1 no hint.
        // t2 partition size are all smaller than 200, t2 has SHJ hint. The result is true.
        withSQLConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "200") {
          checkJoinStrategy(true)
        }
        // t1 no hint.
        // Not all partition size of t2 are smaller than 100, t2 no hint. The result is false.
        withSQLConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "100") {
          checkJoinStrategy(false)
        }
        // t1, t2 partition size are all smaller than 1000, t1 and t2 can use SHJ.
        // The result is true.
        withSQLConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD.key -> "1000") {
          checkJoinStrategy(true)
        }
      }
    }
  }

  testGluten("SPARK-32932: Do not use local shuffle read at final stage on write command") {
    withSQLConf(
      SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    ) {
      val data =
        for (
          i <- 1L to 10L;
          j <- 1L to 3L
        ) yield (i, j)

      val df = data.toDF("i", "j").repartition($"j")
      var noLocalread: Boolean = false
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          qe.executedPlan match {
            case plan @ (_: DataWritingCommandExec | _: V2TableWriteExec) =>
              noLocalread = collect(plan) {
                case exec: AQEShuffleReadExec if exec.isLocalRead => exec
              }.isEmpty
            case _ => // ignore other events
          }
        }
        override def onFailure(
            funcName: String,
            qe: QueryExecution,
            exception: Exception): Unit = {}
      }
      spark.listenerManager.register(listener)

      withTable("t") {
        df.write.partitionBy("j").saveAsTable("t")
        sparkContext.listenerBus.waitUntilEmpty()
        assert(noLocalread)
        noLocalread = false
      }

      // Test DataSource v2
      val format = classOf[NoopDataSource].getName
      df.write.format(format).mode("overwrite").save()
      sparkContext.listenerBus.waitUntilEmpty()
      assert(noLocalread)
      noLocalread = false

      spark.listenerManager.unregister(listener)
    }
  }

  testGluten(
    "SPARK-30953: InsertAdaptiveSparkPlan should apply AQE on child plan of v2 write commands") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      var plan: SparkPlan = null
      val listener = new QueryExecutionListener {
        override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
          plan = qe.executedPlan
        }
        override def onFailure(
            funcName: String,
            qe: QueryExecution,
            exception: Exception): Unit = {}
      }
      spark.listenerManager.register(listener)
      withTable("t1") {
        val format = classOf[NoopDataSource].getName
        Seq((0, 1)).toDF("x", "y").write.format(format).mode("overwrite").save()

        sparkContext.listenerBus.waitUntilEmpty()
        assert(plan.isInstanceOf[V2TableWriteExec])
        val childPlan = plan.asInstanceOf[V2TableWriteExec].child
        assert(childPlan.isInstanceOf[ColumnarToCarrierRowExecBase])
        assert(
          childPlan
            .asInstanceOf[ColumnarToCarrierRowExecBase]
            .child
            .isInstanceOf[AdaptiveSparkPlanExec])

        spark.listenerManager.unregister(listener)
      }
    }
  }

  testGluten("SPARK-35650: Coalesce number of partitions by AEQ") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
      Seq("REPARTITION", "REBALANCE(key)")
        .foreach {
          repartition =>
            val query = s"SELECT /*+ $repartition */ * FROM testData"
            val (_, adaptivePlan) = runAdaptiveAndVerifyResult(query)
            collect(adaptivePlan) { case r: AQEShuffleReadExec => r } match {
              case Seq(aqeShuffleRead) =>
                assert(aqeShuffleRead.partitionSpecs.size === 1)
                assert(!aqeShuffleRead.isLocalRead)
              case _ =>
                fail("There should be a AQEShuffleReadExec")
            }
        }
    }
  }

  testGluten("SPARK-35650: Use local shuffle read if can not coalesce number of partitions") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
      val query = "SELECT /*+ REPARTITION */ * FROM testData"
      val (_, adaptivePlan) = runAdaptiveAndVerifyResult(query)
      collect(adaptivePlan) { case r: AQEShuffleReadExec => r } match {
        case Seq(aqeShuffleRead) =>
          assert(aqeShuffleRead.partitionSpecs.size === 4)
          assert(aqeShuffleRead.isLocalRead)
        case _ =>
          fail("There should be a AQEShuffleReadExec")
      }
    }
  }

  testGluten("SPARK-35725: Support optimize skewed partitions in RebalancePartitions") {
    withTempView("v") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1"
      ) {

        spark.sparkContext
          .parallelize((1 to 10).map(i => TestData(if (i > 4) 5 else i, i.toString)), 3)
          .toDF("c1", "c2")
          .createOrReplaceTempView("v")

        def checkPartitionNumber(
            query: String,
            skewedPartitionNumber: Int,
            totalNumber: Int): Unit = {
          val (_, adaptive) = runAdaptiveAndVerifyResult(query)
          val read = collect(adaptive) { case read: AQEShuffleReadExec => read }
          assert(read.size == 1)
          assert(
            read.head.partitionSpecs.count(_.isInstanceOf[PartialReducerPartitionSpec]) ==
              skewedPartitionNumber)
          assert(read.head.partitionSpecs.size == totalNumber)
        }

        // Changed ADVISORY_PARTITION_SIZE_IN_BYTES from 150 to 120 because Gluten has smaller
        // partition size.
        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "120") {
          // partition size [0,208,54,54,54]
          checkPartitionNumber("SELECT /*+ REBALANCE(c1) */ * FROM v", 2, 4)
          // partition size [108, 54, 60, 108, 54, 108, 54]
          checkPartitionNumber("SELECT /*+ REBALANCE */ * FROM v", 6, 7)
        }

        // no skewed partition should be optimized
        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10000") {
          checkPartitionNumber("SELECT /*+ REBALANCE(c1) */ * FROM v", 0, 1)
        }
      }
    }
  }

  testGluten("SPARK-35888: join with a 0-partition table") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName
    ) {
      withTempView("t2") {
        // create a temp view with 0 partition
        spark
          .createDataFrame(sparkContext.emptyRDD[Row], new StructType().add("b", IntegerType))
          .createOrReplaceTempView("t2")
        val (_, adaptive) =
          runAdaptiveAndVerifyResult("SELECT * FROM testData2 t1 left semi join t2 ON t1.a=t2.b")
        val aqeReads = collect(adaptive) { case c: AQEShuffleReadExec => c }
        assert(aqeReads.length == 2)
        aqeReads.foreach {
          c =>
            val stats = c.child.asInstanceOf[QueryStageExec].getRuntimeStatistics
            assert(stats.sizeInBytes >= 0)
            assert(stats.rowCount.get >= 0)
        }
      }
    }
  }

  testGluten("SPARK-35968: AQE coalescing should not produce too small partitions by default") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val (_, adaptive) =
        runAdaptiveAndVerifyResult("SELECT sum(id) FROM RANGE(10) GROUP BY id % 3")
      val coalesceRead = collect(adaptive) {
        case r: AQEShuffleReadExec if r.hasCoalescedPartition => r
      }
      assert(coalesceRead.length == 1)
      // RANGE(10) is a very small dataset and AQE coalescing should produce one partition.
      assert(coalesceRead.head.partitionSpecs.length == 1)
    }
  }

  testGluten("SPARK-35794: Allow custom plugin for cost evaluator") {
    CostEvaluator.instantiate(
      classOf[SimpleShuffleSortCostEvaluator].getCanonicalName,
      spark.sparkContext.getConf)
    intercept[IllegalArgumentException] {
      CostEvaluator.instantiate(
        classOf[InvalidCostEvaluator].getCanonicalName,
        spark.sparkContext.getConf)
    }

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
      val query = "SELECT * FROM testData join testData2 ON key = a where value = '1'"

      withSQLConf(
        SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key ->
          "org.apache.spark.sql.execution.adaptive.SimpleShuffleSortCostEvaluator") {
        val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(query)
        val smj = findTopLevelSortMergeJoin(plan)
        assert(smj.size == 1)
        val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
        assert(bhj.size == 1)
        checkNumLocalShuffleReads(adaptivePlan)
      }

      withSQLConf(
        SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key ->
          "org.apache.spark.sql.execution.adaptive.InvalidCostEvaluator") {
        intercept[IllegalArgumentException] {
          runAdaptiveAndVerifyResult(query)
        }
      }
    }
  }

  testGluten("SPARK-36020: Check logical link in remove redundant projects") {
    withTempView("t") {
      spark
        .range(10)
        .selectExpr(
          "id % 10 as key",
          "cast(id * 2 as int) as a",
          "cast(id * 3 as int) as b",
          "array(id, id + 1, id + 3) as c")
        .createOrReplaceTempView("t")
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "800") {
        val query =
          """
            |WITH tt AS (
            | SELECT key, a, b, explode(c) AS c FROM t
            |)
            |SELECT t1.key, t1.c, t2.key, t2.c
            |FROM (SELECT a, b, c, key FROM tt WHERE a > 1) t1
            |JOIN (SELECT a, b, c, key FROM tt) t2
            |  ON t1.key = t2.key
            |""".stripMargin
        val (origin, adaptive) = runAdaptiveAndVerifyResult(query)
        assert(findTopLevelSortMergeJoin(origin).size == 1)
        assert(findTopLevelBroadcastHashJoinTransform(adaptive).size == 1)
      }
    }
  }

  testGluten(
    "SPARK-36032: Use inputPlan instead of currentPhysicalPlan to initialize logical link") {
    withTempView("v") {
      spark.sparkContext
        .parallelize((1 to 10).map(i => TestData(i, i.toString)), 2)
        .toDF("c1", "c2")
        .createOrReplaceTempView("v")

      Seq("-1", "10000").foreach {
        aqeBhj =>
          withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> aqeBhj,
            SQLConf.SHUFFLE_PARTITIONS.key -> "1"
          ) {
            val (origin, adaptive) = runAdaptiveAndVerifyResult("""
                                                                  |SELECT * FROM v t1 JOIN (
                                                                  | SELECT c1 + 1 as c3 FROM v
                                                                  |)t2 ON t1.c1 = t2.c3
                                                                  |SORT BY c1
          """.stripMargin)
            if (aqeBhj.toInt < 0) {
              // 1 sort since spark plan has no shuffle for SMJ
              assert(findTopLevelSort(origin).size == 1)
              // 2 sorts in SMJ
              assert(findTopLevelSortTransform(adaptive).size == 2)
            } else {
              assert(findTopLevelSort(origin).size == 1)
              // 1 sort at top node and BHJ has no sort
              assert(findTopLevelSortTransform(adaptive).size == 1)
            }
          }
      }
    }
  }

  testGluten("SPARK-37742: AQE reads invalid InMemoryRelation stats and mistakenly plans BHJ") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1048584",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName
    ) {
      // Spark estimates a string column as 20 bytes so with 60k rows, these relations should be
      // estimated at ~120m bytes which is greater than the broadcast join threshold.
      val joinKeyOne = "00112233445566778899"
      val joinKeyTwo = "11223344556677889900"
      Seq
        .fill(60000)(joinKeyOne)
        .toDF("key")
        .createOrReplaceTempView("temp")
      Seq
        .fill(60000)(joinKeyTwo)
        .toDF("key")
        .createOrReplaceTempView("temp2")

      Seq(joinKeyOne).toDF("key").createOrReplaceTempView("smallTemp")
      spark.sql("SELECT key as newKey FROM temp").persist()

      // This query is trying to set up a situation where there are three joins.
      // The first join will join the cached relation with a smaller relation.
      // The first join is expected to be a broadcast join since the smaller relation will
      // fit under the broadcast join threshold.
      // The second join will join the first join with another relation and is expected
      // to remain as a sort-merge join.
      // The third join will join the cached relation with another relation and is expected
      // to remain as a sort-merge join.
      val query =
        s"""
           |SELECT t3.newKey
           |FROM
           |  (SELECT t1.newKey
           |  FROM (SELECT key as newKey FROM temp) as t1
           |        JOIN
           |        (SELECT key FROM smallTemp) as t2
           |        ON t1.newKey = t2.key
           |  ) as t3
           |  JOIN
           |  (SELECT key FROM temp2) as t4
           |  ON t3.newKey = t4.key
           |UNION
           |SELECT t1.newKey
           |FROM
           |    (SELECT key as newKey FROM temp) as t1
           |    JOIN
           |    (SELECT key FROM temp2) as t2
           |    ON t1.newKey = t2.key
           |""".stripMargin
      val df = spark.sql(query)
      df.collect()
      val adaptivePlan = df.queryExecution.executedPlan
      val bhj = findTopLevelBroadcastHashJoinTransform(adaptivePlan)
      assert(bhj.length == 1)
    }
  }

  testGluten("test log level") {
    def verifyLog(expectedLevel: Level): Unit = {
      val logAppender = new LogAppender("adaptive execution")
      logAppender.setThreshold(expectedLevel)
      withLogAppender(
        logAppender,
        loggerNames = Seq(AdaptiveSparkPlanExec.getClass.getName.dropRight(1)),
        level = Some(Level.TRACE)) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "300") {
          sql("SELECT * FROM testData join testData2 ON key = a where value = '1'").collect()
        }
      }
      Seq("Plan changed", "Final plan").foreach {
        msg =>
          assert(logAppender.loggingEvents.exists {
            event =>
              event.getMessage.getFormattedMessage.contains(msg) && event.getLevel == expectedLevel
          })
      }
    }

    // Verify default log level
    verifyLog(Level.DEBUG)

    // Verify custom log level
    val levels = Seq(
      "TRACE" -> Level.TRACE,
      "trace" -> Level.TRACE,
      "DEBUG" -> Level.DEBUG,
      "debug" -> Level.DEBUG,
      "INFO" -> Level.INFO,
      "info" -> Level.INFO,
      "WARN" -> Level.WARN,
      "warn" -> Level.WARN,
      "ERROR" -> Level.ERROR,
      "error" -> Level.ERROR,
      "deBUG" -> Level.DEBUG
    )

    levels.foreach {
      level =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_LOG_LEVEL.key -> level._1) {
          verifyLog(level._2)
        }
    }
  }
}
