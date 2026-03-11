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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BatchScanExecTransformer, FileSourceScanExecTransformer, FilterExecTransformerBase}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf

abstract class GlutenDynamicPartitionPruningSuiteBase
  extends DynamicPartitionPruningSuiteBase
  with GlutenSQLTestsTrait {

  import testImplicits._

  override def beforeAll(): Unit = {
    prepareWorkDir()
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def testNameBlackList: Seq[String] = Seq(
    // overwritten with different plan
    "SPARK-38674: Remove useless deduplicate in SubqueryBroadcastExec",
    "Make sure dynamic pruning works on uncorrelated queries"
  )

  // === Following cases override super class's cases ===

  ignoreGluten("DPP should not be rewritten as an existential join") {
    // ignored: BroadcastHashJoinExec is from Vanilla Spark
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.5",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false"
    ) {
      val df = sql(s"""
                      |SELECT * FROM product p WHERE p.store_id NOT IN
                      | (SELECT f.store_id FROM fact_sk f JOIN dim_store d ON
                      |  f.store_id = d.store_id
                      |    WHERE d.state_province = 'NL'
                      | )
       """.stripMargin)

      val found = df.queryExecution.executedPlan.find {
        case _ @BroadcastHashJoinExec(_, _, _: ExistenceJoin, _, _, _, _, _) => true
        case _ => false
      }

      assert(found.isEmpty)
    }
  }

  testGluten("no partition pruning when the build side is a stream") {
    withTable("fact") {
      val input = MemoryStream[Int]
      val stream = input.toDF.select($"value".as("one"), ($"value" * 3).as("code"))
      spark
        .range(100)
        .select($"id", ($"id" + 1).as("one"), ($"id" + 2).as("two"), ($"id" + 3).as("three"))
        .write
        .partitionBy("one")
        .format(tableFormat)
        .mode("overwrite")
        .saveAsTable("fact")
      val table = sql("SELECT * from fact f")

      // join a partitioned table with a stream
      val joined = table.join(stream, Seq("one")).where("code > 40")
      val query = joined.writeStream.format("memory").queryName("test").start()
      input.addData(1, 10, 20, 40, 50)
      try {
        query.processAllAvailable()
      } finally {
        query.stop()
      }
      // search dynamic pruning predicates on the executed plan
      val plan = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
      val ret = plan.find {
        case s: FileSourceScanExecTransformer =>
          s.partitionFilters.exists {
            case _: DynamicPruningExpression => true
            case _ => false
          }
        case s: FileSourceScanExec =>
          s.partitionFilters.exists {
            case _: DynamicPruningExpression => true
            case _ => false
          }
        case _ => false
      }
      assert(ret.isDefined == false)
    }
  }

  testGluten("Make sure dynamic pruning works on uncorrelated queries") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql("""
                     |SELECT d.store_id,
                     |       SUM(f.units_sold),
                     |       (SELECT SUM(f.units_sold)
                     |        FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
                     |        WHERE d.country = 'US') AS total_prod
                     |FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
                     |WHERE d.country = 'US'
                     |GROUP BY 1
        """.stripMargin)
      checkAnswer(df, Row(4, 50, 70) :: Row(5, 10, 70) :: Row(6, 10, 70) :: Nil)

      val plan = df.queryExecution.executedPlan
      val countSubqueryBroadcasts =
        collectWithSubqueries(plan) {
          case _: SubqueryBroadcastExec => 1
          case _: ColumnarSubqueryBroadcastExec => 1
        }.sum

      val countReusedSubqueryBroadcasts =
        collectWithSubqueries(plan) {
          case ReusedSubqueryExec(_: SubqueryBroadcastExec) => 1
          case ReusedSubqueryExec(_: ColumnarSubqueryBroadcastExec) => 1
        }.sum

      assert(countSubqueryBroadcasts == 1)
      assert(countReusedSubqueryBroadcasts == 1)
    }
  }

  testGluten(
    "SPARK-32509: Unused Dynamic Pruning filter shouldn't affect " +
      "canonicalization and exchange reuse") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.V2_BUCKETING_ENABLED.key -> "false") {
        val df = sql(""" WITH view1 as (
                       |   SELECT f.store_id FROM fact_stats f WHERE f.units_sold = 70
                       | )
                       |
                       | SELECT * FROM view1 v1 join view1 v2 WHERE v1.store_id = v2.store_id
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
        val reuseExchangeNodes = collect(df.queryExecution.executedPlan) {
          case se: ReusedExchangeExec => se
        }
        assert(
          reuseExchangeNodes.size == 1,
          "Expected plan to contain 1 ReusedExchangeExec " +
            s"nodes. Found ${reuseExchangeNodes.size}")

        checkAnswer(df, Row(15, 15) :: Nil)
      }
    }
  }

  testGluten("SPARK-38674: Remove useless deduplicate in SubqueryBroadcastExec") {
    withTable("duplicate_keys") {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        Seq[(Int, String)]((1, "NL"), (1, "NL"), (3, "US"), (3, "US"), (3, "US"))
          .toDF("store_id", "country")
          .write
          .format(tableFormat)
          .saveAsTable("duplicate_keys")

        val df = sql("""
                       |SELECT date_id, product_id FROM fact_sk f
                       |JOIN duplicate_keys s
                       |ON f.store_id = s.store_id WHERE s.country = 'US' AND date_id > 1050
          """.stripMargin)

        checkPartitionPruningPredicate(df, withSubquery = false, withBroadcast = true)

        val subqueryBroadcastExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: ColumnarSubqueryBroadcastExec => s
        }
        assert(subqueryBroadcastExecs.size === 1)
        subqueryBroadcastExecs.foreach {
          subqueryBroadcastExec =>
            assert(subqueryBroadcastExec.metrics("numOutputRows").value === 1)
        }

        checkAnswer(df, Row(1060, 2) :: Row(1060, 2) :: Row(1060, 2) :: Nil)
      }
    }
  }

  // === Following methods override super class's methods ===

  override protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: FileSourceScanExecTransformer =>
        s.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
      case s: FileSourceScanExec =>
        s.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
      case s: BatchScanExecTransformer =>
        s.runtimeFilters.collect { case d: DynamicPruningExpression => d.child }
      case s: BatchScanExec =>
        s.runtimeFilters.collect { case d: DynamicPruningExpression => d.child }
      case _ => Nil
    }
  }

  override def checkPartitionPruningPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case InSubqueryExec(_, _: SubqueryExec, _, _, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _, _, _) => b
      case InSubqueryExec(_, b: ColumnarSubqueryBroadcastExec, _, _, _, _) => b
    }

    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(
      hasSubquery == withSubquery,
      s"$hasFilter trigger DPP with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(
      subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast trigger DPP with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach {
      s =>
        s.child match {
          case _: ReusedExchangeExec => // reuse check ok.
          case BroadcastQueryStageExec(_, _: ReusedExchangeExec, _) => // reuse check ok.
          case b: BroadcastExchangeLike =>
            val hasReuse = plan.find {
              case ReusedExchangeExec(_, e) => e eq b
              case _ => false
            }.isDefined
            assert(hasReuse, s"$s\nshould have been reused in\n$plan")
          case a: AdaptiveSparkPlanExec =>
            val broadcastQueryStage = collectFirst(a) { case b: BroadcastQueryStageExec => b }
            val broadcastPlan = broadcastQueryStage.get.broadcast
            val hasReuse = find(plan) {
              case ReusedExchangeExec(_, e) => e eq broadcastPlan
              case b: BroadcastExchangeLike => b eq broadcastPlan
              case _ => false
            }.isDefined
            assert(hasReuse, s"$s\nshould have been reused in\n$plan")
          case _ =>
            fail(s"Invalid child node found in\n$s")
        }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach {
      s =>
        val subquery = s match {
          case r: ReusedSubqueryExec => r.child
          case o => o
        }
        assert(
          subquery.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined == isMainQueryAdaptive)
    }
  }

  override def checkDistinctSubqueries(df: DataFrame, n: Int): Unit = {
    df.collect()

    val buf = collectDynamicPruningExpressions(df.queryExecution.executedPlan).collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _, _, _) =>
        b.indices
      case InSubqueryExec(_, b: ColumnarSubqueryBroadcastExec, _, _, _, _) =>
        b.indices
    }
    assert(buf.distinct.size == n)
  }

  override def checkUnpushedFilters(df: DataFrame): Boolean = {
    find(df.queryExecution.executedPlan) {
      case FilterExec(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case _: DynamicPruningExpression => true
          case _ => false
        }
      case transformer: FilterExecTransformerBase =>
        splitConjunctivePredicates(transformer.cond).exists {
          case _: DynamicPruningExpression => true
          case _ => false
        }
      case FilterTransformer(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case _: DynamicPruningExpression => true
          case _ => false
        }
      case _ => false
    }.isDefined
  }

  object FilterTransformer {
    def unapply(plan: SparkPlan): Option[(Expression, SparkPlan)] = {
      plan match {
        case transformer: FilterExecTransformerBase =>
          Some((transformer.cond, transformer.input))
        case _ => None
      }
    }
  }
}

abstract class GlutenDynamicPartitionPruningV1Suite extends GlutenDynamicPartitionPruningSuiteBase {

  import testImplicits._

  /** Check the static scan metrics with and without DPP */
  testGluten("static scan metrics", DisableAdaptiveExecution("DPP in AQE must reuse broadcast")) {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false"
    ) {
      withTable("fact", "dim") {
        val numPartitions = 10

        spark
          .range(10)
          .map(x => Tuple3(x, x + 1, 0))
          .toDF("did", "d1", "d2")
          .write
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("dim")

        spark
          .range(100)
          .map(x => Tuple2(x, x % numPartitions))
          .toDF("f1", "fid")
          .write
          .partitionBy("fid")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("fact")

        def getFactScan(plan: SparkPlan): SparkPlan = {
          val scanOption =
            find(plan) {
              case s: FileSourceScanExec =>
                s.output.exists(_.find(_.argString(maxFields = 100).contains("fid")).isDefined)
              case s: FileSourceScanExecTransformer =>
                s.output.exists(_.find(_.argString(maxFields = 100).contains("fid")).isDefined)
              case s: BatchScanExec =>
                // we use f1 col for v2 tables due to schema pruning
                s.output.exists(_.find(_.argString(maxFields = 100).contains("f1")).isDefined)
              case s: BatchScanExecTransformer =>
                // we use f1 col for v2 tables due to schema pruning
                s.output.exists(_.find(_.argString(maxFields = 100).contains("f1")).isDefined)
              case _ => false
            }
          assert(scanOption.isDefined)
          scanOption.get
        }

        def getDriverMetrics(plan: SparkPlan, key: String): Option[SQLMetric] = plan match {
          case fs: FileSourceScanExec => fs.driverMetrics.get(key)
          case fs: FileSourceScanExecTransformer => fs.driverMetrics.get(key)
          case _ => None
        }

        // No dynamic partition pruning, so no static metrics
        // All files in fact table are scanned
        val df1 = sql("SELECT sum(f1) FROM fact")
        df1.collect()
        val scan1 = getFactScan(df1.queryExecution.executedPlan)
        assert(!scan1.metrics.contains("staticFilesNum"))
        assert(!scan1.metrics.contains("staticFilesSize"))

        val allFilesNum: Long = getDriverMetrics(scan1, "numFiles").map(_.value).getOrElse(-1)
        val allFilesSize: Long = getDriverMetrics(scan1, "filesSize").map(_.value).getOrElse(-1)
        val allPartitions: Long =
          getDriverMetrics(scan1, "numPartitions").map(_.value).getOrElse(-1)
        assert(allPartitions === numPartitions)
        val pruningTimeVal1: Long =
          getDriverMetrics(scan1, "pruningTime").map(_.value).getOrElse(-1)
        assert(pruningTimeVal1 === 0)

        // No dynamic partition pruning, so no static metrics
        // Only files from fid = 5 partition are scanned
        val df2 = sql("SELECT sum(f1) FROM fact WHERE fid = 5")
        df2.collect()
        val scan2 = getFactScan(df2.queryExecution.executedPlan)
        assert(!scan2.metrics.contains("staticFilesNum"))
        assert(!scan2.metrics.contains("staticFilesSize"))
        val partFilesNum: Long = getDriverMetrics(scan2, "numFiles").map(_.value).getOrElse(-1)
        val partFilesSize: Long = getDriverMetrics(scan2, "filesSize").map(_.value).getOrElse(-1)
        val partPartitions: Long =
          getDriverMetrics(scan2, "numPartitions").map(_.value).getOrElse(-1)
        assert(0 < partFilesNum && partFilesNum < allFilesNum)
        assert(0 < partFilesSize && partFilesSize < allFilesSize)
        assert(partPartitions === 1)
        val pruningTimeVal2: Long =
          getDriverMetrics(scan2, "pruningTime").map(_.value).getOrElse(-1)
        assert(pruningTimeVal2 === 0)

        // Dynamic partition pruning is used
        // Static metrics are as-if reading the whole fact table
        // "Regular" metrics are as-if reading only the "fid = 5" partition
        val df3 = sql("SELECT sum(f1) FROM fact, dim WHERE fid = did AND d1 = 6")
        df3.collect()
        val scan3 = getFactScan(df3.queryExecution.executedPlan)
        val staticFilesNumVal: Long =
          getDriverMetrics(scan3, "staticFilesNum").map(_.value).getOrElse(-1)
        val staticFilesSizeVal: Long =
          getDriverMetrics(scan3, "staticFilesSize").map(_.value).getOrElse(-1)
        val numFilesVal: Long = getDriverMetrics(scan3, "numFiles").map(_.value).getOrElse(-1)
        val filesSizeVal: Long = getDriverMetrics(scan3, "filesSize").map(_.value).getOrElse(-1)
        val numPartitionsVal: Long =
          getDriverMetrics(scan3, "numPartitions").map(_.value).getOrElse(-1)
        val pruningTimeVal3: Long =
          getDriverMetrics(scan3, "pruningTime").map(_.value).getOrElse(-1)
        assert(staticFilesNumVal == allFilesNum)
        assert(staticFilesSizeVal == allFilesSize)
        assert(numFilesVal == partFilesNum)
        assert(filesSizeVal == partFilesSize)
        assert(numPartitionsVal === 1)
        assert(pruningTimeVal3 > -1)
      }
    }
  }
}

class GlutenDynamicPartitionPruningV1SuiteAEOff
  extends GlutenDynamicPartitionPruningV1Suite
  with DisableAdaptiveExecutionSuite {

  import testImplicits._

  testGluten(
    "override static scan metrics",
    DisableAdaptiveExecution("DPP in AQE must reuse broadcast")) {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      //      "spark.gluten.enabled" -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false"
    ) {
      withTable("fact", "dim") {
        val numPartitions = 10

        spark
          .range(10)
          .map(x => Tuple3(x, x + 1, 0))
          .toDF("did", "d1", "d2")
          .write
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("dim")

        spark
          .range(100)
          .map(x => Tuple2(x, x % numPartitions))
          .toDF("f1", "fid")
          .write
          .partitionBy("fid")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("fact")

        def getFactScan(plan: SparkPlan): SparkPlan = {
          val scanOption =
            find(plan) {
              case s: FileSourceScanExecTransformer =>
                s.output.exists(_.find(_.argString(maxFields = 100).contains("fid")).isDefined)
              case s: FileSourceScanExec =>
                s.output.exists(_.find(_.argString(maxFields = 100).contains("fid")).isDefined)
              case s: BatchScanExecTransformer =>
                // we use f1 col for v2 tables due to schema pruning
                s.output.exists(_.find(_.argString(maxFields = 100).contains("f1")).isDefined)
              case s: BatchScanExec =>
                // we use f1 col for v2 tables due to schema pruning
                s.output.exists(_.find(_.argString(maxFields = 100).contains("f1")).isDefined)
              case _ => false
            }
          assert(scanOption.isDefined)
          scanOption.get
        }

        def getDriverMetrics(plan: SparkPlan, key: String): Option[SQLMetric] = plan match {
          case fs: FileSourceScanExec => fs.driverMetrics.get(key)
          case fs: FileSourceScanExecTransformer => fs.driverMetrics.get(key)
          case _ => None
        }

        // No dynamic partition pruning, so no static metrics
        // All files in fact table are scanned
        val df1 = sql("SELECT sum(f1) FROM fact")
        df1.collect()
        val scan1 = getFactScan(df1.queryExecution.executedPlan)
        assert(!scan1.metrics.contains("staticFilesNum"))
        assert(!scan1.metrics.contains("staticFilesSize"))
        val allFilesNum: Long = getDriverMetrics(scan1, "numFiles").map(_.value).getOrElse(-1)
        val allFilesSize: Long = getDriverMetrics(scan1, "filesSize").map(_.value).getOrElse(-1)
        val allPartitions: Long =
          getDriverMetrics(scan1, "numPartitions").map(_.value).getOrElse(-1)
        assert(allPartitions === numPartitions)
        val pruningTimeVal1: Long =
          getDriverMetrics(scan1, "pruningTime").map(_.value).getOrElse(-1)
        assert(pruningTimeVal1 === 0)

        // No dynamic partition pruning, so no static metrics
        // Only files from fid = 5 partition are scanned
        val df2 = sql("SELECT sum(f1) FROM fact WHERE fid = 5")
        df2.collect()
        val scan2 = getFactScan(df2.queryExecution.executedPlan)
        assert(!scan2.metrics.contains("staticFilesNum"))
        assert(!scan2.metrics.contains("staticFilesSize"))
        val partFilesNum: Long = getDriverMetrics(scan2, "numFiles").map(_.value).getOrElse(-1)
        val partFilesSize: Long = getDriverMetrics(scan2, "filesSize").map(_.value).getOrElse(-1)
        val partPartitions: Long =
          getDriverMetrics(scan2, "numPartitions").map(_.value).getOrElse(-1)
        assert(0 < partFilesNum && partFilesNum < allFilesNum)
        assert(0 < partFilesSize && partFilesSize < allFilesSize)
        assert(partPartitions === 1)
        val pruningTimeVal2: Long =
          getDriverMetrics(scan2, "pruningTime").map(_.value).getOrElse(-1)
        assert(pruningTimeVal2 === 0)

        // Dynamic partition pruning is used
        // Static metrics are as-if reading the whole fact table
        // "Regular" metrics are as-if reading only the "fid = 5" partition
        val df3 = sql("SELECT sum(f1) FROM fact, dim WHERE fid = did AND d1 = 6")
        df3.collect()
        val scan3 = getFactScan(df3.queryExecution.executedPlan)
        val staticFilesNumVal: Long =
          getDriverMetrics(scan3, "staticFilesNum").map(_.value).getOrElse(-1)
        val staticFilesSizeVal: Long =
          getDriverMetrics(scan3, "staticFilesSize").map(_.value).getOrElse(-1)
        val numFilesVal: Long = getDriverMetrics(scan3, "numFiles").map(_.value).getOrElse(-1)
        val filesSizeVal: Long = getDriverMetrics(scan3, "filesSize").map(_.value).getOrElse(-1)
        val numPartitionsVal: Long =
          getDriverMetrics(scan3, "numPartitions").map(_.value).getOrElse(-1)
        val pruningTimeVal3: Long =
          getDriverMetrics(scan3, "pruningTime").map(_.value).getOrElse(-1)
        assert(staticFilesNumVal == allFilesNum)
        assert(staticFilesSizeVal == allFilesSize)
        assert(numFilesVal == partFilesNum)
        assert(filesSizeVal == partFilesSize)
        assert(numPartitionsVal === 1)
        assert(pruningTimeVal3 > -1)
      }
    }
  }
}

class GlutenDynamicPartitionPruningV1SuiteAEOn
  extends GlutenDynamicPartitionPruningV1Suite
  with EnableAdaptiveExecutionSuite {

  testGluten("SPARK-39447: Avoid AssertionError in AdaptiveSparkPlanExec.doExecuteBroadcast") {
    val df = sql("""
                   |WITH empty_result AS (
                   |  SELECT * FROM fact_stats WHERE product_id < 0
                   |)
                   |SELECT *
                   |FROM   (SELECT /*+ SHUFFLE_MERGE(fact_sk) */ empty_result.store_id
                   |        FROM   fact_sk
                   |               JOIN empty_result
                   |                 ON fact_sk.product_id = empty_result.product_id) t2
                   |       JOIN empty_result
                   |         ON t2.store_id = empty_result.store_id
      """.stripMargin)

    checkPartitionPruningPredicate(df, false, false)
    checkAnswer(df, Nil)
  }

  testGluten(
    "SPARK-37995: PlanAdaptiveDynamicPruningFilters should use prepareExecutedPlan " +
      "rather than createSparkPlan to re-plan subquery") {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false"
    ) {
      val df = sql("""
                     |SELECT f.date_id, f.store_id FROM fact_sk f
                     |JOIN dim_store s ON f.store_id = s.store_id AND s.country = 'NL'
                     |WHERE s.state_province != (SELECT max(state_province) FROM dim_stats)
        """.stripMargin)

      checkPartitionPruningPredicate(df, true, false)
      checkAnswer(df, Row(1000, 1) :: Row(1010, 2) :: Row(1020, 2) :: Nil)
    }
  }

  testGluten("Filter with DynamicPruningExpression") {
    withTable("fact_stats_non_partition") {
      spark
        .table("fact_stats")
        .write
        .format(tableFormat)
        .saveAsTable("fact_stats_non_partition")

      withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        val df = sql("""
                       |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM (
                       | select * from fact_stats
                       | union all
                       | select * from fact_stats_non_partition
                       |) f
                       |JOIN dim_stats s
                       |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)
        checkAnswer(
          df,
          Row(1030, 2, 10, 3) ::
            Row(1040, 2, 50, 3) ::
            Row(1050, 2, 50, 3) ::
            Row(1060, 2, 50, 3) ::
            Row(1030, 2, 10, 3) ::
            Row(1040, 2, 50, 3) ::
            Row(1050, 2, 50, 3) ::
            Row(1060, 2, 50, 3) :: Nil
        )
        val filters = collect(df.queryExecution.executedPlan) { case f: FilterExec => f }
        assert(filters.isEmpty)
        val filterTransformerWithDPPs = collect(df.queryExecution.executedPlan) {
          case f: FilterExecTransformerBase
              if f.cond.exists(_.isInstanceOf[DynamicPruningExpression]) =>
            f
        }
        assert(filterTransformerWithDPPs.nonEmpty)
      }
    }
  }
}

abstract class GlutenDynamicPartitionPruningV2Suite extends GlutenDynamicPartitionPruningSuiteBase {
  override protected def runAnalyzeColumnCommands: Boolean = false

  override protected def initState(): Unit = {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.defaultCatalog", "testcat")
  }
}

class GlutenDynamicPartitionPruningV2SuiteAEOff
  extends GlutenDynamicPartitionPruningV2Suite
  with DisableAdaptiveExecutionSuite

class GlutenDynamicPartitionPruningV2SuiteAEOn
  extends GlutenDynamicPartitionPruningV2Suite
  with EnableAdaptiveExecutionSuite

// Test DPP with file scan disabled by user for some reason, which can also mock the situation
// that scan is not transformable.
class GlutenDynamicPartitionPruningV1SuiteAEOnDisableScan
  extends GlutenDynamicPartitionPruningV1SuiteAEOn {
  override def sparkConf: SparkConf = {
    super.sparkConf.set(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")
  }
}

// Same as above except AQE is off.
class GlutenDynamicPartitionPruningV1SuiteAEOffDisableScan
  extends GlutenDynamicPartitionPruningV2SuiteAEOff {
  override def sparkConf: SparkConf = {
    super.sparkConf.set(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")
  }
}

// Test DPP with batch scan disabled by user for some reason, which can also mock the situation
// that scan is not transformable.
class GlutenDynamicPartitionPruningV2SuiteAEOnDisableScan
  extends GlutenDynamicPartitionPruningV2SuiteAEOn {
  override def sparkConf: SparkConf = {
    super.sparkConf.set(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key, "false")
  }
}

// Same as above except AQE is off.
class GlutenDynamicPartitionPruningV2SuiteAEOffDisableScan
  extends GlutenDynamicPartitionPruningV2SuiteAEOff {
  override def sparkConf: SparkConf = {
    super.sparkConf.set(GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key, "false")
  }
}
