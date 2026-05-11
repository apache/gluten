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

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec}

import org.scalatest.DoNotDiscover

@DoNotDiscover
class GlutenClickHouseColumnarShuffleAQESuite extends MergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with ColumnarShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
  }

  final override val testCases: Seq[Int] = Seq(
    2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 16, 17, 19, 20, 21
  )
  setupTestCase()

  test("TPCH Q1") {
    customCheck(1) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])

        val colCustomShuffleReaderExecs = collect(df.queryExecution.executedPlan) {
          case csr: AQEShuffleReadExec => csr
        }
        assert(colCustomShuffleReaderExecs.size == 2)
        val coalescedPartitionSpec0 = colCustomShuffleReaderExecs.head.partitionSpecs.head
          .asInstanceOf[CoalescedPartitionSpec]
        assert(coalescedPartitionSpec0.startReducerIndex == 0)
        assert(coalescedPartitionSpec0.endReducerIndex == 4)
        val coalescedPartitionSpec1 = colCustomShuffleReaderExecs(1).partitionSpecs.head
          .asInstanceOf[CoalescedPartitionSpec]
        assert(coalescedPartitionSpec1.startReducerIndex == 0)
        assert(coalescedPartitionSpec1.endReducerIndex == 5)
    }
  }

  test("TPCH Q11") {
    customCheck(11) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q15") {
    customCheck(15) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 2)
    }
  }

  test("TPCH Q18") {
    customCheck(18) {
      df =>
        val hashAggregates = collect(df.queryExecution.executedPlan) {
          case hash: HashAggregateExecBaseTransformer => hash
        }
        assert(hashAggregates.size == 3)
    }
  }

  test("TPCH Q22") {
    customCheck(22) {
      df =>
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
        val adaptiveSparkPlanExec = collectWithSubqueries(df.queryExecution.executedPlan) {
          case adaptive: AdaptiveSparkPlanExec => adaptive
        }
        assert(adaptiveSparkPlanExec.size == 3)
        assert(adaptiveSparkPlanExec(1) == adaptiveSparkPlanExec(2))
    }
  }

  test("GLUTEN-6768 rerorder hash join") {
    withSQLConf(
      (CHConfig.prefixOf("enable_reorder_hash_join_tables"), "true"),
      ("spark.sql.adaptive.enabled", "true")) {
      spark.sql("create table t1(a int, b int) using parquet")
      spark.sql("create table t2(a int, b int) using parquet")

      spark.sql("insert into t1 select id as a, id as b from range(100000)")
      spark.sql("insert into t1 select id as a, id as b from range(100)")

      def isExpectedJoinNode(plan: SparkPlan, joinType: JoinType, buildSide: BuildSide): Boolean = {
        plan match {
          case join: CHShuffledHashJoinExecTransformer =>
            join.joinType == joinType && join.buildSide == buildSide
          case _ => false
        }
      }

      def collectExpectedJoinNode(
          plan: SparkPlan,
          joinType: JoinType,
          buildSide: BuildSide): Seq[SparkPlan] = {
        if (isExpectedJoinNode(plan, joinType, buildSide)) {
          Seq(plan) ++ plan.children.flatMap(collectExpectedJoinNode(_, joinType, buildSide))
        } else {
          plan.children.flatMap(collectExpectedJoinNode(_, joinType, buildSide))
        }
      }

      var sql =
        """
          |select * from t2 left join t1 on t1.a = t2.a
          |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            val joins = df.queryExecution.executedPlan.collect {
              case adpativeNode: AdaptiveSparkPlanExec =>
                collectExpectedJoinNode(adpativeNode.executedPlan, RightOuter, BuildRight)
              case _ => Seq()
            }
            assert(joins.size == 1)
        }
      )

      sql = """
              |select * from t2 right join t1 on t1.a = t2.a
              |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            val joins = df.queryExecution.executedPlan.collect {
              case adpativeNode: AdaptiveSparkPlanExec =>
                collectExpectedJoinNode(adpativeNode.executedPlan, LeftOuter, BuildRight)
              case _ => Seq()
            }
            assert(joins.size == 1)
        }
      )

      sql = """
              |select * from t1 right join t2 on t1.a = t2.a
              |""".stripMargin
      compareResultsAgainstVanillaSpark(
        sql,
        true,
        {
          df =>
            val joins = df.queryExecution.executedPlan.collect {
              case adpativeNode: AdaptiveSparkPlanExec =>
                collectExpectedJoinNode(adpativeNode.executedPlan, RightOuter, BuildRight)
              case _ => Seq()
            }
            assert(joins.size == 1)
        }
      )

      spark.sql("drop table t1")
      spark.sql("drop table t2")
    }
  }

  test("GLUTEN-6768 change mixed join condition into multi join on clauses") {
    withSQLConf(
      (CHConfig.runtimeConfig("prefer_multi_join_on_clauses"), "true"),
      (CHConfig.runtimeConfig("multi_join_on_clauses_build_side_row_limit"), "1000000")
    ) {

      spark.sql("create table t1(a int, b int, c int, d int) using parquet")
      spark.sql("create table t2(a int, b int, c int, d int) using parquet")

      spark.sql("""
                  |insert into t1
                  |select id % 2 as a, id as b, id + 1 as c, id + 2 as d from range(1000)
                  |""".stripMargin)
      spark.sql("""
                  |insert into t2
                  |select id % 2 as a, id as b, id + 1 as c, id + 2 as d from range(1000)
                  |""".stripMargin)

      var sql = """
                  |select * from t1 join t2 on
                  |t1.a = t2.a and (t1.b = t2.b or t1.c = t2.c or t1.d = t2.d)
                  |order by t1.a, t1.b, t1.c, t1.d
                  |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })

      sql = """
              |select * from t1 join t2 on
              |t1.a = t2.a and (t1.b = t2.b or t1.c = t2.c or (t1.c = t2.c and t1.d = t2.d))
              |order by t1.a, t1.b, t1.c, t1.d
              |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })

      sql = """
              |select * from t1 join t2 on
              |t1.a = t2.a and (t1.b = t2.b or t1.c = t2.c or (t1.d = t2.d and t1.c >= t2.c))
              |order by t1.a, t1.b, t1.c, t1.d
              |""".stripMargin
      compareResultsAgainstVanillaSpark(sql, true, { _ => })

      spark.sql("drop table t1")
      spark.sql("drop table t2")
    }
  }

  test("GLUTEN-2221 empty hash aggregate exec") {
    val sql1 =
      """
        | select count(1) from (
        |   select (c/all_pv)/d as t from (
        |     select t0.*, t1.b pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t2 join(
        |     select sum(t1.b) all_pv from (
        |       select * from values (1,2,2,1), (2,3,4,1), (3,4,6,1) as data(a,b,c,d)
        |     ) as t0 join (
        |       select * from values(1,5),(2,5),(2,6) as data(a,b)
        |     ) as t1
        |     on t0.a = t1.a
        |   ) t3
        | )""".stripMargin
    compareResultsAgainstVanillaSpark(sql1, true, { _ => })
  }
}
