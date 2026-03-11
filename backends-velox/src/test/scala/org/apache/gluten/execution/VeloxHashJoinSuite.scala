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
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.{ColumnarSubqueryBroadcastExec, InputIteratorTransformer}

class VeloxHashJoinSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")

  test("generate hash join plan - v1") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      (GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true")
    ) {
      createTPCHNotNullTables()
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect { case shj: ShuffledHashJoinExecTransformer => shj }
      // scalastyle:off println
      System.out.println(plan)
      // scalastyle:on println line=68 column=19
      assert(joins.length == 2)

      // Children of Join should be seperated into different `TransformContext`s.
      assert(joins.forall(_.children.forall(_.isInstanceOf[InputIteratorTransformer])))

      // WholeStageTransformer should be inserted for joins and its children separately.
      val wholeStages = plan.collect { case wst: WholeStageTransformer => wst }
      assert(wholeStages.length == 5)

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: InputIteratorTransformer => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum
      assert(countSHJ == 2)
    }
  }

  test("generate hash join plan - v2") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "false"),
      (GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "true"),
      ("spark.sql.sources.useV1SourceList", "avro")
    ) {
      createTPCHNotNullTables()
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect { case shj: ShuffledHashJoinExecTransformer => shj }
      assert(joins.length == 2)

      // The computing is combined into one single whole stage transformer.
      val wholeStages = plan.collect { case wst: WholeStageTransformer => wst }

      if (
        SparkShimLoader.getSparkVersion.startsWith("3.3.") ||
        SparkShimLoader.getSparkVersion.startsWith("3.4.")
      ) {
        assert(wholeStages.length == 3)
      } else {
        assert(wholeStages.length == 5)
      }

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: InputIteratorTransformer => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum

      assert(countSHJ == 2)
    }
  }

  test("ColumnarBuildSideRelation transform support multiple key columns") {
    Seq("true", "false").foreach(
      enabledOffheapBroadcast =>
        withSQLConf(
          VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key ->
            enabledOffheapBroadcast) {
          withTable("t1", "t2") {
            val df1 =
              (0 until 50)
                .map(i => (i % 2, i % 3, s"${i % 25}"))
                .toDF("t1_c1", "t1_c2", "date")
                .as("df1")
            val df2 = (0 until 50)
              .map(i => (i % 11, i % 13, s"${i % 10}"))
              .toDF("t2_c1", "t2_c2", "date")
              .as("df2")
            df1.write.partitionBy("date").saveAsTable("t1")
            df2.write.partitionBy("date").saveAsTable("t2")

            val df = sql("""
                           |SELECT t1.date, t1.t1_c1, t2.t2_c2
                           |FROM t1
                           |JOIN t2 ON t1.date = t2.date
                           |WHERE t1.date=if(3 <= t2.t2_c2, if(3 < t2.t2_c1, 3, t2.t2_c1), t2.t2_c2)
                           |ORDER BY t1.date DESC, t1.t1_c1 DESC, t2.t2_c2 DESC
                           |LIMIT 1
                           |""".stripMargin)

            checkAnswer(df, Row("3", 1, 4) :: Nil)
            // collect the DPP plan.
            val subqueryBroadcastExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
              case subqueryBroadcast: ColumnarSubqueryBroadcastExec => subqueryBroadcast
            }
            assert(subqueryBroadcastExecs.size == 2)
            val buildKeysAttrs = subqueryBroadcastExecs
              .flatMap(_.buildKeys)
              .map(e => e.collect { case a: AttributeReference => a })
            // the buildKeys function can accept expressions with multiple columns.
            assert(buildKeysAttrs.exists(_.size > 1))
          }
        })
  }

  test("duplicate projections") {
    withTable("t1", "t2", "t3") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").write.saveAsTable("t1")
      Seq(1, 2, 3).toDF("c1").write.saveAsTable("t2")
      Seq(1, 2, 3).toDF("c1").write.saveAsTable("t3")
      // Test HashProbe.
      val q1 =
        """
          |select tt1.* from
          |(select c1,c2, c2 as a,c2 as b from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |""".stripMargin
      val q2 =
        """
          |select tt1.* from
          |(select c1, c2 as a,c2 as b from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |limit 1
          |""".stripMargin
      val q3 =
        """
          |select tt1.* from
          |(select c1, c2 as a,c2 as b from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |left join t3
          |on tt1.c1 = t3.c1
          |""".stripMargin
      val q4 =
        """
          |select tt1.* from
          |(select c1, c2, c2 from t1) tt1
          |left join t2
          |on tt1.c1 = t2.c1
          |""".stripMargin

      // Test FilterProject.
      val q5 =
        """
          |select c1, c2, a, b from
          |(select c1, c2, c2 as a, c2 as b, rand() as c from t1) tt1
          |where c > -1 and b > 1
          |""".stripMargin

      Seq(q1, q2, q3, q4, q5).foreach {
        runQueryAndCompare(_) { _ => }
      }
    }
  }

  test("Hash probe dynamic filter pushdown") {
    withSQLConf(
      VeloxConfig.HASH_PROBE_DYNAMIC_FILTER_PUSHDOWN_ENABLED.key -> "true",
      VeloxConfig.HASH_PROBE_BLOOM_FILTER_PUSHDOWN_MAX_SIZE.key -> "1048576"
    ) {
      withTable("probe_table", "build_table") {
        spark.sql("""
        CREATE TABLE probe_table USING PARQUET
        AS SELECT id as a FROM range(110001)
      """)

        spark.sql("""
        CREATE TABLE build_table USING PARQUET
        AS SELECT id * 1000 as b FROM range(220002)
      """)

        runQueryAndCompare(
          "SELECT a FROM probe_table JOIN build_table ON a = b"
        ) {
          df =>
            val join = find(df.queryExecution.executedPlan) {
              case _: BroadcastHashJoinExecTransformer => true
              case _ => false
            }
            assert(join.isDefined)
            val metrics = join.get.metrics
            assert(metrics.contains("bloomFilterBlocksByteSize"))
            assert(metrics("bloomFilterBlocksByteSize").value > 0)

            assert(metrics.contains("hashProbeDynamicFiltersProduced"))
            assert(metrics("hashProbeDynamicFiltersProduced").value == 1)
        }
      }
    }
  }

  test("Broadcast join preserves original cast expression in join keys") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "10MB"),
      ("spark.sql.adaptive.enabled", "false")
    ) {
      withTable("t1_int", "t2_long") {
        // Create table with INT column
        spark
          .range(100)
          .selectExpr("cast(id as int) as key", "id as value")
          .write
          .saveAsTable("t1_int")

        // Create table with LONG column
        spark.range(50).selectExpr("id as key", "id * 2 as value").write.saveAsTable("t2_long")

        // Join INT with LONG - Spark will insert cast(int to long) in join keys
        val query = """
          SELECT t1.key, t1.value, t2.value as value2
          FROM t1_int t1
          JOIN t2_long t2 ON t1.key = t2.key
          ORDER BY t1.key
        """

        runQueryAndCompare(query) {
          df =>
            // Check that broadcast join is used in Gluten execution
            val plan = df.queryExecution.executedPlan
            val broadcastJoins = plan.collect { case bhj: BroadcastHashJoinExecTransformer => bhj }
            assert(broadcastJoins.nonEmpty, "Should use broadcast hash join")
        }
      }
    }
  }

  test("Broadcast join with multiple cast expressions in join keys") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "10MB"),
      ("spark.sql.adaptive.enabled", "false")
    ) {
      withTable("t1_mixed", "t2_mixed") {
        // Create table with mixed types
        spark
          .range(100)
          .selectExpr("cast(id as int) as key1", "cast(id as short) as key2", "id as value")
          .write
          .saveAsTable("t1_mixed")

        // Create table with different types requiring casts
        spark
          .range(50)
          .selectExpr("id as key1", "cast(id as int) as key2", "id * 2 as value")
          .write
          .saveAsTable("t2_mixed")

        // Join with multiple keys requiring casts
        // key1: cast(int to long), key2: cast(short to int)
        val query = """
          SELECT t1.key1, t1.key2, t1.value, t2.value as value2
          FROM t1_mixed t1
          JOIN t2_mixed t2 ON t1.key1 = t2.key1 AND t1.key2 = t2.key2
          ORDER BY t1.key1, t1.key2
        """

        runQueryAndCompare(query) {
          df =>
            // Check that broadcast join is used in Gluten execution
            val plan = df.queryExecution.executedPlan
            val broadcastJoins = plan.collect { case bhj: BroadcastHashJoinExecTransformer => bhj }
            assert(broadcastJoins.nonEmpty, "Should use broadcast hash join")

            // Verify multiple join keys are handled correctly
            assert(broadcastJoins.head.leftKeys.length == 2)
            assert(broadcastJoins.head.rightKeys.length == 2)
        }
      }
    }
  }
}
