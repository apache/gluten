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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.EmptyRelationExecTransformer
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

/**
 * Test suite for EmptyRelationExecTransformer.
 *
 * EmptyRelationExec is a leaf node AQE creates (Spark 4.0+) when it proves a subtree produces no
 * output. Gluten offloads it to EmptyRelationExecTransformer so that surrounding columnar operators
 * do not need ColumnarToRow / RowToColumnar transitions around the empty relation.
 *
 * Empty-result correctness is asserted on every supported Spark version; the plan-shape assertions
 * that depend on EmptyRelationExec are gated to Spark 4.0+, where the node exists. The raw node is
 * detected through `SparkShims.isEmptyRelationExec` so this suite does not reference a class that
 * is absent on Spark 3.x.
 */
class VeloxEmptyRelationSuite extends VeloxWholeStageTransformerSuite with AdaptiveSparkPlanHelper {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_EMPTY_RELATION_ENABLED.key, "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  private def countTransformers(plan: org.apache.spark.sql.execution.SparkPlan): Int =
    collectWithSubqueries(plan) { case _: EmptyRelationExecTransformer => true }.size

  private def countRawEmptyRelations(plan: org.apache.spark.sql.execution.SparkPlan): Int =
    collectWithSubqueries(plan) {
      case p if SparkShimLoader.getSparkShims.isEmptyRelationExec(p) => true
    }.size

  // --- Empty-result correctness (all supported Spark versions) --------------

  test("WHERE 1=0 produces empty result") {
    val df = spark.sql("SELECT l_orderkey, l_partkey FROM lineitem WHERE 1 = 0")
    assert(df.collect().isEmpty, "Expected empty result for WHERE 1=0")
  }

  test("empty UNION ALL produces empty result") {
    val df = spark.sql("""SELECT l_orderkey FROM lineitem WHERE 1 = 0
                         |UNION ALL
                         |SELECT l_orderkey FROM lineitem WHERE 1 = 0""".stripMargin)
    assert(df.collect().isEmpty)
  }

  test("empty result preserves string-column schema") {
    val df = spark.sql("SELECT l_returnflag, l_linestatus, l_comment FROM lineitem WHERE 1 = 0")
    assert(df.collect().isEmpty)
    assert(df.schema.fieldNames.toSeq == Seq("l_returnflag", "l_linestatus", "l_comment"))
  }

  test("empty result preserves mixed-type schema") {
    val df = spark.sql("""SELECT CAST(1 AS BOOLEAN) AS b,
                         |       CAST(1 AS INT) AS i,
                         |       CAST(1 AS BIGINT) AS l,
                         |       CAST(1.0 AS DOUBLE) AS d,
                         |       'x' AS str,
                         |       CAST(1.00 AS DECIMAL(10,2)) AS dec
                         |WHERE 1 = 0""".stripMargin)
    assert(df.collect().isEmpty)
    assert(df.schema.fields.length == 6)
  }

  test("AQE empty propagation through inner join") {
    val df = spark.sql("""SELECT t1.l_orderkey, t2.l_partkey
                         |FROM (SELECT * FROM lineitem WHERE 1 = 0) t1
                         |INNER JOIN lineitem t2 ON t1.l_orderkey = t2.l_orderkey""".stripMargin)
    assert(df.collect().isEmpty)
  }

  test("AQE empty propagation through aggregation") {
    val df = spark.sql("""SELECT l_returnflag, sum(l_quantity) AS total
                         |FROM lineitem
                         |WHERE 1 = 0
                         |GROUP BY l_returnflag""".stripMargin)
    assert(df.collect().isEmpty)
  }

  test("empty result matches vanilla Spark") {
    val query = "SELECT l_orderkey, l_partkey, l_quantity FROM lineitem WHERE 1 = 0"
    var vanillaResult: Seq[Row] = Seq.empty
    withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
      vanillaResult = spark.sql(query).collect().toSeq
    }
    assert(vanillaResult.isEmpty)
    checkAnswer(spark.sql(query), vanillaResult)
  }

  // --- Plan-shape verification (Spark 4.0+, where EmptyRelationExec exists) --

  test("EmptyRelationExec is offloaded to the columnar transformer") {
    assume(isSparkVersionGE("4.0"))
    // A statically-empty predicate (e.g. WHERE 1 = 0) is folded to an empty LocalRelation by the
    // logical optimizer and never becomes an EmptyRelationExec. That node is produced by AQE's
    // Propagate Empty Relations optimization when a materialized query stage turns out to be empty
    // at runtime, so drive it through an AQE INTERSECT whose left side is empty only at runtime.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df =
        spark.sql("SELECT * FROM lineitem WHERE l_orderkey < 0 INTERSECT SELECT * FROM lineitem")
      assert(df.collect().isEmpty)
      val plan = df.queryExecution.executedPlan
      assert(
        countTransformers(plan) > 0,
        "Expected EmptyRelationExecTransformer in plan:\n" + plan.treeString)
      assert(
        countRawEmptyRelations(plan) == 0,
        "EmptyRelationExec should be fully offloaded to the transformer:\n" + plan.treeString)
    }
  }

  test("EmptyRelationExec is not offloaded when the config is disabled") {
    assume(isSparkVersionGE("4.0"))
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_EMPTY_RELATION_ENABLED.key -> "false") {
      val df =
        spark.sql("SELECT * FROM lineitem WHERE l_orderkey < 0 INTERSECT SELECT * FROM lineitem")
      assert(df.collect().isEmpty)
      val plan = df.queryExecution.executedPlan
      assert(
        countTransformers(plan) == 0,
        "Transformer must not appear when the config is disabled:\n" + plan.treeString)
    }
  }

  test("EmptyRelationExec offload works under AQE runtime propagation") {
    assume(isSparkVersionGE("4.0"))
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val df = spark.sql("""SELECT l.l_orderkey, r.l_partkey
                           |FROM lineitem l
                           |INNER JOIN (
                           |  SELECT l_orderkey, l_partkey
                           |  FROM lineitem
                           |  WHERE l_orderkey = -999999
                           |) r ON l.l_orderkey = r.l_orderkey""".stripMargin)
      assert(df.collect().isEmpty, "Expected empty result from join with impossible predicate")
    }
  }
}
