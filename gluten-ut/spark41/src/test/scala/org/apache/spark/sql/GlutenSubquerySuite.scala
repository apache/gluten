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

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, ExecSubqueryExpression, ReusedSubqueryExec, ScalarSubquery}
import org.apache.spark.sql.internal.SQLConf

class GlutenSubquerySuite extends SubquerySuite with GlutenSQLTestsTrait {

  import testImplicits._

  // Test Canceled: IntegratedUDFTestUtils.shouldTestPythonUDFs was false
  override def testNameBlackList: Seq[String] = Seq(
    "SPARK-28441: COUNT bug in WHERE clause (Filter) with PythonUDF",
    "SPARK-28441: COUNT bug in SELECT clause (Project) with PythonUDF",
    "SPARK-28441: COUNT bug in Aggregate with PythonUDF",
    "SPARK-28441: COUNT bug negative examples with PythonUDF",
    "SPARK-28441: COUNT bug in nested subquery with PythonUDF",
    "SPARK-28441: COUNT bug with nasty predicate expr with PythonUDF",
    "SPARK-28441: COUNT bug in HAVING clause (Filter) with PythonUDF",
    "SPARK-28441: COUNT bug with attribute ref in subquery input and output with PythonUDF"
  )

  testGluten("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source") {
    withTable("a", "b") {
      spark.range(4).selectExpr("id", "id % 2 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(2).write.saveAsTable("b")

      val df = sql("SELECT * FROM a WHERE p <= (SELECT MIN(id) FROM b)")
      checkAnswer(df, Seq(Row(0, 0), Row(2, 0)))
      // need to execute the query before we can examine fs.inputRDDs()
      val fileSourceScanExec = collect(stripAQEPlan(df.queryExecution.executedPlan)) {
        case fs: FileSourceScanExecTransformer => fs
      }
      assert(fileSourceScanExec.size === 1)
      val scan = fileSourceScanExec.head
      assert(scan.partitionFilters.exists(ExecSubqueryExpression.hasSubquery))
      val selectedPartitions = scan.dynamicallySelectedPartitions.toPartitionArray
      assert(selectedPartitions.nonEmpty)
      assert(selectedPartitions.forall(_.filePath.toString.contains("p=0")))
    }
  }

  testGluten("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 USING parquet AS SELECT id AS a, id AS b, id AS c FROM range(10)")
      sql("CREATE TABLE t2 USING parquet AS SELECT id AS x, id AS y FROM range(8)")
      val df = sql(
        """
          |SELECT *
          |FROM   t1
          |WHERE  a IN (SELECT x
          |             FROM   (SELECT x AS x,
          |                            RANK() OVER (PARTITION BY x ORDER BY SUM(y) DESC) AS ranking
          |                     FROM   t2
          |                     GROUP  BY x) tmp1
          |             WHERE  ranking <= 5)
          |""".stripMargin)

      df.collect()
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: ColumnarShuffleExchangeExec => s
      }
      assert(exchanges.size === 1)
    }
  }

  testGluten(
    "SPARK-43402: FileSourceScanExec supports push down data filter with scalar subquery") {
    def checkFileSourceScan(query: String, answer: Seq[Row]): Unit = {
      val df = sql(query)
      checkAnswer(df, answer)
      val fileSourceScanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      sparkContext.listenerBus.waitUntilEmpty()
      assert(fileSourceScanExec.size === 1)
      val scalarSubquery = fileSourceScanExec.head.dataFilters.flatMap(_.collect {
        case s: ScalarSubquery => s
      })
      assert(scalarSubquery.length === 1)
      assert(scalarSubquery.head.plan.isInstanceOf[ReusedSubqueryExec])
      assert(fileSourceScanExec.head.metrics("numFiles").value === 1)
      assert(fileSourceScanExec.head.metrics("numOutputRows").value === answer.size)
    }

    withTable("t1", "t2") {
      withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
        Seq(1, 2, 3).toDF("c1").write.format("parquet").saveAsTable("t1")
        Seq(4, 5, 6).toDF("c2").write.format("parquet").saveAsTable("t2")

        checkFileSourceScan(
          "SELECT * FROM t1 WHERE c1 > (SELECT min(c2) FROM t2)",
          Seq.empty)
        checkFileSourceScan(
          "SELECT * FROM t1 WHERE c1 < (SELECT min(c2) FROM t2)",
          Row(1) :: Row(2) :: Row(3) :: Nil)
      }
    }
  }
}
