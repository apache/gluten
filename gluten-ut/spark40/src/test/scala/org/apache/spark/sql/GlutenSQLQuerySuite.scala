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

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.internal.SQLConf

class GlutenSQLQuerySuite extends SQLQuerySuite with GlutenSQLTestsTrait {
  import testImplicits._

  testGluten("SPARK-28156: self-join should not miss cached view") {
    withTable("table1") {
      withView("table1_vw") {
        withTempView("cachedview") {
          val df = Seq.tabulate(5)(x => (x, x + 1, x + 2, x + 3)).toDF("a", "b", "c", "d")
          df.write.mode("overwrite").format("parquet").saveAsTable("table1")
          sql("drop view if exists table1_vw")
          sql("create view table1_vw as select * from table1")

          val cachedView = sql("select a, b, c, d from table1_vw")

          cachedView.createOrReplaceTempView("cachedview")
          cachedView.persist()

          val queryDf = sql(s"""select leftside.a, leftside.b
                               |from cachedview leftside
                               |join cachedview rightside
                               |on leftside.a = rightside.a
          """.stripMargin)

          val inMemoryTableScan = collect(queryDf.queryExecution.executedPlan) {
            case i: InMemoryTableScanExec => i
          }
          assert(inMemoryTableScan.size == 2)
          checkAnswer(queryDf, Row(0, 1) :: Row(1, 2) :: Row(2, 3) :: Row(3, 4) :: Row(4, 5) :: Nil)
        }
      }
    }

  }

  // Velox throw exception : An unsupported nested encoding was found.
  ignore(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-33338: GROUP BY using literal map should not fail") {
    withTable("t") {
      withTempDir {
        dir =>
          sql(
            s"CREATE TABLE t USING PARQUET LOCATION '${dir.toURI}' AS SELECT map('k1', 'v1') m," +
              s" 'k1' k")
          Seq(
            "SELECT map('k1', 'v1')[k] FROM t GROUP BY 1",
            "SELECT map('k1', 'v1')[k] FROM t GROUP BY map('k1', 'v1')[k]",
            "SELECT map('k1', 'v1')[k] a FROM t GROUP BY a"
          ).foreach(statement => checkAnswer(sql(statement), Row("v1")))
      }
    }
  }

  testGluten("SPARK-33593: Vector reader got incorrect data with binary partition value") {
    Seq("false").foreach(
      value => {
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> value) {
          withTable("t1") {
            sql("""CREATE TABLE t1(name STRING, id BINARY, part BINARY)
                  |USING PARQUET PARTITIONED BY (part)""".stripMargin)
            sql("INSERT INTO t1 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
            checkAnswer(
              sql("SELECT name, cast(id as string), cast(part as string) FROM t1"),
              Row("a", "Spark SQL", "Spark SQL"))
          }
        }

        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
          withTable("t2") {
            sql("""CREATE TABLE t2(name STRING, id BINARY, part BINARY)
                  |USING PARQUET PARTITIONED BY (part)""".stripMargin)
            sql("INSERT INTO t2 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
            checkAnswer(
              sql("SELECT name, cast(id as string), cast(part as string) FROM t2"),
              Row("a", "Spark SQL", "Spark SQL"))
          }
        }
      })
  }

  testGluten(
    "SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar") {
    withTempView("df") {
      Seq("m@ca").toDF("s").createOrReplaceTempView("df")

      val e = intercept[SparkException] {
        sql("SELECT s LIKE 'm%@ca' ESCAPE '%' FROM df").collect()
      }
      assert(
        e.getMessage.contains(
          "Escape character must be followed by '%', '_' or the escape character itself"))

      checkAnswer(sql("SELECT s LIKE 'm@@ca' ESCAPE '@' FROM df"), Row(true))
    }
  }

  testGluten("the escape character is not allowed to end with") {
    withTempView("df") {
      Seq("jialiuping").toDF("a").createOrReplaceTempView("df")

      val e = intercept[SparkException] {
        sql("SELECT a LIKE 'jialiuping%' ESCAPE '%' FROM df").collect()
      }
      assert(
        e.getMessage.contains(
          "Escape character must be followed by '%', '_' or the escape character itself"))
    }
  }

  ignoreGluten("StreamingQueryProgress.numInputRows should be correct") {
    withTempDir {
      dir =>
        val path = dir.toURI.getPath
        val numRows = 20
        val df = spark.range(0, numRows)
        df.write.mode("overwrite").format("parquet").save(path)
        val q = spark.readStream
          .format("parquet")
          .schema(df.schema)
          .load(path)
          .writeStream
          .format("memory")
          .queryName("test")
          .start()
        q.processAllAvailable
        val inputOutputPairs = q.recentProgress.map(p => (p.numInputRows, p.sink.numOutputRows))

        // numInputRows and sink.numOutputRows must be the same
        assert(inputOutputPairs.forall(x => x._1 == x._2))

        // Sum of numInputRows must match the total number of rows of the input
        assert(inputOutputPairs.map(_._1).sum == numRows)
    }
  }

  testGluten("SPARK-47939: Explain should work with parameterized queries") {
    def checkQueryPlan(df: DataFrame, plan: String): Unit = assert(
      df.collect()
        .map(_.getString(0))
        .map(_.replaceAll("#[0-9]+", "#N"))
        // Remove the backend keyword in c2r/r2c.
        .map(_.replaceAll("[A-Za-z]*ColumnarToRow", "ColumnarToRow"))
        .map(_.replaceAll("RowTo[A-Za-z]*Columnar", "RowToColumnar"))
        === Array(plan.stripMargin)
    )

    checkQueryPlan(
      spark.sql("explain select ?", Array(1)),
      """== Physical Plan ==
        |ColumnarToRow
        |+- ^(1) ProjectExecTransformer [1 AS 1#N]
        |   +- ^(1) InputIteratorTransformer[]
        |      +- RowToColumnar
        |         +- *(1) Scan OneRowRelation[]
        |
        |"""
    )
    checkQueryPlan(
      spark.sql("explain select :first", Map("first" -> 1)),
      """== Physical Plan ==
        |ColumnarToRow
        |+- ^(1) ProjectExecTransformer [1 AS 1#N]
        |   +- ^(1) InputIteratorTransformer[]
        |      +- RowToColumnar
        |         +- *(1) Scan OneRowRelation[]
        |
        |"""
    )

    checkQueryPlan(
      spark.sql("explain explain explain select ?", Array(1)),
      """== Physical Plan ==
        |Execute ExplainCommand
        |   +- ExplainCommand ExplainCommand 'PosParameterizedQuery [1], SimpleMode, SimpleMode

        |"""
    )
    checkQueryPlan(
      spark.sql("explain explain explain select :first", Map("first" -> 1)),
      // scalastyle:off
      """== Physical Plan ==
        |Execute ExplainCommand
        |   +- ExplainCommand ExplainCommand 'NameParameterizedQuery [first], [1], SimpleMode, SimpleMode

        |"""
      // scalastyle:on
    )

    checkQueryPlan(
      spark.sql("explain describe select ?", Array(1)),
      """== Physical Plan ==
        |Execute DescribeQueryCommand
        |   +- DescribeQueryCommand select ?

        |"""
    )
    checkQueryPlan(
      spark.sql("explain describe select :first", Map("first" -> 1)),
      """== Physical Plan ==
        |Execute DescribeQueryCommand
        |   +- DescribeQueryCommand select :first

        |"""
    )

    checkQueryPlan(
      spark.sql("explain extended select * from values (?, ?) t(x, y)", Array(1, "a")),
      """== Parsed Logical Plan ==
        |'PosParameterizedQuery [1, a]
        |+- 'Project [*]
        |   +- 'SubqueryAlias t
        |      +- 'UnresolvedInlineTable [x, y], [[posparameter(39), posparameter(42)]]

        |== Analyzed Logical Plan ==
        |x: int, y: string
        |Project [x#N, y#N]
        |+- SubqueryAlias t
        |   +- LocalRelation [x#N, y#N]

        |== Optimized Logical Plan ==
        |LocalRelation [x#N, y#N]

        |== Physical Plan ==
        |LocalTableScan [x#N, y#N]
        |"""
    )
    checkQueryPlan(
      spark.sql(
        "explain extended select * from values (:first, :second) t(x, y)",
        Map("first" -> 1, "second" -> "a")
      ),
      """== Parsed Logical Plan ==
        |'NameParameterizedQuery [first, second], [1, a]
        |+- 'Project [*]
        |   +- 'SubqueryAlias t
        |      +- 'UnresolvedInlineTable [x, y], [[namedparameter(first), namedparameter(second)]]

        |== Analyzed Logical Plan ==
        |x: int, y: string
        |Project [x#N, y#N]
        |+- SubqueryAlias t
        |   +- LocalRelation [x#N, y#N]

        |== Optimized Logical Plan ==
        |LocalRelation [x#N, y#N]

        |== Physical Plan ==
        |LocalTableScan [x#N, y#N]
        |"""
    )
  }
}
