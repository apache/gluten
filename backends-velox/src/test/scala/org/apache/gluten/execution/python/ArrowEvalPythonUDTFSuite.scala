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
package org.apache.gluten.execution.python

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{OffloadArrowDataExec, WholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.ColumnarArrowEvalPythonUDTFExec
import org.apache.spark.sql.types.StructType

class ArrowEvalPythonUDTFSuite extends WholeStageTransformerSuite {

  import testImplicits._

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  // Spark exposes these values through a package-private PythonEvalType object.
  private val SQL_TABLE_UDF = 300
  private val SQL_ARROW_TABLE_UDF = 301

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")
      .set("spark.executor.cores", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }

  private val explodeUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int, b: int):
      |        if a > 0 and b > 0:
      |            yield a, a - b
      |            yield b, b - a
      |        elif a == 0 and b == 0:
      |            yield 0, 0
      |        else:
      |            ...
      |""".stripMargin

  private val terminateUDTF =
    """
      |class TestUDTF:
      |    def __init__(self):
      |        self._count = 0
      |        self._sum = 0
      |
      |    def eval(self, a: int, b: int):
      |        self._count += 1
      |        self._sum += a
      |        yield a, b
      |
      |    def terminate(self):
      |        yield self._count, self._sum
      |""".stripMargin

  // Yields `a` rows (a, 0), (a, 1), ..., and none if `a` <= 0.
  private val rangeUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int):
      |        for i in range(a):
      |            yield a, i
      |""".stripMargin

  // Yields exactly two rows per input row.
  private val twoRowsUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int):
      |        yield a, 0
      |        yield a, 1
      |""".stripMargin

  // Never yields a row.
  private val emptyUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int):
      |        if a is None:
      |            yield 0, 0
      |""".stripMargin

  // Passes nulls through, and yields nulls for some non-null inputs.
  private val nullsUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int):
      |        if a is None:
      |            yield None, None
      |        else:
      |            yield a, None
      |            yield None, a
      |""".stripMargin

  private val primitiveTypes =
    "s string, d decimal(10, 2), dt date, ts timestamp, l bigint, db double, bo boolean, bin binary"

  // Yields one row of each primitive type per `a % 3`, and an all-null row when `a % 4 == 0`.
  private val primitiveTypesUDTF =
    """
      |import datetime
      |import decimal
      |
      |class TestUDTF:
      |    def eval(self, a: int):
      |        if a % 4 == 0:
      |            yield None, None, None, None, None, None, None, None
      |        for i in range(a % 3):
      |            yield (
      |                ("s%d-%d" % (a, i)) * (i + 1),
      |                decimal.Decimal("%d.%02d" % (a, i)),
      |                datetime.date(2020, 1, 1) + datetime.timedelta(days=a),
      |                datetime.datetime(2020, 1, 1) + datetime.timedelta(hours=a, seconds=i),
      |                a * 1000000000 + i,
      |                a / 3.0,
      |                (a + i) % 2 == 0,
      |                bytes([a % 256, i]))
      |""".stripMargin

  private val arrayTypes = "arr array<int>, sarr array<string>"

  private val arrayTypesUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int):
      |        yield [a, a + 1, None], ["v%d" % a, None]
      |        if a % 2 == 0:
      |            yield [], None
      |""".stripMargin

  private val structMapTypes = "st struct<a: int, b: string>, m map<string, int>"

  private val structMapTypesUDTF =
    """
      |class TestUDTF:
      |    def eval(self, a: int):
      |        yield {"a": a, "b": "v%d" % a}, {"k%d" % a: a, "z": None}
      |        if a % 2 == 0:
      |            yield None, None
      |""".stripMargin

  // The Python UDTF APIs are only available since Spark 3.5 and their signatures differ between
  // Spark versions, hence reflection.
  private def registerUDTF(
      name: String,
      pythonScript: String,
      returnType: String = "x int, y int",
      evalType: Int = SQL_ARROW_TABLE_UDF): Unit = {
    val create = IntegratedUDFTestUtils.getClass.getMethods
      .find(_.getName == "createUserDefinedPythonTableFunction")
      .get
    val schema = StructType.fromDDL(returnType)
    val returnTypeArg =
      if (create.getParameterTypes()(2) == classOf[Option[_]]) Some(schema) else schema
    val udtf = create.invoke(
      IntegratedUDFTestUtils,
      "TestUDTF",
      pythonScript,
      returnTypeArg,
      Int.box(evalType),
      Boolean.box(false))
    val registration = spark.getClass.getMethod("udtf").invoke(spark)
    registration.getClass.getMethods
      .find(_.getName == "registerPython")
      .get
      .invoke(registration, name, udtf)
  }

  private def registerArrowUDTF(name: String, pythonScript: String): Unit =
    registerUDTF(name, pythonScript)

  /**
   * Checks that `query` is offloaded to ColumnarArrowEvalPythonUDTFExec, and returns the same
   * result as vanilla Spark.
   */
  private def checkUDTF(query: => DataFrame, allowEmpty: Boolean = false): DataFrame = {
    var expected: Seq[Row] = Seq.empty
    withSQLConf(GlutenConfig.COLUMNAR_ARROW_UDF_ENABLED.key -> "false") {
      val df = query
      expected = df.collect().toSeq
      assert(getExecutedPlan(df).forall(!_.isInstanceOf[ColumnarArrowEvalPythonUDTFExec]))
    }
    assert(allowEmpty || expected.nonEmpty)
    checkOffloaded(query, expected)
  }

  /** Checks that `query` is offloaded to ColumnarArrowEvalPythonUDTFExec and returns `expected`. */
  private def checkOffloaded(query: => DataFrame, expected: Seq[Row]): DataFrame = {
    val df = query
    checkAnswer(df, expected)
    // With AQE, the final plan is only known after the query runs.
    checkSparkPlan[ColumnarArrowEvalPythonUDTFExec](df)
    df
  }

  /** Runs `query` once and returns the metrics of its ColumnarArrowEvalPythonUDTFExec. */
  private def udtfMetrics(query: => DataFrame): Map[String, Long] = {
    val df = query
    df.collect()
    getExecutedPlan(df)
      .collectFirst { case p: ColumnarArrowEvalPythonUDTFExec => p }
      .get
      .metrics
      .map { case (name, metric) => name -> metric.value }
  }

  /** The plan node that consumes the output of ColumnarArrowEvalPythonUDTFExec. */
  private def udtfParent(df: DataFrame): SparkPlan =
    getExecutedPlan(df)
      .find(_.children.exists(_.isInstanceOf[ColumnarArrowEvalPythonUDTFExec]))
      .get

  /** Checks that the UDTF output reaches Velox as Arrow batches, not as rows. */
  private def checkOffloadedToVelox(df: DataFrame): Unit = {
    val parent = udtfParent(df)
    assert(
      parent.isInstanceOf[OffloadArrowDataExec],
      s"Expected OffloadArrowDataExec above the UDTF, got ${parent.nodeName}:\n" +
        df.queryExecution.executedPlan)
  }

  private def withInputTable(f: => Unit): Unit = {
    withTempView("t") {
      Seq((1, 2), (0, 0), (-1, 3), (3, 1), (2, 2)).toDF("a", "b").createOrReplaceTempView("t")
      f
    }
  }

  /** `n` rows of `a` = 0 .. n - 1 in `numPartitions` partitions. */
  private def withRangeTable(n: Int, numPartitions: Int)(f: => Unit): Unit = {
    withTempView("r") {
      spark
        .range(0, n, 1, numPartitions)
        .selectExpr("cast(id as int) a")
        .createOrReplaceTempView("r")
      f
    }
  }

  testWithMinSparkVersion("arrow udtf: constant arguments", "3.5") {
    registerArrowUDTF("explodeUDTF", explodeUDTF)
    checkUDTF(spark.sql("SELECT * FROM explodeUDTF(5, 2)"))
    checkAnswer(spark.sql("SELECT * FROM explodeUDTF(5, 2)"), Seq(Row(5, 3), Row(2, -3)))
  }

  testWithMinSparkVersion("arrow udtf: lateral join with child columns", "3.5") {
    registerArrowUDTF("explodeUDTF", explodeUDTF)
    withInputTable {
      checkUDTF(spark.sql("SELECT t.*, u.* FROM t, LATERAL explodeUDTF(t.a, t.b) u"))
      // Only part of the child output is required.
      checkUDTF(spark.sql("SELECT t.b, u.y FROM t, LATERAL explodeUDTF(t.a, t.b + 1) u"))
    }
  }

  testWithMinSparkVersion("arrow udtf: terminate", "3.5") {
    registerArrowUDTF("terminateUDTF", terminateUDTF)
    withInputTable {
      checkUDTF(spark.sql("SELECT t.a, u.* FROM t, LATERAL terminateUDTF(t.a, t.b) u"))
    }
  }

  testWithMinSparkVersion("arrow udtf: output coalesced into batches of maxBatchSize", "3.5") {
    registerArrowUDTF("twoRowsUDTF", twoRowsUDTF)
    withRangeTable(300, 1) {
      withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "100") {
        val query = "SELECT r.a, u.* FROM r, LATERAL twoRowsUDTF(r.a) u"
        checkUDTF(spark.sql(query))
        // 300 input rows with 2 output rows each, so 600 rows in 6 full batches of 100.
        val metrics = udtfMetrics(spark.sql(query))
        assert(metrics("numInputRows") == 300)
        assert(metrics("numOutputRows") == 600)
        assert(metrics("numOutputBatches") == 6)
      }
    }
  }

  testWithMinSparkVersion("arrow udtf: output of one input row spans several batches", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    registerArrowUDTF("terminateUDTF", terminateUDTF)
    withRangeTable(12, 1) {
      // Each input row yields up to 11 rows, so its child columns are repeated across batches.
      Seq("1", "3", "7").foreach {
        batchSize =>
          withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> batchSize) {
            checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL rangeUDTF(r.a) u"))
            // `terminate` rows are joined with the last input row, which may have been written
            // to an earlier batch.
            checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL terminateUDTF(r.a, r.a) u"))
          }
      }
    }
  }

  testWithMinSparkVersion("arrow udtf: multiple partitions", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    registerArrowUDTF("terminateUDTF", terminateUDTF)
    withRangeTable(40, 4) {
      checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL rangeUDTF(r.a % 5) u"))
      // One `terminate` row per partition.
      checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL terminateUDTF(r.a, r.a) u"))
    }
  }

  testWithMinSparkVersion("arrow udtf: empty partitions", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    registerArrowUDTF("terminateUDTF", terminateUDTF)
    // 2 rows in 4 partitions: a = 0 in partition 1, a = 1 in partition 3, and partitions 0 and 2
    // are empty.
    withRangeTable(2, 4) {
      checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL rangeUDTF(r.a + 1) u"))
      // Each partition, including the empty ones, yields one `terminate` row (count, sum).
      checkUDTF(spark.sql("SELECT u.* FROM r, LATERAL terminateUDTF(r.a, r.a) u"))
      // With child columns, `terminate` results of an empty partition are joined with a null row.
      // Vanilla Spark (3.5 to 4.2) throws a NullPointerException here, since it joins them with a
      // row that was never set, so the result is checked against explicit rows.
      checkOffloaded(
        spark.sql("SELECT r.a, u.* FROM r, LATERAL terminateUDTF(r.a, r.a) u"),
        Seq(
          Row(null, 0, 0), // partition 0: terminate
          Row(0, 0, 0), // partition 1: eval(0, 0)
          Row(0, 1, 0), // partition 1: terminate
          Row(null, 0, 0), // partition 2: terminate
          Row(1, 1, 1), // partition 3: eval(1, 1)
          Row(1, 1, 1) // partition 3: terminate
        )
      )
    }
  }

  testWithMinSparkVersion("arrow udtf: empty input", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    registerArrowUDTF("terminateUDTF", terminateUDTF)
    withRangeTable(0, 1) {
      checkUDTF(
        spark.sql("SELECT r.a, u.* FROM r, LATERAL rangeUDTF(r.a) u"),
        allowEmpty = true)
      checkUDTF(
        spark.sql("SELECT r.a, u.* FROM r, LATERAL terminateUDTF(r.a, r.a) u"),
        allowEmpty = true)
    }
  }

  testWithMinSparkVersion("arrow udtf: no output rows", "3.5") {
    registerArrowUDTF("emptyUDTF", emptyUDTF)
    withRangeTable(20, 2) {
      val query = "SELECT r.a, u.* FROM r, LATERAL emptyUDTF(r.a) u"
      val df = checkUDTF(spark.sql(query), allowEmpty = true)
      assert(df.collect().isEmpty)
      val metrics = udtfMetrics(spark.sql(query))
      assert(metrics("numInputRows") == 20)
      assert(metrics("numOutputBatches") == 0)
    }
  }

  testWithMinSparkVersion("arrow udtf: null values", "3.5") {
    registerArrowUDTF("nullsUDTF", nullsUDTF)
    withTempView("n") {
      Seq[(Option[Int], Option[String])]((Some(1), None), (None, Some("x")), (Some(2), Some("y")))
        .toDF("a", "s")
        .createOrReplaceTempView("n")
      checkUDTF(spark.sql("SELECT n.*, u.* FROM n, LATERAL nullsUDTF(n.a) u"))
    }
  }

  testWithMinSparkVersion("arrow udtf: primitive result types", "3.5") {
    registerUDTF("primitiveTypesUDTF", primitiveTypesUDTF, primitiveTypes)
    withRangeTable(30, 2) {
      checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL primitiveTypesUDTF(r.a) u"))
      // Consumed by Velox, which imports every type.
      val df = checkUDTF(spark.sql(
        "SELECT r.a, u.* FROM r, LATERAL primitiveTypesUDTF(r.a) u WHERE u.l IS NULL OR u.l > 0"))
      checkOffloadedToVelox(df)
    }
  }

  testWithMinSparkVersion("arrow udtf: array result types", "3.5") {
    registerUDTF("arrayTypesUDTF", arrayTypesUDTF, arrayTypes)
    withRangeTable(10, 1) {
      checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL arrayTypesUDTF(r.a) u"))
    }
  }

  testWithMinSparkVersion("arrow udtf: struct and map result types", "3.5") {
    registerUDTF("structMapTypesUDTF", structMapTypesUDTF, structMapTypes)
    withRangeTable(10, 1) {
      checkUDTF(spark.sql("SELECT r.a, u.* FROM r, LATERAL structMapTypesUDTF(r.a) u"))
    }
  }

  testWithMinSparkVersion("arrow udtf: child columns of all types are repeated", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    withTempView("c") {
      spark
        .range(0, 8, 1, 2)
        .selectExpr(
          "cast(id % 4 as int) a",
          "if(id % 3 = 0, null, repeat(cast(id as string), cast(id as int) + 1)) s",
          "cast(id / 3 as decimal(10, 2)) d",
          "date_add(date'2020-01-01', cast(id as int)) dt",
          "timestamp'2020-01-01 00:00:00' + make_interval(0, 0, 0, 0, cast(id as int)) ts",
          "if(id % 2 = 0, array(id, null), null) arr",
          "named_struct('x', id, 'y', cast(id as string)) st",
          "map(cast(id as string), id) m",
          "cast(cast(id as string) as binary) bin"
        )
        .createOrReplaceTempView("c")
      withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "4") {
        checkUDTF(spark.sql("SELECT c.*, u.* FROM c, LATERAL rangeUDTF(c.a) u"))
      }
    }
  }

  testWithMinSparkVersion("arrow udtf: output consumed by Velox aggregation and join", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    withRangeTable(50, 2) {
      withTempView("k") {
        spark
          .range(0, 10)
          .selectExpr("cast(id as int) k", "id * 10 v")
          .createOrReplaceTempView("k")
        withSQLConf(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "16") {
          val agg = checkUDTF(
            spark.sql(
              "SELECT u.y, count(*), sum(u.x) FROM r, LATERAL rangeUDTF(r.a % 7) u " +
                "GROUP BY u.y"))
          checkOffloadedToVelox(agg)

          val join = checkUDTF(
            spark.sql(
              "SELECT q.a, q.y, k.v FROM " +
                "(SELECT r.a, u.y FROM r, LATERAL rangeUDTF(r.a % 7) u) q " +
                "JOIN k ON q.y = k.k"))
          checkOffloadedToVelox(join)
        }
      }
    }
  }

  // Result of `SELECT r.a, u.* FROM r, LATERAL rangeUDTF(r.a) u` for r.a = 0 .. 3.
  private val rangeOf4Rows =
    Seq(Row(1, 1, 0), Row(2, 2, 0), Row(2, 2, 1), Row(3, 3, 0), Row(3, 3, 1), Row(3, 3, 2))

  testWithMinSparkVersion("arrow udtf: fall back with large var types", "3.5") {
    registerArrowUDTF("rangeUDTF", rangeUDTF)
    withRangeTable(4, 1) {
      withSQLConf("spark.sql.execution.arrow.useLargeVarTypes" -> "true") {
        val df = spark.sql("SELECT r.a, u.* FROM r, LATERAL rangeUDTF(r.a) u")
        checkAnswer(df, rangeOf4Rows)
        assert(getExecutedPlan(df).forall(!_.isInstanceOf[ColumnarArrowEvalPythonUDTFExec]))
      }
    }
  }

  testWithMinSparkVersion("arrow udtf: non-Arrow UDTF is not offloaded", "3.5") {
    registerUDTF("pickledRangeUDTF", rangeUDTF, evalType = SQL_TABLE_UDF)
    withRangeTable(4, 1) {
      val df = spark.sql("SELECT r.a, u.* FROM r, LATERAL pickledRangeUDTF(r.a) u")
      checkAnswer(df, rangeOf4Rows)
      val plan = getExecutedPlan(df)
      assert(plan.forall(!_.isInstanceOf[ColumnarArrowEvalPythonUDTFExec]))
      // BatchEvalPythonUDTFExec only exists since Spark 3.5, hence the name.
      assert(plan.exists(_.getClass.getSimpleName == "BatchEvalPythonUDTFExec"))
    }
  }
}
