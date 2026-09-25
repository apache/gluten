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
import org.apache.gluten.execution.WholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.execution.python.ColumnarArrowEvalPythonUDTFExec
import org.apache.spark.sql.types.StructType

class ArrowEvalPythonUDTFSuite extends WholeStageTransformerSuite {

  import testImplicits._

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  // Spark exposes this value through a package-private PythonEvalType object.
  private val SQL_ARROW_TABLE_UDF = 301

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")
      .set("spark.executor.cores", "1")
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

  // The Python UDTF APIs are only available since Spark 3.5 and their signatures differ between
  // Spark versions, hence reflection.
  private def registerArrowUDTF(name: String, pythonScript: String): Unit = {
    val create = IntegratedUDFTestUtils.getClass.getMethods
      .find(_.getName == "createUserDefinedPythonTableFunction")
      .get
    val returnType = StructType.fromDDL("x int, y int")
    val returnTypeArg =
      if (create.getParameterTypes()(2) == classOf[Option[_]]) Some(returnType) else returnType
    val udtf = create.invoke(
      IntegratedUDFTestUtils,
      "TestUDTF",
      pythonScript,
      returnTypeArg,
      Int.box(SQL_ARROW_TABLE_UDF),
      Boolean.box(false))
    val registration = spark.getClass.getMethod("udtf").invoke(spark)
    registration.getClass.getMethods
      .find(_.getName == "registerPython")
      .get
      .invoke(registration, name, udtf)
  }

  private def checkUDTF(query: => DataFrame): Unit = {
    var expected: Seq[Row] = Seq.empty
    withSQLConf(GlutenConfig.COLUMNAR_ARROW_UDF_ENABLED.key -> "false") {
      val df = query
      assert(getExecutedPlan(df).forall(!_.isInstanceOf[ColumnarArrowEvalPythonUDTFExec]))
      expected = df.collect().toSeq
    }
    assert(expected.nonEmpty)
    val df = query
    checkSparkPlan[ColumnarArrowEvalPythonUDTFExec](df)
    checkAnswer(df, expected)
  }

  private def withInputTable(f: => Unit): Unit = {
    withTempView("t") {
      Seq((1, 2), (0, 0), (-1, 3), (3, 1), (2, 2)).toDF("a", "b").createOrReplaceTempView("t")
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
}
