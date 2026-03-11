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

import org.apache.gluten.execution.WholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.api.python.ColumnarArrowEvalPythonExec
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.util.SparkVersionUtil

class ArrowEvalPythonExecSuite extends WholeStageTransformerSuite {

  import IntegratedUDFTestUtils._
  import testImplicits.localSeqToDatasetHolder
  import testImplicits.newProductEncoder

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"
  private val pyarrowTestUDFString =
    newTestScalarPandasUDF(name = "pyarrowUDF", returnType = Some(StringType))
  private val pyarrowTestUDFLong =
    newTestScalarPandasUDF(name = "pyarrowUDF", returnType = Some(LongType))

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")
      .set("spark.executor.cores", "1")
  }

  // TODO: fix on spark-4.1
  testWithMaxSparkVersion("arrow_udf test: without projection", "4.0") {
    lazy val base =
      Seq(("1", 1), ("1", 2), ("2", 1), ("2", 2), ("3", 1), ("3", 2), ("0", 1), ("3", 0))
        .toDF("a", "b")
    lazy val expected = Seq(
      ("1", "1"),
      ("1", "1"),
      ("2", "2"),
      ("2", "2"),
      ("3", "3"),
      ("3", "3"),
      ("0", "0"),
      ("3", "3")
    ).toDF("a", "p_a")

    val df2 = base.select("a").withColumn("p_a", pyarrowTestUDFString(base("a")))
    checkSparkPlan[ColumnarArrowEvalPythonExec](df2)
    checkAnswer(df2, expected)
  }

  // TODO: fix on spark-4.1
  testWithMaxSparkVersion("arrow_udf test: with unrelated projection", "4.0") {
    lazy val base =
      Seq(("1", 1), ("1", 2), ("2", 1), ("2", 2), ("3", 1), ("3", 2), ("0", 1), ("3", 0))
        .toDF("a", "b")
    lazy val expected = Seq(
      ("1", 1, "1", 2),
      ("1", 2, "1", 4),
      ("2", 1, "2", 2),
      ("2", 2, "2", 4),
      ("3", 1, "3", 2),
      ("3", 2, "3", 4),
      ("0", 1, "0", 2),
      ("3", 0, "3", 0)
    ).toDF("a", "b", "p_a", "d_b")

    val df =
      base.withColumn("p_a", pyarrowTestUDFString(base("a"))).withColumn("d_b", base("b") * 2)
    checkSparkPlan[ColumnarArrowEvalPythonExec](df)
    checkAnswer(df, expected)
  }

  // TODO: fix on spark-4.1
  testWithMaxSparkVersion("arrow_udf test: with preprojection", "4.0") {
    lazy val base =
      Seq(("1", 1), ("1", 2), ("2", 1), ("2", 2), ("3", 1), ("3", 2), ("0", 1), ("3", 0))
        .toDF("a", "b")
    lazy val expected = Seq(
      ("1", 1, 2, "1", 2),
      ("1", 2, 4, "1", 4),
      ("2", 1, 2, "2", 2),
      ("2", 2, 4, "2", 4),
      ("3", 1, 2, "3", 2),
      ("3", 2, 4, "3", 4),
      ("0", 1, 2, "0", 2),
      ("3", 0, 0, "3", 0)
    ).toDF("a", "b", "d_b", "p_a", "p_b")
    val df = base
      .withColumn("d_b", base("b") * 2)
      .withColumn("p_a", pyarrowTestUDFString(base("a")))
      .withColumn("p_b", pyarrowTestUDFLong(base("b") * 2))
    checkAnswer(df, expected)
  }

  private def newTestScalarPandasUDF(
      name: String,
      returnType: Option[DataType] = None): TestScalarPandasUDF = {
    if (SparkVersionUtil.gteSpark40) {
      // After https://github.com/apache/spark/pull/42864 which landed in Spark 4.0, the return
      // type of the UDF must be explicitly specified when creating the UDF instance with column
      // expressions as parameter.
      classOf[TestScalarPandasUDF]
        .getConstructor(classOf[String], classOf[Option[DataType]])
        .newInstance(name, returnType)
    } else {
      TestScalarPandasUDF(name)
    }
  }
}
