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
import org.apache.gluten.execution.{LoadArrowDataExec, OffloadArrowDataExec, WholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.api.python.ColumnarArrowEvalPythonExec
import org.apache.spark.sql.{DataFrame, IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.functions.{count, max, sum}
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
  // Spark exposes these values through a package-private PythonEvalType object.
  private val SQL_BATCHED_UDF = 100
  private val SQL_ARROW_BATCHED_UDF = 101
  private lazy val arrowBatchedTestUDFString =
    newTestArrowBatchedPythonUDF(name = "arrowBatchedUDF", returnType = Some(StringType))

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

  testWithMinSparkVersion("arrow_udf test: chained UDFs over small batches with nulls", "4.0") {
    withSQLConf("spark.gluten.sql.columnar.maxBatchSize" -> "64") {
      val base = spark
        .range(0, 1000, 1, 2)
        .selectExpr(
          "id",
          "if(id % 11 = 0, null, cast(id % 7 as string)) as s",
          "if(id % 5 = 0, null, id * 1.5) as d")
      // Velox operators both below and above the UDF, over many small batches.
      def query(): DataFrame = base
        .filter("id > 10")
        .withColumn("p_s", pyarrowTestUDFString(base("s")))
        .filter("p_s <> '3'")
        .groupBy("p_s")
        .agg(sum("d").as("sum_d"), count("*").as("cnt"))
      var expected: Seq[Row] = Seq.empty
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        expected = query().collect().toSeq
      }
      val df = query()
      checkAnswer(df, expected)
      checkSparkPlan[ColumnarArrowEvalPythonExec](df)
      // Only Arrow data loading and offloading are needed around the UDFs.
      val plan = getExecutedPlan(df)
      val udfs = plan.collect { case p: ColumnarArrowEvalPythonExec => p }
      assert(udfs.size == 2, plan)
      assert(udfs.forall(_.child.isInstanceOf[LoadArrowDataExec]), plan)
      val offloadedUdfs = plan.collect {
        case p: OffloadArrowDataExec if p.child.isInstanceOf[ColumnarArrowEvalPythonExec] => p
      }
      assert(offloadedUdfs.size == 2, plan)
    }
  }

  testWithMinSparkVersion("arrow batched python udf over parquet scan", "4.0") {
    withTempPath {
      f =>
        Seq(("MAIL", 1), ("RAIL", 2), ("SHIP", 3)).toDF("shipmode", "id").write.parquet(
          f.getCanonicalPath)
        val base = spark.read.parquet(f.getCanonicalPath)
        val arrowUdfCol = arrowBatchedTestUDFString(base("shipmode")).as("shipmode_arrow")
        val df = base.select(arrowUdfCol).agg(max("shipmode_arrow").as("max_shipmode"))
        val expected = Seq(Tuple1("SHIP")).toDF("max_shipmode")

        checkAnswer(df, expected)
        checkSparkPlan[ColumnarArrowEvalPythonExec](df)
    }
  }

  private def newTestArrowBatchedPythonUDF(
      name: String,
      returnType: Option[DataType] = None): UserDefinedPythonFunction = {
    val pythonUDF =
      newTestPythonUDF(name, returnType, Some(SQL_ARROW_BATCHED_UDF))
    val udf = pythonUDF.getClass
      .getMethod("udf")
      .invoke(pythonUDF)
      .asInstanceOf[UserDefinedPythonFunction]
    if (SparkVersionUtil.gteSpark41) {
      udf
    } else {
      udf.copy(
        udf.name,
        udf.func,
        udf.dataType,
        SQL_ARROW_BATCHED_UDF,
        udf.udfDeterministic)
    }
  }

  private def newTestPythonUDF(
      name: String,
      returnType: Option[DataType] = None,
      pythonEvalType: Option[Int] = None): TestPythonUDF = {
    if (SparkVersionUtil.gteSpark41) {
      classOf[TestPythonUDF]
        .getConstructor(
          classOf[String],
          classOf[Option[DataType]],
          Integer.TYPE,
          java.lang.Boolean.TYPE)
        .newInstance(
          name,
          returnType,
          pythonEvalType.getOrElse(SQL_BATCHED_UDF).asInstanceOf[Integer],
          java.lang.Boolean.TRUE)
    } else if (SparkVersionUtil.gteSpark40) {
      // After https://github.com/apache/spark/pull/42864 which landed in Spark 4.0, the return
      // type of the UDF must be explicitly specified when creating the UDF instance with column
      // expressions as parameter.
      classOf[TestPythonUDF]
        .getConstructor(classOf[String], classOf[Option[DataType]])
        .newInstance(name, returnType)
    } else {
      TestPythonUDF(name)
    }
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
