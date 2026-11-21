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
package org.apache.spark.sql.execution.python

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformerSuite

import org.apache.spark.api.python.{ColumnarArrowEvalPythonExec, PythonBroadcast, PythonEvalType, PythonUtils, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.types.LongType

import java.nio.file.Paths
import java.util.Base64

import scala.sys.process.Process

// Under org.apache.spark because PythonEvalType and SimplePythonFunction are private[spark].
class ColumnarArrowEvalPythonExecSuite extends WholeStageTransformerSuite {

  import IntegratedUDFTestUtils.{pythonExec, pythonVer}
  import testImplicits._

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  test("scalar iterator UDFs fall back") {
    val child = spark.range(1).queryExecution.sparkPlan
    val exec =
      ColumnarArrowEvalPythonExec(Nil, Nil, child, PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)
    assert(!exec.doValidate().ok())
    assert(exec.copy(evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF).doValidate().ok())
  }

  private lazy val pysparkPath = {
    val python = Paths.get(sparkHome, "python").toAbsolutePath
    s"${python.resolve("lib").resolve(PythonUtils.PY4J_ZIP_NAME)}:$python"
  }

  /** A scalar iterator pandas UDF whose function `f(iterator)` has the given body. */
  private def iteratorUdf(name: String, body: String): UserDefinedPythonFunction = {
    val code =
      s"""import base64
         |import pandas as pd
         |from pyspark.serializers import CloudPickleSerializer
         |from pyspark.sql.types import LongType
         |def f(iterator):
         |$body
         |print(base64.b64encode(CloudPickleSerializer().dumps((f, LongType()))).decode())
         |""".stripMargin
    val command = Base64.getDecoder.decode(
      Process(Seq(pythonExec, "-c", code), None, "PYTHONPATH" -> pysparkPath).!!.trim)
    val env = new java.util.HashMap[String, String]()
    env.put("PYTHONPATH", pysparkPath)
    new UserDefinedPythonFunction(
      name,
      new SimplePythonFunction(
        command,
        env,
        new java.util.ArrayList[String](),
        pythonExec,
        pythonVer,
        new java.util.ArrayList[Broadcast[PythonBroadcast]](),
        null),
      LongType,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
      udfDeterministic = true
    )
  }

  Seq(
    "merges" ->
      """    batches = list(iterator)
        |    if batches:
        |        yield pd.concat(batches, ignore_index=True)""".stripMargin,
    "splits" ->
      """    for batch in iterator:
        |        for i in range(len(batch)):
        |            yield batch.iloc[i:i + 1]""".stripMargin
  ).foreach {
    case (name, body) =>
      test(s"scalar iterator UDF that $name batches returns every row") {
        withSQLConf(
          GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "2",
          // Spark 4.2's columnar input path also joins the UDF output with its input batch by
          // batch, so it fails these UDFs. The key is unknown, and ignored, before Spark 4.2.
          "spark.sql.execution.arrow.pythonUDF.columnarInput.enabled" -> "false"
        ) {
          withTempPath {
            path =>
              (1L to 8L).toDF("id").coalesce(1).write.parquet(path.getCanonicalPath)
              val input = spark.read.parquet(path.getCanonicalPath)
              val df = input.withColumn("copied", iteratorUdf(name, body)(input("id")))
              checkAnswer(df, (1L to 8L).map(i => Row(i, i)))
              val plan = getExecutedPlan(df)
              assert(!plan.exists(_.isInstanceOf[ColumnarArrowEvalPythonExec]))
              assert(plan.exists(_.isInstanceOf[ArrowEvalPythonExec]))
          }
        }
      }
  }
}
