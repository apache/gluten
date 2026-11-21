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
package org.apache.spark.api.python

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformerSuite
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.{ArrowColumnVectors, RefCountedArrowColumnVector}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, UserDefinedPythonFunction}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}

import java.util.Base64

import scala.sys.process.Process

class ColumnarArrowBatchOwnershipSuite extends WholeStageTransformerSuite {

  import IntegratedUDFTestUtils.{pythonExec, pythonVer}
  import testImplicits._
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  test("iterator pandas UDF validation falls back without starting a Python worker") {
    val child = spark.range(1).queryExecution.sparkPlan
    val iteratorPlan = ColumnarArrowEvalPythonExec(
      Nil,
      Nil,
      child,
      PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)
    assert(!iteratorPlan.doValidate().ok())
    assert(iteratorPlan.copy(evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF).doValidate().ok())
  }

  private def batch(name: String, values: Seq[Int]): ColumnarBatch = {
    val vector = new IntVector(name, ArrowBufferAllocators.contextInstance())
    vector.allocateNew(values.size)
    values.zipWithIndex.foreach { case (value, index) => vector.setSafe(index, value) }
    val root = VectorSchemaRoot.of(vector)
    root.setRowCount(values.size)
    ArrowColumnVectors.toBatch(root)
  }

  test("joined Python output can be offloaded before upstream releases its input") {
    TaskResources.runUnsafe {
      val allocator = ArrowBufferAllocators.contextInstance()
      val input = batch("input", Seq(11, 12))
      val output = batch("output", Seq(101, 102))
      // Hold the exact ownership state of a writer that has not advanced upstream yet.
      ColumnarBatches.retain(input)
      assert(input.column(0).asInstanceOf[RefCountedArrowColumnVector].refCnt() == 2)
      assert(output.column(0).asInstanceOf[RefCountedArrowColumnVector].refCnt() == 1)
      var joined: ColumnarBatch = null
      var loaded: ColumnarBatch = null
      try {
        joined = ColumnarArrowEvalPythonExec.joinWithIndependentInput(input, output)
        // Exercise the native transition while the upstream and cache still own the input.
        ColumnarBatches.offload(allocator, joined)
        assert(input.column(0).getInt(0) == 11)
        assert(input.column(0).asInstanceOf[RefCountedArrowColumnVector].refCnt() == 2)
        ColumnarBatches.release(input) // Cache reference.
        loaded = ColumnarBatches.load(allocator, joined)
        input.close() // Upstream reference.
        assert(loaded.numRows() == 2)
        assert((0 until 2).map(loaded.column(0).getInt) == Seq(11, 12))
        assert((0 until 2).map(loaded.column(1).getInt) == Seq(101, 102))
      } finally {
        if (loaded != null) loaded.close()
        else if (joined != null) joined.close()
        input.close()
        input.close()
        output.close()
      }
      assert(allocator.getAllocatedMemory == 0)
    }
  }
  private def iteratorUdf(name: String, body: String): UserDefinedPythonFunction = {
    val code = s"""import base64
                  |import pandas as pd
                  |from pyspark.serializers import CloudPickleSerializer
                  |from pyspark.sql.types import LongType
                  |def f(iterator):
                  |$body
                  |print(base64.b64encode(CloudPickleSerializer().dumps((f, LongType()))).decode())
                  |""".stripMargin
    val command = Base64.getDecoder.decode(Process(Seq(pythonExec, "-c", code)).!!.trim)
    val environment = new java.util.HashMap[String, String]()
    sys.env.get("PYTHONPATH").foreach(environment.put("PYTHONPATH", _))
    new UserDefinedPythonFunction(
      name,
      SimplePythonFunction(
        command,
        environment,
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
    "merge" -> "    batches = list(iterator)\n    if batches:\n        yield pd.concat(batches, ignore_index=True)",
    "split" -> "    for batch in iterator:\n        for i in range(len(batch)):\n            yield batch.iloc[i:i + 1]"
  ).foreach {
    case (name, body) =>
      testWithMaxSparkVersion(
        s"scalar iterator pandas UDF preserves rows when batches $name",
        "4.0") {
        withSQLConf(
          GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "2",
          "spark.sql.execution.arrow.maxRecordsPerBatch" -> "2") {
          withTempPath {
            path =>
              (1L to 6L).toDF("id").coalesce(1).write.parquet(path.getCanonicalPath)
              val input = spark.read.parquet(path.getCanonicalPath)
              val result = input.withColumn("copied", iteratorUdf(name, body)(input("id")))
              checkAnswer(result, (1L to 6L).map(i => Row(i, i)))
              assert(collect(result.queryExecution.executedPlan) {
                case p: ColumnarArrowEvalPythonExec => p
              }.isEmpty)
              assert(collect(result.queryExecution.executedPlan) {
                case p: ArrowEvalPythonExec => p
              }.nonEmpty)
          }
        }
      }
  }

}
