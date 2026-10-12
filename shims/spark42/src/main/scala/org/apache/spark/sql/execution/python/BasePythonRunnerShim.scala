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

import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonWorker}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.DataOutputStream

abstract class BasePythonRunnerShim(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argMetas: Array[Array[(Int, Option[String])]],
    pythonMetrics: Map[String, SQLMetric])
  extends BasePythonRunner[ColumnarBatch, ColumnarBatch](
    funcs.map(_._1),
    evalType,
    argMetas.map(_.map(_._1)),
    None,
    pythonMetrics) {

  protected def createNewWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer

  protected def writeUdf(
      dataOut: DataOutputStream,
      argOffsets: Array[Array[(Int, Option[String])]]): Unit = {
    PythonUDFRunner.writeUDFs(
      dataOut,
      funcs,
      argOffsets.map(_.map(pair => ArgumentMetadata(pair._1, pair._2))))
  }

  override protected def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    createNewWriter(env, worker, inputIterator, partitionIndex, context)
  }

  // Spark 4.2 (SPARK-51384) frames the Python worker command as
  // evalType -> runnerConf -> evalConf -> writeCommand, and expects writeCommand to emit only the
  // UDF definitions. Older profiles hand-wrote the config prefix inside writeCommand; on Spark 4.2
  // that config must instead flow through these hooks (mirroring Spark's own ArrowPythonRunner /
  // ArrowPythonWithNamedArgumentRunner), otherwise the worker misreads the extra prefix.
  // The concrete runner supplies the values via pythonRunnerConfMap / pythonInputSchema.
  private val SQL_ARROW_BATCHED_UDF = 101

  protected def pythonRunnerConfMap: Map[String, String] = Map.empty

  protected def pythonInputSchema: StructType = new StructType()

  override def runnerConf: Map[String, String] = super.runnerConf ++ pythonRunnerConfMap

  override def evalConf: Map[String, String] =
    if (evalType == SQL_ARROW_BATCHED_UDF) {
      super.evalConf + ("input_type" -> pythonInputSchema.json)
    } else {
      super.evalConf
    }

}
