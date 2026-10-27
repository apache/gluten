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
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.DataOutputStream
import java.net.Socket

abstract class BasePythonRunnerShim(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argMetas: Array[Array[(Int, Option[String])]],
    pythonMetrics: Map[String, SQLMetric])
  extends BasePythonRunner[ColumnarBatch, ColumnarBatch](
    funcs.map(_._1),
    evalType,
    argMetas.map(_.map(_._1))) {
  // The type aliases below provide consistent type names in child classes,
  // ensuring code compatibility with both Spark 4.0 and earlier versions.
  type Writer = WriterThread
  type PythonWorker = Socket

  protected def createNewWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer

  protected def writeUdf(
      dataOut: DataOutputStream,
      argMetas: Array[Array[(Int, Option[String])]]): Unit = {
    PythonUDFRunner.writeUDFs(dataOut, funcs.map(_._1), argMetas.map(_.map(_._1)))
  }

  override protected def newWriterThread(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    createNewWriter(env, worker, inputIterator, partitionIndex, context)
  }

  protected def pythonRunnerConfMap: Map[String, String] = Map.empty

  protected def pythonInputSchema: StructType = new StructType()
}
