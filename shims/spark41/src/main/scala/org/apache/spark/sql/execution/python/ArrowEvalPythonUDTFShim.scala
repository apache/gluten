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

import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, MutableProjection, NamedArgumentExpression, PythonUDTF}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.mutable.ArrayBuffer

/**
 * Evaluates an Arrow-optimized Python UDTF. Each returned batch holds the flattened UDTF result
 * columns of a single `eval` (one per input row, in input order) or `terminate` call.
 */
trait ArrowPythonUDTFEvaluator extends Serializable {
  def evaluate(iter: Iterator[InternalRow], context: TaskContext): Iterator[ColumnarBatch]
}

object ArrowEvalPythonUDTFShim {
  def unapply(
      plan: SparkPlan): Option[(Expression, Seq[Attribute], Seq[Attribute], SparkPlan, Int)] =
    plan match {
      case p: ArrowEvalPythonUDTFExec =>
        Some((p.udtf, p.requiredChildOutput, p.resultAttrs, p.child, p.evalType))
      case _ => None
    }

  /**
   * Mirrors EvalPythonUDTFExec#doExecute and ArrowEvalPythonUDTFExec#evaluate, without converting
   * the Python output to rows. Must be called on the driver.
   */
  def createEvaluator(
      plan: SparkPlan,
      udtf: Expression,
      evalType: Int,
      childOutput: Seq[Attribute],
      resultAttrs: Seq[Attribute],
      pythonMetrics: Map[String, SQLMetric]): ArrowPythonUDTFEvaluator = {
    val pythonUDTF = udtf.asInstanceOf[PythonUDTF]
    val conf = plan.conf
    val batchSize = conf.arrowMaxRecordsPerBatch
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
    val sessionUUID = Option(plan.session).collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }

    new ArrowPythonUDTFEvaluator {
      override def evaluate(
          iter: Iterator[InternalRow],
          context: TaskContext): Iterator[ColumnarBatch] = {
        // flatten all the arguments
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val argMetas = pythonUDTF.children
          .zip(pythonUDTF.tableArguments.getOrElse(Seq.fill(pythonUDTF.children.length)(false)))
          .map {
            case (e: Expression, isTableArg: Boolean) =>
              val (key, value) = e match {
                case NamedArgumentExpression(key, value) =>
                  (Some(key), value)
                case _ =>
                  (None, e)
              }
              if (allInputs.exists(_.semanticEquals(value))) {
                ArgumentMetadata(allInputs.indexWhere(_.semanticEquals(value)), key, isTableArg)
              } else {
                allInputs += value
                dataTypes += value.dataType
                ArgumentMetadata(allInputs.length - 1, key, isTableArg)
              }
          }.toArray
        val projection = MutableProjection.create(allInputs.toSeq, childOutput)
        projection.initialize(context.partitionId())
        val schema = StructType(dataTypes.zipWithIndex.map {
          case (dt, i) => StructField(s"_$i", dt)
        }.toArray)

        val projectedRowIter = iter.map(projection)
        val batchIter =
          if (batchSize > 0) new BatchIterator(projectedRowIter, batchSize)
          else Iterator(projectedRowIter)

        val outputTypes = resultAttrs.map(_.dataType.transformRecursively {
          case udt: UserDefinedType[_] => udt.sqlType
        })

        val columnarBatchIter = new ArrowPythonUDTFRunner(
          pythonUDTF,
          evalType,
          argMetas,
          schema,
          sessionLocalTimeZone,
          largeVarTypes,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID,
          sessionUUID).compute(batchIter, context.partitionId(), context)

        columnarBatchIter.map {
          batch =>
            // UDTF returns a StructType column in ColumnarBatch. Flatten the columnar batch here.
            val columnVector = batch.column(0).asInstanceOf[ArrowColumnVector]
            val outputVectors = resultAttrs.indices.map(columnVector.getChild)
            val flattenedBatch = new ColumnarBatch(outputVectors.toArray)

            val actualDataTypes =
              (0 until flattenedBatch.numCols()).map(i => flattenedBatch.column(i).dataType())
            if (!equalsIgnoreCompatibleCollation(outputTypes, actualDataTypes)) {
              throw QueryExecutionErrors.arrowDataTypeMismatchError(
                "Python UDTF",
                outputTypes,
                actualDataTypes)
            }

            flattenedBatch.setNumRows(batch.numRows())
            flattenedBatch
        }
      }
    }
  }
}
