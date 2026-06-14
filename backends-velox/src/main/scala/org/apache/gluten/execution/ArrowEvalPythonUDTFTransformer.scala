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
package org.apache.gluten.execution

import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, ArrowPythonUDTFRunner}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.mutable.ArrayBuffer

/**
 * A physical plan that evaluates a [[PythonUDTF]] using Apache Arrow in Gluten. This implementation
 * takes row-based input, converts it to Arrow format, executes the Python UDTF, and returns
 * columnar output directly (unlike Spark's version which converts back to rows).
 *
 * @param udtf
 *   the user-defined Python function
 * @param requiredChildOutput
 *   the required output of the child plan
 * @param resultAttrs
 *   the output schema of the Python UDTF
 * @param child
 *   the child plan
 * @param evalType
 *   the Python eval type
 */
case class ArrowEvalPythonUDTFTransformer(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends UnaryExecNode
  with ValidatablePlan
  with GlutenPlan {

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override protected def doValidateInternal(): ValidationResult = {
    super.doValidateInternal()
  }

  override def batchType(): Convention.BatchType = Convention.BatchType.VanillaBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val largeVarTypes = conf.arrowUseLargeVarTypes
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private[this] val jobArtifactUUID: Option[String] = None

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows")
  )

  protected def evaluate(
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[ColumnarBatch] = {

    val batchIter = if (batchSize > 0) {
      iter.grouped(batchSize).map(_.iterator)
    } else {
      Iterator(iter)
    }

    val outputTypes = resultAttrs.map(_.dataType)

    val columnarBatchIter = new ArrowPythonUDTFRunner(
      udtf,
      evalType,
      argMetas,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      Map.empty, // Python metrics - empty map for now
      jobArtifactUUID
    ).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.map {
      batch =>
        // UDTF returns a StructType column in ColumnarBatch. Flatten the columnar batch here.
        val columnVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = resultAttrs.indices.map(columnVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)

        val actualDataTypes =
          (0 until flattenedBatch.numCols()).map(i => flattenedBatch.column(i).dataType())
        assert(
          outputTypes == actualDataTypes,
          s"Invalid schema from arrow-enabled Python UDTF: " +
            s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}"
        )

        flattenedBatch.setNumRows(batch.numRows())
        flattenedBatch
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "ArrowEvalPythonUDTFTransformer does not support row-based execution")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val inputRDD = child.execute().map(_.copy())
    val numOutputBatches = longMetric("numOutputBatches")
    val numOutputRows = longMetric("numOutputRows")
    val numInputRows = longMetric("numInputRows")

    inputRDD.mapPartitions {
      iter =>
        val context = TaskContext.get()

        // Flatten all the arguments
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val argMetas = udtf.children
          .map {
            e: Expression =>
              val (key, value) = e match {
                case NamedArgumentExpression(key, value) =>
                  (Some(key), value)
                case _ =>
                  (None, e)
              }
              if (allInputs.exists(_.semanticEquals(value))) {
                ArgumentMetadata(allInputs.indexWhere(_.semanticEquals(value)), key)
              } else {
                allInputs += value
                dataTypes += value.dataType
                ArgumentMetadata(allInputs.length - 1, key)
              }
          }
          .toArray

        val projection = MutableProjection.create(allInputs.toSeq, child.output)
        projection.initialize(context.partitionId())
        val schema = StructType(dataTypes.zipWithIndex.map {
          case (dt, i) =>
            StructField(s"_$i", dt)
        }.toArray)

        // Project input rows and count them
        val projectedRowIter = iter.map {
          inputRow =>
            numInputRows += 1
            projection(inputRow)
        }

        // Evaluate and get columnar batch iterator
        val outputBatchIterator = evaluate(argMetas, projectedRowIter, schema, context)

        // Count batches and rows
        outputBatchIterator.map {
          batch =>
            numOutputBatches += 1
            numOutputRows += batch.numRows()
            batch
        }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

// Made with Bob
