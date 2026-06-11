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

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, ArrowPythonUDTFRunner, BatchIterator, EvalPythonExec, PythonUDTF}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * A physical plan that evaluates a [[PythonUDTF]] using Apache Arrow in Gluten. This transformer
 * takes columnar input, converts it to Arrow format, executes the Python UDTF, and returns columnar
 * output.
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
  extends GlutenPlan {

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def children: Seq[SparkPlan] = Seq(child)

  override def batchType(): Convention.BatchType = Convention.BatchType.VanillaBatch

  override def rowType(): Convention.RowType = Convention.RowType.None

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val largeVarTypes = conf.arrowUseLargeVarTypes
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
  private[this] val sessionUUID = {
    Option(session).collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "ArrowEvalPythonUDTFTransformer does not support row-based execution")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val inputRDD = child.executeColumnar()

    inputRDD.mapPartitions {
      iter =>
        val context = TaskContext.get()

        // Flatten all the arguments
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val argMetas = udtf.children.zip(
          udtf.tableArguments.getOrElse(Seq.fill(udtf.children.length)(false))
        ).map {
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

        val schema = StructType(dataTypes.zipWithIndex.map {
          case (dt, i) =>
            StructField(s"_$i", dt)
        }.toArray)

        val outputTypes = resultAttrs.map(_.dataType.transformRecursively {
          case udt: UserDefinedType[_] => udt.sqlType
        })

        // Convert columnar batches to Arrow batches for Python processing
        val arrowBatchIter = iter.flatMap {
          batch =>
            val numInputRows = metrics("numInputRows")
            numInputRows += batch.numRows()

            // Convert ColumnarBatch to Arrow format
            convertToArrowBatch(batch, allInputs, child.output, context)
        }

        // Create batched iterator if batch size is configured
        val batchedIter = if (batchSize > 0) {
          new BatchIterator(arrowBatchIter, batchSize)
        } else {
          Iterator(arrowBatchIter)
        }

        // Execute Python UDTF using ArrowPythonUDTFRunner
        val columnarBatchIter = new ArrowPythonUDTFRunner(
          udtf,
          evalType,
          argMetas,
          schema,
          sessionLocalTimeZone,
          largeVarTypes,
          pythonRunnerConf,
          Map.empty, // Python metrics - can be enhanced later
          jobArtifactUUID,
          sessionUUID
        ).compute(batchedIter, context.partitionId(), context)

        // Process output batches
        columnarBatchIter.map {
          batch =>
            val numOutputRows = metrics("numOutputRows")
            numOutputRows += batch.numRows()

            // UDTF returns a StructType column in ColumnarBatch. Flatten it.
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

  /**
   * Convert a ColumnarBatch to Arrow format for Python processing. This method extracts the
   * required columns based on the projection and converts them to InternalRow format that can be
   * consumed by ArrowPythonRunner.
   */
  private def convertToArrowBatch(
      batch: ColumnarBatch,
      projectionExprs: Seq[Expression],
      childOutput: Seq[Attribute],
      context: TaskContext): Iterator[InternalRow] = {

    val allocator = ArrowBufferAllocators.contextInstance()

    // Create projection to extract required columns
    val projection = UnsafeProjection.create(projectionExprs, childOutput)
    projection.initialize(context.partitionId())

    // Convert columnar batch to row iterator and apply projection
    val rowIter = batch.rowIterator().asScala
    rowIter.map(row => projection(row))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("UDTF", udtf)}
       |${ExplainUtils.generateFieldString("Required Child Output", requiredChildOutput)}
       |${ExplainUtils.generateFieldString("Result Attributes", resultAttrs)}
       |""".stripMargin
  }
}

// Made with Bob
