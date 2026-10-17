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

import org.apache.gluten.execution.ValidatablePlan
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{RowToColumnConverter, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

import java.io.File
import java.util.concurrent.atomic.AtomicLong

/**
 * Columnar counterpart of Spark's ArrowEvalPythonUDTFExec. The Arrow batches returned by the Python
 * worker are emitted directly as columnar output instead of being converted to rows one by one.
 *
 * As in Spark, the Python worker returns one batch per input row (plus the batches of the
 * `terminate` call), so each batch is joined with its input row by filling the
 * `requiredChildOutput` columns with the row's values.
 *
 * @param udtf
 *   the Python UDTF, typed as [[Expression]] since PythonUDTF only exists since Spark 3.5.
 * @param requiredChildOutput
 *   the required output of the child plan.
 * @param resultAttrs
 *   the output schema of the Python UDTF.
 * @param child
 *   the child plan.
 * @param evalType
 *   the Python eval type.
 */
case class ColumnarArrowEvalPythonUDTFExec(
    udtf: Expression,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends UnaryExecNode
  with PythonSQLMetrics
  with ValidatablePlan {

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def batchType(): Convention.BatchType = Convention.BatchType.VanillaBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(ConventionReq.vanillaRow)

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"$nodeName does not support row-based execution")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val evaluator = ArrowEvalPythonUDTFShim.createEvaluator(
      this,
      udtf,
      evalType,
      child.output,
      resultAttrs,
      pythonMetrics)
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val childOutput = child.output
    val requiredOutput = requiredChildOutput
    val pruneChild = child.outputSet != AttributeSet(requiredOutput)
    // Nullable since `terminate` results of an empty partition are joined with a null row.
    val requiredChildSchema = StructType(requiredOutput.map {
      a => StructField(a.name, a.dataType, nullable = true)
    })

    child.execute().map(_.copy()).mapPartitions {
      iter =>
        val context = TaskContext.get()

        // The queue used to buffer input rows so we can drain it to
        // combine input with output from Python.
        val queue = HybridRowQueue(
          context.taskMemoryManager(),
          new File(Utils.getLocalDir(SparkEnv.get.conf)),
          childOutput.length)
        context.addTaskCompletionListener[Unit](_ => queue.close())

        // Number of input rows still in the queue. Batches arriving when it is zero are from the
        // `terminate` call. The input iterator may be consumed by a separate writer thread.
        val count = new AtomicLong(0L)
        val inputIter = iter.map {
          row =>
            queue.add(row.asInstanceOf[UnsafeRow])
            count.incrementAndGet()
            numInputRows += 1
            row
        }

        val pruneChildForResult: InternalRow => InternalRow =
          if (pruneChild) UnsafeProjection.create(requiredOutput, childOutput) else identity
        val converter = new RowToColumnConverter(requiredChildSchema)
        // As in Spark, `terminate` results are joined with the last input row.
        var left: InternalRow = new GenericInternalRow(requiredOutput.length)
        var childVectors: Array[WritableColumnVector] = Array.empty
        context.addTaskCompletionListener[Unit](_ => childVectors.foreach(_.close()))

        evaluator.evaluate(inputIter, context).map {
          result =>
            if (count.get() > 0) {
              left = pruneChildForResult(queue.remove()).copy()
              count.decrementAndGet()
            }
            val numRows = result.numRows()
            // The previous batch has been consumed once the next one is requested.
            childVectors.foreach(_.close())
            childVectors = requiredChildSchema.fields.map {
              f => new OnHeapColumnVector(numRows, f.dataType): WritableColumnVector
            }
            var i = 0
            while (i < numRows) {
              converter.convert(left, childVectors)
              i += 1
            }
            val vectors: Array[ColumnVector] =
              childVectors ++ (0 until result.numCols()).map(result.column)
            numOutputBatches += 1
            numOutputRows += numRows
            new ColumnarBatch(vectors, numRows)
        }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
