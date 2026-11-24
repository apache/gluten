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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.execution.{ValidatablePlan, ValidationResult}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowColumnVectors

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.util.VectorAppender

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Columnar counterpart of Spark's ArrowEvalPythonUDTFExec, for Arrow-optimized Python UDTFs
 * (`SQL_ARROW_TABLE_UDF`). The output is Gluten's Arrow Java batches, so Velox imports it through
 * the Arrow C data interface instead of converting rows.
 *
 * As in Spark, the Python worker returns one batch per input row (plus the batches of the
 * `terminate` call), and each batch is joined with its input row. Here, the results of many input
 * rows are coalesced into batches of up to `spark.gluten.sql.columnar.maxBatchSize` rows in
 * Gluten's Arrow memory. Each input row's `requiredChildOutput` values are written once and
 * repeated for each of its output rows.
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

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override def requiredChildConvention(): Seq[ConventionReq] = Seq(ConventionReq.vanillaRow)

  override lazy val metrics: Map[String, SQLMetric] = pythonMetrics ++ Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches")
  )

  override protected def doValidateInternal(): ValidationResult = {
    if (conf.getConfString(ColumnarArrowEvalPythonUDTFExec.LargeVarTypesKey, "false").toBoolean) {
      // The Python output would have large var types, which can't be appended to the regular
      // var-width vectors of the output batches.
      return ValidationResult.failed(
        s"${ColumnarArrowEvalPythonUDTFExec.LargeVarTypesKey} is not supported")
    }
    if (output.exists(_.dataType.existsRecursively(_.isInstanceOf[UserDefinedType[_]]))) {
      return ValidationResult.failed("User-defined types are not supported")
    }
    ValidationResult.succeeded
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"$nodeName does not support row-based execution")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val evaluator =
      ArrowEvalPythonUDTFShim.createEvaluator(this, udtf, evalType, child.output, pythonMetrics)
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val childOutput = child.output
    val requiredOutput = requiredChildOutput
    val pruneChild = child.outputSet != AttributeSet(requiredOutput)
    // Nullable since `terminate` results of an empty partition are joined with a null row.
    val parentSchema = StructType(requiredOutput.map {
      a => StructField(a.name, a.dataType, nullable = true)
    })
    val resultTypes = resultAttrs.map(_.dataType)
    val maxBatchRows = glutenConf.maxBatchSize

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
        val queuedRows = new AtomicLong(0L)
        val inputIter = iter.map {
          row =>
            queue.add(row.asInstanceOf[UnsafeRow])
            queuedRows.incrementAndGet()
            numInputRows += 1
            row
        }

        val pruneChildForResult: InternalRow => InternalRow =
          if (pruneChild) UnsafeProjection.create(requiredOutput, childOutput) else identity
        val results = evaluator.evaluate(inputIter, context)
        val builder =
          new UDTFOutputBuilder(parentSchema, ArrowBufferAllocators.contextInstance())

        class OutputIterator extends Iterator[ColumnarBatch] {
          // As in Spark, `terminate` results are joined with the last input row.
          private var left: InternalRow = new GenericInternalRow(requiredOutput.length)
          // Index of `left` in the batch being built, or -1 if it isn't written there yet.
          private var leftIndex = -1
          private var typesChecked = false
          // A batch built by hasNext() and not returned by next() yet.
          private var unclaimed: ColumnarBatch = _

          override def hasNext: Boolean = {
            if (unclaimed == null) {
              unclaimed = fill()
            }
            unclaimed != null
          }

          override def next(): ColumnarBatch = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            val batch = unclaimed
            unclaimed = null
            numOutputBatches += 1
            numOutputRows += batch.numRows()
            batch
          }

          def close(): Unit = {
            if (unclaimed != null) {
              unclaimed.close()
              unclaimed = null
            }
            builder.close()
          }

          private def fill(): ColumnarBatch = {
            while (builder.numRows < maxBatchRows && results.hasNext) {
              // Owned by the runner and only valid until the next one is requested, so its data
              // is copied into the builder right away.
              val raw = results.next()
              if (queuedRows.get() > 0) {
                left = pruneChildForResult(queue.remove()).copy()
                queuedRows.decrementAndGet()
                leftIndex = -1
              }
              val numRows = raw.numRows()
              if (numRows > 0) {
                // UDTF returns a StructType column in ColumnarBatch.
                val result = raw.column(0).asInstanceOf[ArrowColumnVector]
                val struct = result.getValueVector.asInstanceOf[StructVector]
                if (!typesChecked) {
                  val actualTypes = (0 until struct.size()).map(i => result.getChild(i).dataType())
                  if (!ColumnarArrowEvalPythonUDTFExec.sameTypes(resultTypes, actualTypes)) {
                    throw new IllegalStateException(
                      "Invalid schema from arrow-enabled Python UDTF: expected " +
                        s"${resultTypes.mkString(", ")}, got ${actualTypes.mkString(", ")}")
                  }
                  typesChecked = true
                }
                if (leftIndex < 0) {
                  leftIndex = builder.addParent(left)
                }
                builder.append(leftIndex, struct, numRows)
              }
            }
            if (builder.numRows == 0) {
              null
            } else {
              // The next batch writes `left` again if more of its results follow.
              leftIndex = -1
              builder.build()
            }
          }
        }

        val outputIter = new OutputIterator
        Iterators
          .wrap(outputIter)
          .recycleIterator(outputIter.close())
          .recyclePayload(_.close())
          .create()
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object ColumnarArrowEvalPythonUDTFExec {
  private val LargeVarTypesKey = "spark.sql.execution.arrow.useLargeVarTypes"

  /**
   * Whether the Python output types match the expected ones, ignoring string collations as Spark
   * 4.x does. Spark's own helpers for this differ between patch releases, so they aren't used.
   */
  private[python] def sameTypes(expected: Seq[DataType], actual: Seq[DataType]): Boolean = {
    expected.length == actual.length && expected.zip(actual).forall {
      case (_: StringType, _: StringType) => true
      case (e, a) => e == a
    }
  }
}

/**
 * Builds output batches from UDTF results and their parent (input) rows, in `allocator`. Each
 * parent row is written once, and its values are repeated for its output rows when the batch is
 * built.
 */
private class UDTFOutputBuilder(parentSchema: StructType, allocator: BufferAllocator) {
  private val parentRoot = ArrowColumnVectors.allocateRoot(parentSchema, allocator)
  private val parentWriter = ArrowWriter.create(parentRoot)
  private var numParents = 0
  // For each output row, the index of its parent row in parentRoot.
  private val parentIndices = new mutable.ArrayBuilder.ofInt
  // Created from the fields of the first result, so that the Python output can be appended as is.
  private var resultVectors: Array[FieldVector] = _
  private var closed = false

  var numRows = 0

  /** Writes a parent row and returns its index in the batch being built. */
  def addParent(row: InternalRow): Int = {
    parentWriter.write(row)
    numParents += 1
    numParents - 1
  }

  /** Appends the `numRows` rows of `result`, all joined with the parent row `parent`. */
  def append(parent: Int, result: StructVector, numRows: Int): Unit = {
    val sources = result.getChildrenFromFields.asScala
    if (resultVectors == null) {
      resultVectors = sources.map {
        source =>
          val target = source.getField.createVector(allocator)
          target.allocateNew()
          target
      }.toArray
    }
    sources.zip(resultVectors).foreach {
      case (source, target) => source.accept(new VectorAppender(target), null)
    }
    var i = 0
    while (i < numRows) {
      parentIndices += parent
      i += 1
    }
    this.numRows += numRows
  }

  /** Builds a batch of the rows appended so far, which owns its vectors, and resets the builder. */
  def build(): ColumnarBatch = {
    parentWriter.finish()
    val indices = parentIndices.result()
    val parents = parentRoot.getFieldVectors.asScala.map(repeat(_, indices))
    val vectors = (parents ++ resultVectors).asJava
    val batch =
      new ColumnarBatch(ArrowColumnVectors.wrap(vectors).toArray[ColumnVector], numRows)
    resultVectors = null
    parentWriter.reset()
    numParents = 0
    parentIndices.clear()
    numRows = 0
    batch
  }

  def close(): Unit = {
    if (!closed) {
      closed = true
      parentRoot.close()
      if (resultVectors != null) {
        resultVectors.foreach(_.close())
        resultVectors = null
      }
    }
  }

  private def repeat(source: FieldVector, indices: Array[Int]): FieldVector = {
    val target = source.getField.createVector(allocator)
    target.setInitialCapacity(indices.length)
    target.allocateNew()
    var i = 0
    while (i < indices.length) {
      target.copyFromSafe(indices(i), i, source)
      i += 1
    }
    target.setValueCount(indices.length)
    target
  }
}
