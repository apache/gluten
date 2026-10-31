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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{BoltColumnarBatches, ColumnarBatches}
import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.expression.InterpretedArrowGenerate
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowColumnVectors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExplainUtils, GenerateExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * By rule <PartialGenerateRule>, if the generator is a instance of <HiveGenericUDTF>, then the
 * generateExec will be changed to ColumnarPartialGenerateExec
 *
 * @param generateExec
 *   the GenerateExec from vanilla
 * @param child
 *   child plan
 */
case class ColumnarPartialGenerateExec(generateExec: GenerateExec, child: SparkPlan)
  extends UnaryExecNode
  with ValidatablePlan {

  private val generatorNullRow = new GenericInternalRow(generateExec.generatorOutput.length)

  private val pruneChildAttributes: ListBuffer[Attribute] = ListBuffer()
  private val pruneChildColumnIndices: ListBuffer[Int] = ListBuffer()
  private val generatorUsedAttributes: ListBuffer[Attribute] = ListBuffer()
  private val generatorUsedColumnIndices: ListBuffer[Int] = ListBuffer()

  private var attrNotExists = false
  private var hasUnsupportedDataType = false

  private val rightSchema =
    ExpressionUtils.structFromAttributes(generateExec.generatorOutput)

  getColumnIndexInChildOutput(
    pruneChildAttributes,
    pruneChildColumnIndices,
    generateExec.requiredChildOutput)
  getColumnIndexInChildOutput(
    generatorUsedAttributes,
    generatorUsedColumnIndices,
    Seq(generateExec.generator))

  private lazy val generator = InterpretedArrowGenerate.create(
    bindReferences(Seq(generateExec.generator), generatorUsedAttributes.toSeq).head)

  @transient override lazy val metrics = Map(
    "time" -> SQLMetrics.createTimingMetric(sparkContext, "total time of partial generate"),
    "bolt_to_arrow_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of bolt to Arrow ColumnarBatch"),
    "arrow_to_bolt_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of Arrow ColumnarBatch to bolt")
  )

  private def getColumnIndexInChildOutput(
      attributes: ListBuffer[Attribute],
      indices: ListBuffer[Int],
      exprs: Seq[Expression]): Unit = {
    exprs.foreach {
      case a: AttributeReference =>
        val index = child.output.indexWhere(s => s.exprId.equals(a.exprId))

        if (index < 0) {
          attrNotExists = true
          log.debug(s"Couldn't find $a in ${child.output.attrs.mkString("[", ",", "]")}")
        } else if (
          BackendsApiManager.getValidatorApiInstance.doSchemaValidate(a.dataType).isDefined
        ) {
          log.debug(s"Expression $a contains unsupported data type ${a.dataType}")
          hasUnsupportedDataType = true
        } else if (!indices.contains(index)) {
          attributes.append(a)
          indices.append(index)
        }
      case p =>
        getColumnIndexInChildOutput(attributes, indices, p.children)
    }
  }

  override def outputPartitioning(): Partitioning = child.outputPartitioning

  override protected def doCanonicalize(): ColumnarPartialGenerateExec = {
    val canonicalized = generateExec.canonicalized.asInstanceOf[GenerateExec]
    this.copy(canonicalized, child.canonicalized)
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (attrNotExists) {
      return ValidationResult.failed("Attribute in the generator does not exists in its child")
    }
    if (hasUnsupportedDataType) {
      return ValidationResult.failed("Attribute in the generator contains unsupported type")
    }
    ValidationResult.succeeded
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val totalTime = longMetric("time")
    val v2a = longMetric("bolt_to_arrow_time")
    val a2v = longMetric("arrow_to_bolt_time")
    child.executeColumnar().mapPartitionsWithIndex {
      (index, batches) =>
        generator.generator.foreach {
          case n: Nondeterministic => n.initialize(index)
          case _ =>
        }
        // Keep the last required-child selection alive until terminate(), even if advancing the
        // upstream iterator recycles its input batches. Spark joins termination output to this row.
        var lastChildInput: Option[ColumnarBatch] = None
        def releaseLastChild(): Unit = {
          val previous = lastChildInput
          lastChildInput = None
          previous.foreach(_.close())
        }
        val res = new Iterator[Iterator[ColumnarBatch]] {
          private var terminated = false
          override def hasNext: Boolean = !terminated

          override def next(): Iterator[ColumnarBatch] = {
            if (terminated) {
              throw new NoSuchElementException("Partial generate partition exhausted")
            }
            if (!batches.hasNext) {
              terminated = true
              try {
                getTerminatedResultBoltArrow(lastChildInput, a2v)
              } finally {
                releaseLastChild()
              }
            } else {
              val batch = batches.next()
              if (batch.numRows() == 0) {
                Iterator.empty
              } else {
                val start = System.currentTimeMillis()
                val pruneChildInputData = ColumnarBatches.select(
                  BackendsApiManager.getBackendName,
                  batch,
                  pruneChildColumnIndices.toArray)
                try {
                  val generatorUsedInputData = ColumnarBatches.select(
                    BackendsApiManager.getBackendName,
                    batch,
                    generatorUsedColumnIndices.toArray)
                  try {
                    releaseLastChild()
                    ColumnarBatches.retain(pruneChildInputData)
                    lastChildInput = Some(pruneChildInputData)
                    getGeneratedResultBoltArrow(
                      pruneChildInputData,
                      generatorUsedInputData,
                      v2a,
                      a2v)
                  } finally {
                    generatorUsedInputData.close()
                  }
                } finally {
                  pruneChildInputData.close()
                  totalTime += System.currentTimeMillis() - start
                }
              }
            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow()
          .recyclePayload(_.close())
          .recycleIterator(releaseLastChild())
          .create()
    }
  }

  private def loadArrowBatch(inputData: ColumnarBatch): ColumnarBatch = {
    if (inputData.numCols() == 0) {
      inputData
    } else {
      ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), inputData)
    }
  }

  private def getResultColumnarBatch(
      rightTargetBatch: ColumnarBatch,
      resultLength: Int,
      leftInputData: ColumnarBatch,
      rowId2RowNum: Array[Int],
      a2v: SQLMetric): ColumnarBatch = {
    val start = System.currentTimeMillis()
    try {
      val rightBoltBatch = BoltColumnarBatches.toBoltBatch(
        ColumnarBatches.offload(ArrowBufferAllocators.contextInstance(), rightTargetBatch))
      if (rightBoltBatch.numCols() != 0) {
        BoltColumnarBatches.repeatedThenCompose(leftInputData, rightBoltBatch, rowId2RowNum)
      } else {
        ColumnarBatches.retain(leftInputData)
        leftInputData
      }
    } finally {
      // offload/toBoltBatch transfer their vectors into rightTargetBatch. Close that ownership once.
      rightTargetBatch.close()
      a2v += System.currentTimeMillis() - start
    }
  }

  private def getTerminatedResultBoltArrow(
      lastChildInput: Option[ColumnarBatch],
      a2v: SQLMetric): Iterator[ColumnarBatch] = {
    generator.terminate() match {
      case None => Iterator.empty
      case Some(rows) =>
        val left = lastChildInput.getOrElse {
          if (pruneChildAttributes.nonEmpty) {
            // Spark's JoinedRow has no left row when termination emits without any input.
            throw new NullPointerException(
              "Generator termination has no input row for required child output")
          }
          new ColumnarBatch(Array.empty[ColumnVector], 1)
        }
        try {
          val repeats = Array.fill(left.numRows())(0)
          repeats(left.numRows() - 1) = rows.size
          val right = ArrowColumnVectors.fromRows(rightSchema, rows.iterator)
          Iterator.single(getResultColumnarBatch(right, rows.size, left, repeats, a2v))
        } finally {
          if (lastChildInput.isEmpty) {
            left.close()
          }
        }
    }
  }

  private def getGeneratedResultBoltArrow(
      pruneChildInputData: ColumnarBatch,
      generatorUsedInputData: ColumnarBatch,
      v2a: SQLMetric,
      a2v: SQLMetric): Iterator[ColumnarBatch] = {
    // select part of child output and child data
    val numRows = generatorUsedInputData.numRows()
    val start = System.currentTimeMillis()
    val rightArrowBatch = loadArrowBatch(generatorUsedInputData)

    v2a += System.currentTimeMillis() - start

    val rowId2RowNum = Array.fill(numRows)(0)
    var inputRowId = 0

    val rowResults = new ArrayBuffer[InternalRow]()
    while (inputRowId < numRows) {
      val row = rightArrowBatch.getRow(inputRowId)
      val resultRowsOption = generator.apply(row)
      if (resultRowsOption.isDefined) {
        val resultRows = resultRowsOption.get
        rowResults ++= resultRows
        rowId2RowNum(inputRowId) = resultRows.size
      } else if (generateExec.outer) {
        rowResults.append(generatorNullRow)
        rowId2RowNum(inputRowId) = 1
      }
      inputRowId = inputRowId + 1
    }
    if (rowResults.isEmpty) {
      return Iterator.empty
    }

    val rightTargetBatch = ArrowColumnVectors.fromRows(rightSchema, rowResults.iterator)

    val resultBatch =
      getResultColumnarBatch(
        rightTargetBatch,
        rowResults.length,
        pruneChildInputData,
        rowId2RowNum,
        a2v)

    // Loading transfers the Arrow vectors into generatorUsedInputData. Its caller owns the sole
    // close; all generated rows have been materialized before this method returns.
    Iterator.single(resultBatch)
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("GenerateExec", generateExec)}
       |""".stripMargin
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + " PartialGenerate " + generateExec

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  override def output: Seq[Attribute] = generateExec.output

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarPartialGenerateExec = {
    copy(child = newChild)
  }
}

object ColumnarPartialGenerateExec {
  def create(original: GenerateExec): ColumnarPartialGenerateExec = {
    ColumnarPartialGenerateExec(original, original.child)
  }
}
