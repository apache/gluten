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
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.expression.InterpretedArrowGenerate
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.{ArrowColumnarBatch, ArrowColumnarRow, ArrowWritableColumnVector}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, Nondeterministic, SpecializedGetters}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExplainUtils, GenerateExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType, StringType, StructType}
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
    SparkShimLoader.getSparkShims.structFromAttributes(generateExec.generatorOutput)

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
    "velox_to_arrow_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of velox to Arrow ColumnarBatch"),
    "arrow_to_velox_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of Arrow ColumnarBatch to velox")
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
    val v2a = longMetric("velox_to_arrow_time")
    val a2v = longMetric("arrow_to_velox_time")
    child.executeColumnar().mapPartitionsWithIndex {
      (index, batches) =>
        generator.generator.foreach {
          case n: Nondeterministic => n.initialize(index)
          case _ =>
        }
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows() == 0) {
              Iterator.empty
            } else {
              val start = System.currentTimeMillis()
              val pruneChildInputData = ColumnarBatches
                .select(BackendsApiManager.getBackendName, batch, pruneChildColumnIndices.toArray)
              val generatorUsedInputData = ColumnarBatches
                .select(
                  BackendsApiManager.getBackendName,
                  batch,
                  generatorUsedColumnIndices.toArray)
              try {
                val generatedBatch =
                  getGeneratedResultVeloxArrow(
                    pruneChildInputData,
                    generatorUsedInputData,
                    batches.hasNext,
                    v2a,
                    a2v)

                totalTime += System.currentTimeMillis() - start
                generatedBatch
              } finally {
                pruneChildInputData.close()
                generatorUsedInputData.close()
              }
            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow()
          .recyclePayload(_.close())
          .create()
    }
  }

  private def loadArrowBatch(inputData: ColumnarBatch): ArrowColumnarBatch = {
    val sparkColumnarBatch = if (inputData.numCols() == 0) {
      inputData
    } else {
      ColumnarBatches.load(ArrowBufferAllocators.contextInstance(), inputData)
    }
    // In spark with version belows 4.0, the `ColumnarRow`'s get method doesn't check whether the
    // column to get is null, so we change it to `ArrowColumnarBatch` manually. `ArrowColumnarBatch`
    // returns `ArrowColumnarRow`, which fixes the bug.
    ColumnarBatches.convertToArrowColumnarBatch(sparkColumnarBatch)
  }

  private def isVariableWidthType(dt: DataType): Boolean = dt match {
    case BinaryType => true
    case StringType => true
    case StructType(fields) => fields.exists(field => isVariableWidthType(field.dataType))
    case ArrayType(elementType, _) => isVariableWidthType(elementType)
    case MapType(keyType, valueType, _) =>
      isVariableWidthType(keyType) || isVariableWidthType(valueType)
    case _ => false
  }

  private def getFieldSize(dt: DataType): (SpecializedGetters, Int) => Long = {
    val size: (SpecializedGetters, Int) => Long = dt match {
      case BinaryType => (input, i) => input.getBinary(i).length
      case StringType =>
        (input, i) => {
          input.getUTF8String(i).numBytes
        }
      case StructType(fields) =>
        val getFieldsSize = fields.map(field => getFieldSize(field.dataType))
        (input, i) => {
          val structData = input.getStruct(i, fields.length)
          val sizes = Array.fill(fields.length)(0L)
          for (i <- sizes.indices) {
            sizes(i) = sizes(i) + getFieldsSize(i)(structData, i)
          }
          sizes.max
        }
      case ArrayType(elementType, _) =>
        val innerSize = getFieldSize(elementType)
        (input, i) => {
          val arrayData = input.getArray(i)
          var size = 0L
          for (i <- 0 until arrayData.numElements()) {
            size = size + innerSize(arrayData, i)
          }
          size
        }
      case MapType(keyType, valueType, _) =>
        val getKeySize = getFieldSize(keyType)
        val getValueSize = getFieldSize(valueType)
        (input, i) => {
          val mapData = input.getMap(i)
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var keySize = 0L
          var valueSize = 0L
          for (i <- 0 until mapData.numElements()) {
            keySize = keySize + getKeySize(keyArray, i)
            valueSize = valueSize + getValueSize(valueArray, i)
          }
          Math.max(keySize, valueSize)
        }
      case _ => (_, _) => 0L // For fixed-width datatype, we let the size be 0.
    }
    (input: SpecializedGetters, i) => {
      if (input.isNullAt(i)) {
        0L
      } else {
        size(input, i)
      }
    }
  }

  private val fieldsSizeGetter = generateExec.generatorOutput.map {
    attribute => getFieldSize(attribute.dataType)
  }.toArray

  private val variableWidthFields = generateExec.generatorOutput.zipWithIndex
    .filter(tuple => isVariableWidthType(tuple._1.dataType))
    .map(_._2)
    .toArray

  private def writeRowUnsafe(rightRow: InternalRow, rightTargetRow: ArrowColumnarRow): Unit = {
    rightTargetRow.writeRowUnsafe(rightRow)
  }

  private def getResultColumnarBatch(
      rightResultVectors: Array[ArrowWritableColumnVector],
      resultLength: Int,
      leftInputData: ColumnarBatch,
      rowId2RowNum: Array[Int],
      a2v: SQLMetric): ColumnarBatch = {
    val rightTargetBatch =
      new ColumnarBatch(rightResultVectors.map(_.asInstanceOf[ColumnVector]), resultLength)
    val start = System.currentTimeMillis()
    val rightVeloxBatch = VeloxColumnarBatches.toVeloxBatch(
      ColumnarBatches
        .offload(ArrowBufferAllocators.contextInstance(), rightTargetBatch))
    val resultBatch = if (rightVeloxBatch.numCols() != 0) {
      val compositeBatch =
        VeloxColumnarBatches.repeatedThenCompose(leftInputData, rightVeloxBatch, rowId2RowNum)
      rightVeloxBatch.close()
      compositeBatch
    } else {
      rightVeloxBatch.close()
      ColumnarBatches.retain(leftInputData)
      leftInputData
    }
    a2v += System.currentTimeMillis() - start
    resultBatch
  }

  private def getGeneratedResultVeloxArrow(
      pruneChildInputData: ColumnarBatch,
      generatorUsedInputData: ColumnarBatch,
      hasNext: Boolean,
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
    if (!hasNext) {
      val resultRowsOption = generator.terminate()
      if (resultRowsOption.isDefined) {
        val resultRows = resultRowsOption.get
        rowResults ++= resultRows
        rowId2RowNum(inputRowId - 1) = rowId2RowNum(inputRowId - 1) + resultRows.size
      }
    }

    if (rowResults.isEmpty) {
      pruneChildInputData.close()
      generatorUsedInputData.close()
      rightArrowBatch.close()
      return Iterator.empty
    }

    val colSizes = Array.fill(generateExec.generatorOutput.length)(0L)
    rowResults.foreach {
      row =>
        for (i <- variableWidthFields) {
          colSizes(i) = colSizes(i) + fieldsSizeGetter(i)(row, i)
        }
    }

    val rightResultVectors: Array[ArrowWritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(rowResults.length, colSizes, rightSchema)
    val rightTargetRow = new ArrowColumnarRow(rightResultVectors)

    rowResults.foreach(row => writeRowUnsafe(row, rightTargetRow))
    rightTargetRow.finishWriteRow()

    val resultBatch =
      getResultColumnarBatch(
        rightResultVectors,
        rowResults.length,
        pruneChildInputData,
        rowId2RowNum,
        a2v)

    Iterators
      .wrap(Iterator.single(resultBatch))
      .recycleIterator({
        rightArrowBatch.close()
        rightResultVectors.foreach(_.close())
      })
      .create()
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
