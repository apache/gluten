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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.{ValidatablePlan, ValidationResult}
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.utils.PullOutProjectHelper
import org.apache.gluten.vectorized.{ArrowColumnVectors, RefCountedArrowColumnVector}

import org.apache.spark.{ContextAwareIterator, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BasePythonRunnerShim, EvalPythonExecBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.utils.SparkVectorUtil
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{SparkVersionUtil, Utils}

import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.Schema

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.nowarn
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ColumnarArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argMetas: Array[Array[(Int, Option[String])]],
    schema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric])
  extends BasePythonRunnerShim(funcs, evalType, argMetas, pythonMetrics) {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  protected def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: scala.Option[scala.Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ReaderIterator(stream, writer, startTime, env, worker, pid, releasedOrClosed, context) {
      private val allocator = ArrowBufferAllocators.contextInstance()

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var pendingOutput: ColumnarBatch = _

      context.addTaskCompletionListener[Unit] {
        _ =>
          Utils.tryWithSafeFinally {
            if (pendingOutput != null) {
              pendingOutput.close()
              pendingOutput = null
            }
          } {
            Utils.tryWithSafeFinally {
              if (reader != null) {
                reader.close(false)
              }
            } {
              if (root != null) {
                root.close()
              }
            }
          }
      }

      private var batchLoaded = true

      override def next(): ColumnarBatch = {
        val batch = super.next()
        // ReaderIterator.hasNext may prefetch without transferring the batch to its caller.
        pendingOutput = null
        batch
      }

      override protected def read(): ColumnarBatch = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              // The reader reuses root across batches. Give each result its own vectors and
              // retained buffer references so prefetch and reader.close cannot invalidate it.
              val recordBatch = new VectorUnloader(root).getRecordBatch
              val outputRoot = VectorSchemaRoot.create(root.getSchema, allocator)
              try {
                new VectorLoader(outputRoot).load(recordBatch)
                pendingOutput = ArrowColumnVectors.toBatch(outputRoot)
                pendingOutput
              } catch {
                case t: Throwable =>
                  outputRoot.close()
                  throw t
              } finally {
                recordBatch.close()
              }
            } else {
              reader.close(false)
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }

  override def createNewWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {
      override protected def writeCommand(dataOut: DataOutputStream): Unit = {
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }
        ColumnarArrowPythonRunner.this.writeUdf(dataOut, argMetas)
      }

      private val inputWriter = new BoltArrowPythonInputWriter(
        inputIterator,
        schema,
        timeZoneId,
        () => ArrowBufferAllocators.contextInstance())

      context.addTaskCompletionListener[Unit] {
        _ =>
          try {
            inputWriter.close()
          } catch {
            case scala.util.control.NonFatal(e) =>
              // Preserve the task's original failure while making truncated streams diagnosable.
              logWarning("Failed to close Arrow Python input during task completion", e)
          }
      }

      def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        inputWriter.writeToStream(dataOut)
      }

      def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        inputWriter.writeNext(dataOut)
      }

    }
  }

}

case class ColumnarArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends EvalPythonExecBase
  with ValidatablePlan {

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override protected def doValidateInternal(): ValidationResult = {
    if (evalType == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF) {
      return ValidationResult.failed(
        "Scalar iterator pandas UDFs require input/output alignment by row count")
    }
    val (_, inputs) = udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
    inputs.foreach {
      input =>
        input.foreach {
          case e: AttributeReference if child.output.exists(_.exprId == e.exprId) =>
          case _: AttributeReference =>
            return ValidationResult.failed("Expression Id does not exist for AttributeReference")
          case _ =>
            return ValidationResult.failed("UDF input is not an instance of AttributeReference")
        }
    }
    super.doValidateInternal()
  }

  override def requiredChildConvention(): Seq[ConventionReq] = List(
    ConventionReq.ofBatch(ConventionReq.BatchType.Is(ArrowJavaBatchType)))

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_arrow_udf")
  )

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone

  private def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
    val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
    val pandasColsByName = Seq(
      SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
        conf.pandasGroupedMapAssignColumnsByName.toString)
    val arrowSafeTypeCheck = Seq(
      SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key ->
        conf.arrowSafeTypeConversion.toString)
    Map(timeZoneConf.toSeq ++ pandasColsByName.toSeq ++ arrowSafeTypeCheck: _*)
  }

  private val pythonRunnerConf = getPythonRunnerConfMap(conf)

  protected def evaluateColumnar(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[(Int, Option[String])]],
      iter: Iterator[ColumnarBatch],
      schema: StructType,
      context: TaskContext): Iterator[ColumnarBatch] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    val columnarBatchIter = new ColumnarArrowPythonRunner(
      funcs,
      evalType,
      argMetas,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf,
      Map()).compute(iter, context.partitionId(), context)

    columnarBatchIter.map {
      batch =>
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        assert(
          outputTypes == actualDataTypes,
          "Invalid schema from arrow_udf: " +
            s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
        batch
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputRows = longMetric("numInputRows")
    val procTime = longMetric("processTime")
    val inputRDD = child.executeColumnar()
    inputRDD.mapPartitions {
      iter =>
        val context = TaskContext.get()
        val (pyFuncs, inputs) = udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val originalOffsets = new ArrayBuffer[Int]
        val argMetas: Array[Array[(Int, Option[String])]] = if (SparkVersionUtil.gteSpark40) {
          inputs.map {
            input =>
              input.map {
                e =>
                  val (key, value) = e match {
                    case EvalPythonExecBase.NamedArgumentExpressionShim(key, value) =>
                      (Some(key), value)
                    case _ =>
                      (Option.empty[String], e)
                  }
                  if (allInputs.exists(_.semanticEquals(value))) {
                    allInputs.indexWhere(_.semanticEquals(value)) -> key
                  } else {
                    val offset = child.output.indexWhere(
                      _.exprId.equals(e.asInstanceOf[AttributeReference].exprId))
                    originalOffsets += offset
                    allInputs += value
                    dataTypes += value.dataType
                    (allInputs.length - 1) -> key
                  }
              }.toArray
          }.toArray
        } else {
          inputs.map {
            input =>
              input.map {
                e =>
                  if (allInputs.exists(_.semanticEquals(e))) {
                    allInputs.indexWhere(_.semanticEquals(e)) -> Option.empty[String]
                  } else {
                    val offset = child.output.indexWhere(
                      _.exprId.equals(e.asInstanceOf[AttributeReference].exprId))
                    originalOffsets += offset
                    allInputs += e
                    dataTypes += e.dataType
                    (allInputs.length - 1) -> Option.empty[String]
                  }
              }.toArray
          }.toArray
        }
        val schema = StructType(dataTypes.zipWithIndex.map {
          case (dt, i) =>
            StructField(s"_$i", dt)
        }.toSeq)

        @nowarn val contextAwareIterator = new ContextAwareIterator(context, iter)
        val inputCbCache = new ArrayBuffer[ColumnarBatch]()
        var start_time: Long = 0
        val inputBatchIter = contextAwareIterator.map {
          inputCb =>
            start_time = System.nanoTime()
            ColumnarBatches.checkLoaded(inputCb)
            ColumnarBatches.retain(inputCb)
            inputCbCache += inputCb
            numInputRows += inputCb.numRows
            val colsForEval = new ArrayBuffer[ColumnVector]()
            for (i <- originalOffsets) {
              colsForEval += inputCb.column(i)
            }
            new ColumnarBatch(colsForEval.toArray, inputCb.numRows())
        }

        val outputColumnarBatchIterator =
          evaluateColumnar(pyFuncs, argMetas, inputBatchIter, schema, context)
        val res =
          outputColumnarBatchIterator.zipWithIndex.map {
            case (outputCb, batchId) =>
              val inputCb = inputCbCache(batchId)
              val batch =
                try {
                  ColumnarArrowEvalPythonExec.joinWithIndependentInput(inputCb, outputCb)
                } catch {
                  case t: Throwable =>
                    outputCb.close()
                    throw t
                }
              val numRows = inputCb.numRows
              numOutputBatches += 1
              numOutputRows += numRows
              ColumnarBatches.checkLoaded(batch)
              // The result has independent input vectors; release only the cache's reference.
              ColumnarBatches.release(inputCb)
              inputCbCache(batchId) = null
              procTime += (System.nanoTime() - start_time) / 1000000
              batch
          }
        Iterators
          .wrap(res)
          .recycleIterator {
            inputCbCache.foreach {
              batch => if (batch != null) ColumnarBatches.release(batch)
            }
          }
          .recyclePayload(_.close())
          .create()
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarArrowEvalPythonExec =
    copy(udfs, resultAttrs, newChild)
}

object ColumnarArrowEvalPythonExec {

  private[python] def joinWithIndependentInput(
      input: ColumnarBatch,
      output: ColumnarBatch): ColumnarBatch = {
    require(input.numRows() == output.numRows(), "Python output batch row count differs from input")
    val fields = (0 until input.numCols()).map(
      i => input.column(i).asInstanceOf[RefCountedArrowColumnVector].getValueVector.getField)
    val recordBatch = SparkVectorUtil.toArrowRecordBatch(input)
    var retainedRoot: VectorSchemaRoot = null
    try {
      retainedRoot = VectorSchemaRoot.create(
        new Schema(fields.asJava),
        ArrowBufferAllocators.contextInstance())
      new VectorLoader(retainedRoot).load(recordBatch)
      // Independent vector containers prevent upstream ownership from affecting result refcounts.
      val inputColumns = ArrowColumnVectors.wrap(retainedRoot.getFieldVectors)
      val joinedColumns = inputColumns.map(_.asInstanceOf[ColumnVector]) ++
        (0 until output.numCols()).map(output.column)
      new ColumnarBatch(joinedColumns, input.numRows())
    } catch {
      case t: Throwable =>
        if (retainedRoot != null) {
          retainedRoot.close()
        }
        throw t
    } finally {
      recordBatch.close()
    }
  }

  def collectFunctions(udf: PythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        assert(!children.exists(_.isInstanceOf[PythonUDF]))
        ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
    }
  }
}

object PullOutArrowEvalPythonPreProjectHelper extends PullOutProjectHelper {

  private def rewriteUDF(
      udf: PythonUDF,
      expressionMap: mutable.HashMap[Expression, NamedExpression]): PythonUDF = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        udf
          .withNewChildren(udf.children.toIndexedSeq.map {
            func => rewriteUDF(func.asInstanceOf[PythonUDF], expressionMap)
          })
          .asInstanceOf[PythonUDF]
      case children =>
        val newUDFChildren = udf.children.map {
          case literal: Literal => literal
          case other => replaceExpressionWithAttribute(other, expressionMap)
        }
        udf.withNewChildren(newUDFChildren).asInstanceOf[PythonUDF]
    }
  }

  def pullOutPreProject(arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan = {
    val (_, inputs) =
      arrowEvalPythonExec.udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
    val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
    val allInputs = new ArrayBuffer[Expression]
    for (input <- inputs) {
      input.foreach {
        e =>
          if (!allInputs.exists(_.semanticEquals(e))) {
            allInputs += e
            replaceExpressionWithAttribute(e, expressionMap)
          }
      }
    }
    if (!expressionMap.isEmpty) {
      val preProject = ProjectExec(
        eliminateProjectList(arrowEvalPythonExec.child.outputSet, expressionMap.values.toSeq),
        arrowEvalPythonExec.child)
      val newUDFs = arrowEvalPythonExec.udfs.map(f => rewriteUDF(f, expressionMap))
      val newArrowEvalPythonExec = arrowEvalPythonExec.copy(udfs = newUDFs, child = preProject)
      newArrowEvalPythonExec.copyTagsFrom(arrowEvalPythonExec)
      newArrowEvalPythonExec
    } else {
      arrowEvalPythonExec
    }
  }
}
