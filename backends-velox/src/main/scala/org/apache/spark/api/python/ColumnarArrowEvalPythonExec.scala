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
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.{ContextAwareIterator, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BasePythonRunnerShim, EvalPythonExecBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil, SparkVectorUtil}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{SparkVersionUtil, Utils}

import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.nowarn
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
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      context.addTaskCompletionListener[Unit] {
        _ =>
          if (reader != null) {
            reader.close(false)
          }
          if (root != null) {
            root.close()
          }
      }

      private var batchLoaded = true

      override protected def read(): ColumnarBatch = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              batch
            } else {
              reader.close(false)
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                schema = SparkArrowUtil.fromArrowSchema(root.getSchema())
                vectors = ArrowWritableColumnVector
                  .loadColumns(root.getRowCount, root.getFieldVectors)
                  .toArray[ColumnVector]
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
        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }
        ColumnarArrowPythonRunner.this.writeUdf(dataOut, argMetas)
      }

      // For Spark earlier than 4.0. It overrides the corresponding abstract method
      // in Writer class. We omitted the override keyword for compatibility consideration.
      def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        writeToStreamHelper(dataOut)
      }

      // For Spark 4.0. It overrides the corresponding abstract method in Writer class.
      // We omitted the override keyword for compatibility consideration.
      def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        writeToStreamHelper(dataOut)
      }

      def writeToStreamHelper(dataOut: DataOutputStream): Boolean = {
        if (!inputIterator.hasNext) {
          // See https://issues.apache.org/jira/browse/SPARK-44705:
          // Starting from Spark 4.0, we should return false once the iterator is drained out,
          // otherwise Spark won't stop calling this method repeatedly.
          return false
        }
        var numRows: Long = 0
        val arrowSchema = SparkSchemaUtil.toArrowSchema(schema, timeZoneId)
        val allocator = ArrowBufferAllocators.contextInstance()
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val loader = new VectorLoader(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()
          while (inputIterator.hasNext) {
            val nextBatch = inputIterator.next()
            numRows += nextBatch.numRows

            val cols = (0 until nextBatch.numCols).toList.map(
              i =>
                nextBatch
                  .asInstanceOf[ColumnarBatch]
                  .column(i)
                  .asInstanceOf[ArrowWritableColumnVector]
                  .getValueVector)
            val nextRecordBatch =
              SparkVectorUtil.toArrowRecordBatch(nextBatch.numRows, cols)
            loader.load(nextRecordBatch)
            writer.writeBatch()
            if (nextRecordBatch != null) {
              nextRecordBatch.close()
            }
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
          true
        } {
          root.close()
          // allocator can't close now or the data will loss
          // allocator.close()
        }
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

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doValidateInternal(): ValidationResult = {
    val (_, inputs) = udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
    inputs.foreach {
      input =>
        input.foreach {
          case e: AttributeReference if child.output.exists(_.exprId == e.exprId) =>
          // Valid case, continue validation
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
        // We only write the referred cols by UDFs to python worker. So we need
        // get corresponding offsets
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val originalOffsets = new ArrayBuffer[Int]
        val argMetas: Array[Array[(Int, Option[String])]] = if (SparkVersionUtil.gteSpark40) {
          // Spark 4.0 requires ArgumentMetadata rather than trivial integer-based offset.
          // See https://issues.apache.org/jira/browse/SPARK-44918.
          inputs.map {
            input =>
              input.map {
                e =>
                  val (key, value) = e match {
                    case EvalPythonExecBase.NamedArgumentExpressionShim(key, value) =>
                      (Some(key), value)
                    case _ =>
                      (None, e)
                  }
                  val pair: (Int, Option[String]) = if (allInputs.exists(_.semanticEquals(value))) {
                    allInputs.indexWhere(_.semanticEquals(value)) -> key
                  } else {
                    val offset = child.output.indexWhere(
                      _.exprId.equals(e.asInstanceOf[AttributeReference].exprId))
                    originalOffsets += offset
                    allInputs += value
                    dataTypes += value.dataType
                    (allInputs.length - 1) -> key
                  }
                  pair
              }.toArray
          }.toArray
        } else {
          inputs.map {
            input =>
              input.map {
                e =>
                  val pair: (Int, Option[String]) = if (allInputs.exists(_.semanticEquals(e))) {
                    allInputs.indexWhere(_.semanticEquals(e)) -> None
                  } else {
                    val offset = child.output.indexWhere(
                      _.exprId.equals(e.asInstanceOf[AttributeReference].exprId))
                    originalOffsets += offset
                    allInputs += e
                    dataTypes += e.dataType
                    (allInputs.length - 1) -> None
                  }
                  pair
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
            // 0. cache input for later merge
            inputCbCache += inputCb
            numInputRows += inputCb.numRows
            // We only need to pass the referred cols data to python worker for evaluation.
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
              val joinedVectors = (0 until inputCb.numCols).toArray.map(
                i => inputCb.column(i)) ++ (0 until outputCb.numCols).toArray.map(
                i => outputCb.column(i))
              // Columns in outputCb has random 0 or 1 refCnt and will fail checks in ensureOffload,
              // so we do a hard reset here.
              (0 until joinedVectors.length).foreach(
                i => {
                  adjustRefCnt(joinedVectors(i).asInstanceOf[ArrowWritableColumnVector], 1)
                })
              val numRows = inputCb.numRows
              numOutputBatches += 1
              numOutputRows += numRows
              val batch = new ColumnarBatch(joinedVectors, numRows)
              ColumnarBatches.checkLoaded(batch)
              procTime += (System.nanoTime() - start_time) / 1000000
              batch
          }
        Iterators
          .wrap(res)
          .recycleIterator {
            inputCbCache.foreach(ColumnarBatches.release(_))
          }
          .recyclePayload(_.close())
          .create()
    }
  }

  private def adjustRefCnt(vector: ArrowWritableColumnVector, to: Long): Unit = {
    val from = vector.refCnt()
    if (from == to) {
      return
    }
    if (from > to) {
      do {
        vector.close()
      } while (vector.refCnt() != to)
      return
    }
    // from < to
    do {
      vector.retain()
    } while (vector.refCnt() != to)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarArrowEvalPythonExec =
    copy(udfs, resultAttrs, newChild)
}

object ColumnarArrowEvalPythonExec {

  def collectFunctions(udf: PythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        // There should be no PythonUDF, or the children can't be evaluated directly.
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
    // pull out preproject
    val (_, inputs) =
      arrowEvalPythonExec.udfs.map(ColumnarArrowEvalPythonExec.collectFunctions).unzip
    val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
    // flatten all the arguments
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
      // Need preproject.
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
