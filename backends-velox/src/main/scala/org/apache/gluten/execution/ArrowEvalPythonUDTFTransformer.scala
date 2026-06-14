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

import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, ArrowPythonUDTFRunner, BatchIterator, PythonUDTF}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.types.DataType.equalsIgnoreCompatibleCollation
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * A physical plan that evaluates a [[PythonUDTF]] using Apache Arrow in Gluten. This implementation
 * takes row-based input, converts it to Arrow format, executes the Python UDTF, and returns
 * columnar output.
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
  extends UnaryExecNode with ValidatablePlan {

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override protected def doValidateInternal(): ValidationResult = {
    super.doValidateInternal()
  }
  
  override def requiredChildConvention(): Seq[ConventionReq] = {
    Seq(ConventionReq.vanillaRow)
  }

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
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows")
  )

  override def supportsColumnar: Boolean = true

  protected def evaluate(
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[ColumnarBatch]] = {

    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val outputTypes = resultAttrs.map(_.dataType.transformRecursively {
      case udt: UserDefinedType[_] => udt.sqlType
    })

    val columnarBatchIter = new ArrowPythonUDTFRunner(
      udtf,
      evalType,
      argMetas,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      Map.empty, // Python metrics
      jobArtifactUUID,
      sessionUUID
    ).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.map {
      batch =>
        val numOutputRows = metrics("numOutputRows")
        numOutputRows += batch.numRows()

        // UDTF returns a StructType column in ColumnarBatch
        // Return the batch as-is wrapped in an iterator
        Iterator.single(batch)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "ArrowEvalPythonUDTFTransformer does not support row-based execution")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val inputRDD = child.execute().map(_.copy())
    val numOutputBatches = metrics("numOutputBatches")
    val numInputRows = metrics("numInputRows")

    inputRDD.mapPartitions {
      iter =>
        val context = TaskContext.get()

        // Flatten all the arguments
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val argMetas = udtf.children
          .zip(
            udtf.tableArguments.getOrElse(Seq.fill(udtf.children.length)(false))
          )
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

        // Flatten the nested iterator and count batches
        outputBatchIterator.flatMap {
          batchIter =>
            batchIter.map {
              batch =>
                numOutputBatches += 1
                batch
            }
        }
    }
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
