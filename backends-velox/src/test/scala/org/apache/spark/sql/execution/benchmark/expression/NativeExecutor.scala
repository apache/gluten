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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.columnarbatch.ColumnarBatchJniWrapper
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.ColumnarCoalesceExec
import org.apache.gluten.execution.ColumnarToRowExecBase
import org.apache.gluten.execution.GPUStageMode
import org.apache.gluten.execution.VeloxResizeBatchesExec
import org.apache.gluten.execution.WholeStageTransformer
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.Arm
import org.apache.gluten.vectorized.ColumnarBatchOutIterator
import org.apache.gluten.vectorized.ExpressionBenchmarkJni

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import java.nio.file.Path
import java.util.Collections

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

private[expression] class NativeExecutor(
    plan: ExpressionPlan,
    nativeInputs: Seq[Array[ColumnarBatch]],
    batchSize: Int,
    shuffleDir: () => Path,
    collector: Option[ColumnarBatch => Unit]) {
  private val caseTask = TaskContext.get()
  require(caseTask != null, "Native execution resources require a case task scope")

  private def withCaseResources[T](body: => T): T = {
    val current = TaskContext.get()
    TaskContext.setTaskContext(caseTask)
    try body
    finally TaskContext.setTaskContext(current)
  }
  @tailrec
  private def nativeStage(root: SparkPlan): WholeStageTransformer = root match {
    case whole: WholeStageTransformer => whole
    case scan: RDDScanTransformer if plan.window =>
      WholeStageTransformer(InputIteratorTransformer(ColumnarInputAdapter(scan)))(0)
    case c: ColumnarToRowExecBase if root eq plan.nativePlan => nativeStage(c.child)
    case r: VeloxResizeBatchesExec if plan.nativeExchange.exists(_.child eq r) =>
      require(!r.executionMode.contains(GPUStageMode), "GPU batch resizing is unsupported")
      nativeStage(r.child)
    case other => throw new IllegalArgumentException(s"Unsupported native stage: $other")
  }
  @tailrec
  private def nativeInput(node: SparkPlan): SparkPlan = node match {
    case InputIteratorTransformer(ColumnarInputAdapter(child)) => child
    case other if other.children.size == 1 => nativeInput(other.children.head)
    case other => throw new IllegalArgumentException(s"Expected one native input: $other")
  }
  private lazy val runtime = withCaseResources {
    Runtimes.contextInstance(
      BackendsApiManager.getBackendName,
      "NativeExecutor",
      Collections.singletonMap(
        "spark.gluten.sql.columnar.backend.velox.spillStrategy",
        "none")
    )
  }
  private lazy val jni = new ExpressionBenchmarkJni(runtime)
  private val nativeMetrics = new Array[Long](2)
  private lazy val inputHandles = withCaseResources {
    val owned = ArrayBuffer.empty[Long]
    try {
      val handles = nativeInputs.map(_.map {
        batch =>
          val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
          if (batch.numCols() == 0) owned += handle
          handle
      }).toArray
      TaskResources.addRecycler("native input handles", 100)(
        owned.foreach(ColumnarBatchJniWrapper.close))
      handles
    } catch {
      case error: Throwable =>
        owned.foreach(ColumnarBatchJniWrapper.close)
        throw error
    }
  }
  private lazy val nativeHandle = withCaseResources {
    lazy val nativeStages =
      plan.nativeExchange.toSeq.map(e => nativeStage(e.child)) :+
        nativeStage(plan.nativePlan)
    def resizeOutput: Boolean = plan.nativeExchange.exists {
      exchange =>
        nativeInput(nativeStages.last.child) match {
          case r: VeloxResizeBatchesExec =>
            require(
              (r.child eq exchange) && !r.executionMode.contains(GPUStageMode),
              "Unsupported shuffle reader resize")
            true
          case other =>
            require(other eq exchange, "Native stage input differs")
            false
        }
    }
    val resize = VeloxConfig.get
    val range = resize.veloxResizeBatchesShuffleInputOutputRange
    val handle = jni.compile(
      plan.inPlanningScope {
        nativeStages.map {
          stage =>
            // Inputs are already combined locally; preserve work below the coalesce boundary.
            val child = stage.child.transformUp {
              case InputIteratorTransformer(ColumnarInputAdapter(
                    ColumnarCoalesceExec(1, child: WholeStageTransformer))) => child.child
            }
            stage.copy(child = child)(stage.transformStageId).substraitPlan.toProtobuf.toByteArray
        }.toArray
      },
      batchSize,
      plan.nativeExchange.exists(_.outputPartitioning.isInstanceOf[
        HashPartitioning]),
      plan.nativeExchange.exists(_.child.isInstanceOf[VeloxResizeBatchesExec]),
      resizeOutput,
      range.min,
      range.max,
      resize.veloxPreferredBatchBytes,
      resize.enableVeloxResizeBatchesCopyRanges,
      if (plan.nativeExchange.isDefined) shuffleDir().toString else ""
    )
    TaskResources.addRecycler("native pipeline", 110)(jni.close(handle))
    handle
  }
  private def output(handle: Long): ColumnarBatchOutIterator = {
    new ColumnarBatchOutIterator(runtime, handle) {
      override def close0(): Unit = jni.close(handle)
    }
  }
  def execute(): Unit = consume(output(jni.execute(nativeHandle, inputHandles, nativeMetrics)))
  protected def consume(output: ColumnarBatchOutIterator): Unit = {
    nativeMetrics(0) = 0
    Arm.withResource(output) {
      output =>
        while (output.hasNext) {
          val batch = output.next()
          nativeMetrics(0) += batch.numRows()
          Arm.withResource(batch)(b => collector.foreach(_(b)))
        }
    }
  }
  def metadata: Map[String, String] = Map(
    "native.retainedPoolBytes" -> jni.usedBytes().toString,
    "native.outputRows" -> nativeMetrics(0).toString,
    "native.serializedBytes" -> nativeMetrics(1).toString
  )
}
