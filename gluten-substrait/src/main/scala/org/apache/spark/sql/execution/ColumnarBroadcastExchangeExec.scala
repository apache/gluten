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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.ValidatablePlan
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkFatalException

import java.util.UUID
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.concurrent.Promise
import scala.util.control.NonFatal

case class ColumnarBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan)
  extends BroadcastExchangeLike
  with ValidatablePlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genColumnarBroadcastExchangeMetrics(sparkContext)

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  override lazy val relationFuture: java.util.concurrent.Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      BroadcastExchangeExec.executionContext) {
      try {
        SparkShimLoader.getSparkShims.setJobDescriptionOrTagForBroadcastExchange(sparkContext, this)
        val relation = GlutenTimeMetric.millis(longMetric("collectTime")) {
          _ =>
            // this created relation ignore HashedRelationBroadcastMode isNullAware, because we
            // cannot get child output rows, then compare the hash key is null, if not null,
            // compare the isNullAware, so gluten will not generate HashedRelationWithAllNullKeys
            // or EmptyHashedRelation, this difference will cause performance regression in some
            // cases.
            // For the above reason, the same implementation can be used for both
            // HashedRelationBroadcastMode as well as IdentityBroadcastMode.
            BackendsApiManager.getSparkPlanExecApiInstance.createBroadcastRelation(
              mode,
              child,
              longMetric("numOutputRows"),
              longMetric("dataSize"))
        }

        val broadcasted = GlutenTimeMetric.millis(longMetric("broadcastTime")) {
          _ =>
            // Broadcast the relation
            SparkShimLoader.getSparkShims.broadcastInternal(
              sparkContext,
              relation.asInstanceOf[Any])
        }

        // Update driver metrics
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

        promise.success(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(
            new OutOfMemoryError(
              "Not enough memory to build and broadcast the table to all " +
                "worker nodes. As a workaround, you can either disable broadcast by setting " +
                s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
              .initCause(oe.getCause))
          promise.failure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.failure(ex)
          throw ex
        case e: Throwable =>
          promise.failure(e)
          throw e
      }
    }
  }

  override val runId: UUID = UUID.randomUUID

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override def doCanonicalize(): SparkPlan = {
    ColumnarBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "ColumnarBroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          SparkShimLoader.getSparkShims.cancelJobGroupForBroadcastExchange(sparkContext, this)
          relationFuture.cancel(true)
        }
        throw new SparkException(
          s"""
             |Could not execute broadcast in $timeout secs.
             |You can increase the timeout for broadcasts via
             |${SQLConf.BROADCAST_TIMEOUT.key} or disable broadcast join
             |by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1
            """.stripMargin,
          ex
        )
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarBroadcastExchangeExec =
    copy(child = newChild)

  // Ported from BroadcastExchangeExec
  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }
}
