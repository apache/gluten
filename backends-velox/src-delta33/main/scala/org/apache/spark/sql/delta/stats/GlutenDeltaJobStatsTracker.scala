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
package org.apache.spark.sql.delta.stats

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.expression.{ConverterUtils, TransformerState}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadOthers
import org.apache.gluten.extension.columnar.rewrite.PullOutPreProject
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.vectorized.{ColumnarBatchInIterator, ColumnarBatchOutIterator, NativePlanEvaluator}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow, Expression, Projection, SortOrder, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, SparkDirectoryUtil}

import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util.UUID
import java.util.concurrent.{Callable, Executors, Future, SynchronousQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** Gluten's stats tracker with vectorized aggregation inside to produce statistics efficiently. */
private[stats] class GlutenDeltaJobStatsTracker(val delegate: DeltaJobStatisticsTracker)
  extends WriteJobStatsTracker {
  import GlutenDeltaJobStatsTracker._

  @transient private val hadoopConf: Configuration = {
    val clazz = classOf[DeltaJobStatisticsTracker]
    val method = clazz.getDeclaredField("hadoopConf")
    method.setAccessible(true)
    method.get(delegate).asInstanceOf[Configuration]
  }
  @transient private val path = delegate.path
  private val dataCols = delegate.dataCols
  private val statsColExpr = delegate.statsColExpr

  private val srlHadoopConf = new SerializableConfiguration(hadoopConf)
  private val rootUri = path.getFileSystem(hadoopConf).makeQualified(path).toUri

  override def newTaskInstance(): WriteTaskStatsTracker = {
    val rootPath = new Path(rootUri)
    val hadoopConf = srlHadoopConf.value
    // Build and offload the local statistics aggregation plan here on the executor, where the
    // native plan is actually run. Only when the WHOLE plan is offloaded to Velox do we use the
    // native, vectorized stats tracker; otherwise (for example a TIMESTAMP_NTZ column whose
    // aggregate Velox cannot offload) we fall back to row-based statistics collection through the
    // original Delta tracker. Deciding here -- instead of assuming the offloaded plan is always a
    // WholeStageTransformer -- avoids the ClassCastException reported in apache/gluten#12538 and
    // allocates no native resources on the fallback path.
    tryBuildOffloadedStatsPlan(dataCols, statsColExpr) match {
      case Some(offloadedPlan) =>
        new GlutenDeltaTaskStatsTracker(statsColExpr, offloadedPlan, rootPath, hadoopConf)
      case None =>
        new GlutenDeltaJobStatsFallbackTracker(delegate).newTaskInstance()
    }
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    delegate.processStats(stats, jobCommitTime)
  }
}

object GlutenDeltaJobStatsTracker extends Logging {
  def apply(tracker: WriteJobStatsTracker): WriteJobStatsTracker = tracker match {
    case tracker: BasicWriteJobStatsTracker =>
      new GlutenDeltaJobStatsRowCountingTracker(tracker)
    case tracker: DeltaJobStatisticsTracker =>
      new GlutenDeltaJobStatsTracker(tracker)
    case tracker =>
      logWarning(
        "Gluten Delta: Creating fallback job stats tracker," +
          " this involves frequent columnar-to-row conversions which may cause performance" +
          " issues.")
      new GlutenDeltaJobStatsFallbackTracker(tracker)
  }

  /** The complete-mode declarative aggregates embedded in a Delta statistics expression. */
  private def statsAggregates(statsColExpr: Expression): Seq[AggregateExpression] =
    statsColExpr.collect {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
        assert(ae.mode == Complete)
        ae
    }

  /**
   * Build the local Velox aggregation plan used to compute Delta write statistics and try to
   * offload it to Velox. Returns the offloaded [[TransformSupport]] tree only when the WHOLE plan
   * was offloaded; otherwise returns None so the caller collects statistics the row-based way.
   *
   * A root [[WholeStageTransformer]] is not sufficient: [[ColumnarCollapseTransformStages]] can
   * wrap an offloaded parent above a vanilla child, so an unsupported type/expression (for example
   * a TIMESTAMP_NTZ aggregate) could still leave a non-offloaded node in the tree. We therefore
   * require every node except the [[StatisticsInputNode]] leaf to be a [[TransformSupport]].
   */
  private def tryBuildOffloadedStatsPlan(
      dataCols: Seq[Attribute],
      statsColExpr: Expression): Option[TransformSupport] = {
    try {
      val aggregates = statsAggregates(statsColExpr)
      val statsAttrs = aggregates.flatMap(_.aggregateFunction.aggBufferAttributes)
      val statsResultAttrs = aggregates.flatMap(_.aggregateFunction.inputAggBufferAttributes)
      val aggOp = SortAggregateExec(
        None,
        isStreaming = false,
        None,
        Seq.empty,
        aggregates,
        statsAttrs,
        0,
        statsResultAttrs,
        StatisticsInputNode(dataCols)
      )
      val projOp = ProjectExec(statsResultAttrs, aggOp)
      // Invoke the legacy transform rule to get a local Velox aggregation query plan.
      val offloads = Seq(OffloadOthers()).map(_.toStrcitRule())
      val validatorBuilder: GlutenConfig => Validator = conf =>
        Validators.newValidator(conf, offloads)
      val rewrites = Seq(PullOutPreProject)
      val config = GlutenConfig.get
      val transformRule =
        HeuristicTransform.WithRewrites(validatorBuilder(config), rewrites, offloads)
      val transformed = transformRule(projOp)
      if (!isFullyOffloaded(transformed)) {
        None
      } else {
        ColumnarCollapseTransformStages(config)(transformed) match {
          case wst: WholeStageTransformer => Some(wst.child.asInstanceOf[TransformSupport])
          case _ => None
        }
      }
    } catch {
      case NonFatal(t) =>
        logWarning(
          "Gluten Delta: could not offload the statistics collection plan to Velox; falling back" +
            " to row-based statistics collection.",
          t)
        None
    }
  }

  /**
   * Whether every node in a transformed statistics plan was offloaded to Velox. The only allowed
   * non-[[TransformSupport]] node is the [[StatisticsInputNode]] leaf that feeds the incoming
   * columnar batches into the native aggregation.
   */
  private def isFullyOffloaded(plan: SparkPlan): Boolean =
    !plan.exists(p => !p.isInstanceOf[TransformSupport] && !p.isInstanceOf[StatisticsInputNode])

  /** A columnar-based statistics collection for Gluten + Delta Lake. */
  private class GlutenDeltaTaskStatsTracker(
      statsColExpr: Expression,
      offloadedPlan: TransformSupport,
      rootPath: Path,
      hadoopConf: Configuration)
    extends WriteTaskStatsTracker {
    // We use one single thread to ensure the statistic files are written in serial.
    // Do not increase the thread number, otherwise sanity will not be guaranteed.
    private val resultThreadRunner = Executors.newSingleThreadExecutor()
    private val evaluator = NativePlanEvaluator.create(
      BackendsApiManager.getBackendName,
      Map.empty[String, String].asJava)
    private val c2r = new VeloxColumnarToRowExec.Converter(new SQLMetric("convertTime"))
    private val inputBatchQueue = new SynchronousQueue[Option[ColumnarBatch]]()
    private val aggregates: Seq[AggregateExpression] = statsAggregates(statsColExpr)
    private val declarativeAggregates: Seq[DeclarativeAggregate] = aggregates.map {
      ae => ae.aggregateFunction.asInstanceOf[DeclarativeAggregate]
    }
    private val resultExpr: Expression = statsColExpr.transform {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
        ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression
    }
    private val aggBufferAttrs: Seq[Attribute] =
      declarativeAggregates.flatMap(_.aggBufferAttributes)
    private val emptyRow: InternalRow = {
      val initializeStats = GenerateMutableProjection.generate(
        expressions = declarativeAggregates.flatMap(_.initialValues),
        inputSchema = Seq.empty,
        useSubexprElimination = false
      )
      val buffer = new SpecificInternalRow(aggBufferAttrs.map(_.dataType))
      initializeStats.target(buffer).apply(EmptyRow)
    }
    private val getStats: Projection = UnsafeProjection.create(
      exprs = Seq(resultExpr),
      inputSchema = aggBufferAttrs
    )
    private val taskContext = TaskContext.get()
    private val veloxAggTask: ColumnarBatchOutIterator = {
      val substraitContext = new SubstraitContext
      TransformerState.enterValidation
      val transformedNode =
        try {
          offloadedPlan.transform(substraitContext)
        } finally {
          TransformerState.finishValidation
        }
      val outNames = offloadedPlan.output.map(ConverterUtils.genColumnNameWithExprId).asJava
      val planNode =
        PlanBuilder.makePlan(substraitContext, Lists.newArrayList(transformedNode.root), outNames)

      val spillDirPath = SparkDirectoryUtil
        .get()
        .namespace("gluten-spill")
        .mkChildDirRoundRobin(UUID.randomUUID.toString)
        .getAbsolutePath
      val nativeOutItr = evaluator
        .createKernelWithBatchIterator(
          planNode.toProtobuf.toByteArray,
          null,
          null,
          0,
          BackendsApiManager.getSparkPlanExecApiInstance.rewriteSpillPath(spillDirPath)
        )
      nativeOutItr
    }

    private val resultJsonMap: mutable.Map[String, String] = mutable.Map()

    private var currentPath: String = _
    private var currentJsonFuture: Future[String] = _

    override def newPartition(partitionValues: InternalRow): Unit = {}

    override def newFile(filePath: String): Unit = {
      assert(currentPath == null)
      veloxAggTask.addIteratorSplits(Array(newIteratorFromInputQueue()))
      veloxAggTask.requestBarrier()
      currentJsonFuture = resultThreadRunner.submit(new Callable[String] {
        private val resultThreadName =
          s"Gluten Delta Statistics Writer - ${System.identityHashCode(this)}"
        override def call(): String = {
          Thread.currentThread().setName(resultThreadName)
          TaskContext.setTaskContext(taskContext)
          val outBatches = veloxAggTask.asScala.toSeq
          val row: InternalRow = if (outBatches.isEmpty) {
            // No input was received. Returns the default aggregation values.
            emptyRow
          } else {
            assert(outBatches.size == 1)
            val batch = outBatches.head
            val rows = c2r.toRowIterator(batch).toSeq
            assert(
              rows.size == 1,
              "Only one single output row is expected from the global aggregation.")
            batch.close()
            rows.head
          }
          val jsonStats = getStats(row).getString(0)
          jsonStats
        }
        currentPath = filePath
      })
    }

    override def closeFile(filePath: String): Unit = {
      def signalEoS(): Unit = {
        while (true) {
          if (currentJsonFuture.isDone) {
            currentJsonFuture.get()
            // The future should be an error since we haven't signaled EoS yet.
            throw new IllegalStateException("Unreachable code.")
          }
          val queued = inputBatchQueue.offer(None, 500, java.util.concurrent.TimeUnit.MILLISECONDS)
          if (queued) {
            // The future should be done after we signal EoS.
            return
          }
        }
      }
      assert(filePath == currentPath)
      signalEoS()
      val fileName = new Path(filePath).getName
      val json = currentJsonFuture.get()
      resultJsonMap(fileName) = json
      currentPath = null
    }

    override def newRow(filePath: String, row: InternalRow): Unit = {
      assert(filePath == currentPath)
      row match {
        case _: PlaceholderRow =>
        case t: TerminalRow =>
          val batch = t.batch()
          // Counts up the reference count for the batch since it
          // will be consumed by Velox aggregation task asynchronously.
          ColumnarBatches.retain(batch)
          inputBatchQueue.put(Some(batch))
      }
    }

    override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
      veloxAggTask.noMoreSplits()
      veloxAggTask.close()
      DeltaFileStatistics(resultJsonMap.toMap)
    }

    private def newIteratorFromInputQueue(): ColumnarBatchInIterator = {
      val itr = new ColumnarBatchInIterator(
        BackendsApiManager.getBackendName,
        Iterators
          .wrap(new Iterator[ColumnarBatch] {
            private var batch: ColumnarBatch = _

            override def hasNext: Boolean = {
              assert(batch == null)
              while (!Thread.currentThread().isInterrupted) {
                val tmp =
                  try {
                    inputBatchQueue.take()
                  } catch {
                    case _: InterruptedException =>
                      Thread.currentThread().interrupt()
                      return false
                  }
                if (tmp.isDefined) {
                  batch = tmp.get
                  return true
                }
                return false
              }
              throw new IllegalStateException()
            }

            override def next(): ColumnarBatch = {
              assert(batch != null)
              try {
                batch
              } finally {
                batch = null
              }
            }
          })
          .recyclePayload(b => b.close())
          .create()
          .asJava
      )
      itr
    }
  }

  private case class StatisticsInputNode(dataSchema: Seq[Attribute])
    extends GlutenPlan
    with LeafExecNode {
    override def output: Seq[Attribute] = dataSchema
    override def batchType(): Convention.BatchType = VeloxBatchType
    override def rowType(): Convention.RowType = Convention.RowType.None
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def outputOrdering: Seq[SortOrder] = Seq.empty
  }
}
