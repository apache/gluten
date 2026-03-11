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
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.expression.{ConverterUtils, TransformerState}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadOthers
import org.apache.gluten.extension.columnar.rewrite.PullOutPreProject
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.vectorized.{ArrowWritableColumnVector, ColumnarBatchInIterator, ColumnarBatchOutIterator, NativePlanEvaluator}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, EmptyRow, Expression, Projection, SortOrder, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, LeafExecNode, ProjectExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{SerializableConfiguration, SparkDirectoryUtil}

import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util.UUID
import java.util.concurrent.{Callable, Executors, Future, SynchronousQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable

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
    new GlutenDeltaTaskStatsTracker(dataCols, statsColExpr, rootPath, hadoopConf)
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

  /** A columnar-based statistics collection for Gluten + Delta Lake. */
  private class GlutenDeltaTaskStatsTracker(
      dataCols: Seq[Attribute],
      statsColExpr: Expression,
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
    private val aggregates: Seq[AggregateExpression] = statsColExpr.collect {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
        assert(ae.mode == Complete)
        ae
    }
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
    private val dummyKeyAttr = {
      // FIXME: We have to force the use of Velox's streaming aggregation since hash aggregation
      //  doesn't support task barriers. But as streaming aggregation should always be keyed, we
      //  have to do a small hack here by adding a dummy key for the global aggregation.
      AttributeReference("__GLUTEN_DELTA_DUMMY_KEY__", IntegerType)()
    }
    private val statsAttrs = aggregates.flatMap(_.aggregateFunction.aggBufferAttributes)
    private val statsResultAttrs = aggregates.flatMap(_.aggregateFunction.inputAggBufferAttributes)
    private val veloxAggTask: ColumnarBatchOutIterator = {
      val inputNode = StatisticsInputNode(Seq(dummyKeyAttr), dataCols)
      val aggOp = SortAggregateExec(
        None,
        isStreaming = false,
        None,
        Seq(dummyKeyAttr),
        aggregates,
        statsAttrs,
        0,
        dummyKeyAttr +: statsResultAttrs,
        inputNode
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
      val veloxTransformer = transformRule(projOp)
      val wholeStageTransformer = ColumnarCollapseTransformStages(config)(veloxTransformer)
        .asInstanceOf[WholeStageTransformer]
        .child
        .asInstanceOf[TransformSupport]
      val substraitContext = new SubstraitContext
      TransformerState.enterValidation
      val transformedNode =
        try {
          wholeStageTransformer.transform(substraitContext)
        } finally {
          TransformerState.finishValidation
        }
      val outNames = wholeStageTransformer.output.map(ConverterUtils.genColumnNameWithExprId).asJava
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
      assert(filePath == currentPath)
      val fileName = new Path(filePath).getName
      inputBatchQueue.put(None)
      val json = currentJsonFuture.get()
      resultJsonMap(fileName) = json
      currentPath = null
    }

    override def newRow(filePath: String, row: InternalRow): Unit = {
      assert(filePath == currentPath)
      row match {
        case _: PlaceholderRow =>
        case t: TerminalRow =>
          val valueBatch = t.batch()
          val numRows = valueBatch.numRows()
          val dummyKeyVec = ArrowWritableColumnVector
            .allocateColumns(numRows, new StructType().add(dummyKeyAttr.name, IntegerType))
            .head
          (0 until numRows).foreach(i => dummyKeyVec.putInt(i, 1))
          val dummyKeyBatch = VeloxColumnarBatches.toVeloxBatch(
            ColumnarBatches.offload(
              ArrowBufferAllocators.contextInstance(),
              new ColumnarBatch(Array[ColumnVector](dummyKeyVec), numRows)))
          val compositeBatch = VeloxColumnarBatches.compose(dummyKeyBatch, valueBatch)
          dummyKeyBatch.close()
          valueBatch.close()
          inputBatchQueue.put(Some(compositeBatch))
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
                      return false;
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

  private case class StatisticsInputNode(keySchema: Seq[Attribute], dataSchema: Seq[Attribute])
    extends GlutenPlan
    with LeafExecNode {
    override def output: Seq[Attribute] = keySchema ++ dataSchema
    override def batchType(): Convention.BatchType = VeloxBatchType
    override def rowType0(): Convention.RowType = Convention.RowType.None
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def outputOrdering: Seq[SortOrder] = {
      keySchema.map(key => SortOrder(key, Ascending))
    }
  }
}
