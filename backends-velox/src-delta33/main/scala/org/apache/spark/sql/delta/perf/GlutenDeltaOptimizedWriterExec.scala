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
package org.apache.spark.sql.delta.perf

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ValidatablePlan, ValidationResult}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.vectorized.ColumnarBatchSerializerInstance

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.ColumnarShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.BinPackingUtils
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarShuffleExchangeExec, GenerateTransformStageId}
import org.apache.spark.sql.execution.{ShuffledColumnarBatchRDD, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage._
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/** Gluten's vectorized version of [[DeltaOptimizedWriterExec]]. */
case class GlutenDeltaOptimizedWriterExec(
    child: SparkPlan,
    partitionColumns: Seq[String],
    @transient deltaLog: DeltaLog
) extends ValidatablePlan
  with UnaryExecNode
  with DeltaLogging {

  override def output: Seq[Attribute] = child.output

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")
  ) ++ readMetrics ++ writeMetrics

  private lazy val childNumPartitions = child.executeColumnar().getNumPartitions

  private lazy val numPartitions: Int = {
    val targetShuffleBlocks = getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS)
    math.min(
      math.max(targetShuffleBlocks / childNumPartitions, 1),
      getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS))
  }

  @transient private var cachedShuffleRDD: ShuffledColumnarBatchRDD = _

  @transient private lazy val mapTracker = SparkEnv.get.mapOutputTracker

  private lazy val columnarShufflePlan = {
    val resolver = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    val saltedPartitioning = HashPartitioning(
      partitionColumns.map(
        p =>
          output
            .find(o => resolver(p, o.name))
            .getOrElse(throw DeltaErrors.failedFindPartitionColumnInOutputPlan(p))),
      numPartitions)
    val shuffle =
      ShuffleExchangeExec(saltedPartitioning, child)
    val columnarShuffle =
      BackendsApiManager.getSparkPlanExecApiInstance.genColumnarShuffleExchange(shuffle)
    val columnarShuffleWithWst =
      GenerateTransformStageId()(
        ColumnarCollapseTransformStages(new GlutenConfig(conf))(columnarShuffle))
    columnarShuffleWithWst.asInstanceOf[ColumnarShuffleExchangeExec]
  }

  /** Creates a ShuffledRowRDD for facilitating the shuffle in the map side. */
  private def getShuffleRDD: ShuffledColumnarBatchRDD = {
    if (cachedShuffleRDD == null) {
      val columnarShuffleRdd =
        columnarShufflePlan.executeColumnar().asInstanceOf[ShuffledColumnarBatchRDD]
      cachedShuffleRDD = columnarShuffleRdd
    }
    cachedShuffleRDD
  }

  private def computeBins(): Array[List[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])]] = {
    // Get all shuffle information
    val shuffleStats = getShuffleStats()

    // Group by blockId instead of block manager
    val blockInfo = shuffleStats.flatMap {
      case (bmId, blocks) =>
        blocks.map {
          case (blockId, size, index) =>
            (blockId, (bmId, size, index))
        }
    }.toMap

    val maxBinSize =
      ByteUnit.BYTE.convertFrom(getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE), ByteUnit.MiB)

    val bins = shuffleStats.toSeq
      .flatMap(_._2)
      .groupBy(_._1.asInstanceOf[ShuffleBlockId].reduceId)
      .flatMap {
        case (_, blocks) =>
          BinPackingUtils.binPackBySize[(BlockId, Long, Int), BlockId](
            blocks,
            _._2, // size
            _._1, // blockId
            maxBinSize)
      }

    bins
      .map {
        bin =>
          var binSize = 0L
          val blockLocations =
            new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long, Int)]]()
          for (blockId <- bin) {
            val (bmId, size, index) = blockInfo(blockId)
            binSize += size
            val blocksAtBM =
              blockLocations.getOrElseUpdate(bmId, new ArrayBuffer[(BlockId, Long, Int)]())
            blocksAtBM.append((blockId, size, index))
          }
          (binSize, blockLocations.toList)
      }
      .toArray
      .sortBy(_._1)(Ordering[Long].reverse) // submit largest blocks first
      .map(_._2)
  }

  /** Performs the shuffle before the write, so that we can bin-pack output data. */
  private def getShuffleStats(): Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    val dep = getShuffleRDD.dependency
    // Gets the shuffle output stats
    def getStats() =
      mapTracker.getMapSizesByExecutorId(dep.shuffleId, 0, Int.MaxValue, 0, numPartitions).toArray

    // Executes the shuffle map stage in case we are missing output stats
    def awaitShuffleMapStage(): Unit = {
      assert(dep != null, "Shuffle dependency should not be null")
      // hack to materialize the shuffle files in a fault tolerant way
      ThreadUtils.awaitResult(sparkContext.submitMapStage(dep), Duration.Inf)
    }

    try {
      val res = getStats()
      if (res.isEmpty) awaitShuffleMapStage()
      getStats()
    } catch {
      case e: FetchFailedException =>
        logWarning(log"Failed to fetch shuffle blocks for the optimized writer. Retrying", e)
        awaitShuffleMapStage()
        getStats()
        throw e
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    // Single partitioned tasks can simply be written.
    if (childNumPartitions <= 1) {
      return ValidationResult.succeeded
    }
    columnarShufflePlan.doValidate()
  }

  override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Single partitioned tasks can simply be written.
    if (childNumPartitions <= 1) {
      return child.executeColumnar()
    }
    val shuffledRDD = getShuffleRDD

    val partitions = computeBins()

    recordDeltaEvent(
      deltaLog,
      "delta.optimizeWrite.planned",
      data = Map(
        "originalPartitions" -> childNumPartitions,
        "outputPartitions" -> partitions.length,
        "shufflePartitions" -> numPartitions,
        "numShuffleBlocks" -> getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS),
        "binSize" -> getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE),
        "maxShufflePartitions" ->
          getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS)
      )
    )

    new GlutenDeltaOptimizedWriterRDD(
      sparkContext,
      shuffledRDD.dependency,
      readMetrics,
      new OptimizedWriterBlocks(partitions))
  }

  private def getConf[T](entry: ConfigEntry[T]): T = {
    conf.getConf(entry)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenDeltaOptimizedWriterExec =
    copy(child = newChild)

  override def batchType(): Convention.BatchType = VeloxBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None
}

/**
 * A specialized implementation similar to `ShuffledRowRDD`, where a partition reads a prepared set
 * of shuffle blocks.
 */
private class GlutenDeltaOptimizedWriterRDD(
    @transient sparkContext: SparkContext,
    var dep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
    metrics: Map[String, SQLMetric],
    @transient blocks: OptimizedWriterBlocks)
  extends RDD[ColumnarBatch](sparkContext, Seq(dep))
  with DeltaLogging {

  override def getPartitions: Array[Partition] = Array.tabulate(blocks.bins.length) {
    i => ShuffleBlockRDDPartition(i, blocks.bins(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)

    val blocks = if (context.stageAttemptNumber() > 0) {
      // We lost shuffle blocks, so we need to now get new manager addresses
      val executorTracker = SparkEnv.get.mapOutputTracker
      val oldBlockLocations = split.asInstanceOf[ShuffleBlockRDDPartition].blocks

      // assumes we bin-pack by reducerId
      val reducerId = oldBlockLocations.head._2.head._1.asInstanceOf[ShuffleBlockId].reduceId
      // Get block addresses
      val newLocations = executorTracker
        .getMapSizesByExecutorId(dep.shuffleId, reducerId)
        .flatMap {
          case (bmId, newBlocks) =>
            newBlocks.map(blockInfo => (blockInfo._3, (bmId, blockInfo)))
        }
        .toMap

      val blockLocations = new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long, Int)]]()
      oldBlockLocations.foreach {
        case (_, oldBlocks) =>
          oldBlocks.foreach {
            oldBlock =>
              val (bmId, blockInfo) = newLocations(oldBlock._3)
              val blocksAtBM =
                blockLocations.getOrElseUpdate(bmId, new ArrayBuffer[(BlockId, Long, Int)]())
              blocksAtBM.append(blockInfo)
          }
      }

      blockLocations.iterator
    } else {
      split.asInstanceOf[ShuffleBlockRDDPartition].blocks.iterator
    }

    val reader = new GlutenOptimizedWriterShuffleReader(dep, context, blocks, sqlMetricsReporter)
    reader.read().map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dep = null
  }
}

/** A simplified implementation of the `BlockStoreShuffleReader` for reading shuffle blocks. */
private class GlutenOptimizedWriterShuffleReader(
    dep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
    context: TaskContext,
    blocks: Iterator[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])],
    readMetrics: ShuffleReadMetricsReporter)
  extends ShuffleReader[Int, ColumnarBatch] {

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[Int, ColumnarBatch]] = {
    val serializerManager = dep match {
      case _: ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch] =>
        ColumnarShuffleManager.bypassDecompressionSerializerManger
      case _ =>
        SparkEnv.get.serializerManager
    }
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.blockStoreClient,
      SparkEnv.get.blockManager,
      SparkEnv.get.mapOutputTracker,
      blocks,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      false
    ).toCompletionIterator

    // Create a key/value iterator for each stream
    val recordIter = dep match {
      case columnarDep: ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch] =>
        // If the dependency is a ColumnarShuffleDependency, we use the columnar serializer.
        columnarDep.serializer
          .newInstance()
          .asInstanceOf[ColumnarBatchSerializerInstance]
          .deserializeStreams(wrappedStreams)
          .asKeyValueIterator
      case _ =>
        val serializerInstance = dep.serializer.newInstance()
        // Create a key/value iterator for each stream
        wrappedStreams.flatMap {
          case (blockId, wrappedStream) =>
            // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
            // NextIterator. The NextIterator makes sure that close() is called on the
            // underlying InputStream when all records have been read.
            serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
        }
    }
    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[(Any, Any)](context, recordIter)
      .asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]]
  }
}
