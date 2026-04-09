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
package org.apache.spark.sql.perf

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.ShuffleWriterType
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.sort.{ColumnarShuffleHandle, ColumnarShuffleManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.perf.{DeltaOptimizedWriterExec, OptimizedWriterBlocks}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.BinPackingUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.metric.SQLColumnarShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockFetcherIterator, ShuffleBlockId}
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
 * An execution node which shuffles data to a target output of `DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS`
 * blocks, hash partitioned on the table partition columns. We group all blocks by their
 * reducer_id's and bin-pack into `DELTA_OPTIMIZE_WRITE_BIN_SIZE` bins. Then we launch a Spark task
 * per bin to write out a single file for each bin.
 *
 * @param child
 *   The execution plan
 * @param partitionColumns
 *   The partition columns of the table. Used for hash partitioning the write
 * @param deltaLog
 *   The DeltaLog for the table. Used for logging only
 */

case class DeltaOptimizedWriterTransformer(
    child: SparkPlan,
    partitionColumns: Seq[String],
    @transient deltaLog: DeltaLog
) extends UnaryExecNode
  with GlutenPlan
  with DeltaLogging {

  lazy val shuffleWriterType: ShuffleWriterType =
    BackendsApiManager.getSparkPlanExecApiInstance.getShuffleWriterType(outputPartitioning, output)

  override def output: Seq[Attribute] = child.output

  private lazy val readMetrics =
    SQLColumnarShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  private[sql] lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance
      .genColumnarShuffleExchangeMetrics(
        sparkContext,
        shuffleWriterType) ++ readMetrics ++ writeMetrics

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  private lazy val childNumPartitions = inputColumnarRDD.getNumPartitions

  private lazy val numPartitions: Int = {
    if (childNumPartitions > 0) {
      val targetShuffleBlocks = getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS)
      math.min(
        math.max(targetShuffleBlocks / childNumPartitions, 1),
        getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS))
    } else {
      childNumPartitions
    }
  }

  @transient private var cachedShuffleRDD: ShuffledColumnarBatchRDD = _

  @transient override def outputPartitioning: Partitioning = {
    val resolver = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    val saltedPartitioning = HashPartitioning(
      partitionColumns.map(
        p =>
          output
            .find(o => resolver(p, o.name))
            .getOrElse(throw DeltaErrors.failedFindPartitionColumnInOutputPlan(p))),
      numPartitions)
    saltedPartitioning
  }

  @transient private lazy val mapTracker = SparkEnv.get.mapOutputTracker

  private def getShuffleRDD: ShuffledColumnarBatchRDD = {
    if (cachedShuffleRDD == null) {
      val resolver = org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      val saltedPartitioning = HashPartitioning(
        partitionColumns.map(
          p =>
            output
              .find(o => resolver(p, o.name))
              .getOrElse(throw DeltaErrors.failedFindPartitionColumnInOutputPlan(p))),
        numPartitions)
      val shuffledRDD =
        ColumnarShuffleExchangeExec(saltedPartitioning, child, ENSURE_REQUIREMENTS, null)
          .executeColumnar()
          .asInstanceOf[ShuffledColumnarBatchRDD]
      cachedShuffleRDD = shuffledRDD
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

  private def getShuffleStats(): Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    val dep = getShuffleRDD.dependency

    // Gets the shuffle output stats
    def getStats =
      mapTracker.getMapSizesByExecutorId(dep.shuffleId, 0, Int.MaxValue, 0, numPartitions).toArray

    // Executes the shuffle map stage in case we are missing output stats
    def awaitShuffleMapStage(): Unit = {
      assert(dep != null, "Shuffle dependency should not be null")
      // hack to materialize the shuffle files in a fault tolerant way
      ThreadUtils.awaitResult(sparkContext.submitMapStage(dep), Duration.Inf)
    }

    try {
      val res = getStats
      if (res.isEmpty) awaitShuffleMapStage()
      getStats
    } catch {
      case e: FetchFailedException =>
        logWarning("Failed to fetch shuffle blocks for the optimized writer. Retrying", e)
        awaitShuffleMapStage()
        getStats
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (childNumPartitions <= 1) return inputColumnarRDD
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
    new ColumnarDeltaOptimizedWriterRDD(
      sparkContext,
      shuffledRDD.dependency,
      readMetrics,
      new OptimizedWriterBlocks(partitions))
  }

  private def getConf[T](entry: ConfigEntry[T]): T = {
    conf.getConf(entry)
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): DeltaOptimizedWriterTransformer =
    copy(child = newChild)

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }
}

private class ColumnarDeltaOptimizedWriterRDD(
    @transient sparkContext: SparkContext,
    var dep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
    metrics: Map[String, SQLMetric],
    @transient blocks: OptimizedWriterBlocks
) extends RDD[ColumnarBatch](sparkContext, Seq(dep))
  with DeltaLogging {

  override protected def getPartitions: Array[Partition] = Array.tabulate(blocks.bins.length) {
    i => ColumnarShuffleBlockRDDPartition(i, blocks.bins(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val sqlMetricsReporter = new SQLColumnarShuffleReadMetricsReporter(tempMetrics, metrics)

    val blocks = if (context.stageAttemptNumber() > 0) {
      // We lost shuffle blocks, so we need to now get new manager addresses
      val executorTracker = SparkEnv.get.mapOutputTracker
      val oldBlockLocations = split.asInstanceOf[ColumnarShuffleBlockRDDPartition].blocks

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
      split.asInstanceOf[ColumnarShuffleBlockRDDPartition].blocks.iterator
    }

    val serializerManager =
      if (dep.shuffleHandle.isInstanceOf[ColumnarShuffleHandle[_, _]]) {
        ColumnarShuffleManager.bypassDecompressionSerializerManger
      } else {
        SparkEnv.get.serializerManager
      }

    val reader =
      new OptimizedWriterShuffleReader(dep, context, blocks, sqlMetricsReporter, serializerManager)
    reader.read().map(_._2)
  }
}

private case class ColumnarShuffleBlockRDDPartition(
    index: Int,
    blocks: List[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])])
  extends Partition

/** A simplified implementation of the `BlockStoreShuffleReader` for reading shuffle blocks. */
private class OptimizedWriterShuffleReader(
    dep: ShuffleDependency[Int, _, ColumnarBatch],
    context: TaskContext,
    blocks: Iterator[(BlockManagerId, ArrayBuffer[(BlockId, Long, Int)])],
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager)
  extends ShuffleReader[Int, ColumnarBatch] {

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[Int, ColumnarBatch]] = {

    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.blockStoreClient,
      SparkEnv.get.blockManager,
      SparkEnv.get.mapOutputTracker,
      blocks,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt.useExtraMemory", false),
      SparkEnv.get.conf.getBoolean("spark.shuffle.checksum.enabled", true),
      SparkEnv.get.conf.get("spark.shuffle.checksum.algorithm", "ADLER32"),
      readMetrics,
      false
    )

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams
      .flatMap {
        case (_, wrappedStream) =>
          // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
          // NextIterator. The NextIterator makes sure that close() is called on the
          // underlying InputStream when all records have been read.
          serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
      }
      .asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]]

    new InterruptibleIterator[Product2[Int, ColumnarBatch]](context, recordIter)
  }
}

object DeltaOptimizedWriterTransformer {
  def support(plan: SparkPlan): Boolean = plan.isInstanceOf[DeltaOptimizedWriterExec]

  def from(plan: SparkPlan): DeltaOptimizedWriterTransformer = {
    plan match {
      case p: DeltaOptimizedWriterExec =>
        DeltaOptimizedWriterTransformer(p.child, p.partitionColumns, p.deltaLog)
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform ColumnarDeltaOptimizedWriterExec from ${plan.getClass.getSimpleName}")
    }
  }
}
