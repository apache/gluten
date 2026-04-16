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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.IteratorApi
import org.apache.gluten.execution._
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.metrics.IMetrics
import org.apache.gluten.sql.shims.{DeltaShimLoader, SparkShimLoader}
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.substrait.rel._
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.vectorized.{BatchIterator, CHNativeExpressionEvaluator, CloseableCHColumnBatchIterator}

import org.apache.spark.{InterruptibleIterator, Partition, SparkConf, TaskContext}
import org.apache.spark.affinity.CHAffinity
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.CHColumnarShuffleWriter
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.clickhouse.ExtensionTableBuilder
import org.apache.spark.sql.execution.datasources.mergetree.PartSerializer
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.lang.{Long => JLong}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.ZoneOffset
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class CHIteratorApi extends IteratorApi with Logging with LogLevelUtil {

  private def getFileSchema(schema: StructType, names: Seq[String]): StructType = {
    val dataSchema = ArrayBuffer[StructField]()
    schema.foreach {
      field =>
        // case-insensitive schema matching
        val newField = names.find(_.equalsIgnoreCase(field.name)) match {
          case Some(name) => StructField(name, field.dataType, field.nullable, field.metadata)
          case _ => field
        }
        dataSchema += newField
    }
    StructType(dataSchema.toSeq)
  }

  private def createNativeIterator(
      splitInfoByteArray: Array[Array[Byte]],
      wsPlan: Array[Byte],
      materializeInput: Boolean,
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]]): BatchIterator = {

    /** Generate closeable ColumnBatch iterator. */
    val listIterator =
      inputIterators
        .map {
          case i: CloseableCHColumnBatchIterator => i
          case it => new CloseableCHColumnBatchIterator(it)
        }
        .map(it => new ColumnarNativeIterator(it.asJava))
        .asJava
    CHNativeExpressionEvaluator.createKernelWithBatchIterator(
      wsPlan,
      splitInfoByteArray,
      listIterator,
      materializeInput,
      partitionIndex
    )

  }

  private def createCloseIterator(
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      updateInputMetrics: Option[InputMetricsWrapper => Unit] = None,
      nativeIter: BatchIterator): CloseableCHColumnBatchIterator = {

    val iter = new CollectMetricIterator(
      nativeIter,
      updateNativeMetrics,
      updateInputMetrics,
      updateInputMetrics.map(_ => context.taskMetrics().inputMetrics).orNull)

    context.addTaskFailureListener((ctx, _) => { iter.cancel() })

    context.addTaskCompletionListener[Unit](_ => iter.close())
    new CloseableCHColumnBatchIterator(iter, Some(pipelineTime))
  }

  // only set file schema for text format table
  private def setFileSchemaForLocalFiles(
      localFilesNode: LocalFilesNode,
      scan: BasicScanExecTransformer): Unit = {
    if (scan.fileFormat == ReadFileFormat.TextReadFormat) {
      val names =
        ConverterUtils.collectAttributeNamesWithoutExprId(scan.output)
      localFilesNode.setFileSchema(getFileSchema(scan.getDataSchema, names.asScala.toSeq))
    }
  }

  override def genSplitInfo(
      partitionIndex: Int,
      partitions: Seq[Partition],
      partitionSchema: StructType,
      dataSchema: StructType,
      fileFormat: ReadFileFormat,
      metadataColumnNames: Seq[String],
      properties: Map[String, String]): SplitInfo = {
    // todo: support multi partitions
    assert(partitions.size == 1)
    val partition = partitions.head
    partition match {
      case p: GlutenMergeTreePartition =>
        ExtensionTableBuilder
          .makeExtensionTable(
            p.database,
            p.table,
            p.snapshotId,
            p.relativeTablePath,
            p.absoluteTablePath,
            p.orderByKey,
            p.lowCardKey,
            p.minmaxIndexKey,
            p.bfIndexKey,
            p.setIndexKey,
            p.primaryKey,
            PartSerializer.fromMergeTreePartSplits(p.partList.toSeq),
            p.tableSchema,
            p.clickhouseTableConfigs.asJava,
            CHAffinity.getNativeMergeTreePartitionLocations(p).toList.asJava
          )
      case f: FilePartition =>
        val paths = new JArrayList[String]()
        val starts = new JArrayList[JLong]()
        val lengths = new JArrayList[JLong]()
        val fileSizes = new JArrayList[JLong]()
        val modificationTimes = new JArrayList[JLong]()
        val partitionColumns = new JArrayList[JMap[String, String]]
        val metadataColumns = new JArrayList[JMap[String, String]]
        val otherMetadataColumns = new JArrayList[JMap[String, Object]]
        val dateFormatter = DateFormatter()
        val timestampFormatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)
        f.files.foreach {
          file =>
            paths.add(new URI(file.filePath.toString()).toASCIIString)
            starts.add(JLong.valueOf(file.start))
            lengths.add(JLong.valueOf(file.length))
            val metadataColumn =
              SparkShimLoader.getSparkShims
                .generateMetadataColumns(file, metadataColumnNames)
                .asJava
            metadataColumns.add(metadataColumn)
            val partitionColumn = new JHashMap[String, String]()
            for (i <- 0 until file.partitionValues.numFields) {
              val partitionColumnValue = if (file.partitionValues.isNullAt(i)) {
                ExternalCatalogUtils.DEFAULT_PARTITION_NAME
              } else {
                val pn = file.partitionValues.get(i, partitionSchema.fields(i).dataType)
                partitionSchema.fields(i).dataType match {
                  case _: BinaryType =>
                    new String(pn.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8)
                  case _: DateType =>
                    dateFormatter.format(pn.asInstanceOf[Integer])
                  case _: DecimalType =>
                    pn.asInstanceOf[Decimal].toJavaBigInteger.toString
                  case _: TimestampType =>
                    timestampFormatter.format(pn.asInstanceOf[java.lang.Long])
                  case _ => pn.toString
                }
              }
              partitionColumn.put(
                ConverterUtils.normalizeColName(partitionSchema.names(i)),
                partitionColumnValue)
            }
            partitionColumns.add(partitionColumn)

            fileSizes.add(file.fileSize)
            modificationTimes.add(file.modificationTime)

            val otherConstantMetadataColumnValues =
              DeltaShimLoader.getDeltaShims.convertRowIndexFilterIdEncoded(
                partitionColumn.size(),
                file,
                SparkShimLoader.getSparkShims.getOtherConstantMetadataColumnValues(file)
              )
            otherMetadataColumns.add(otherConstantMetadataColumnValues)
        }
        val preferredLocations =
          CHAffinity.getFilePartitionLocations(paths.asScala.toArray, f.preferredLocations())
        LocalFilesBuilder.makeLocalFiles(
          f.index,
          paths,
          starts,
          lengths,
          fileSizes,
          modificationTimes,
          partitionColumns,
          metadataColumns,
          fileFormat,
          preferredLocations.toList.asJava,
          mapAsJavaMap(properties),
          otherMetadataColumns
        )
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported input partition: $partition.")
    }
  }

  /**
   * Generate native row partition.
   *
   * @return
   */
  override def genPartitions(
      wsCtx: WholeStageTransformContext,
      splitInfos: Seq[Seq[SplitInfo]],
      leaves: Seq[LeafTransformSupport]): Seq[BaseGlutenPartition] = {
    // Only serialize plan once, save lots time when plan is complex.
    val planByteArray = wsCtx.root.toProtobuf.toByteArray
    splitInfos.zipWithIndex.map {
      case (splits, index) =>
        val splitInfos = splits.zipWithIndex.map {
          case (split, i) =>
            split match {
              case filesNode: LocalFilesNode if leaves(i).isInstanceOf[BasicScanExecTransformer] =>
                setFileSchemaForLocalFiles(
                  filesNode,
                  leaves(i).asInstanceOf[BasicScanExecTransformer])
                filesNode
              case splitInfo => splitInfo
            }
        }

        GlutenPartition(
          index,
          planByteArray,
          splitInfos.toArray
        )
    }
  }

  /**
   * Generate Iterator[ColumnarBatch] for first stage.
   *
   * @return
   */
  override def genFirstStageIterator(
      inputPartition: BaseGlutenPartition,
      context: TaskContext,
      pipelineTime: SQLMetric,
      updateInputMetrics: InputMetricsWrapper => Unit,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      inputIterators: Seq[Iterator[ColumnarBatch]] = Seq(),
      enableCudf: Boolean = false
  ): Iterator[ColumnarBatch] = {

    require(
      inputPartition.isInstanceOf[GlutenPartition],
      "CH backend only accepts GlutenPartition in GlutenWholeStageColumnarRDD.")
    val splitInfoByteArray = inputPartition
      .asInstanceOf[GlutenPartition]
      .splitInfos
      .map(splitInfo => splitInfo.toProtobuf.toByteArray)
      .toArray
    val wsPlan = inputPartition.plan
    val materializeInput = false

    new InterruptibleIterator(
      context,
      createCloseIterator(
        context,
        pipelineTime,
        updateNativeMetrics,
        Some(updateInputMetrics),
        createNativeIterator(
          splitInfoByteArray,
          wsPlan,
          materializeInput,
          partitionIndex,
          inputIterators)
      )
    )
  }

  // Generate Iterator[ColumnarBatch] for final stage.
  // scalastyle:off argcount
  override def genFinalStageIterator(
      context: TaskContext,
      inputIterators: Seq[Iterator[ColumnarBatch]],
      sparkConf: SparkConf,
      rootNode: PlanNode,
      pipelineTime: SQLMetric,
      updateNativeMetrics: IMetrics => Unit,
      partitionIndex: Int,
      materializeInput: Boolean,
      enableCudf: Boolean,
      supportsValueStreamDynamicFilter: Boolean): Iterator[ColumnarBatch] = {
    // scalastyle:on argcount

    // Final iterator does not contain scan split, so pass empty split info to native here.
    val splitInfoByteArray = new Array[Array[Byte]](0)
    val wsPlan = rootNode.toProtobuf.toByteArray

    // we need to complete dependency RDD's firstly
    createCloseIterator(
      context,
      pipelineTime,
      updateNativeMetrics,
      None,
      createNativeIterator(
        splitInfoByteArray,
        wsPlan,
        materializeInput,
        partitionIndex,
        inputIterators)
    )
  }

}

class CollectMetricIterator(
    val nativeIterator: BatchIterator,
    val updateNativeMetrics: IMetrics => Unit,
    val updateInputMetrics: Option[InputMetricsWrapper => Unit] = None,
    val inputMetrics: InputMetrics = null
) extends Iterator[ColumnarBatch] {
  private var outputRowCount = 0L
  private var outputVectorCount = 0L
  private var metricsUpdated = false
  // Whether the stage is executed completely using ClickHouse pipeline.
  private var wholeStagePipeline = true

  override def hasNext: Boolean = {
    // The hasNext call is triggered only when there is a fallback.
    wholeStagePipeline = false
    nativeIterator.hasNext
  }

  override def next(): ColumnarBatch = {
    val cb = nativeIterator.next()
    outputVectorCount += 1
    outputRowCount += cb.numRows()
    cb
  }

  def close(): Unit = {
    collectStageMetrics()
    nativeIterator.close()
  }

  def cancel(): Unit = {
    collectStageMetrics()
    nativeIterator.cancel()
  }

  private def collectStageMetrics(): Unit = {
    if (!metricsUpdated) {
      val nativeMetrics = nativeIterator.getMetrics
      if (wholeStagePipeline) {
        outputRowCount = Math.max(outputRowCount, CHColumnarShuffleWriter.getTotalOutputRows)
        outputVectorCount =
          Math.max(outputVectorCount, CHColumnarShuffleWriter.getTotalOutputBatches)
      }
      nativeMetrics.setFinalOutputMetrics(outputRowCount, outputVectorCount)
      updateNativeMetrics(nativeMetrics)
      updateInputMetrics.foreach(_(inputMetrics))
      metricsUpdated = true
    }
  }
}
