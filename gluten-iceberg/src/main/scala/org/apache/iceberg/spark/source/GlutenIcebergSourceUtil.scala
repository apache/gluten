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
package org.apache.iceberg.spark.source

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.SparkDataSourceRDDPartition
import org.apache.gluten.substrait.rel.{IcebergLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

import org.apache.iceberg._
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.util.TableScanUtil

import java.lang.{Class, Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

object GlutenIcebergSourceUtil extends Logging {
  private val InputFileNameCol = "input_file_name"
  private val InputFileBlockStartCol = "input_file_block_start"
  private val InputFileBlockLengthCol = "input_file_block_length"

  def getClassOfSparkBatchQueryScan(): Class[SparkBatchQueryScan] = {
    classOf[SparkBatchQueryScan]
  }

  def deleteExists(p: SparkDataSourceRDDPartition): Boolean = {
    p.inputPartitions.exists {
      case ip: SparkInputPartition =>
        val tasks = ip.taskGroup[ScanTask]().tasks().asScala
        asFileScanTask(tasks.toList).exists(task => !task.deletes().isEmpty())
      case _ => throw new UnsupportedOperationException(s"Unsupported InputPartition type")
    }
  }

  def genSplitInfo(
      partition: SparkDataSourceRDDPartition,
      readPartitionSchema: StructType,
      metadataColumnNames: Seq[String]): SplitInfo = {
    val paths = new JArrayList[String]()
    val starts = new JArrayList[JLong]()
    val lengths = new JArrayList[JLong]()
    val partitionColumns = new JArrayList[JMap[String, String]]()
    val deleteFilesList = new JArrayList[JList[DeleteFile]]()
    val metadataColumns = new JArrayList[JMap[String, String]]()
    var fileFormat = ReadFileFormat.UnknownFormat

    partition.inputPartitions.foreach {
      case partition: SparkInputPartition =>
        val tasks = partition.taskGroup[ScanTask]().tasks().asScala
        asFileScanTask(tasks.toList).foreach {
          task =>
            val filePath = task.file().path().toString
            paths.add(BackendsApiManager.getTransformerApiInstance.encodeFilePathIfNeed(filePath))
            starts.add(task.start())
            lengths.add(task.length())
            partitionColumns.add(getPartitionColumns(task, readPartitionSchema))
            deleteFilesList.add(task.deletes())
            metadataColumns.add(
              genMetadataColumns(metadataColumnNames, filePath, task.start(), task.length()))
            val currentFileFormat = convertFileFormat(task.file().format())
            if (fileFormat == ReadFileFormat.UnknownFormat) {
              fileFormat = currentFileFormat
            } else if (fileFormat != currentFileFormat) {
              throw new UnsupportedOperationException(
                s"Only one file format is supported, " +
                  s"find different file format $fileFormat and $currentFileFormat")
            }
        }
      case o =>
        throw new GlutenNotSupportException(s"Unsupported input partition type: $o")
    }
    IcebergLocalFilesBuilder.makeIcebergLocalFiles(
      partition.index,
      paths,
      starts,
      lengths,
      partitionColumns,
      fileFormat,
      SoftAffinity
        .getFilePartitionLocations(paths.asScala.toArray, partition.preferredLocations())
        .toList
        .asJava,
      deleteFilesList,
      metadataColumns
    )
  }

  private def genMetadataColumns(
      metadataColumnNames: Seq[String],
      filePath: String,
      start: Long,
      length: Long): JHashMap[String, String] = {
    val metadataColumns = new JHashMap[String, String]()
    metadataColumnNames.foreach {
      name =>
        name.toLowerCase(Locale.ROOT) match {
          case InputFileNameCol => metadataColumns.put(name, filePath)
          case InputFileBlockStartCol => metadataColumns.put(name, start.toString)
          case InputFileBlockLengthCol => metadataColumns.put(name, length.toString)
          case _ =>
        }
    }
    metadataColumns
  }

  def getFileFormat(sparkScan: Scan): ReadFileFormat = sparkScan match {
    case scan: SparkBatchQueryScan =>
      val tasks = scan.tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          task.file().format() match {
            case FileFormat.PARQUET => return ReadFileFormat.ParquetReadFormat
            case FileFormat.ORC => return ReadFileFormat.OrcReadFormat
            case _ =>
          }
      }
      throw new GlutenNotSupportException("Iceberg Only support parquet and orc file format.")
    case _ =>
      throw new GlutenNotSupportException("Only support iceberg SparkBatchQueryScan.")
  }

  def getReadPartitionSchema(sparkScan: Scan): StructType = sparkScan match {
    case scan: SparkBatchQueryScan =>
      val tasks = scan.tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          val spec = task.spec()
          if (spec.isPartitioned) {
            val readFields = scan.readSchema().fields.map(_.name).toSet
            // Iceberg will generate some non-table fields as partition fields, such as x_bucket,
            // which will not appear in readFields, they also cannot be filtered.
            val tableFields = spec.schema().columns().asScala.map(_.name()).toSet
            val voidTransformFields = scan
              .table()
              .spec()
              .fields()
              .asScala
              .filter(
                f => {
                  f.transform().isVoid
                })
              .map(_.name())
              .toSet
            val partitionFields =
              spec
                .partitionType()
                .fields()
                .asScala
                .filter(f => !tableFields.contains(f.name) || readFields.contains(f.name()))
                .filter(f => !voidTransformFields.contains(f.name()))
            partitionFields.foreach {
              field => TypeUtil.validatePartitionColumnType(field.`type`().typeId())
            }

            val icebergSchema = new Schema(partitionFields.toList.asJava)
            return SparkSchemaUtil.convert(icebergSchema)
          } else {
            return new StructType()
          }
      }
      throw new UnsupportedOperationException(
        "Failed to get partition schema from iceberg SparkBatchQueryScan.")
    case _ =>
      throw new UnsupportedOperationException("Only support iceberg SparkBatchQueryScan.")
  }

  private def asFileScanTask(tasks: List[ScanTask]): List[FileScanTask] = {
    if (tasks.forall(_.isFileScanTask)) {
      tasks.map(_.asFileScanTask())
    } else if (tasks.forall(_.isInstanceOf[CombinedScanTask])) {
      tasks.flatMap(_.asCombinedScanTask().tasks().asScala)
    } else {
      throw new UnsupportedOperationException(
        "Only support iceberg CombinedScanTask and FileScanTask.")
    }
  }

  private def getPartitionColumns(
      task: FileScanTask,
      readPartitionSchema: StructType): JHashMap[String, String] = {
    val partitionColumns = new JHashMap[String, String]()
    val readPartitionFields = readPartitionSchema.fields.map(_.name).toSet
    val spec = task.spec()
    val partition = task.partition()
    if (spec.isPartitioned) {
      val partitionFields = spec
        .partitionType()
        .fields()
        .asScala
        .zipWithIndex
        .filter(f => readPartitionFields.contains(f._1.name()))
      partitionFields.foreach {
        case (field, index) =>
          val partitionValue = partition.get(index, field.`type`().typeId().javaClass())
          val partitionType = field.`type`()
          if (partitionValue != null) {
            partitionColumns.put(
              field.name(),
              TypeUtil.getPartitionValueString(partitionType, partitionValue))
          } else {
            partitionColumns.put(field.name(), ExternalCatalogUtils.DEFAULT_PARTITION_NAME)
          }
      }
    }
    partitionColumns
  }

  private def convertFileFormat(icebergFileFormat: FileFormat): ReadFileFormat =
    icebergFileFormat match {
      case FileFormat.PARQUET => ReadFileFormat.ParquetReadFormat
      case FileFormat.ORC => ReadFileFormat.OrcReadFormat
      case _ =>
        throw new GlutenNotSupportException("Iceberg Only support parquet and orc file format.")
    }

  def regeneratePartitions(
      inputPartitions: Seq[Partition],
      smallFileThreshold: Double): Seq[Partition] = {
    if (inputPartitions.isEmpty) {
      return Seq.empty
    }

    val icebergPartitions: Seq[SparkDataSourceRDDPartition] = inputPartitions.map {
      case partition: SparkDataSourceRDDPartition => partition
      case other =>
        throw new GlutenNotSupportException(
          s"Unsupported partition type: ${other.getClass.getSimpleName}")
    }

    val partitionedTasks = Array.fill(icebergPartitions.size)(mutable.ArrayBuffer.empty[ScanTask])

    def getSparkInputPartitionContext(
        inputPartition: SparkInputPartition): SparkPartitionContext = {
      val clazz = classOf[SparkInputPartition]
      def readField[T](fieldName: String): T = {
        val field = clazz.getDeclaredField(fieldName)
        field.setAccessible(true)
        field.get(inputPartition).asInstanceOf[T]
      }

      SparkPartitionContext(
        groupingKeyType = readField[org.apache.iceberg.types.Types.StructType]("groupingKeyType"),
        tableBroadcast = readField[Broadcast[Table]]("tableBroadcast"),
        branch = inputPartition.branch(),
        expectedSchemaString = readField[String]("expectedSchemaString"),
        caseSensitive = inputPartition.isCaseSensitive,
        preferredLocations = inputPartition.preferredLocations(),
        cacheDeleteFilesOnExecutors = inputPartition.cacheDeleteFilesOnExecutors()
      )
    }

    def getScanTasks(inputPartition: SparkInputPartition): Seq[ScanTask] = {
      inputPartition.taskGroup[ScanTask]().tasks().asScala.toSeq.map {
        case task if task.isFileScanTask => task
        case task: CombinedScanTask => task
        case other =>
          throw new GlutenNotSupportException(
            s"Unsupported scan task type: ${other.getClass.getSimpleName}")
      }
    }

    def getScanTaskSize(scanTask: ScanTask): Long = scanTask match {
      case task if task.isFileScanTask => task.asFileScanTask().length()
      case task: CombinedScanTask => task.tasks().asScala.map(_.length()).sum
      case other =>
        throw new GlutenNotSupportException(
          s"Unsupported scan task type: ${other.getClass.getSimpleName}")
    }

    def addToBucket(
        heap: mutable.PriorityQueue[(Long, Int, Int)],
        scanTask: ScanTask,
        taskSize: Long): Unit = {
      val (size, numFiles, idx) = heap.dequeue()
      partitionedTasks(idx) += scanTask
      heap.enqueue((size + taskSize, numFiles + 1, idx))
    }

    def initializeHeap(
        ordering: Ordering[(Long, Int, Int)]): mutable.PriorityQueue[(Long, Int, Int)] = {
      val heap = mutable.PriorityQueue.empty[(Long, Int, Int)](ordering)
      icebergPartitions.indices.foreach(i => heap.enqueue((0L, 0, i)))
      heap
    }

    def createSparkInputPartition(
        context: SparkPartitionContext,
        tasks: Seq[ScanTask]): SparkInputPartition = {
      val taskGroup = new BaseScanTaskGroup[ScanTask](TableScanUtil.mergeTasks(tasks.asJava))
      new SparkInputPartition(
        context.groupingKeyType,
        taskGroup,
        context.tableBroadcast,
        context.branch,
        context.expectedSchemaString,
        context.caseSensitive,
        context.preferredLocations,
        context.cacheDeleteFilesOnExecutors
      )
    }

    val sparkInputPartitions = icebergPartitions.flatMap(_.inputPartitions).map {
      case partition: SparkInputPartition => partition
      case other =>
        throw new GlutenNotSupportException(
          s"Unsupported input partition type: ${other.getClass.getSimpleName}")
    }

    val context = getSparkInputPartitionContext(sparkInputPartitions.head)
    val scanTasks = sparkInputPartitions.flatMap(getScanTasks)
    val sortedScanTasks = scanTasks
      .zip(scanTasks.map(getScanTaskSize))
      .sortBy(_._2)(Ordering.Long.reverse)

    val sizeFirstOrdering = Ordering
      .by[(Long, Int, Int), (Long, Int)] { case (size, numFiles, _) => (size, numFiles) }
      .reverse

    if (smallFileThreshold > 0) {
      val smallFileTotalSize = sortedScanTasks.map(_._2).sum * smallFileThreshold
      val numFirstOrdering = Ordering
        .by[(Long, Int, Int), (Int, Long)] { case (size, numFiles, _) => (numFiles, size) }
        .reverse
      val heapByFileNum = initializeHeap(numFirstOrdering)

      var numSmallFiles = 0
      var smallFileSize = 0L
      sortedScanTasks.reverseIterator
        .takeWhile(task => task._2 + smallFileSize <= smallFileTotalSize)
        .foreach {
          case (task, taskSize) =>
            addToBucket(heapByFileNum, task, taskSize)
            numSmallFiles += 1
            smallFileSize += taskSize
        }

      val heapByFileSize = mutable.PriorityQueue.empty[(Long, Int, Int)](sizeFirstOrdering)
      while (heapByFileNum.nonEmpty) {
        heapByFileSize.enqueue(heapByFileNum.dequeue())
      }

      sortedScanTasks.take(sortedScanTasks.size - numSmallFiles).foreach {
        case (task, taskSize) =>
          addToBucket(heapByFileSize, task, taskSize)
      }
    } else {
      val heapByFileSize = initializeHeap(sizeFirstOrdering)
      sortedScanTasks.foreach {
        case (task, taskSize) =>
          addToBucket(heapByFileSize, task, taskSize)
      }
    }

    partitionedTasks.zipWithIndex.map {
      case (tasks, idx) =>
        val newPartition = createSparkInputPartition(context, tasks.toSeq)
        new SparkDataSourceRDDPartition(idx, Seq(newPartition))
    }
  }
}

case class SparkPartitionContext(
    groupingKeyType: org.apache.iceberg.types.Types.StructType,
    tableBroadcast: Broadcast[Table],
    branch: String,
    expectedSchemaString: String,
    caseSensitive: Boolean,
    preferredLocations: Array[String],
    cacheDeleteFilesOnExecutors: Boolean)
