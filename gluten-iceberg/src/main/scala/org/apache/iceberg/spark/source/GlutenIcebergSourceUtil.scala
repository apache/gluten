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

import org.apache.gluten.ContentFileUtil
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.SparkDataSourceRDDPartition
import org.apache.gluten.substrait.rel.{IcebergLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

import org.apache.iceberg._
import org.apache.iceberg.spark.SparkSchemaUtil

import java.lang.{Class, Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

object GlutenIcebergSourceUtil {

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
      readPartitionSchema: StructType): SplitInfo = {
    val paths = new JArrayList[String]()
    val starts = new JArrayList[JLong]()
    val lengths = new JArrayList[JLong]()
    val partitionColumns = new JArrayList[JMap[String, String]]()
    val deleteFilesList = new JArrayList[JList[DeleteFile]]()
    var fileFormat = ReadFileFormat.UnknownFormat

    partition.inputPartitions.foreach {
      case partition: SparkInputPartition =>
        val tasks = partition.taskGroup[ScanTask]().tasks().asScala
        asFileScanTask(tasks.toList).foreach {
          task =>
            paths.add(
              BackendsApiManager.getTransformerApiInstance
                .encodeFilePathIfNeed(ContentFileUtil.getFilePath(task.file())))
            starts.add(task.start())
            lengths.add(task.length())
            partitionColumns.add(getPartitionColumns(task, readPartitionSchema))
            deleteFilesList.add(task.deletes())
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
      deleteFilesList
    )
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
}
