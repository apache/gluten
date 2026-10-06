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
package org.apache.gluten.utils

import org.apache.spark.sql.catalyst.expressions.{InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}

import org.apache.hadoop.fs.Path

import java.time.ZoneOffset

import scala.collection.mutable

object FileMetadataUtil {

  /**
   * Collects the values of the requested metadata columns for one file. `metadataColumnNames`
   * carries both the `input_file_*` function names and the `_metadata` field names, so the two
   * groups are resolved separately.
   */
  def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String] = Seq.empty): Map[String, String] = {
    val requested = metadataColumnNames.toSet
    val originMetadataColumn = Seq(
      InputFileName().prettyName -> file.filePath.toString,
      InputFileBlockStart().prettyName -> file.start.toString,
      InputFileBlockLength().prettyName -> file.length.toString
    ).collect { case (name, value) if requested.contains(name) => name -> value }.toMap
    val metadataColumn: mutable.Map[String, String] = mutable.Map(originMetadataColumn.toSeq: _*)
    val path = new Path(file.filePath.toString)
    for (columnName <- metadataColumnNames) {
      columnName match {
        case FileFormat.FILE_PATH => metadataColumn += (FileFormat.FILE_PATH -> path.toString)
        case FileFormat.FILE_NAME => metadataColumn += (FileFormat.FILE_NAME -> path.getName)
        case FileFormat.FILE_SIZE =>
          metadataColumn += (FileFormat.FILE_SIZE -> file.fileSize.toString)
        case FileFormat.FILE_MODIFICATION_TIME =>
          val fileModifyTime = TimestampFormatter
            .getFractionFormatter(ZoneOffset.UTC)
            .format(file.modificationTime * 1000L)
          metadataColumn += (FileFormat.FILE_MODIFICATION_TIME -> fileModifyTime)
        case FileFormat.FILE_BLOCK_START =>
          metadataColumn += (FileFormat.FILE_BLOCK_START -> file.start.toString)
        case FileFormat.FILE_BLOCK_LENGTH =>
          metadataColumn += (FileFormat.FILE_BLOCK_LENGTH -> file.length.toString)
        case _ =>
      }
    }
    metadataColumn.toMap
  }
}
