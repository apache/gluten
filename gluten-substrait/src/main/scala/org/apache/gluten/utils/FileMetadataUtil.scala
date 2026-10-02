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
   *
   * @param injectedFileColAliases
   *   Map from schema column name to canonical input-file function prettyName for attributes
   *   injected by PushDownInputFileExpression.PreOffload. When a caseSensitive=false name collision
   *   forced the injected attribute to use a mangled schema name (e.g.
   *   "__gluten_input_file_col__input_file_name__"), this map tells generateMetadataColumns which
   *   file-path/start/length value to populate for that schema name. When schema name equals the
   *   canonical prettyName (no collision), the entry is redundant but harmless.
   */
  def generateMetadataColumns(
      file: PartitionedFile,
      metadataColumnNames: Seq[String] = Seq.empty,
      injectedFileColAliases: Map[String, String] = Map.empty): Map[String, String] = {
    // Build the canonical-prettyName → value table once.
    val canonicalValues = Map(
      InputFileName().prettyName -> file.filePath.toString,
      InputFileBlockStart().prettyName -> file.start.toString,
      InputFileBlockLength().prettyName -> file.length.toString
    )

    val requested = metadataColumnNames.toSet
    // Standard path: schema name IS the canonical prettyName (no mangling).
    val originMetadataColumn = canonicalValues.collect {
      case (name, value) if requested.contains(name) => name -> value
    }
    // Alias path: schema name differs from canonical prettyName (mangled under caseSensitive=false
    // collision). Look up the canonical value and emit it under the schema name so that Velox can
    // find it by the column name it has in its NamedStruct schema.
    val aliasedMetadataColumn = injectedFileColAliases.collect {
      case (schemaName, canonName) if requested.contains(schemaName) =>
        canonicalValues.get(canonName).map(schemaName -> _)
    }.flatten
    val metadataColumn: mutable.Map[String, String] =
      mutable.Map((originMetadataColumn ++ aliasedMetadataColumn).toSeq: _*)
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
