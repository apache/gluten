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

import org.apache.spark.sql.connector.write.{Write, WriterCommitMessage}

import org.apache.iceberg._
import org.apache.iceberg.spark.SparkWriteConf
import org.apache.iceberg.spark.source.SparkWrite.TaskCommit
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{ListType, MapType}

object IcebergWriteUtil {

  private lazy val writeSchemaField = {
    val field = classOf[SparkWrite].getDeclaredField("writeSchema")
    field.setAccessible(true)
    field
  }

  private lazy val writePropertiesField = {
    val field = classOf[SparkWrite].getDeclaredField("writeProperties")
    field.setAccessible(true)
    field
  }

  private lazy val writeConfField = {
    val field = classOf[SparkWrite].getDeclaredField("writeConf")
    field.setAccessible(true)
    field
  }

  private lazy val tableField = {
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    field
  }

  private lazy val fileFormatField = {
    val field = classOf[SparkWrite].getDeclaredField("format")
    field.setAccessible(true)
    field
  }

  private lazy val queryIdField = {
    val field = classOf[SparkWrite].getDeclaredField("queryId")
    field.setAccessible(true)
    field
  }

  def supportsWrite(write: Write): Boolean = {
    write.isInstanceOf[SparkWrite]
  }

  def hasUnsupportedDataType(write: Write): Boolean = {
    getWriteSchema(write).columns().stream().anyMatch(d => hasUnsupportedDataType(d.`type`()))
  }

  private def hasUnsupportedDataType(dataType: Type): Boolean = {
    dataType match {
      case l: ListType => hasUnsupportedDataType(l.elementType())
      case m: MapType =>
        hasUnsupportedDataType(m.keyType()) || hasUnsupportedDataType(m.valueType())
      case s: org.apache.iceberg.types.Types.StructType =>
        s.fields().stream().anyMatch(f => hasUnsupportedDataType(f.`type`()))
      case t if t.typeId() == TypeID.UUID || t.typeId() == TypeID.FIXED => true
      case _ => false
    }
  }

  def getWriteSchema(write: Write): Schema = {
    assert(write.isInstanceOf[SparkWrite])
    writeSchemaField.get(write).asInstanceOf[Schema]
  }

  def getWriteProperty(write: Write): java.util.Map[String, String] = {
    writePropertiesField.get(write).asInstanceOf[java.util.Map[String, String]]
  }

  def getWriteConf(write: Write): SparkWriteConf = {
    writeConfField.get(write).asInstanceOf[SparkWriteConf]
  }

  def getTable(write: Write): Table = {
    tableField.get(write).asInstanceOf[Table]
  }

  def getFileFormat(write: Write): FileFormat = {
    fileFormatField.get(write).asInstanceOf[FileFormat]
  }

  def getQueryId(write: Write): String = {
    queryIdField.get(write).asInstanceOf[String]
  }

  def getDirectory(write: Write): String = {
    val loc = getTable(write).locationProvider().newDataLocation("")
    loc.substring(0, loc.length - 1)
  }

  def getSortOrder(write: Write): SortOrder = {
    getTable(write).sortOrder()
  }

  def getPartitionSpec(write: Write): PartitionSpec = {
    getTable(write).specs().get(getWriteConf(write).outputSpecId())
  }

  // Similar to the UnpartitionedDataWriter#commit
  def commitDataFiles(dataFiles: Array[DataFile]): WriterCommitMessage = {
    val commit = new TaskCommit(dataFiles)
    commit.reportOutputMetrics()
    commit
  }

}
