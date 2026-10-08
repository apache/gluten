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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import org.apache.iceberg.{BaseTable, FileFormat, HasTableOperations, Schema, StaticTableOperations, StructLike, Table, TableProperties}
import org.apache.iceberg.encryption.EncryptionManager
import org.apache.iceberg.io.FileWriterFactory
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Types

object GlutenEqualityDeleteWriterUtil {
  def scanTable(table: Table): SparkTable = {
    val metadata = table.asInstanceOf[HasTableOperations].operations().current()
    val properties = new java.util.HashMap[String, String](metadata.properties())
    properties.put(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "false")
    val readProperties = java.util.Collections.unmodifiableMap[String, String](properties)
    val encryptionManager = table.encryption()
    // Prevent spark.wap.branch from conflicting with the scan's explicit snapshot ID.
    val readTable = new BaseTable(
      new StaticTableOperations(metadata, table.io(), table.locationProvider()),
      table.name()) {
      override def properties(): java.util.Map[String, String] = readProperties
      override def encryption(): EncryptionManager = encryptionManager
    }
    new SparkTable(readTable, false)
  }

  def writerFactory(
      table: Table,
      dataSchema: Schema,
      deleteSchema: Schema,
      equalityIds: Array[Int],
      dataFormat: FileFormat,
      deleteFormat: FileFormat,
      properties: java.util.Map[String, String]): FileWriterFactory[InternalRow] =
    SparkFileWriterFactory.builderFor(table)
      .dataSchema(dataSchema)
      .dataSparkType(SparkSchemaUtil.convert(dataSchema))
      .dataFileFormat(dataFormat)
      .deleteFileFormat(deleteFormat)
      .equalityFieldIds(equalityIds)
      .equalityDeleteRowSchema(deleteSchema)
      .equalityDeleteSparkType(SparkSchemaUtil.convert(deleteSchema))
      .writeProperties(properties)
      .build()

  def wrap(row: InternalRow, sparkType: StructType, icebergType: Types.StructType): StructLike =
    new InternalRowWrapper(sparkType, icebergType).wrap(row)
}
