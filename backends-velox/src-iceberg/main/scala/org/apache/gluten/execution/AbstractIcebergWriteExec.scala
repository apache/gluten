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
package org.apache.gluten.execution

import org.apache.gluten.IcebergNestedFieldVisitor
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, IcebergDataWriteFactory}

import org.apache.spark.sql.types.StructType

import org.apache.iceberg.spark.source.IcebergWriteUtil
import org.apache.iceberg.types.TypeUtil

import scala.collection.JavaConverters._

abstract class AbstractIcebergWriteExec extends IcebergWriteExec {

  // the writer factory works for both batch and streaming
  private def createIcebergDataWriteFactory(schema: StructType): IcebergDataWriteFactory = {
    val writeSchema = IcebergWriteUtil.getWriteSchema(write)
    val nestedField = TypeUtil.visit(writeSchema, new IcebergNestedFieldVisitor)
    // Filter out metadata columns from the Spark output schema and reorder to match Iceberg schema
    // Spark 4.0 may include metadata columns in the output schema during UPDATE operations,
    // but these should not be written to the Iceberg table
    val writeFieldNames = writeSchema.columns().asScala.map(_.name()).toSet
    val filteredSchema = StructType(
      schema.fields.filter(field => writeFieldNames.contains(field.name))
    )
    IcebergDataWriteFactory(
      filteredSchema,
      getFileFormat(IcebergWriteUtil.getFileFormat(write)),
      IcebergWriteUtil.getDirectory(write),
      getCodec,
      getPartitionSpec,
      IcebergWriteUtil.getSortOrder(write),
      nestedField,
      IcebergWriteUtil.getQueryId(write)
    )
  }

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createIcebergDataWriteFactory(schema)
  }

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createIcebergDataWriteFactory(schema)
  }
}
