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

import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.c.{ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import java.util

import scala.collection.JavaConverters._

object ArrowUtil {

  private val defaultTimeZoneId = SparkSchemaUtil.getLocalTimezoneID

  private def getResultType(dataType: DataType): ArrowType = {
    getResultType(dataType, defaultTimeZoneId)
  }

  private def getResultType(dataType: DataType, timeZoneId: String): ArrowType = {
    dataType match {
      case other =>
        SparkArrowUtil.toArrowType(dataType, timeZoneId)
    }
  }

  def toArrowSchema(attributes: Seq[Attribute]): Schema = {
    val fields = attributes.map(
      attr => {
        Field
          .nullable(s"${attr.name}#${attr.exprId.id}", getResultType(attr.dataType))
      })
    new Schema(fields.toList.asJava)
  }

  def toArrowSchema(
      cSchema: ArrowSchema,
      allocator: BufferAllocator,
      provider: CDataDictionaryProvider): Schema = {
    val schema = Data.importSchema(allocator, cSchema, provider)
    val originFields = schema.getFields
    val fields = new util.ArrayList[Field](originFields.size)
    originFields.forEach {
      field =>
        val dt = SparkArrowUtil.fromArrowField(field)
        fields.add(
          SparkArrowUtil.toArrowField(field.getName, dt, true, SparkSchemaUtil.getLocalTimezoneID))
    }
    new Schema(fields)
  }

  def toSchema(batch: ColumnarBatch): Schema = {
    val fields = new java.util.ArrayList[Field](batch.numCols)
    for (i <- 0 until batch.numCols) {
      val col: ColumnVector = batch.column(i)
      fields.add(col match {
        case vector: ArrowWritableColumnVector =>
          vector.getValueVector.getField
        case _ =>
          throw new UnsupportedOperationException(
            s"Unexpected vector type: ${col.getClass.toString}")
      })
    }
    new Schema(fields)
  }

}
