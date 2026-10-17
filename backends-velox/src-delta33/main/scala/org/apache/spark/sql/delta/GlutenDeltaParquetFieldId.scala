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
package org.apache.spark.sql.delta

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

private[delta] object GlutenDeltaParquetFieldId {
  private case class ParquetFieldId(fieldId: Int, children: Seq[ParquetFieldId])

  def withParquetFieldIds(
      options: Map[String, String],
      dataSchema: StructType,
      deltaMetadata: Metadata): Map[String, String] = {
    if (!IcebergCompatV2.isEnabled(deltaMetadata)) {
      options
    } else {
      val fieldIds = serialize(dataSchema)
      if (fieldIds.isEmpty) {
        options
      } else {
        options + (GlutenConfig.PARQUET_FIELD_IDS -> fieldIds)
      }
    }
  }

  private def serialize(schema: StructType): String =
    schema.fields.map(field => encode(buildField(field))).mkString(",")

  private def buildField(field: StructField): ParquetFieldId = {
    ParquetFieldId(columnFieldId(field), childrenFor(field.dataType, field, Seq(field.name)))
  }

  private def buildSyntheticField(
      ownerField: StructField,
      fieldIdPath: Seq[String],
      dataType: DataType): ParquetFieldId = {
    ParquetFieldId(
      nestedFieldId(ownerField, fieldIdPath),
      childrenFor(dataType, ownerField, fieldIdPath))
  }

  private def childrenFor(
      dataType: DataType,
      ownerField: StructField,
      fieldIdPath: Seq[String]): Seq[ParquetFieldId] = dataType match {
    case StructType(fields) =>
      fields.map(buildField)
    case ArrayType(elementType, _) =>
      Seq(
        buildSyntheticField(
          ownerField,
          fieldIdPath :+ DeltaColumnMapping.PARQUET_LIST_ELEMENT_FIELD_NAME,
          elementType))
    case MapType(keyType, valueType, _) =>
      Seq(
        buildSyntheticField(
          ownerField,
          fieldIdPath :+ DeltaColumnMapping.PARQUET_MAP_KEY_FIELD_NAME,
          keyType),
        buildSyntheticField(
          ownerField,
          fieldIdPath :+ DeltaColumnMapping.PARQUET_MAP_VALUE_FIELD_NAME,
          valueType)
      )
    case _ =>
      Seq.empty
  }

  private def columnFieldId(field: StructField): Int = {
    val key = DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY
    if (field.metadata.contains(key)) field.metadata.getLong(key).toInt else -1
  }

  private def nestedFieldId(field: StructField, fieldIdPath: Seq[String]): Int = {
    val key = DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY
    if (!field.metadata.contains(key)) {
      -1
    } else {
      val nestedIds = field.metadata.getMetadata(key)
      val nestedKey = fieldIdPath.mkString(".")
      if (nestedIds.contains(nestedKey)) nestedIds.getLong(nestedKey).toInt else -1
    }
  }

  private def encode(fieldId: ParquetFieldId): String = {
    if (fieldId.children.isEmpty) {
      fieldId.fieldId.toString
    } else {
      fieldId.children.map(encode).mkString(s"${fieldId.fieldId}(", ",", ")")
    }
  }
}
