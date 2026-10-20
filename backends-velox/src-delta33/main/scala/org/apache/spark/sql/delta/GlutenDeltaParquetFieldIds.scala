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
import org.apache.gluten.proto.ParquetFieldIdNode
import org.apache.gluten.substrait.rel.ParquetFieldId

import org.apache.spark.sql.delta.DeltaColumnMapping.{PARQUET_FIELD_ID_METADATA_KEY, PARQUET_FIELD_NESTED_IDS_METADATA_KEY, PARQUET_LIST_ELEMENT_FIELD_NAME, PARQUET_MAP_KEY_FIELD_NAME, PARQUET_MAP_VALUE_FIELD_NAME}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Base64

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * The Parquet field IDs of the files the Velox writer writes for a Delta table with Iceberg
 * compatibility V1 or V2: the IDs Delta's writer writes.
 *
 * Delta's writer writes the `parquet.field.id` of each struct field as its field ID, as Spark does
 * when spark.sql.parquet.fieldId.write.enabled is on. From IcebergCompatV2 on, it also writes the
 * IDs of array elements and map keys and values (DeltaParquetWriteSupport). Delta keeps those in
 * the `parquet.field.nested.ids` of the nearest enclosing struct field, keyed by their path from
 * that field, such as `items.element`. The Velox writer drops StructField metadata, so the IDs are
 * passed to it in a write option.
 *
 * Delta's writer writes the files of other column-mapped tables, and of other IcebergCompat
 * versions.
 */
object GlutenDeltaParquetFieldIds {

  /** The IcebergCompat versions whose files the Velox writer writes. */
  private val nativeIcebergCompatVersions = Set(1, 2)

  /**
   * Why the Velox writer cannot write the field IDs of the file columns `schema` of a table with
   * `metadata`, or None if it can, or if they have none to write.
   */
  def unsupported(metadata: Metadata, schema: StructType, sqlConf: SQLConf): Option[String] =
    IcebergCompat.getEnabledVersion(metadata) match {
      case Some(version) if nativeIcebergCompatVersions.contains(version) =>
        try {
          of(schema, metadata, sqlConf)
          None
        } catch {
          // Delta's writer fails the write.
          case NonFatal(e) => Some(s"Parquet field IDs cannot be written: ${e.getMessage}")
        }
      case Some(version) =>
        Some(s"IcebergCompatV$version is not supported by the Velox writer")
      case None if metadata.columnMappingMode != NoMapping =>
        // Delta writes Parquet field IDs for both name and id column mapping.
        Some("Parquet field IDs for column mapping are not written by the Velox writer")
      case None => None
    }

  /** `options` with the field IDs of `dataSchema`, when the Velox writer writes them. */
  def withFieldIds(
      options: Map[String, String],
      dataSchema: StructType,
      metadata: Metadata,
      sqlConf: SQLConf): Map[String, String] =
    if (IcebergCompat.getEnabledVersion(metadata).exists(nativeIcebergCompatVersions.contains)) {
      options + (GlutenConfig.PARQUET_FIELD_IDS -> encode(of(dataSchema, metadata, sqlConf)))
    } else {
      options
    }

  /** The field IDs of the columns of `schema`, as Delta's writer writes them. */
  def of(schema: StructType, metadata: Metadata, sqlConf: SQLConf): Seq[ParquetFieldId] = {
    val builder = new Builder(
      writeFieldIds = sqlConf.parquetFieldIdWriteEnabled,
      writeNestedIds = IcebergCompat.getEnabledVersion(metadata).exists(_ >= 2))
    schema.fields.toSeq.map(builder.fieldId)
  }

  /** The write option: a Base64-encoded ParquetFieldIdNode without an ID around the columns. */
  def encode(fieldIds: Seq[ParquetFieldId]): String = {
    val root = ParquetFieldIdNode.newBuilder().setFieldId(-1)
    fieldIds.foreach(fieldId => root.addChildren(toProto(fieldId)))
    Base64.getEncoder.encodeToString(root.build().toByteArray)
  }

  private def toProto(fieldId: ParquetFieldId): ParquetFieldIdNode = {
    val node = ParquetFieldIdNode.newBuilder().setFieldId(fieldId.fieldId())
    fieldId.children().asScala.foreach(child => node.addChildren(toProto(child)))
    node.build()
  }

  private class Builder(writeFieldIds: Boolean, writeNestedIds: Boolean) {

    def fieldId(field: StructField): ParquetFieldId = {
      val id = if (writeFieldIds && field.metadata.contains(PARQUET_FIELD_ID_METADATA_KEY)) {
        field.metadata.getLong(PARQUET_FIELD_ID_METADATA_KEY).toInt
      } else {
        -1
      }
      new ParquetFieldId(id, children(field.dataType, field, Seq(field.name)).asJava)
    }

    /** The nodes below a node of type `dataType`, at `path` from the struct field `owner`. */
    private def children(
        dataType: DataType,
        owner: StructField,
        path: Seq[String]): Seq[ParquetFieldId] = dataType match {
      case struct: StructType => struct.fields.toSeq.map(fieldId)
      case array: ArrayType =>
        Seq(nested(array.elementType, owner, path :+ PARQUET_LIST_ELEMENT_FIELD_NAME))
      case map: MapType =>
        Seq(
          nested(map.keyType, owner, path :+ PARQUET_MAP_KEY_FIELD_NAME),
          nested(map.valueType, owner, path :+ PARQUET_MAP_VALUE_FIELD_NAME))
      case udt: UserDefinedType[_] => children(udt.sqlType, owner, path)
      case _ => Seq.empty
    }

    private def nested(dataType: DataType, owner: StructField, path: Seq[String]) =
      new ParquetFieldId(nestedId(owner, path), children(dataType, owner, path).asJava)

    private def nestedId(owner: StructField, path: Seq[String]): Int = {
      val key = path.mkString(".")
      val metadata = owner.metadata
      if (!writeNestedIds) {
        -1
      } else if (
        metadata.contains(PARQUET_FIELD_NESTED_IDS_METADATA_KEY) &&
        metadata.getMetadata(PARQUET_FIELD_NESTED_IDS_METADATA_KEY).contains(key)
      ) {
        metadata.getMetadata(PARQUET_FIELD_NESTED_IDS_METADATA_KEY).getLong(key).toInt
      } else {
        throw new IllegalStateException(s"Column ${owner.name} has no Parquet field ID for $key")
      }
    }
  }
}
