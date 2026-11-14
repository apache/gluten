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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, LanceColumnarBatchDataWriterFactory}

import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, Write}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType, TimestampType}

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.lance.spark.utils.BlobUtils
import org.lance.spark.write.{LanceBatchWrite, SparkWrite}

import java.util.{List => JList}

import scala.collection.JavaConverters._

/**
 * A Lance batch write offloaded to Velox. Executors export their Velox batches as an Arrow C stream
 * that lance-spark writes into uncommitted fragments; lance-spark's [[LanceBatchWrite]] then
 * commits them on the driver, exactly as for its own row writer.
 *
 * Implementations take the [[LanceBatchWrite]] as a constructor argument, built once when the node
 * is created. `SparkWrite.toBatch` builds a new one on every call, and each opens the table to pin
 * its version, while the planner copies the node after validating it. A field keeps one batch write
 * through those copies, so the validation, the writer factory and the commit all use it.
 */
trait LanceWriteExec extends ColumnarV2TableWriteExec {

  private def lanceBatchWrite: LanceBatchWrite = batchWrite.asInstanceOf[LanceBatchWrite]

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory =
    LanceColumnarBatchDataWriterFactory(
      lanceBatchWrite.getFragmentWriteSpec,
      lanceBatchWrite.getCommitSchema.toJson)

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory =
    throw new UnsupportedOperationException("Streaming writes to Lance are not offloaded")

  override def doValidateInternal(): ValidationResult = {
    if (!LanceWriteExec.supportsWrite(write)) {
      return ValidationResult.failed(s"Not a Lance write: ${write.getClass.getName}")
    }
    write match {
      case r: RequiresDistributionAndOrdering
          if !r.requiredDistribution().isInstanceOf[UnspecifiedDistribution] =>
        return ValidationResult.failed("Lance table requires a write distribution (sharded)")
      case _ =>
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    LanceWriteExec.timestampInNullableStruct(query.schema) match {
      case Some(name) =>
        return ValidationResult.failed(
          s"Timestamp column $name is inside a struct that may be null")
      case None =>
    }
    LanceWriteExec.unsupportedColumn(lanceBatchWrite.getCommitSchema) match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }
}

object LanceWriteExec {
  def supportsWrite(write: Write): Boolean = write.isInstanceOf[SparkWrite]

  /**
   * The first column whose committed Arrow type Velox cannot export with the same layout, if any.
   * Velox exports plain Utf8, Binary and List, microsecond timestamps and 128-bit decimals. The
   * writer converts the plain lists of top-level fixed-size list columns, such as vectors.
   */
  def unsupportedColumn(schema: Schema): Option[String] =
    firstUnsupported(schema.getFields, "", topLevel = true)

  /**
   * The first timestamp inside a struct that Spark says may be null, if any. Velox converts the
   * timestamps of a struct's null rows too, where an expression or a join may leave any value, and
   * a value out of range fails the export, as it does for Velox's own Parquet writer. Spark's
   * nullability is used rather than the commit schema's, which marks every Lance column nullable.
   */
  def timestampInNullableStruct(schema: StructType): Option[String] =
    timestampInNullableStruct(schema, "", mayBeNull = false, inNullableStruct = false)

  private def timestampInNullableStruct(
      dataType: DataType,
      name: String,
      mayBeNull: Boolean,
      inNullableStruct: Boolean): Option[String] = dataType match {
    case TimestampType => if (inNullableStruct) Some(name) else None
    case s: StructType =>
      val inNullable = inNullableStruct || mayBeNull
      s.fields.iterator
        .map {
          f =>
            val child = if (name.isEmpty) f.name else s"$name.${f.name}"
            timestampInNullableStruct(f.dataType, child, f.nullable, inNullable)
        }
        .collectFirst { case Some(found) => found }
    case a: ArrayType =>
      timestampInNullableStruct(a.elementType, s"$name.element", a.containsNull, inNullableStruct)
    case m: MapType =>
      // Keys are never null, but a struct inside a key may be.
      timestampInNullableStruct(m.keyType, s"$name.key", mayBeNull = false, inNullableStruct)
        .orElse(
          timestampInNullableStruct(
            m.valueType,
            s"$name.value",
            m.valueContainsNull,
            inNullableStruct))
    case _ => None
  }

  // Lazy, so the check stops at the first unsupported field.
  private def firstUnsupported(
      fields: JList[Field],
      prefix: String,
      topLevel: Boolean): Option[String] =
    fields.asScala.iterator
      .map(unsupported(_, prefix, topLevel))
      .collectFirst { case Some(reason) => reason }

  private def unsupported(field: Field, prefix: String, topLevel: Boolean): Option[String] = {
    val name = prefix + field.getName
    if (isBlob(field)) {
      return Some(s"Lance blob column $name is not offloaded")
    }
    field.getType match {
      case t if isSignedIntOrFloat(t) => None
      case _: ArrowType.Bool | _: ArrowType.Utf8 | _: ArrowType.Binary => None
      case d: ArrowType.Decimal if d.getBitWidth == 128 => None
      case d: ArrowType.Date if d.getUnit == DateUnit.DAY => None
      case t: ArrowType.Timestamp if t.getUnit == TimeUnit.MICROSECOND => None
      case _: ArrowType.List | _: ArrowType.Struct | _: ArrowType.Map =>
        firstUnsupported(field.getChildren, s"$name.", topLevel = false)
      case _: ArrowType.FixedSizeList if topLevel =>
        val elementType = field.getChildren.get(0).getType
        if (isSignedIntOrFloat(elementType)) None
        else Some(s"Fixed-size list column $name has elements of Arrow type $elementType")
      case other =>
        Some(s"Column $name has Arrow type $other, which Velox cannot export for Lance")
    }
  }

  /** Blob v1 columns carry lance-spark's encoding marker, blob v2 columns an Arrow extension. */
  private def isBlob(field: Field): Boolean =
    BlobUtils.isBlobArrowField(field) || Option(field.getMetadata).exists(
      metadata =>
        BlobUtils.ARROW_EXTENSION_BLOB_V2 == metadata.get(BlobUtils.ARROW_EXTENSION_NAME_KEY))

  private def isSignedIntOrFloat(arrowType: ArrowType): Boolean = arrowType match {
    case i: ArrowType.Int => i.getIsSigned
    case f: ArrowType.FloatingPoint => f.getPrecision != FloatingPointPrecision.HALF
    case _ => false
  }
}
