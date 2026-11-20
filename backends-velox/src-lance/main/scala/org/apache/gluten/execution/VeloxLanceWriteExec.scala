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

import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, LanceColumnarBatchDataWriterFactory}

import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType, StringType, StructType, TimestampNTZType, TimestampType}

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import java.util.{List => JList}

import scala.collection.JavaConverters._

/** A Lance write offloaded to Velox, whose tasks export their Velox batches to Lance. */
trait VeloxLanceWriteExec extends LanceWriteExec {
  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory =
    LanceColumnarBatchDataWriterFactory(
      lanceBatchWrite.getFragmentWriteSpec,
      lanceBatchWrite.getCommitSchema.toJson)

  override protected def unsupportedColumn: Option[String] =
    VeloxLanceWriteExec
      .undefinedUnderNullStruct(query.schema)
      .map(name => s"Column $name is inside a struct that may be null")
      .orElse(VeloxLanceWriteExec.unexportableColumn(lanceBatchWrite.getCommitSchema))
}

object VeloxLanceWriteExec {

  /**
   * The first column whose committed Arrow type Velox cannot export with the same layout, if any.
   * Velox exports plain Utf8, Binary and List, microsecond timestamps and 128-bit decimals. The
   * writer converts the plain lists of top-level fixed-size list columns, which lance-spark writes
   * for float and double vectors.
   */
  def unexportableColumn(schema: Schema): Option[String] =
    firstUnexportable(schema.getFields, "", topLevel = true)

  /**
   * The first column inside a struct that Spark says may be null whose export reads the values
   * under the struct's null rows, if any. Those values are undefined: a CASE WHEN, a join, or
   * flattening the batch before the export may leave anything there. Velox exports them anyway: an
   * out-of-range timestamp fails the export, and strings, binaries and the elements of arrays and
   * maps are copied through views and offsets that may point anywhere. Other values are copied as
   * they are, and the writer zeroes them. Spark's nullability is used rather than the commit
   * schema's, which marks every Lance column nullable.
   *
   * TODO: Remove once the Velox that Gluten builds includes facebookincubator/velox#20126, which
   * exports the children of a struct's null rows as null.
   */
  def undefinedUnderNullStruct(schema: StructType): Option[String] =
    undefinedUnderNullStruct(schema, "", mayBeNull = false, inNullableStruct = false)

  private def undefinedUnderNullStruct(
      dataType: DataType,
      name: String,
      mayBeNull: Boolean,
      inNullableStruct: Boolean): Option[String] = dataType match {
    case s: StructType =>
      val inNullable = inNullableStruct || mayBeNull
      s.fields.iterator
        .map {
          f =>
            val child = if (name.isEmpty) f.name else s"$name.${f.name}"
            undefinedUnderNullStruct(f.dataType, child, f.nullable, inNullable)
        }
        .collectFirst { case Some(found) => found }
    case TimestampType | TimestampNTZType | _: StringType | BinaryType | _: ArrayType | _: MapType
        if inNullableStruct =>
      Some(name)
    case a: ArrayType =>
      undefinedUnderNullStruct(a.elementType, s"$name.element", a.containsNull, inNullableStruct)
    case m: MapType =>
      // Keys are never null, but a struct inside a key may be.
      undefinedUnderNullStruct(m.keyType, s"$name.key", mayBeNull = false, inNullableStruct)
        .orElse(
          undefinedUnderNullStruct(
            m.valueType,
            s"$name.value",
            m.valueContainsNull,
            inNullableStruct))
    case _ => None
  }

  // Lazy, so the check stops at the first column Velox cannot export.
  private def firstUnexportable(
      fields: JList[Field],
      prefix: String,
      topLevel: Boolean): Option[String] =
    fields.asScala.iterator
      .map(unexportable(_, prefix, topLevel))
      .collectFirst { case Some(reason) => reason }

  private def unexportable(field: Field, prefix: String, topLevel: Boolean): Option[String] = {
    val name = prefix + field.getName
    field.getType match {
      case i: ArrowType.Int if i.getIsSigned => None
      case f: ArrowType.FloatingPoint if isFloatOrDouble(f) => None
      case _: ArrowType.Bool | _: ArrowType.Utf8 | _: ArrowType.Binary => None
      case d: ArrowType.Decimal if d.getBitWidth == 128 => None
      case d: ArrowType.Date if d.getUnit == DateUnit.DAY => None
      case t: ArrowType.Timestamp if t.getUnit == TimeUnit.MICROSECOND => None
      case _: ArrowType.List | _: ArrowType.Struct | _: ArrowType.Map =>
        firstUnexportable(field.getChildren, s"$name.", topLevel = false)
      case _: ArrowType.FixedSizeList if topLevel =>
        field.getChildren.get(0).getType match {
          case f: ArrowType.FloatingPoint if isFloatOrDouble(f) => None
          case elementType =>
            Some(s"Fixed-size list column $name has elements of Arrow type $elementType")
        }
      case other =>
        Some(s"Column $name has Arrow type $other, which Velox cannot export for Lance")
    }
  }

  private def isFloatOrDouble(f: ArrowType.FloatingPoint): Boolean =
    f.getPrecision != FloatingPointPrecision.HALF
}
