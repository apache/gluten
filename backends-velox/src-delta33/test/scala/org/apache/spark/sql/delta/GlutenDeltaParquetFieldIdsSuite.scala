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

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest

import org.scalatest.funsuite.AnyFunSuite

import java.util.Base64

import scala.collection.JavaConverters._

/** GlutenDeltaParquetFieldIds on the physical schemas Delta gives its writer. */
@ExtendedSQLTest
class GlutenDeltaParquetFieldIdsSuite extends AnyFunSuite {

  private def compat(version: Int): Metadata =
    Metadata(configuration = Map(
      DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
      s"delta.enableIcebergCompatV$version" -> "true"))

  private val sqlConf = new SQLConf

  /** A struct field as Delta's physical schema has it. */
  private def field(
      name: String,
      dataType: DataType,
      id: Long,
      nestedIds: (String, Long)*): StructField = {
    val metadata =
      new MetadataBuilder().putLong(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY, id)
    if (nestedIds.nonEmpty) {
      val nested = new MetadataBuilder()
      nestedIds.foreach { case (path, nestedId) => nested.putLong(path, nestedId) }
      metadata.putMetadata(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY, nested.build())
    }
    StructField(name, dataType, nullable = true, metadata.build())
  }

  private def render(fieldIds: Seq[ParquetFieldId]): String =
    fieldIds.map(render).mkString(",")

  private def render(fieldId: ParquetFieldId): String = {
    val children = fieldId.children().asScala.toSeq
    if (children.isEmpty) fieldId.fieldId().toString
    else s"${fieldId.fieldId()}(${render(children)})"
  }

  private def fieldIds(version: Int, fields: StructField*): String =
    render(GlutenDeltaParquetFieldIds.of(StructType(fields), compat(version), sqlConf))

  test("columns take their own IDs, not their position") {
    assert(
      fieldIds(2, field("a", IntegerType, 1), field("c", StringType, 3), field("b", IntegerType, 2))
        == "1,3,2")
  }

  test("struct fields take their own IDs") {
    val struct = StructType(Seq(field("x", IntegerType, 2), field("y", IntegerType, 3)))
    assert(fieldIds(2, field("s", struct, 1)) == "1(2,3)")
  }

  test("an array has one node for its element, a map two, for its key and value") {
    assert(fieldIds(2, field("a", ArrayType(IntegerType), 1, "a.element" -> 2)) == "1(2)")
    assert(
      fieldIds(2, field("m", MapType(StringType, IntegerType), 1, "m.key" -> 2, "m.value" -> 3))
        == "1(2,3)")
  }

  test("nested IDs are keyed by their path from the nearest enclosing struct field") {
    // Within one field the path grows; DeltaParquetWriteSupport keys nested arrays the same way.
    assert(
      fieldIds(
        2,
        field(
          "a",
          ArrayType(MapType(StringType, ArrayType(IntegerType))),
          1,
          "a.element" -> 2,
          "a.element.key" -> 3,
          "a.element.value" -> 4,
          "a.element.value.element" -> 5)
      ) == "1(2(3,4(5)))")
    // A struct field starts a new path: `items.element`, not `payload.items.element`.
    val payload = StructType(Seq(field("items", ArrayType(IntegerType), 2, "items.element" -> 3)))
    assert(fieldIds(2, field("payload", payload, 1)) == "1(2(3))")
    // Fields of an array's struct element are struct fields too.
    val element = StructType(Seq(field("sku", StringType, 3), field("qty", IntegerType, 4)))
    assert(fieldIds(2, field("items", ArrayType(element), 1, "items.element" -> 2)) == "1(2(3,4))")
  }

  test("a missing nested ID fails, as in Delta's writer") {
    val payload =
      StructType(Seq(field("items", ArrayType(IntegerType), 2, "payload.items.element" -> 3)))
    val e = intercept[IllegalStateException](fieldIds(2, field("payload", payload, 1)))
    assert(e.getMessage.contains("items.element"))
    assert(
      GlutenDeltaParquetFieldIds
        .unsupported(compat(2), StructType(Seq(field("payload", payload, 1))), sqlConf)
        .exists(_.contains("items.element")))
  }

  test("IcebergCompatV1 writes no nested IDs") {
    assert(fieldIds(1, field("a", ArrayType(IntegerType), 1)) == "1(-1)")
  }

  test("columns without a field ID, such as row tracking columns, have none") {
    assert(fieldIds(2, field("a", IntegerType, 1), StructField("_row-id-col", LongType)) == "1,-1")
  }

  test("spark.sql.parquet.fieldId.write.enabled=false leaves out the struct field IDs") {
    val conf = new SQLConf
    conf.setConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED, false)
    val schema = StructType(Seq(field("a", ArrayType(IntegerType), 1, "a.element" -> 2)))
    assert(render(GlutenDeltaParquetFieldIds.of(schema, compat(2), conf)) == "-1(2)")
  }

  test("only IcebergCompat V1 and V2 tables pass their field IDs to the Velox writer") {
    val schema = StructType(Seq(field("a", IntegerType, 1)))
    for (version <- Seq(1, 2)) {
      assert(GlutenDeltaParquetFieldIds.unsupported(compat(version), schema, sqlConf).isEmpty)
      val options = GlutenDeltaParquetFieldIds
        .withFieldIds(Map("compression" -> "zstd"), schema, compat(version), sqlConf)
      assert(options("compression") == "zstd")
      val root = ParquetFieldIdNode.parseFrom(
        Base64.getDecoder.decode(options(GlutenConfig.PARQUET_FIELD_IDS)))
      assert(root.getFieldId == -1)
      assert(root.getChildrenList.asScala.map(_.getFieldId) == Seq(1))
    }
    for (
      configuration <- Seq(
        Map.empty[String, String],
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id"))
    ) {
      val metadata = Metadata(configuration = configuration)
      assert(GlutenDeltaParquetFieldIds.withFieldIds(Map.empty, schema, metadata, sqlConf).isEmpty)
    }
    // Delta's writer writes the files of column-mapped tables without Iceberg compatibility.
    val idMapping = Metadata(configuration = Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id"))
    assert(GlutenDeltaParquetFieldIds.unsupported(idMapping, schema, sqlConf).isDefined)
    assert(GlutenDeltaParquetFieldIds.unsupported(Metadata(), schema, sqlConf).isEmpty)
  }
}
