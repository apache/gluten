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
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, MetadataBuilder, StringType, StructField, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the Delta column-mapping -> Velox Parquet field-id serialization.
 *
 * These run without a SparkSession, a metastore, or the `iceberg` build profile, so they execute in
 * every CI lane. They pin the three non-local invariants the serializer depends on, none of which
 * are visible from GlutenDeltaParquetFieldId.scala itself:
 *
 *   1. `StructField.name` is the PHYSICAL name by the time `prepareWrite` runs
 *      (TransactionalWrite.createPhysicalAttributes -> DeltaColumnMapping.createPhysicalSchema).
 *   2. The nested-ids lookup path RESETS at every StructField boundary -- a field nested three
 *      levels deep is keyed `<ownName>.element`, not by a dotted path from the top-level column.
 *      This mirrors DeltaColumnMapping's own producer, which recurses with `Seq(getPhysicalName)`.
 *   3. The emitted tree is the LOGICAL (Spark/Velox) tree, not the Parquet physical tree: an array
 *      contributes exactly ONE child (the element, with no intervening `list` group) and a map
 *      exactly TWO (key then value, with no `key_value` group). This is what Velox's
 *      validateSchemaRecursive requires.
 *
 * Invariant 2 in particular fails silently: a wrong path yields -1 for every element/key/value with
 * no exception anywhere, and Delta's own `isParquetIcebergCompatV2` only checks that ids are
 * present, not that they are correct.
 */
class GlutenDeltaParquetFieldIdSuite extends AnyFunSuite {

  private val icebergCompatV2 =
    Metadata(configuration = Map(DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key -> "true"))

  /** A StructField carrying Delta column-mapping metadata, as it looks at write time. */
  private def mapped(
      name: String,
      dataType: DataType,
      id: Long,
      nestedIds: (String, Long)*): StructField = {
    val builder = new MetadataBuilder()
      .putLong(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY, id)
    if (nestedIds.nonEmpty) {
      val nested = nestedIds.foldLeft(new MetadataBuilder()) {
        case (acc, (key, value)) => acc.putLong(key, value)
      }
      builder.putMetadata(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY, nested.build())
    }
    StructField(name, dataType, nullable = true, builder.build())
  }

  private def serialize(fields: StructField*): Option[String] =
    GlutenDeltaParquetFieldId
      .withParquetFieldIds(Map.empty, StructType(fields), icebergCompatV2)
      .get(GlutenConfig.PARQUET_FIELD_IDS)

  test("flat schema emits one id per column, in schema order") {
    assert(serialize(mapped("a", IntegerType, 1), mapped("b", StringType, 2)).contains("1,2"))
  }

  test("ids follow column-mapping metadata, not ordinal position") {
    // Under IcebergCompat, partition columns are appended to the END of the write schema, so the
    // ids are non-monotonic. Anything derived from ordinal position would silently mis-assign.
    val actual = serialize(
      mapped("id", IntegerType, 1),
      mapped("name", StringType, 3),
      mapped("part", IntegerType, 2))
    assert(actual.contains("1,3,2"))
  }

  test("struct children are emitted as a nested group") {
    val actual = serialize(
      mapped(
        "s",
        StructType(Seq(mapped("x", IntegerType, 2), mapped("y", IntegerType, 3))),
        1))
    assert(actual.contains("1(2,3)"))
  }

  test("array contributes exactly one child, keyed <field>.element") {
    val actual = serialize(mapped("arr", ArrayType(IntegerType), 1, "arr.element" -> 101))
    assert(actual.contains("1(101)"))
  }

  test("map contributes exactly two children, key then value") {
    val actual = serialize(
      mapped("m", MapType(StringType, StringType), 1, "m.key" -> 102, "m.value" -> 103))
    assert(actual.contains("1(102,103)"))
  }

  test("array of struct nests the struct fields under the synthetic element node") {
    val element = StructType(Seq(mapped("sku", StringType, 6), mapped("qty", IntegerType, 7)))
    val actual = serialize(mapped("items", ArrayType(element), 5, "items.element" -> 105))
    assert(actual.contains("5(105(6,7))"))
  }

  test("nested arrays accumulate the path within a single field") {
    // No StructField boundary is crossed here, so the path DOES accumulate: `a.element.element`.
    val actual = serialize(
      mapped(
        "a",
        ArrayType(ArrayType(IntegerType)),
        1,
        "a.element" -> 107,
        "a.element.element" -> 108))
    assert(actual.contains("1(107(108))"))
  }

  test("nested-id paths reset at every struct boundary") {
    // Invariant 2. `items` is nested inside `payload`, but its element is keyed by the field's own
    // name alone -- `items.element`, NOT `payload.items.element`.
    val payload = StructType(Seq(mapped("items", ArrayType(IntegerType), 2, "items.element" -> 3)))
    assert(serialize(mapped("payload", payload, 1)).contains("1(2(3))"))
  }

  test("a dotted path from the top-level column does NOT resolve, and degrades silently") {
    // The regression guard for invariant 2: keying the same schema the "obvious" way produces -1
    // for the element with no error. This is what a well-meaning refactor of the path handling
    // would do, and nothing downstream would catch it.
    val payload =
      StructType(Seq(mapped("items", ArrayType(IntegerType), 2, "payload.items.element" -> 3)))
    assert(serialize(mapped("payload", payload, 1)).contains("1(2(-1))"))
  }

  test("a field with no column-mapping metadata degrades to the -1 sentinel") {
    // Current behaviour, pinned deliberately. -1 is reachable for Delta's internal columns
    // (row-tracking `_row-id-col-*` / `_row-commit-version-col-*`), which carry no field id.
    // Velox drops negative ids, so those columns are written without a field_id -- byte-identical
    // to what vanilla Spark emits for them. Making this explicit rather than accidental is
    // tracked separately; this test exists so that change is a deliberate edit, not a surprise.
    assert(serialize(StructField("plain", IntegerType)).contains("-1"))
  }

  test("no field ids are emitted when IcebergCompatV2 is disabled") {
    val options = GlutenDeltaParquetFieldId.withParquetFieldIds(
      Map.empty,
      StructType(Seq(mapped("a", IntegerType, 1))),
      Metadata())
    assert(!options.contains(GlutenConfig.PARQUET_FIELD_IDS))
  }

  test("unrelated write options are preserved") {
    val options = GlutenDeltaParquetFieldId.withParquetFieldIds(
      Map("compression" -> "snappy"),
      StructType(Seq(mapped("a", IntegerType, 1))),
      icebergCompatV2)
    assert(options.get("compression").contains("snappy"))
    assert(options.get(GlutenConfig.PARQUET_FIELD_IDS).contains("1"))
  }

  test("an empty schema emits no option rather than an empty value") {
    val options =
      GlutenDeltaParquetFieldId.withParquetFieldIds(Map.empty, StructType(Nil), icebergCompatV2)
    assert(!options.contains(GlutenConfig.PARQUET_FIELD_IDS))
  }
}
