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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Types

class VeloxIcebergFieldIdSuite extends IcebergSuite {
  testWithMinSparkVersion(
    "field IDs: flat mixed files, case folding, type promotion, and name reuse",
    "3.4") {
    withTable("field_id_verify_flat") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE field_id_verify_flat (
                    |  id INT,
                    |  LegacyName STRING,
                    |  recycled BIGINT,
                    |  widened INT,
                    |  stable STRING
                    |) USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO field_id_verify_flat
                    |VALUES
                    |  (1, 'old-a', 101L, 10, 's1'),
                    |  (2, 'old-b', NULL, 20, 's2')
                    |""".stripMargin)

        val table = loadIcebergTable("field_id_verify_flat")
        table
          .updateSchema()
          .renameColumn("LegacyName", "DisplayName")
          .deleteColumn("recycled")
          .updateColumn("widened", Types.LongType.get())
          .moveFirst("DisplayName")
          .commit()
        table.updateSchema().addColumn("recycled", Types.BooleanType.get()).commit()
        spark.catalog.refreshTable("field_id_verify_flat")

        spark.sql("""
                    |INSERT INTO field_id_verify_flat
                    |VALUES
                    |  ('new-c', 3, 30000000000L, 's3', true),
                    |  ('new-d', 4, 40L, 's4', false)
                    |""".stripMargin)
      }

      verifyNative("""
                     |SELECT displayname, id, recycled, widened, stable
                     |FROM field_id_verify_flat
                     |ORDER BY id
                     |""".stripMargin)
      verifyNative("""
                     |SELECT id, displayname
                     |FROM field_id_verify_flat
                     |WHERE displayname = 'old-a'
                     |""".stripMargin)
      verifyNative("""
                     |SELECT id, recycled
                     |FROM field_id_verify_flat
                     |WHERE recycled = true
                     |""".stripMargin)
      verifyNative("SELECT count(*), sum(widened) FROM field_id_verify_flat")
    }
  }

  testWithMinSparkVersion(
    "field IDs: deeply nested struct rename, reorder, and name reuse",
    "3.4") {
    withTable("field_id_verify_struct") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE field_id_verify_struct (
                    |  id INT,
                    |  profile STRUCT<
                    |    legacy_name: STRING,
                    |    address: STRUCT<old_city: STRING, zip: INT>,
                    |    obsolete: BIGINT,
                    |    score: INT
                    |  >
                    |) USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO field_id_verify_struct
                    |VALUES
                    |  (1, named_struct(
                    |    'legacy_name', 'old-a',
                    |    'address', named_struct('old_city', 'old-city', 'zip', 111),
                    |    'obsolete', 100L,
                    |    'score', 10)),
                    |  (2, NULL)
                    |""".stripMargin)

        val table = loadIcebergTable("field_id_verify_struct")
        table
          .updateSchema()
          .renameColumn("profile.legacy_name", "name")
          .renameColumn("profile.address.old_city", "city")
          .deleteColumn("profile.obsolete")
          .moveFirst("profile.score")
          .moveFirst("profile.address.zip")
          .commit()
        table
          .updateSchema()
          .addColumn("profile", "obsolete", Types.BooleanType.get())
          .commit()
        spark.catalog.refreshTable("field_id_verify_struct")

        spark.sql("""
                    |INSERT INTO field_id_verify_struct
                    |VALUES
                    |  (3, named_struct(
                    |    'score', 30,
                    |    'name', 'new-c',
                    |    'address', named_struct('zip', 333, 'city', 'new-city'),
                    |    'obsolete', true))
                    |""".stripMargin)
      }

      verifyNative("SELECT id, profile FROM field_id_verify_struct ORDER BY id")
      verifyNative("""
                     |SELECT
                     |  id,
                     |  profile.name,
                     |  profile.address.city,
                     |  profile.address.zip,
                     |  profile.obsolete,
                     |  profile.score
                     |FROM field_id_verify_struct
                     |ORDER BY id
                     |""".stripMargin)
      verifyNative("""
                     |SELECT id
                     |FROM field_id_verify_struct
                     |WHERE profile.address.city = 'old-city'
                     |""".stripMargin)
    }
  }

  testWithMinSparkVersion(
    "field IDs: array element struct evolution with nulls and mixed files",
    "3.4") {
    withTable("field_id_verify_array") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE field_id_verify_array (
                    |  id INT,
                    |  items ARRAY<STRUCT<legacy: STRING, obsolete: BIGINT, amount: INT>>
                    |) USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO field_id_verify_array
                    |VALUES
                    |  (1, array(
                    |    named_struct('legacy', 'old-item', 'obsolete', 99L, 'amount', 10),
                    |    CAST(NULL AS STRUCT<legacy: STRING, obsolete: BIGINT, amount: INT>))),
                    |  (2, NULL)
                    |""".stripMargin)

        val table = loadIcebergTable("field_id_verify_array")
        table
          .updateSchema()
          .renameColumn("items.element.legacy", "label")
          .deleteColumn("items.element.obsolete")
          .moveFirst("items.element.amount")
          .commit()
        table
          .updateSchema()
          .addColumn("items.element", "obsolete", Types.BooleanType.get())
          .commit()
        spark.catalog.refreshTable("field_id_verify_array")

        spark.sql("""
                    |INSERT INTO field_id_verify_array
                    |VALUES
                    |  (3, array(
                    |    named_struct('amount', 30, 'label', 'new-item', 'obsolete', true),
                    |    CAST(NULL AS STRUCT<amount: INT, label: STRING, obsolete: BOOLEAN>)))
                    |""".stripMargin)
      }

      verifyNative("SELECT id, items FROM field_id_verify_array ORDER BY id")
      verifyNative("""
                     |SELECT id, items[0].label, items[0].amount, items[0].obsolete
                     |FROM field_id_verify_array
                     |ORDER BY id
                     |""".stripMargin)
    }
  }

  testWithMinSparkVersion(
    "field IDs: map value struct evolution with nulls and mixed files",
    "3.4") {
    withTable("field_id_verify_map") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE field_id_verify_map (
                    |  id INT,
                    |  attrs MAP<
                    |    STRING,
                    |    STRUCT<legacy: STRING, obsolete: BIGINT, rank: INT>
                    |  >
                    |) USING iceberg
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO field_id_verify_map
                    |VALUES
                    |  (1, map(
                    |    'a', named_struct('legacy', 'old-value', 'obsolete', 88L, 'rank', 1),
                    |    'null-value',
                    |      CAST(NULL AS STRUCT<legacy: STRING, obsolete: BIGINT, rank: INT>))),
                    |  (2, NULL)
                    |""".stripMargin)

        val table = loadIcebergTable("field_id_verify_map")
        table
          .updateSchema()
          .renameColumn("attrs.value.legacy", "label")
          .deleteColumn("attrs.value.obsolete")
          .moveFirst("attrs.value.rank")
          .commit()
        table
          .updateSchema()
          .addColumn("attrs.value", "obsolete", Types.BooleanType.get())
          .commit()
        spark.catalog.refreshTable("field_id_verify_map")

        spark.sql("""
                    |INSERT INTO field_id_verify_map
                    |VALUES
                    |  (3, map(
                    |    'a', named_struct('rank', 3, 'label', 'new-value', 'obsolete', true),
                    |    'null-value',
                    |      CAST(NULL AS STRUCT<rank: INT, label: STRING, obsolete: BOOLEAN>)))
                    |""".stripMargin)
      }

      verifyNative("SELECT id, attrs FROM field_id_verify_map ORDER BY id")
      verifyNative("""
                     |SELECT id, attrs['a'].label, attrs['a'].rank, attrs['a'].obsolete
                     |FROM field_id_verify_map
                     |ORDER BY id
                     |""".stripMargin)
    }
  }

  testWithMinSparkVersion(
    "field IDs: renamed identity-partition column across mixed files",
    "3.4") {
    withTable("field_id_verify_partition") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE field_id_verify_partition (
                    |  id INT,
                    |  category STRING,
                    |  payload STRING
                    |) USING iceberg
                    |PARTITIONED BY (category)
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO field_id_verify_partition
                    |VALUES (1, 'old-a', 'p1'), (2, 'old-b', 'p2')
                    |""".stripMargin)

        val table = loadIcebergTable("field_id_verify_partition")
        table.updateSchema().renameColumn("category", "group_name").commit()
        spark.catalog.refreshTable("field_id_verify_partition")

        spark.sql("""
                    |INSERT INTO field_id_verify_partition
                    |VALUES (3, 'new-c', 'p3')
                    |""".stripMargin)
      }

      verifyNative("""
                     |SELECT id, group_name, payload
                     |FROM field_id_verify_partition
                     |ORDER BY id
                     |""".stripMargin)
      verifyNative("""
                     |SELECT id
                     |FROM field_id_verify_partition
                     |WHERE group_name = 'old-a'
                     |""".stripMargin)
    }
  }

  testWithMinSparkVersion(
    "field IDs: time travel uses the snapshot schema after name reuse",
    "3.4") {
    withTable("field_id_time_travel") {
      var oldSnapshotId = 0L
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE field_id_time_travel (
                    |  id INT,
                    |  recycled STRING
                    |) USING iceberg
                    |""".stripMargin)
        spark.sql("INSERT INTO field_id_time_travel VALUES (1, 'old-value')")

        val table = loadIcebergTable("field_id_time_travel")
        oldSnapshotId = table.currentSnapshot().snapshotId()
        table.updateSchema().deleteColumn("recycled").commit()
        table
          .updateSchema()
          .addColumn("recycled", Types.BooleanType.get())
          .commit()
        spark.catalog.refreshTable("field_id_time_travel")

        spark.sql("INSERT INTO field_id_time_travel VALUES (2, true)")
      }

      verifyNative(
        s"""
           |SELECT id, recycled
           |FROM field_id_time_travel VERSION AS OF $oldSnapshotId
           |ORDER BY id
           |""".stripMargin)
    }
  }

  private def verifyNative(sqlText: String): Unit = {
    runQueryAndCompare(sqlText) {
      df => checkGlutenPlan[IcebergScanTransformer](df)
    }
  }

  private def loadIcebergTable(name: String) = {
    spark.sessionState.catalogManager
      .catalog("spark_catalog")
      .asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array("default"), name))
      .asInstanceOf[SparkTable]
      .table()
  }
}
