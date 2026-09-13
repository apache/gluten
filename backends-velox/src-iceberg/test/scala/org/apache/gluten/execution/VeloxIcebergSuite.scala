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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.utils.GlutenSuiteUtils

import org.apache.iceberg.UpdateSchema
import org.apache.iceberg.expressions.Literal
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.{ResolvingFileIO, StorageCredential}
import org.apache.iceberg.spark.{Spark3Util, SparkCatalog}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.{Type, Types}

import scala.collection.JavaConverters._

class VeloxIcebergSuite extends IcebergSuite {
  private def withCredentialTable(catalogName: String)(
      f: (String, ResolvingFileIO) => Unit): Unit = {
    withTempDir {
      dir =>
        withSQLConf(
          s"spark.sql.catalog.$catalogName" -> classOf[SparkCatalog].getName,
          s"spark.sql.catalog.$catalogName.type" -> "hadoop",
          s"spark.sql.catalog.$catalogName.warehouse" -> dir.toURI.toString,
          s"spark.sql.catalog.$catalogName.io-impl" -> classOf[ResolvingFileIO].getName,
          s"spark.sql.catalog.$catalogName.cache-enabled" -> "false"
        ) {
          val catalog = spark.sessionState.catalogManager.catalog(catalogName)
            .asInstanceOf[SparkCatalog].icebergCatalog().asInstanceOf[HadoopCatalog]
          val tableName = s"$catalogName.default.t"
          try {
            withTable(tableName) {
              withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
                spark.sql(s"CREATE TABLE $tableName (id INT) USING iceberg")
                spark.sql(s"INSERT INTO $tableName VALUES (1), (2)")
              }
              f(
                tableName,
                Spark3Util.loadIcebergTable(spark, tableName).io()
                  .asInstanceOf[ResolvingFileIO])
            }
          } finally {
            try {
              GlutenSuiteUtils.waitUntilEmpty(spark.sparkContext)
            } finally {
              catalog.close()
            }
          }
        }
    }
  }

  test("iceberg vended FileIO credentials fall back") {
    withCredentialTable("vended_credentials") {
      (tableName, io) =>
        val originalProperties = io.properties().asScala.toMap
        val keys = Map("s3.access-key-id" -> "test-key", "s3.secret-access-key" -> "test-secret")
        // These are the FileIO inputs supplied by REST catalogs. Exercise the real scan path
        // without duplicating Iceberg's REST client/server implementation.
        val cases = Seq(
          ("S3 key pair", keys, Seq.empty[StorageCredential]),
          ("S3 session credentials", keys + ("s3.session-token" -> "test-token"), Seq.empty),
          (
            "storage-credentials",
            Map.empty[String, String],
            Seq(StorageCredential.create("s3://bucket/table", keys.asJava))),
          (
            "credential refresh",
            Map("client.refresh-credentials-endpoint" -> "credentials"),
            Seq.empty)
        )
        cases.foreach {
          case (name, properties, credentials) =>
            withClue(name) {
              io.initialize((originalProperties ++ properties).asJava)
              io.setCredentials(credentials.asJava)
              runQueryAndCompare(s"SELECT id FROM $tableName", noFallBack = false) {
                df =>
                  checkAnswer(df, Seq(Row(1), Row(2)))
                  checkGlutenPlanCount[IcebergScanTransformer](df, 0)
                  val scans = getExecutedPlan(df).collect { case scan: BatchScanExec => scan }
                  assert(scans.size == 1)
                  val reason = FallbackTags.get(scans.head.logicalLink.get).reason()
                  assert(reason.contains("catalog-vended credentials"))
                  assert(!reason.contains("test-secret") && !reason.contains("test-token"))
              }
            }
        }
    }
  }

  test("iceberg catalog authentication and storage settings still offload") {
    withCredentialTable("catalog_settings") {
      (tableName, io) =>
        io.initialize((io.properties().asScala.toMap ++ Map(
          "credential" -> "catalog-secret",
          "token" -> "catalog-token",
          "s3.endpoint" -> "http://unused",
          "client.region" -> "us-east-1")).asJava)
        runQueryAndCompare(s"SELECT id FROM $tableName") {
          df =>
            checkAnswer(df, Seq(Row(1), Row(2)))
            checkGlutenPlanCount[IcebergScanTransformer](df, 1)
        }
    }
  }

  test("iceberg parquet split uses name mapping for projected columns") {
    withTable("iceberg_parquet_name_mapping") {
      withSQLConf(VeloxConfig.PARQUET_USE_COLUMN_NAMES.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_parquet_name_mapping (
                    |  id BIGINT,
                    |  amount DECIMAL(12, 2),
                    |  note STRING
                    |)
                    |USING iceberg
                    |TBLPROPERTIES ('write.format.default' = 'parquet')
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_parquet_name_mapping
                    |VALUES (CAST(1 AS BIGINT), CAST(10.50 AS DECIMAL(12, 2)), 'a')
                    |""".stripMargin)

        runQueryAndCompare("SELECT amount FROM iceberg_parquet_name_mapping") {
          df =>
            checkAnswer(df, Seq(Row(BigDecimal("10.50"))))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
      }
    }
  }

  testWithMinSparkVersion("iceberg v3 initial default for an added column", "3.4") {
    withTable("iceberg_v3_initial_default") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_v3_initial_default (id INT)
                    |USING iceberg
                    |TBLPROPERTIES ('format-version' = '3')
                    |""".stripMargin)
        spark.sql("INSERT INTO iceberg_v3_initial_default VALUES (1), (2)")

        val catalog = spark.sessionState.catalogManager
          .catalog("spark_catalog")
          .asInstanceOf[TableCatalog]
        val updateSchema = catalog
          .loadTable(Identifier.of(Array("default"), "iceberg_v3_initial_default"))
          .asInstanceOf[SparkTable]
          .table()
          .updateSchema()
        classOf[UpdateSchema]
          .getMethod(
            "addColumn",
            classOf[String],
            classOf[Type],
            classOf[Literal[_]])
          .invoke(updateSchema, "country", Types.StringType.get(), Literal.of("IN"))
        updateSchema.commit()
        spark.catalog.refreshTable("iceberg_v3_initial_default")
      }

      runQueryAndCompare(
        "SELECT id, country FROM iceberg_v3_initial_default ORDER BY id") {
        df =>
          checkAnswer(df, Seq(Row(1, "IN"), Row(2, "IN")))
          checkGlutenPlan[IcebergScanTransformer](df)
      }
    }
  }
}
