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
import org.apache.gluten.execution.DeltaScanTransformer
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StructType}

class GlutenDeltaColumnMappingSuite extends GlutenQueryTest with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.parquet.fieldId.read.enabled", "true")
      .set("spark.sql.parquet.fieldId.write.enabled", "true")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.ansi.enabled", "false")
  }

  Seq(false, true).foreach {
    vectorized =>
      test(s"Delta id mapping reads field IDs instead of physical names, vectorized=$vectorized") {
        assume(BackendTestUtils.isVeloxBackendLoaded())
        withSQLConf("spark.sql.parquet.enableVectorizedReader" -> vectorized.toString) {
          withTable("delta_id_read") {
            // Isolate the reader from Spark 3.5's native writer, which does not preserve field IDs.
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              spark.sql("""
                          |CREATE TABLE delta_id_read (a STRING, b STRING) USING delta
                          |TBLPROPERTIES ('delta.columnMapping.mode' = 'id')
                          |""".stripMargin)
              spark.sql("INSERT INTO delta_id_read VALUES ('original', 'redirected')")
            }
            val log = DeltaLog.forTable(spark, TableIdentifier("delta_id_read"))
            val otherId = log.update().metadata.schema("b").metadata
              .getLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY)
            spark.sql("ALTER TABLE delta_id_read DROP COLUMN b")

            Seq(100L -> Row(null), otherId -> Row("redirected")).foreach {
              case (fieldId, expected) =>
                val metadata = log.update().metadata
                val field = metadata.schema("a")
                val updated = field.copy(metadata = new MetadataBuilder()
                  .withMetadata(field.metadata)
                  .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, fieldId)
                  .build())
                log.withNewTransaction {
                  txn =>
                    txn.commit(
                      Seq(metadata.copy(
                        schemaString = StructType(Seq(updated)).json,
                        configuration = metadata.configuration +
                          (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "10000"))),
                      DeltaOperations.ManualUpdate
                    )
                }

                val df = spark.sql("SELECT a FROM delta_id_read")
                val scan = df.queryExecution.sparkPlan.collect {
                  case fileScan: FileSourceScanExec => fileScan
                }.head
                assert(
                  !scan.requiredSchema("a").metadata
                    .contains(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY))
                val validation = DeltaScanTransformer(scan).doValidate()
                assert(!validation.ok())
                assert(validation.reason().contains(DeltaScanTransformer.ID_MAPPING_UNSUPPORTED))
                checkAnswer(df, Seq(expected))
                assert(
                  collect(df.queryExecution.executedPlan) {
                    case _: DeltaScanTransformer => true
                  }.isEmpty,
                  df.queryExecution.executedPlan)
                assert(
                  collect(df.queryExecution.executedPlan) {
                    case _: FileSourceScanExec => true
                  }.nonEmpty,
                  df.queryExecution.executedPlan)
            }
          }
        }
      }
  }
}
