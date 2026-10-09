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
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.iceberg.FileFormat
import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord, Record}
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.spark.source.SparkTable

class GlutenIcebergEqualityDeleteSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = null
  override protected val fileFormat: String = null

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.iceberg.type", "hadoop")

  test("equality and position deletes apply together when the equality key is unprojected") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    val name = "iceberg.default.mixed_deletes"
    withTempDir {
      dir =>
        withSQLConf("spark.sql.catalog.iceberg.warehouse" -> dir.toURI.toString) {
          withTable(name) {
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              spark.sql(s"CREATE TABLE $name (id BIGINT, payload STRING) USING iceberg")
              spark.range(10).selectExpr("id", "cast(id as string) AS payload")
                .coalesce(1).writeTo(name).append()
              val table = spark.sessionState.catalogManager.catalog("iceberg")
                .asInstanceOf[TableCatalog].loadTable(Identifier.of(
                  Array("default"),
                  "mixed_deletes"))
                .asInstanceOf[SparkTable].table()
              val tasks = table.newScan().planFiles()
              val dataFile =
                try { tasks.iterator().next().file().path() }
                finally { tasks.close() }
              val schema = table.schema().select("id")
              val factory = new GenericAppenderFactory(
                table.schema(),
                table.spec(),
                Array(table.schema().findField("id").fieldId()),
                schema,
                null)
              val files =
                OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build()
              val eqWriter =
                factory.newEqDeleteWriter(files.newOutputFile(), FileFormat.PARQUET, null)
              try {
                val record = GenericRecord.create(schema)
                record.setField("id", Long.box(4))
                eqWriter.write(record)
              } finally { eqWriter.close() }
              val posWriter =
                factory.newPosDeleteWriter(files.newOutputFile(), FileFormat.PARQUET, null)
              try {
                posWriter.write(PositionDelete.create[Record]().set(dataFile, 1L, null))
                posWriter.write(PositionDelete.create[Record]().set(dataFile, 3L, null))
              } finally { posWriter.close() }
              table.newRowDelta().addDeletes(eqWriter.toDeleteFile())
                .addDeletes(posWriter.toDeleteFile()).commit()
              spark.catalog.refreshTable(name)
            }
            runQueryAndCompare(s"SELECT payload FROM $name") {
              df =>
                assert(
                  getExecutedPlan(df).exists(_.getClass.getSimpleName == "IcebergScanTransformer"))
                checkAnswer(df, Seq(0, 2, 5, 6, 7, 8, 9).map(i => Row(i.toString)))
            }
          }
        }
    }
  }
}
