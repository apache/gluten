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
package org.apache.gluten.execution.enhanced

import org.apache.gluten.execution.{IcebergScanTransformer, IcebergSuite}
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.sql.Row

@EnhancedFeaturesTest
class VeloxIcebergDeletionVectorSuite extends IcebergSuite {

  test("read a Spark deletion vector natively") {
    withTable("iceberg_dv_delete") {
      createV3Table("iceberg_dv_delete")
      spark.sql("INSERT INTO iceberg_dv_delete VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

      withSQLConf("spark.gluten.sql.enable.enhancedFeatures" -> "false") {
        spark.sql("DELETE FROM iceberg_dv_delete WHERE id = 1").collect()
      }

      val sparkDvRead = spark.sql("SELECT * FROM iceberg_dv_delete ORDER BY id")
      checkAnswer(sparkDvRead, Seq(Row(2, "b"), Row(3, "c"), Row(4, "d")))
      assert(getExecutedPlan(sparkDvRead).collect {
        case _: IcebergScanTransformer => true
      }.nonEmpty)
    }
  }

  private def createV3Table(table: String): Unit = {
    spark.sql(s"""
                 |CREATE TABLE $table (id INT, data STRING)
                 |USING iceberg
                 |TBLPROPERTIES (
                 |  'format-version' = '3',
                 |  'write.delete.mode' = 'merge-on-read'
                 |)
                 |""".stripMargin)
  }
}
