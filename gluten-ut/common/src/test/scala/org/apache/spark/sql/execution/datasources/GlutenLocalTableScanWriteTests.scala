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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode.NO_CODEGEN
import org.apache.spark.sql.execution.{LocalTableScanExec, LocalTableScanTransformer, ProjectExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

trait GlutenLocalTableScanWriteTests
  extends QueryTest
  with GlutenSQLTestsBaseTrait
  with AdaptiveSparkPlanHelper {

  for {
    adaptive <- Seq("true", "false")
    plannedWrite <- Seq("true", "false")
  } {
    testGluten(
      s"local table scan INSERT VALUES codegen (AQE=$adaptive, planned write=$plannedWrite)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive,
        SQLConf.PLANNED_WRITE_ENABLED.key -> plannedWrite,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation",
        GlutenConfig.COLUMNAR_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
        GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "false",
        GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false"
      ) {
        withTable("local_scan_write") {
          sql("CREATE TABLE local_scan_write (id BIGINT) USING PARQUET")
          FileFormatWriter.executedPlan = None
          sql("INSERT INTO local_scan_write VALUES (1), (2)").collect()

          val plan = FileFormatWriter.executedPlan.getOrElse {
            fail("Expected the executed file-writer plan")
          }
          assert(
            collect(plan) {
              case project: ProjectExec if project.child.isInstanceOf[LocalTableScanExec] =>
                project
            }.nonEmpty,
            plan)
          if (conf.codegenFactoryMode != NO_CODEGEN.toString) {
            assert(collect(plan) { case _: WholeStageCodegenExec => true }.nonEmpty, plan)
          }
          assert(collect(plan) { case _: LocalTableScanTransformer => true }.isEmpty, plan)
          checkAnswer(sql("SELECT * FROM local_scan_write"), Seq(Row(1L), Row(2L)))
        }
      }
    }
  }
}
