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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.util.Locale

class GlutenFailOnFallbackSuite extends WholeStageTransformerSuite {

  protected val resourcePath: String = null
  protected val fileFormat: String = null

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf.set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    }
    conf
  }

  Seq(true, false).foreach {
    useV1 =>
      test(s"existence-default scan fallback and native controls (useV1=$useV1)") {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) "parquet" else "")) {
          withTempPath {
            dir =>
              spark.range(1).write.parquet(dir.getCanonicalPath)
              val readSchema = StructType(Seq(
                StructField("id", LongType),
                StructField("value", IntegerType).withExistenceDefaultValue("7"),
                StructField("s", StringType).withExistenceDefaultValue("'abc'"),
                StructField("null_default", IntegerType)
                  .withExistenceDefaultValue("CAST(NULL AS INT)")
              ))
              def read: DataFrame = spark.read.schema(readSchema).parquet(dir.getCanonicalPath)

              def checkScan(df: DataFrame, defaultColumns: Seq[String]): Unit = {
                val plan = getExecutedPlan(df)
                val nativeScans = plan.filter {
                  case _: FileSourceScanExecTransformer if useV1 => true
                  case _: BatchScanExecTransformer if !useV1 => true
                  case _ => false
                }
                val sparkScans = plan.filter {
                  case _: FileSourceScanExec if useV1 => true
                  case _: BatchScanExec if !useV1 => true
                  case _ => false
                }
                if (defaultColumns.isEmpty) {
                  assert(nativeScans.size == 1, df.queryExecution.executedPlan)
                  assert(sparkScans.isEmpty, df.queryExecution.executedPlan)
                } else {
                  assert(nativeScans.isEmpty, df.queryExecution.executedPlan)
                  assert(sparkScans.size == 1, df.queryExecution.executedPlan)
                  val scan = sparkScans.head
                  val reason = FallbackTags.getOption(scan)
                    .orElse(scan.logicalLink.flatMap(FallbackTags.getOption))
                  assert(
                    reason.exists(_.reason().contains(
                      "Native file scans do not support non-null existence defaults " +
                        s"(EXISTS_DEFAULT) on columns: ${defaultColumns.mkString(", ")}.")),
                    reason.toString
                  )
                }
              }

              checkScan(read.select("value", "s", "null_default"), Seq("value", "s"))
              checkScan(read.filter("value = 7").select("id"), Seq("value"))
              checkScan(read.select("id"), Nil)
              checkScan(read.selectExpr("count(*)"), Nil)
              checkScan(read.select("id", "null_default"), Nil)

              withSQLConf(
                GlutenConfig.SCAN_FALLBACK_ON_EXISTENCE_DEFAULTS_ENABLED.key -> "false") {
                checkScan(read.select("value", "s", "null_default"), Nil)
                checkScan(read.filter("value = 7").select("id"), Nil)
              }
              checkScan(read.select("value", "s"), Seq("value", "s"))
          }
        }
      }
  }

  test("failOnFallback throws only when a fallback is present") {
    withTable("t") {
      spark.range(10).write.format("parquet").saveAsTable("t")

      // failOnFallback explicitly disabled: a fallback must NOT throw.
      withSQLConf(
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "false",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        sql("SELECT * FROM t").collect()
      }

      // failOnFallback enabled, fully native query (no fallback): must NOT throw.
      withSQLConf(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true") {
        sql("SELECT * FROM t").collect()
      }

      // failOnFallback enabled, fallback present: must throw with the config key
      // and the offending operator/reason in the message.
      withSQLConf(
        GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key -> "true",
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        val ex = intercept[GlutenException] {
          sql("SELECT * FROM t").collect()
        }
        assert(ex.getMessage.contains(GlutenConfig.FALLBACK_FAIL_ON_FALLBACK.key))
        assert(ex.getMessage.contains("fell back to Spark"))
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("scan"))
      }
    }
  }
}
