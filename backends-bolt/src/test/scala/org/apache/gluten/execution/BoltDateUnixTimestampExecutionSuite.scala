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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{ToUnixTimestamp, UnixTimestamp}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types.{DateType, IntegerType, StructType}

import java.sql.Date

class BoltDateUnixTimestampExecutionSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "placeholder"
  override protected val fileFormat: String = "parquet"

  for (
    timezone <- Seq("UTC", "-08:00", "Asia/Shanghai");
    parserPolicy <- Seq("LEGACY", "CORRECTED", "EXCEPTION");
    function <- Seq("unix_timestamp", "to_unix_timestamp")
  ) {
    test(s"$function executes DATE with ignored formats in $timezone under $parserPolicy") {
      withTempPath {
        directory =>
          val schema = new StructType().add("id", IntegerType).add("d", DateType)
          val rows = Seq(
            Row(0, Date.valueOf("1970-01-01")),
            Row(1, Date.valueOf("2015-07-24")),
            Row(2, Date.valueOf("2020-03-08")),
            Row(3, Date.valueOf("2020-11-01")),
            Row(4, null))
          // Use Spark's writer to isolate the native DATE overload from native write behavior.
          withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
            spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
              .write.parquet(directory.getCanonicalPath)
          }
          withTempView("date_unix_timestamp_input") {
            spark.read.parquet(directory.getCanonicalPath)
              .createOrReplaceTempView("date_unix_timestamp_input")
            withSQLConf(
              "spark.sql.session.timeZone" -> timezone,
              "spark.sql.legacy.timeParserPolicy" -> parserPolicy) {
              val query = s"""
                             |SELECT id,
                             |  $function(d, 'yyyy-MM-dd') AS valid_format,
                             |  $function(d, 'invalid-pattern') AS invalid_format,
                             |  $function(d, 'YYYY') AS week_year_format,
                             |  $function(d, cast(null AS string)) AS null_format
                             |FROM date_unix_timestamp_input
                             |""".stripMargin
              runQueryAndCompare(query) {
                df =>
                  val plan = getExecutedPlan(df)
                  val projects = plan.collect { case p: ProjectExecTransformer => p }
                  assert(projects.nonEmpty, df.queryExecution.executedPlan.toString)
                  assert(!plan.exists(_.isInstanceOf[ProjectExec]))
                  val nativeFunctions = projects.flatMap(_.projectList).flatMap {
                    expression =>
                      expression.collect {
                        case value: UnixTimestamp => value
                        case value: ToUnixTimestamp => value
                      }
                  }
                  assert(nativeFunctions.size == 4)
                  assert(nativeFunctions.forall(_.children.head.dataType == DateType))
              }
            }
          }
      }
    }
  }
}
