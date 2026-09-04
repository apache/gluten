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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{HashAggregateExecBaseTransformer, ProjectExecTransformer}

import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.internal.SQLConf

import java.time.LocalDateTime

class GlutenTimestampNtzAggregateSuite extends GlutenSQLTestsTrait {

  import testImplicits._

  testGluten("min and max") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          Seq(
            "1969-12-31 23:59:59.999999",
            "2024-01-01 00:00:00.123456"
          ).toDF("input")
            .selectExpr("cast(input as timestamp_ntz) as ts")
            .write
            .parquet(path.getCanonicalPath)

          val result = spark.read.parquet(path.getCanonicalPath).agg(min($"ts"), max($"ts"))
          checkAnswer(
            result,
            Row(
              LocalDateTime.parse("1969-12-31T23:59:59.999999"),
              LocalDateTime.parse("2024-01-01T00:00:00.123456")))
          assert(
            getExecutedPlan(result).exists(_.isInstanceOf[HashAggregateExecBaseTransformer]),
            result.queryExecution.executedPlan.treeString)
      }
    }
  }

  testGluten("unsupported project falls back") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false") {
      withTempPath {
        path =>
          Seq("2024-01-01 00:00:00.123456")
            .toDF("input")
            .selectExpr("cast(input as timestamp_ntz) as ts")
            .write
            .parquet(path.getCanonicalPath)

          val result = spark.read
            .parquet(path.getCanonicalPath)
            .selectExpr("to_json(named_struct('ts', ts))")
          checkAnswer(result, Row("""{"ts":"2024-01-01T00:00:00.123"}"""))
          assert(
            !getExecutedPlan(result).exists(_.isInstanceOf[ProjectExecTransformer]),
            result.queryExecution.executedPlan.treeString)
      }
    }
  }
}
