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

import org.apache.gluten.config.VeloxDeltaConfig

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.SparkVersionUtil

class VeloxDeltaSuite extends DeltaSuite {
  test("delta: change data feed scan offload can be disabled") {
    withTable("delta_cdf_disabled") {
      spark.sql("""
                  |create table delta_cdf_disabled (id int, name string) using delta
                  |tblproperties ("delta.enableChangeDataFeed" = "true")
                  |""".stripMargin)
      spark.sql("""
                  |insert into delta_cdf_disabled values (1, "v1"), (2, "v2")
                  |""".stripMargin)

      withSQLConf(VeloxDeltaConfig.ENABLE_CHANGE_DATA_FEED_SCAN.key -> "false") {
        val df = spark.sql("""
                             |select id, name, _change_type, _commit_version
                             |from table_changes('delta_cdf_disabled', 1)
                             |""".stripMargin)
        checkAnswer(
          df,
          Seq(
            Row(1, "v1", "insert", 1L),
            Row(2, "v2", "insert", 1L)))
        assert(
          df.queryExecution.executedPlan.collect {
            case scan: DeltaScanTransformer => scan
          }.isEmpty,
          df.queryExecution.executedPlan)
      }
    }
  }

  Seq("name", "id").foreach {
    mode =>
      test(s"map-key pruning is disabled under column mapping mode = $mode") {
        withTable("delta_cm_map") {
          spark.sql(s"""
                       |create table delta_cm_map
                       |  (id bigint, m map<string, struct<s string, t bigint>>)
                       |using delta
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          spark.sql(
            "insert into delta_cm_map select id, " +
              "map('a', named_struct('s', concat('v', cast(id % 7 as string)), 't', id), " +
              "'b', named_struct('s', 'x', 't', id * 2)) from range(0, 500)")
          val pruningFlag = "spark.gluten.sql.columnar.backend.velox.scanMapKeyPruningEnabled"
          withSQLConf(pruningFlag -> "true", "spark.sql.adaptive.enabled" -> "false") {
            runQueryAndCompare(
              "select sum(m['a'].t), max(m['a'].s) from delta_cm_map where m['a'].s = 'v3'") {
              df =>
                val subfields = getExecutedPlan(df)
                  .collect { case scan: DeltaScanTransformer => scan.requiredMapSubfields }
                  .flatten
                assert(subfields.isEmpty, s"physical column names cannot be pruned: $subfields")
            }
          }
        }
      }
  }

  testWithMinSparkVersion("map-key pruning on delta scan with and without deletion vector", "3.4") {
    withTempPath {
      p =>
        val path = p.getCanonicalPath
        spark
          .range(0, 2000)
          .selectExpr(
            "id",
            "map('a', named_struct('s', concat('v', cast(id % 7 as string)), 't', id), " +
              "'b', named_struct('s', 'x', 't', id * 2), " +
              "'c', named_struct('s', 'y', 't', id + 1)) as m"
          )
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
        val sql = s"SELECT count(*), sum(m['a'].t), max(m['a'].s) " +
          s"FROM delta.`$path` WHERE m['a'].s = 'v3'"
        val pruningFlag = "spark.gluten.sql.columnar.backend.velox.scanMapKeyPruningEnabled"

        def scanSubfields(df: DataFrame): Map[String, Seq[String]] =
          getExecutedPlan(df)
            .collect { case scan: DeltaScanTransformer => scan.requiredMapSubfields }
            .flatten
            .toMap
            .map { case (col, paths) => col -> paths.map(_.toString) }

        withSQLConf(pruningFlag -> "true", "spark.sql.adaptive.enabled" -> "false") {
          runQueryAndCompare(sql) {
            df =>
              val subfields = scanSubfields(df)
              assert(subfields.contains("m"), s"expected pruning on m, got $subfields")
              assert(subfields("m").exists(_.contains("m[\"a\"]")), subfields("m").toString)
              assert(!subfields("m").exists(_.contains("[\"b\"]")), subfields("m").toString)
          }
        }

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 10 = 0")
        if (SparkVersionUtil.gteSpark35) {
          withSQLConf(pruningFlag -> "true", "spark.sql.adaptive.enabled" -> "false") {
            runQueryAndCompare(sql) {
              df =>
                assert(
                  df.queryExecution.executedPlan
                    .collect { case _: DeltaScanTransformer => true }
                    .nonEmpty)
                assert(scanSubfields(df).contains("m"))
            }
          }
        }
    }
  }
}
