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
package org.apache.gluten.expressions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer
import org.apache.gluten.expression.ExpressionMappings
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

class GlutenExpressionMappingSuite
  extends GlutenQueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    }
    conf
  }

  testWithMinSparkVersion("test expression blacklist", "3.2") {
    val names = ExpressionMappings.expressionsMap.values.toSet
    assert(names.contains("regexp_replace"))
    assert(names.contains("regexp_extract"))

    withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "regexp_replace,regexp_extract,add") {
      val names = ExpressionMappings.expressionsMap.values.toSet
      assert(!names.contains("regexp_replace"))
      assert(!names.contains("regexp_extract"))
      assert(names.contains("regexp_extract_all"))
      assert(!names.contains("add"))
      spark.sql("CREATE TABLE t USING PARQUET AS SELECT 1 as c")
      withTable("t") {
        val df = spark.sql("SELECT c + 1 FROM t")
        checkAnswer(df, Row(2))
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExecTransformer]).isEmpty)
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExec]).isDefined)
      }
    }
  }

  testWithMinSparkVersion("test blacklisting regexp expressions", "3.2") {
    val names = ExpressionMappings.expressionsMap.values.toSet
    assert(names.contains("rlike"))
    assert(names.contains("regexp_replace"))
    assert(names.contains("regexp_extract"))
    assert(names.contains("regexp_extract_all"))
    assert(names.contains("split"))

    withSQLConf(
      GlutenConfig.EXPRESSION_BLACK_LIST.key -> "",
      GlutenConfig.FALLBACK_REGEXP_EXPRESSIONS.key -> "true") {
      val names = ExpressionMappings.expressionsMap.values.toSet
      assert(!names.contains("rlike"))
      assert(!names.contains("regexp_replace"))
      assert(!names.contains("regexp_extract"))
      assert(!names.contains("regexp_extract_all"))
      assert(!names.contains("split"))

      spark.sql("CREATE TABLE t USING PARQUET AS SELECT 'abc100' as c")
      withTable("t") {
        val df = spark.sql("SELECT regexp_replace(c, '(\\d+)', 'something')  FROM t")
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExecTransformer]).isEmpty)
      }
    }
  }

  testWithMinSparkVersion(
    "GLUTEN-7213: Check fallback reason with CheckOverflowInTableInsert",
    "3.4") {
    withTable("t1", "t2") {
      sql("create table t1 (a float) using parquet")
      sql("insert into t1 values(1.1)")
      sql("create table t2 (b decimal(10,4)) using parquet")

      val msg =
        "CheckOverflowInTableInsert is used in ANSI mode, but Gluten does not support ANSI mode."
      import org.apache.spark.sql.execution.GlutenImplicits._
      val fallbackSummary = sql("insert overwrite t2 select * from t1").fallbackSummary()
      assert(fallbackSummary.fallbackNodeToReason.flatMap(_.values).exists(_.contains(msg)))
    }
  }
}
