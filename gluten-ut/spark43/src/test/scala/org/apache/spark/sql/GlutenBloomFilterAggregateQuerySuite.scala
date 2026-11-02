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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.HashAggregateExecBaseTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

class GlutenBloomFilterAggregateQuerySuite
  extends BloomFilterAggregateQuerySuite
  with GlutenSQLTestsTrait
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  val veloxBloomFilterMaxNumBits = 4194304L

  testGluten("Test bloom_filter_agg with big RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS") {
    val table = "bloom_filter_test"
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.key -> "5000000") {
      val numEstimatedItems = 5000000L
      val sqlString = s"""
                         |SELECT every(might_contain(
                         |            (SELECT bloom_filter_agg(col,
                         |              cast($numEstimatedItems as long),
                         |              cast($veloxBloomFilterMaxNumBits as long))
                         |             FROM $table),
                         |            col)) positive_membership_test
                         |FROM $table
                      """.stripMargin
      withTempView(table) {
        (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
          .toDF("col")
          .createOrReplaceTempView(table)
        checkAnswer(spark.sql(sqlString), Row(true))
      }
    }
  }

  testGluten("Test that might_contain on bloom_filter_agg with empty input") {
    checkAnswer(
      spark.sql("""SELECT might_contain((select bloom_filter_agg(cast(id as long))
                  | from range(1, 1)), cast(123 as long))""".stripMargin),
      Row(null)
    )

    checkAnswer(
      spark.sql("""SELECT might_contain((select bloom_filter_agg(cast(id as long))
                  | from range(1, 1)), null)""".stripMargin),
      Row(null))
  }

  testGluten("Test bloom_filter_agg filter fallback") {
    val table = "bloom_filter_test"
    val numEstimatedItems = 5000000L
    val sqlString = s"""
                       |SELECT col positive_membership_test
                       |FROM $table
                       |WHERE might_contain(
                       |            (SELECT bloom_filter_agg(col,
                       |              cast($numEstimatedItems as long),
                       |              cast($veloxBloomFilterMaxNumBits as long))
                       |             FROM $table), col)
                      """.stripMargin
    withTempView(table) {
      (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
        .toDF("col")
        .createOrReplaceTempView(table)
      withSQLConf(
        GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> "false"
      ) {
        val df = spark.sql(sqlString)
        df.collect
        assert(
          collectWithSubqueries(df.queryExecution.executedPlan) {
            case h if h.isInstanceOf[HashAggregateExecBaseTransformer] => h
          }.size == 2,
          df.queryExecution.executedPlan
        )
      }
      if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
        withSQLConf(
          GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false"
        ) {
          val df = spark.sql(sqlString)
          df.collect
          assert(
            collectWithSubqueries(df.queryExecution.executedPlan) {
              case h if h.isInstanceOf[HashAggregateExecBaseTransformer] => h
            }.size == 2,
            df.queryExecution.executedPlan
          )
        }
      }
    }
  }

  testGluten("Test bloom_filter_agg agg fallback") {
    val table = "bloom_filter_test"
    val numEstimatedItems = 5000000L
    val sqlString = s"""
                       |SELECT col positive_membership_test
                       |FROM $table
                       |WHERE might_contain(
                       |            (SELECT bloom_filter_agg(col,
                       |              cast($numEstimatedItems as long),
                       |              cast($veloxBloomFilterMaxNumBits as long))
                       |             FROM $table), col)
                      """.stripMargin

    withTempView(table) {
      (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
        .toDF("col")
        .createOrReplaceTempView(table)
      withSQLConf(
        GlutenConfig.COLUMNAR_HASHAGG_ENABLED.key -> "false"
      ) {
        val df = spark.sql(sqlString)
        df.collect
        assert(
          collectWithSubqueries(df.queryExecution.executedPlan) {
            case h if h.isInstanceOf[HashAggregateExecBaseTransformer] => h
          }.isEmpty,
          df.queryExecution.executedPlan
        )
      }
    }
  }
}

class GlutenBloomFilterAggregateQuerySuiteCGOff extends GlutenBloomFilterAggregateQuerySuite {
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.sql.codegen.factoryMode", "NO_CODEGEN")
  }
}
