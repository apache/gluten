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

/**
 * This test utility allows developer compares the test result with vanilla Spark easily, and can
 * check the fallback status.
 */
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.test.FallbackUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}

import java.util.concurrent.atomic.AtomicBoolean

abstract class GlutenQueryComparisonTest extends GlutenQueryTest {

  private val isFallbackCheckDisabled0 = new AtomicBoolean(false)

  final protected def disableFallbackCheck: Boolean =
    isFallbackCheckDisabled0.compareAndSet(false, true)

  protected def needCheckFallback: Boolean = !isFallbackCheckDisabled0.get()

  protected def vanillaSparkConfs(): Seq[(String, String)] = {
    List((GlutenConfig.GLUTEN_ENABLED.key, "false"))
  }

  /**
   * Some rule on LogicalPlan will not only apply in select query, the total df.load() should in
   * spark environment with gluten disabled config.
   *
   * @param sql
   * @return
   */
  protected def runAndCompare(sql: String): DataFrame = {
    var expected: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      expected = spark.sql(sql).collect()
    }
    val df = spark.sql(sql)
    checkAnswer(df, expected)
    df
  }

  protected def runQueryAndCompare(
      sqlStr: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true,
      cache: Boolean = false)(customCheck: DataFrame => Unit): DataFrame = {

    compareDfResultsAgainstVanillaSpark(
      () => spark.sql(sqlStr),
      compareResult,
      customCheck,
      noFallBack,
      cache)
  }

  protected def compareResultsAgainstVanillaSpark(
      sql: String,
      compareResult: Boolean = true,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true,
      cache: Boolean = false): DataFrame = {
    compareDfResultsAgainstVanillaSpark(
      () => spark.sql(sql),
      compareResult,
      customCheck,
      noFallBack,
      cache)
  }

  /**
   * run a query with native engine as well as vanilla spark then compare the result set for
   * correctness check
   */
  protected def compareDfResultsAgainstVanillaSpark(
      dataframe: () => DataFrame,
      compareResult: Boolean = true,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true,
      cache: Boolean = false): DataFrame = {
    var expected: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      val df = dataframe()
      expected = df.collect()
    }
    // By default, we will fallback complex type scan but here we should allow
    // to test support of complex type
    spark.conf.set("spark.gluten.sql.complexType.scan.fallback.enabled", "false")
    val df = dataframe()
    if (cache) {
      df.cache()
    }
    try {
      if (compareResult) {
        checkAnswer(df, expected)
      } else {
        df.collect()
      }
    } finally {
      if (cache) {
        df.unpersist()
      }
    }
    checkDataFrame(noFallBack, customCheck, df)
    df
  }

  protected def checkDataFrame(
      noFallBack: Boolean,
      customCheck: DataFrame => Unit,
      df: DataFrame): Unit = {
    if (needCheckFallback) {
      GlutenQueryComparisonTest.checkFallBack(df, noFallBack)
    }
    customCheck(df)
  }

}

object GlutenQueryComparisonTest extends Logging {

  def checkFallBack(
      df: DataFrame,
      noFallback: Boolean = true,
      skipAssert: Boolean = false): Unit = {
    // When noFallBack is true, it means there is no fallback plan,
    // otherwise there must be some fallback plans.
    val hasFallbacks = FallbackUtil.hasFallback(df.queryExecution.executedPlan)
    if (!skipAssert) {
      assert(
        !hasFallbacks == noFallback,
        s"FallBack $noFallback check error: ${df.queryExecution.executedPlan}")
    } else {
      logWarning(s"FallBack $noFallback check error: ${df.queryExecution.executedPlan}")
    }
  }
}
