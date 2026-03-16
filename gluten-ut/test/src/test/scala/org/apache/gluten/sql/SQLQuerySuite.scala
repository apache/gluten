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
package org.apache.gluten.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformerSuite
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

class SQLQuerySuite extends WholeStageTransformerSuite {
  protected val resourcePath: String = null
  protected val fileFormat: String = null

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

  testWithMinSparkVersion("Incorrect decimal casting for partition read", "4.0") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withTable("dynparttest2") {
        val data =
          Seq[(Integer, Integer)](
            (1, 1),
            (1, 3),
            (2, 3),
            (3, 3),
            (4, null),
            (5, null)
          )
        val df = spark.createDataFrame(data).toDF("key", "value")
        df.createOrReplaceTempView("src")

        // decimal with fractional truncation
        sql("create table dynparttest2 (value int) partitioned by (pdec decimal(5, 1))")
        sql("""
              |insert into table dynparttest2 partition(pdec)
              | select count(*), cast('100.12' as decimal(5, 1)) as pdec from src
          """.stripMargin)
        checkAnswer(
          sql("select * from dynparttest2"),
          Seq(Row(6, new java.math.BigDecimal("100.1"))))
      }

      // small decimal with scale > 0
      withTable("dynparttest_small") {
        sql("create table dynparttest_small (value int) partitioned by (pdec decimal(3, 2))")
        sql("""
              |insert into table dynparttest_small partition(pdec)
              | select count(*), cast('1.23' as decimal(3, 2)) as pdec from src
          """.stripMargin)
        checkAnswer(
          sql("select * from dynparttest_small"),
          Seq(Row(6, new java.math.BigDecimal("1.23"))))
      }

      // zero scale with no fractional part
      withTable("dynparttest_zero_scale") {
        sql("create table dynparttest_zero_scale (value int) partitioned by (pdec decimal(10, 0))")
        sql("""
              |insert into table dynparttest_zero_scale partition(pdec)
              | select count(*), cast('42' as decimal(10, 0)) as pdec from src
          """.stripMargin)
        checkAnswer(
          sql("select * from dynparttest_zero_scale"),
          Seq(Row(6, new java.math.BigDecimal("42"))))
      }

      // negative value with scale
      withTable("dynparttest_neg") {
        sql("create table dynparttest_neg (value int) partitioned by (pdec decimal(5, 2))")
        sql("""
              |insert into table dynparttest_neg partition(pdec)
              | select count(*), cast('-3.14' as decimal(5, 2)) as pdec from src
          """.stripMargin)
        checkAnswer(
          sql("select * from dynparttest_neg"),
          Seq(Row(6, new java.math.BigDecimal("-3.14"))))
      }

      // multiple distinct partition values
      withTable("dynparttest_multi") {
        sql("create table dynparttest_multi (value int) partitioned by (pdec decimal(4, 1))")
        sql("""
              |insert into table dynparttest_multi partition(pdec)
              | select count(*), cast('10.5' as decimal(4, 1)) as pdec from src
          """.stripMargin)
        sql("""
              |insert into table dynparttest_multi partition(pdec)
              | select count(*), cast('20.3' as decimal(4, 1)) as pdec from src
          """.stripMargin)
        checkAnswer(
          sql("select * from dynparttest_multi order by pdec"),
          Seq(Row(6, new java.math.BigDecimal("10.5")), Row(6, new java.math.BigDecimal("20.3"))))
        // partition pruning
        checkAnswer(
          sql("select * from dynparttest_multi where pdec = 10.5"),
          Seq(Row(6, new java.math.BigDecimal("10.5"))))
      }
    }
  }

  test("Support run with Vector reader in FileSourceScan or BatchScan") {
    withSQLConf(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false"
    ) {
      withTable("t1") {
        sql("""CREATE TABLE t1(name STRING, id BINARY, part BINARY)
              |USING PARQUET PARTITIONED BY (part)""".stripMargin)
        sql("INSERT INTO t1 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
        checkAnswer(
          sql("SELECT name, cast(id as string), cast(part as string) FROM t1"),
          Row("a", "Spark SQL", "Spark SQL"))
      }
    }
  }
}
