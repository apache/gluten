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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

abstract class HudiSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  test("hudi: time travel") {
    withTable("hudi_tm") {
      spark.sql(s"""
                   |create table hudi_tm (id int, name string) using hudi
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into hudi_tm values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into hudi_tm values (3, "v3"), (4, "v4")
                   |""".stripMargin)
      val df = spark.sql(" select _hoodie_commit_time from hudi_tm;")
      val value = df.collectAsList().get(0).getAs[String](0)
      val df1 = runQueryAndCompare("select id, name from hudi_tm timestamp AS OF " + value) {
        checkGlutenPlan[HudiScanTransformer]
      }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)
      val df2 =
        runQueryAndCompare("select name from hudi_tm timestamp AS OF " + value + " where id = 2") {
          checkGlutenPlan[HudiScanTransformer]
        }
      checkLengthAndPlan(df2, 1)
      checkAnswer(df2, Row("v2") :: Nil)
    }
  }

  test("hudi: soft delete") {
    withTable("hudi_pf") {
      spark.sql(s"""
                   |create table hudi_pf (id int, name string) using hudi
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into hudi_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                   |""".stripMargin)
      spark.sql(s"""
                   |delete from hudi_pf where name = "v2"
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select id, name from hudi_pf") {
        checkGlutenPlan[HudiScanTransformer]
      }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(3, "v1") :: Nil)
    }
  }

  // FIXME: flaky leaked file systems issue
  ignore("hudi: mor") {
    withTable("hudi_mor") {
      spark.sql(s"""
                   |create table hudi_mor (id int, name string, ts bigint)
                   |using hudi
                   |tblproperties (
                   |  type = 'mor',
                   |  primaryKey = 'id',
                   |  preCombineField = 'ts'
                   |)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into hudi_mor values (1, "v1", 1000), (2, "v2", 2000),
                   | (3, "v1", 3000), (4, "v2", 4000)
                   |""".stripMargin)
      spark.sql(s"""
                   |delete from hudi_mor where id = 1
                   |""".stripMargin)
      val df1 =
        runQueryAndCompare("select id, name from hudi_mor where name = 'v1'", true, false, false) {
          _ =>
        }
      checkAnswer(df1, Row(3, "v1") :: Nil)
    }
  }

  test("hudi: partition filters") {
    withTable("hudi_pf") {
      spark.sql(s"""
                   |create table hudi_pf (id int, name string) using hudi partitioned by (name)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into hudi_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select id, name from hudi_pf where name = 'v1'") { _ => }
      val hudiScanTransformer = df1.queryExecution.executedPlan.collect {
        case f: HudiScanTransformer => f
      }.head
      // No data filters as only partition filters exist
      assert(hudiScanTransformer.filterExprs().size == 0)
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(3, "v1") :: Nil)
    }
  }
}
