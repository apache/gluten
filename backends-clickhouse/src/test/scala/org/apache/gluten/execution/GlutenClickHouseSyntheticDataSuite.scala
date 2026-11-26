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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

import java.time.LocalDate

class GlutenClickHouseSyntheticDataSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // spark.sparkContext.setLogLevel("WARN")

  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.sql.legacy.createHiveTableByDefault", "false")
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
    /* .set("spark.sql.catalogImplementation", "hive")
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${
        metaStorePathAbsolute + "/metastore_db"};create=true") */
  }

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()
    super.afterAll()
  }

  test("test all data types all agg") {

    val supportedTypes = {
      // aggregator on "float"/"byte"/short"/"string" are not supported in gluten yet
      //        "byte" ::
      //          "short" ::
      "int" ::
        "bigint" ::
        //          "float" ::
        "double" ::
        "date" ::
        //          "string" ::
        "boolean" ::
        Nil
    }

    def prepareTables(): Unit = {
      withSQLConf(vanillaSparkConfs(): _*) {
        spark.sql(s"DROP TABLE IF EXISTS table_all_types")

//        val fields = supportedTypes.map(x => s"c_${x} ${x}").mkString(",")
//        val ddl =
        //        s"CREATE TABLE IF NOT EXISTS table_all_types ( $fields )  USING PARQUET "
//        println(s"the ddl is: $ddl")
//        spark.sql(ddl);

        val x = spark
        import x.implicits._

        val source = Seq(
          (
            //          Some(Byte.MinValue),
            //          Some(Short.MinValue),
            Some(Int.MinValue),
            Some(Long.MinValue),
//            Some(Float.MinValue),
            Some(Double.MinValue),
            Some(LocalDate.of(1970, 1, 1)),
//            Some("a"),
            Some(false)),
          (
            //          Some(Byte.MaxValue),
            //          Some(Short.MaxValue),
            Some(Int.MaxValue),
            Some(Long.MaxValue),
//            Some(Float.MaxValue),
            Some(Double.MaxValue),
            Some(LocalDate.of(2070, 1, 1)),
//            Some("Z"),
            Some(true))
//          (None, None, None, None, None)
        )
        val df_source = source.toDF(supportedTypes.map(x => s"c_$x"): _*)
//        df_source.createOrReplaceTempView("table_all_types_temp")
//        spark.sql("insert into table_all_types select * from table_all_types_temp")
        df_source.createOrReplaceTempView("table_all_types")
      }
    }

    prepareTables()

    var sqlStr: String = null
    var expected: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      val supportedAggs = "count" :: "avg" :: "sum" :: "min" :: "max" :: Nil
      val selected = supportedAggs
        .flatMap(
          agg => {
            Range(0, supportedTypes.size).map(i => s"$agg(c_${supportedTypes.apply(i)})")
          })
        .filterNot(
          x => {
            List(
              // unsupported by spark, so it's ok
              "avg(c_date)",
              "avg(c_string)",
              "avg(c_boolean)",
              "sum(c_date)",
              "sum(c_string)",
              "sum(c_boolean)",
              "avg(c_bigint)"
              // supported by spark, unsupported by gluten, need to fix
              // 1. byte/short/float/string case
            ).contains(x)
          })
        .mkString(",")
      sqlStr = s"select $selected from table_all_types"

      val df = spark.sql(sqlStr)
      expected = df.collect()
    }
    val df = spark.sql(sqlStr)
    df.collect()
    GlutenQueryComparisonTest.checkFallBack(df)
    checkAnswer(df, expected)
  }

  test("test data function in https://github.com/Kyligence/ClickHouse/issues/88") {
    withTable("test_table") {
      var sqlStr: String = null
      var expected: Seq[Row] = null

      val x = spark
      import x.implicits._

      withSQLConf(vanillaSparkConfs(): _*) {
        spark.sql("create table test_table(c_date date) using parquet")
        Seq(LocalDate.of(2020, 1, 1), LocalDate.of(1970, 1, 1))
          .toDF("c_date")
          .createOrReplaceTempView("test_table_temp")
        spark.sql("insert into test_table select * from test_table_temp")

        sqlStr = s"""
                    |select cast(c_date as date) as a, cast ('1998-04-08' as date) as b
                    |   from test_table
                    |   where c_date between (cast ('1998-04-08' as date) - interval '30' day)
                    |                    and (cast ('1998-04-08' as date) + interval '30' day)
                    |order by a desc
                    |""".stripMargin

        val df = spark.sql(sqlStr)
        expected = df.collect()
      }
      val df = spark.sql(sqlStr)
      df.collect()
      GlutenQueryComparisonTest.checkFallBack(df)
      checkAnswer(df, expected)
    }
  }

  test("sql on Seq based(row based) DataFrame") {
    var sqlStr: String = null
    var expected: Seq[Row] = null

    val x = spark
    import x.implicits._

    withSQLConf(vanillaSparkConfs(): _*) {
      spark.sql("drop table if exists test_table")
//      spark.sql("create table if not exists test_table(c_date date) using parquet")
//      Seq(LocalDate.of(2020, 1, 1), LocalDate.of(1970, 1, 1))
//        .toDF("c_date")
//        .createOrReplaceTempView("test_table")
      Seq(LocalDate.of(2020, 1, 1), LocalDate.of(1970, 1, 1), null)
        .toDF("c_date")
        .createOrReplaceTempView("test_table")

      sqlStr = s"""
                  | select max(c_date) from test_table
                  |""".stripMargin

      val df = spark.sql(sqlStr)
      expected = df.collect()
    }
    val df = spark.sql(sqlStr)
    df.collect()
    GlutenQueryComparisonTest.checkFallBack(df)
    checkAnswer(df, expected)
  }

}
