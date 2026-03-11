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
package org.apache.spark.sql.hive.execution

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

class GlutenHiveSQLQueryCHSuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
  }

  testGluten("5182: Fix failed to parse post join filters") {
    withSQLConf(
      "spark.sql.hive.convertMetastoreParquet" -> "false",
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {
      sql("DROP TABLE IF EXISTS test_5182_0;")
      sql("DROP TABLE IF EXISTS test_5182_1;")
      sql(
        "CREATE TABLE test_5182_0 (from_uid STRING, vgift_typeid int, vm_count int, " +
          "status bigint, ts bigint, vm_typeid int) " +
          "USING hive OPTIONS(fileFormat 'parquet') PARTITIONED BY (`day` STRING);")
      sql(
        "CREATE TABLE test_5182_1 (typeid int, groupid int, ss_id bigint, " +
          "ss_start_time bigint, ss_end_time bigint) " +
          "USING hive OPTIONS(fileFormat 'parquet');")
      sql(
        "INSERT INTO test_5182_0 partition(day='2024-03-31') " +
          "VALUES('uid_1', 2, 10, 1, 11111111111, 2);")
      sql("INSERT INTO test_5182_1 VALUES(2, 1, 1, 1000000000, 2111111111);")
      val df = spark.sql(
        "select ee.from_uid as uid,day, vgift_typeid, money from " +
          "(select t_a.day, if(cast(substr(t_a.ts,1,10) as bigint) between " +
          "t_b.ss_start_time and t_b.ss_end_time, t_b.ss_id, 0) ss_id, " +
          "t_a.vgift_typeid, t_a.from_uid, vm_count money from " +
          "(select from_uid,day,vgift_typeid,vm_count,ts from test_5182_0 " +
          "where day between '2024-03-30' and '2024-03-31' and status=1 and vm_typeid=2) t_a " +
          "left join test_5182_1 t_b on t_a.vgift_typeid=t_b.typeid " +
          "where t_b.groupid in (1,2)) ee where ss_id=1;")
      checkAnswer(df, Seq(Row("uid_1", "2024-03-31", 2, 10)))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_5182_0"),
      ignoreIfNotExists = true,
      purge = false)
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_5182_1"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("5249: Reading csv may throw Unexpected empty column") {
    withSQLConf(
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false"
    ) {
      sql("DROP TABLE IF EXISTS test_5249;")
      sql(
        "CREATE TABLE test_5249 (name STRING, uid STRING) " +
          "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
          "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
          "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';")
      sql("INSERT INTO test_5249 VALUES('name_1', 'id_1');")
      val df = spark.sql(
        "SELECT name, uid, count(distinct uid) total_uid_num from test_5249 " +
          "group by name, uid with cube;")
      checkAnswer(
        df,
        Seq(
          Row("name_1", "id_1", 1),
          Row("name_1", null, 1),
          Row(null, "id_1", 1),
          Row(null, null, 1)))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_5249"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("GLUTEN-7116: Support outer explode") {
    sql("create table if not exists test_7116 (id int, name string)")
    sql("insert into test_7116 values (1, 'a,b'), (2, null), (null, 'c,d'), (3, '')")
    val query =
      """
        |select id, col_name
        |from test_7116 lateral view outer explode(split(name, ',')) as col_name
        |""".stripMargin
    val df = sql(query)
    checkAnswer(
      df,
      Seq(Row(1, "a"), Row(1, "b"), Row(2, null), Row(null, "c"), Row(null, "d"), Row(3, "")))
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_7116"),
      ignoreIfNotExists = true,
      purge = false)
  }
}
