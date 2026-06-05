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
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.nio.file.Files

class GlutenNothingValueCheck extends GlutenClickHouseWholeStageTransformerSuite with Logging {

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
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(GlutenConfig.GLUTEN_SUPPORTED_SCALA_UDFS.key, "compare_substrings:compare_substrings")
      .set(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
        ConstantFolding.ruleName + "," + NullPropagation.ruleName)
  }

  def createTable(tableName: String, rows: Seq[Row], schema: StructType): Unit = {
    val file = Files.createTempFile(tableName, ".parquet").toFile
    file.deleteOnExit()
    val data = sparkContext.parallelize(rows)
    val dataframe = spark.createDataFrame(data, schema)
    dataframe.coalesce(1).write.format("parquet").mode("overwrite").parquet(file.getAbsolutePath)
    spark.catalog.createTable(tableName, file.getAbsolutePath, "parquet")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val schema1 = StructType(
      Array(
        StructField("k1", IntegerType, nullable = true),
        StructField("k2", IntegerType, nullable = true),
        StructField("v1", IntegerType, nullable = true),
        StructField("v2", IntegerType, nullable = true),
        StructField("v3", IntegerType, nullable = true)
      )
    )

    val data1 = Seq(
      Row(1, 1, 1, 1, 1),
      Row(1, 1, null, 2, 1),
      Row(1, 2, 1, 1, null),
      Row(2, 1, 1, null, 1),
      Row(2, 2, 1, 1, 1),
      Row(2, 2, 1, null, 1),
      Row(2, 2, 2, null, 3),
      Row(2, 3, 0, null, 1),
      Row(2, 4, 1, 2, 3),
      Row(3, 1, 4, 5, 6),
      Row(4, 2, 7, 8, 9),
      Row(5, 3, 10, 11, 12)
    )
    createTable("t1", data1, schema1)
    createTable("t2", data1, schema1)
    createTable("t3", data1, schema1)
  }

  test("nothing array: convert nullable to non-nullable") {
    val sql =
      """
        |select array() as x union all select array(123) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array: convert nullable to nullable 1") {
    val sql =
      """
        |select array() as x union all select array(123, null) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array: convert nullable to nullable 2") {
    val sql =
      """
        |select array() as x union all select array(null) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array: null array") {
    val sql =
      """
        |select array(null)
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing map: convert nullable to non-nullable") {
    val sql =
      """
        |select map() as x union all select map(123, 456) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing map: convert nullable to nullable 1") {
    val sql =
      """
        |select map() as x union all select map(1, null, 2, 23) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array in map 1") {
    val sql =
      """
        |select map(1, null) as x union all select map(1, array(456)) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array in map 2") {
    val sql =
      """
        |select map(1, array()) as x union all select map(1, array(456)) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array in map 3") {
    val sql =
      """
        |select map(1, array()) as x union all select map(1, array(456, null)) as x
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

  test("nothing array in shuffle") {
    val sql =
      """
        |select t1.k1 as a, t1.k2 as b, t2.k1 as c, t2.k2 as d, t1.x as x, t1.o as o from (
        |   select k1, k2, array() as x, '' as o from t1
        |   union all
        |   select k1, k2, array(123) as x, '123' as o from t2
        |) t1 left join (
        |   select k1, k2 from t3
        |) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        |order by t1.k1, t1.k2, t2.k1, t2.k2, t1.o
        |""".stripMargin
    compareResultsAgainstVanillaSpark(sql, true, { _ => })
  }

}
