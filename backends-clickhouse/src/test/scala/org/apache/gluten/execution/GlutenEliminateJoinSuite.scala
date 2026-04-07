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

import org.apache.gluten.backendsapi.clickhouse._
import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.nio.file.Files

class GlutenEliminateJoinSuite extends GlutenClickHouseWholeStageTransformerSuite with Logging {

  private var parquetPath: String = _

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
      .set(CHConfig.runtimeSettings("max_memory_usage_ratio_for_streaming_aggregating"), "0.01")
      .set(CHConfig.runtimeSettings("high_cardinality_threshold_for_streaming_aggregating"), "0.2")
      .set(CHBackendSettings.GLUTEN_JOIN_AGGREGATE_TO_AGGREGATE_UNION, "true")
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
        StructField("k3", StringType, nullable = true),
        StructField("v1", IntegerType, nullable = true),
        StructField("v2", IntegerType, nullable = true),
        StructField("v3", IntegerType, nullable = true)
      )
    )

    val data1 = Seq(
      Row(1, 1, "1", 1, 1, 1),
      Row(1, 1, "1", null, 2, 1),
      Row(1, 2, "2", 1, 1, null),
      Row(2, 1, "1", 1, null, 1),
      Row(2, 2, "1", 1, 1, 1),
      Row(2, 2, "2", 1, null, 1),
      Row(2, 2, "2", 2, null, 3),
      Row(2, 3, "3", 0, null, 1),
      Row(2, 4, "4", 1, 2, 3),
      Row(3, 1, "1", 4, 5, 6),
      Row(4, 2, "2", 7, 8, 9),
      Row(5, 3, "3", 10, 11, 12)
    )
    createTable("t1", data1, schema1)
    createTable("t2", data1, schema1)
    createTable("t3", data1, schema1)
  }

  test("Eliminate two aggregates join") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t2.k2, s1, s2 from (
          select k1, k2, sum(v1) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, count(v1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("Eliminate two aggregate joins with attribute reordered") {
    val sql = """
        select t1.k1, t1.k3, t2.k1, t2.k3, s1, s2 from (
          select k1, k3, sum(v1) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k3
        ) t1 left join (
          select k1, k3, count(v1) s2 from (
            select * from t2 where k1 != 3
          )group by k3, k1
        ) t2 on t1.k1 = t2.k1 and t1.k3 = t2.k3
        order by t1.k1, t1.k3, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })

  }

  test("Elimiate three aggreages join") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t3.k2, s1, s2, s3 from (
          select k1, k2, sum(v1) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, count(v1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        left join (
          select k1, k2, count(v2) s3 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t1.k2, s1, s2, s3
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("Left one join uneliminable") {
    val sql = """
        select t1.k1, t1.k2, s1, s2 from (
          select * from t1 where k1 != 1
        ) t1 left join (
          select k1, k2, count(v1) s1 from (
            select * from t2 where k1 != 1
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        left join (
          select k1, k2, count(v2) s2 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })
  }

  test("reorder join orders 1") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, s1, s2 from (
          select k1, k2, count(v1) s1 from (
            select * from t2 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select * from t1 where k1 != 1
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        left join (
          select k1, k2, count(v2) s2 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t1.k2, t2.k1, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })
  }

  test("reorder join orders 2") {
    val sql = """
        select t1.k1, t2.k1, s1, s2 from (
          select k1, k2, count(v1) s1 from (
            select * from t2 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, count(v2) as s3 from t1 where k1 != 1
          group by k1
        ) t2 on t1.k1 = t2.k1
        left join (
          select k1, k2, count(v2) s2 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t2.k1, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      }
    )
  }

  test("aggregate literal") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t2.k2, s1, s2 from (
          select k1, k2, sum(2) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, count(1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("aggregate avg") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t2.k2, s1, s2 from (
          select k1, k2, avg(2) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, avg(1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }
  test("aggregate min/max") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t2.k2, s1, s2 from (
          select k1, k2, min(2) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, max(1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("aggregate count distinct") {
    val sql = """
        select t1.k1, t2.k1, s1, s2, s3, s4 from (
          select k1, count(distinct v1) s1, count(distinct v2) as s2 from (
            select * from t1 where k1 != 1
          )group by k1
        ) t1 left join (
          select k1, count(distinct v1) s3, count(distinct v3) as s4 from (
            select * from t2 where k1 != 3
          )group by k1
        ) t2 on t1.k1 = t2.k1
        order by t1.k1, s1, s2, s3, s4
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("distinct") {
    val sql = """
        select t1.k1, t2.k1, s1 from (
          select k1, count(v1) as s1 from (
            select * from t1 where k1 != 1
          )group by k1
        ) t1 left join (
          select distinct k1 from (
            select * from t2 where k1 != 3
          ) t2
        ) t2 on t1.k1 = t2.k1
        order by t1.k1, s1
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("different join keys and grouping keys 1") {
    val sql = """
        select t1.k1, t2.k1, s1 from (
          select k1, count(v1) as s1 from (
            select * from t1 where k1 != 1
          )group by k1
        ) t1 left join (
          select distinct k1, k2 from (
            select * from t2 where k1 != 3
          ) t2
        ) t2 on t1.k1 = t2.k1
        order by t1.k1, s1
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })
  }

  test("not attribute grouping keys 1") {
    val sql = """
        select t1.k1, t2.k2, s1 from (
          select k1 + 1 as k1, count(v1) as s1 from (
            select * from t1 where k1 != 1
          )group by k1 + 1
        ) t1 left join (
          select distinct k2 from (
            select * from t2 where k1 != 3
          ) t2
        ) t2 on t1.k1 = t2.k2
        order by t1.k1, t2.k2, s1
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("not attribute grouping keys 2") {
    val sql = """
        select t1.k1, t2.k2, s1 from (
          select k1 + 1 as k1, count(v1) as s1 from (
            select * from t1 where k1 != 1
          )group by 1
        ) t1 left join (
          select distinct k2 from (
            select * from t2 where k1 != 3
          ) t2
        ) t2 on t1.k1 = t2.k2
        order by t1.k1, t2.k2, s1
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.isEmpty)
      })
  }

  test("const expression aggregate expression") {
    val sql = """
        select t1.k1, t2.k1, s1 from (
          select k1 + 1 as k1, 1 as s1 from (
            select * from t1 where k1 != 1
          )group by 1
        ) t1 left join (
          select distinct k1 from (
            select * from t2 where k1 != 3
          ) t2
        ) t2 on t1.k1 = t2.k1
        order by t1.k1, t2.k1, s1
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })
  }

  // Ensure the isAnyJoin will never lost after apply other rules
  test("lost any join setting") {
    spark.sql("drop table if exists t_9267_1")
    spark.sql("drop table if exists t_9267_2")
    spark.sql("create table t_9267_1 (a bigint, b bigint) using parquet")
    spark.sql("create table t_9267_2 (a bigint, b bigint) using parquet")
    spark.sql("insert into t_9267_1 select id as a, id as b from range(20000000)")
    spark.sql("insert into t_9267_2 select id as a, id as b from range(5000000)")
    spark.sql("insert into t_9267_2 select id as a, id as b from range(5000000)")

    val sql =
      """
        |select count(1) as n1, count(a1, b1, a2) as n2 from(
        |  select t1.a as a1, t1.b as b1, t2.a as a2 from (
        |    select * from t_9267_1 where a >= 0 and b < 100000000 and b >= 0
        |  ) t1 left join (
        |    select a, b from t_9267_2 group by a, b
        |  ) t2 on t1.a = t2.a and t1.b = t2.b
        |)""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })

    spark.sql("drop table t_9267_1")
    spark.sql("drop table t_9267_2")
  }

  test("build left side") {
    spark.sql("drop table if exists t_9267_1")
    spark.sql("drop table if exists t_9267_2")
    spark.sql("create table t_9267_1 (a bigint, b bigint) using parquet")
    spark.sql("create table t_9267_2 (a bigint, b bigint) using parquet")
    spark.sql("insert into t_9267_1 select id as a, id as b from range(2000000)")
    spark.sql("insert into t_9267_2 select id as a, id as b from range(500000)")
    spark.sql("insert into t_9267_2 select id as a, id as b from range(500000)")

    // left table is smaller, it will be used as the build side.
    val sql =
      """
        |select count(1) as n1, count(a1, b1, a2) as n2 from(
        |  select t1.a as a1, t1.b as b1, t2.a as a2 from (
        |    select a, b from t_9267_2 group by a, b
        |  ) t1 left join (
        |    select * from t_9267_1 where a >= 0 and b != 100000000 and b >= 0
        |  ) t2 on t1.a = t2.a and t1.b = t2.b
        |)""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })

    spark.sql("drop table t_9267_1")
    spark.sql("drop table t_9267_2")
  }

  test("right keys are in used") {
    spark.sql("drop table if exists t_join_1")
    spark.sql("drop table if exists t_join_2")
    spark.sql("drop table if exists t_join_3")

    spark.sql("create table t_join_1 (a bigint, b bigint) using parquet")
    spark.sql("create table t_join_2 (a bigint, b bigint) using parquet")
    spark.sql("create table t_join_3 (a bigint, b bigint) using parquet")

    spark.sql("insert into t_join_1 select id % 10 as a, id as b from range(10)")
    spark.sql("insert into t_join_2 select id % 7 as a, id as b from range(20)")
    spark.sql("insert into t_join_3 select id % 10 as a, id as b from range(20)")

    val sql =
      """
        |select a1, b, a2, a3, s2, s3 from (
        |  select a as a1, b from t_join_1
        |) t1 left join (
        |  select a as a2, sum(b) as s2 from t_join_2 group by a
        |) t2 on a1 = a2 left join (
        |  select a as a3, sum(b) as s3 from t_join_3 group by a
        |) t3 on a1 = a3
        |order by a1, b, a2, a3, s2, s3
        |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })

    spark.sql("drop table t_join_1")
    spark.sql("drop table t_join_2")
    spark.sql("drop table t_join_3")
  }
}
