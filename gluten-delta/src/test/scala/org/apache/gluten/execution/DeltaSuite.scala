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
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

abstract class DeltaSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  // FIXME: This folder doesn't exist in module gluten-delta so should be provided by
  //  backend modules that rely on this suite.
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  // IdMapping is supported in Delta 2.2 (related to Spark3.3.1)
  test("column mapping mode = id") {
    withTable("delta_cm1") {
      spark.sql(s"""
                   |create table delta_cm1 (id int, name string) using delta
                   |tblproperties ("delta.columnMapping.mode"= "id")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_cm1 values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_cm1") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

      val df2 = runQueryAndCompare("select name from delta_cm1 where id = 2") { _ => }
      checkLengthAndPlan(df2, 1)
      checkAnswer(df2, Row("v2") :: Nil)
    }
  }

  // NameMapping is supported in Delta 2.0 (related to Spark3.2.0)
  testWithMinSparkVersion("column mapping mode = name", "3.2") {
    withTable("delta_cm2") {
      spark.sql(s"""
                   |create table delta_cm2 (id int, name string) using delta
                   |tblproperties ("delta.columnMapping.mode"= "name")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_cm2 values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_cm2") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

      val df2 = runQueryAndCompare("select name from delta_cm2 where id = 2") { _ => }
      checkLengthAndPlan(df2, 1)
      checkAnswer(df2, Row("v2") :: Nil)
    }
  }

  test("delta: time travel") {
    withTable("delta_tm") {
      spark.sql(s"""
                   |create table delta_tm (id int, name string) using delta
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_tm values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_tm values (3, "v3"), (4, "v4")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_tm VERSION AS OF 1") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)
      val df2 = runQueryAndCompare("select * from delta_tm VERSION AS OF 2") { _ => }
      checkLengthAndPlan(df2, 4)
      checkAnswer(df2, Row(1, "v1") :: Row(2, "v2") :: Row(3, "v3") :: Row(4, "v4") :: Nil)
      val df3 = runQueryAndCompare("select name from delta_tm VERSION AS OF 2 where id = 2") {
        _ =>
      }
      checkLengthAndPlan(df3, 1)
      checkAnswer(df3, Row("v2") :: Nil)
    }
  }

  testWithMinSparkVersion("delta: partition filters", "3.2") {
    withTable("delta_pf") {
      spark.sql(s"""
                   |create table delta_pf (id int, name string) using delta partitioned by (name)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_pf where name = 'v1'") { _ => }
      val deltaScanTransformer = df1.queryExecution.executedPlan.collect {
        case f: DeltaScanTransformer => f
      }.head
      // No data filters as only partition filters exist
      assert(deltaScanTransformer.filterExprs().size == 0)
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(3, "v1") :: Nil)
    }
  }

  testWithMinSparkVersion("basic test with stats.skipping disabled", "3.2") {
    withTable("delta_test2") {
      withSQLConf("spark.databricks.delta.stats.skipping" -> "false") {
        spark.sql(s"""
                     |create table delta_test2 (id int, name string) using delta
                     |""".stripMargin)
        spark.sql(s"""
                     |insert into delta_test2 values (1, "v1"), (2, "v2")
                     |""".stripMargin)
        val df1 = runQueryAndCompare("select * from delta_test2") { _ => }
        checkLengthAndPlan(df1, 2)
        checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

        val df2 = runQueryAndCompare("select name from delta_test2 where id = 2") { _ => }
        checkLengthAndPlan(df2, 1)
        checkAnswer(df2, Row("v2") :: Nil)
      }
    }
  }

  testWithMinSparkVersion("column mapping with complex type", "3.2") {
    withTable("t1") {
      val simpleNestedSchema = new StructType()
        .add("a", StringType, true)
        .add("b", new StructType().add("c", StringType, true).add("d", IntegerType, true))
        .add("map", MapType(StringType, StringType), true)
        .add("arr", ArrayType(IntegerType), true)

      val simpleNestedData = spark.createDataFrame(
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))).asJava,
        simpleNestedSchema)

      spark.sql(
        """CREATE TABLE t1
          | (a STRING,b STRUCT<c: STRING NOT NULL, d: INT>,map MAP<STRING, STRING>,arr ARRAY<INT>)
          | USING DELTA
          | PARTITIONED BY (`a`)
          | TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)

      simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")

      val df1 = runQueryAndCompare("select * from t1") { _ => }
      checkAnswer(
        df1,
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))))
      spark.sql(s"Alter table t1 RENAME COLUMN b to b1")
      spark.sql(
        "insert into t1 " +
          "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      val df2 = runQueryAndCompare("select b1 from t1") { _ => }
      checkAnswer(df2, Seq(Row(Row("str1.1", 1)), Row(Row("str1.2", 2)), Row(Row("str1.3", 3))))

      spark.sql(s"Alter table t1 RENAME COLUMN b1.c to c1")
      val df3 = runQueryAndCompare("select * from t1") { _ => }
      checkAnswer(
        df3,
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22)),
          Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33))
        )
      )
    }
  }

  testWithMinSparkVersion("deletion vector", "3.4") {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath
        val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
        val values2 = Seq(6, 7, 8, 9, 10)
        val df2 = values2.toDF("id")
        df1.union(df2).coalesce(1).write.format("delta").save(path)
        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        checkAnswer(spark.read.format("delta").load(path), df1.union(df2))
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (${values2.mkString(", ")})")
        import org.apache.spark.sql.execution.GlutenImplicits._
        val df = spark.read.format("delta").load(path)
        assert(
          df.fallbackSummary.fallbackNodeToReason
            .flatMap(_.values)
            .exists(_.contains("Deletion vector is not supported in native")))
        checkAnswer(df, df1)
    }
  }

  testWithMinSparkVersion("delta: push down input_file_name expression", "3.2") {
    withTable("source_table") {
      withTable("target_table") {
        spark.sql(s"""
                     |CREATE TABLE source_table(id INT, name STRING, age INT) USING delta;
                     |""".stripMargin)

        spark.sql(s"""
                     |CREATE TABLE target_table(id INT, name STRING, age INT) USING delta;
                     |
                     |""".stripMargin)

        spark.sql(s"""
                     |INSERT INTO source_table VALUES(1, 'a', 10),(2, 'b', 20);
                     |""".stripMargin)

        spark.sql(s"""
                     |INSERT INTO target_table VALUES(1, 'c', 10),(3, 'c', 30);
                     |""".stripMargin)

        spark.sql(s"""
                     |MERGE INTO target_table AS target
                     |USING source_table AS source
                     |ON target.id = source.id
                     |WHEN MATCHED THEN
                     |UPDATE SET
                     |  target.name = source.name,
                     |  target.age = source.age
                     |WHEN NOT MATCHED THEN
                     |INSERT (id, name, age) VALUES (source.id, source.name, source.age);
                     |""".stripMargin)

        val df1 = runQueryAndCompare("SELECT * FROM target_table") { _ => }
        checkAnswer(df1, Row(1, "a", 10) :: Row(2, "b", 20) :: Row(3, "c", 30) :: Nil)
      }
    }
  }

  testWithMinSparkVersion("delta: need to validate delta expression before execution", "3.2") {
    withTable("source_table") {
      withTable("target_table") {
        spark.sql(s"""
                     |CREATE TABLE source_table
                     |(id BIGINT, name STRING, age STRING, dt STRING, month STRING)
                     |USING DELTA
                     |PARTITIONED BY (month)
                     |""".stripMargin)

        spark.sql(s"""
                     |CREATE TABLE target_table
                     |(id BIGINT, name STRING, age BIGINT, dt STRING, month STRING)
                     |USING DELTA
                     |PARTITIONED BY (month)
                     |""".stripMargin)

        spark.sql(s"""
                     |INSERT INTO source_table VALUES
                     |(1, 'a', '10', '2025-03-12', '2025-03'),
                     |(2, 'b', '20', '2025-03-11', '2025-03');
                     |""".stripMargin)

        spark.sql(s"""
                     |INSERT INTO target_table VALUES
                     |(1, 'c', 10, '2025-03-12', '2025-03'),
                     |(3, 'c', 30, '2025-03-12', '2025-03');
                     |""".stripMargin)

        spark.sql(s"""
                     |MERGE INTO target_table tar
                     |USING source_table src
                     |ON tar.id = src.id
                     |WHEN MATCHED THEN UPDATE
                     |SET
                     |tar.id = src.id,
                     |tar.name = src.name,
                     |tar.age = src.age,
                     |tar.dt = '2025-03-12',
                     |tar.month = src.month
                     |WHEN NOT MATCHED THEN INSERT
                     |(tar.id, tar.name, tar.age, tar.dt, tar.month)
                     |VALUES
                     |(src.id, src.name, src.age, '2025-03-12', src.month)
                     |""".stripMargin)

        val df1 = runQueryAndCompare("SELECT * FROM target_table") { _ => }
        checkAnswer(
          df1,
          Seq(
            Row(1, "a", 10, "2025-03-12", "2025-03"),
            Row(3, "c", 30, "2025-03-12", "2025-03"),
            Row(2, "b", 20, "2025-03-12", "2025-03")
          )
        )
      }
    }
  }

  test("delta: filter should be offloaded with scan") {
    withSQLConf("spark.gluten.sql.columnar.scanOnly" -> "true") {
      // TODO: When RAS is enabled, filter is not offloaded along with scan.
      withSQLConf("spark.gluten.ras.enabled" -> "false") {
        withTable("delta_pf") {
          spark.sql(s"""
                       |create table test (id int, name string) using delta
                       |""".stripMargin)
          spark.sql(s"""
                       |insert into test values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                       |""".stripMargin)
          runQueryAndCompare(
            "select id from test where name > 'v1'",
            compareResult = true,
            noFallBack = false) {
            df =>
              val plan = df.queryExecution.executedPlan
              assert(plan.collect { case node: BasicScanExecTransformer => node }.nonEmpty)
              assert(plan.collect { case node: FilterExecTransformerBase => node }.nonEmpty)
          }
        }
      }
    }
  }

  // TIMESTAMP_NTZ was introduced in Spark 3.4 / Delta 2.4
  testWithMinSparkVersion(
    "delta: create table with TIMESTAMP_NTZ should fallback and return correct results",
    "3.4") {
    withTable("delta_ntz") {
      spark.sql("CREATE TABLE delta_ntz(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ) USING DELTA")
      spark.sql("""INSERT INTO delta_ntz VALUES
                  |('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456')""".stripMargin)
      val df = runQueryAndCompare("select * from delta_ntz", noFallBack = false) { _ => }
      checkAnswer(
        df,
        Row(
          "foo",
          java.sql.Timestamp.valueOf("2022-01-02 03:04:05.123456"),
          java.time.LocalDateTime.of(2022, 1, 2, 3, 4, 5, 123456000)))
    }
  }

  testWithMinSparkVersion(
    "delta: TIMESTAMP_NTZ as partition column should fallback and return correct results",
    "3.4") {
    withTable("delta_ntz_part") {
      spark.sql("""CREATE TABLE delta_ntz_part(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ)
                  |USING DELTA PARTITIONED BY (c3)""".stripMargin)
      spark.sql("""INSERT INTO delta_ntz_part VALUES
                  |('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456'),
                  |('bar','2023-06-15 10:30:00.000000','2023-06-15 10:30:00.000000')""".stripMargin)
      val df = runQueryAndCompare("select * from delta_ntz_part order by c1", noFallBack = false) {
        _ =>
      }
      checkAnswer(
        df,
        Seq(
          Row(
            "bar",
            java.sql.Timestamp.valueOf("2023-06-15 10:30:00"),
            java.time.LocalDateTime.of(2023, 6, 15, 10, 30, 0, 0)),
          Row(
            "foo",
            java.sql.Timestamp.valueOf("2022-01-02 03:04:05.123456"),
            java.time.LocalDateTime.of(2022, 1, 2, 3, 4, 5, 123456000))
        )
      )
    }
  }

  testWithMinSparkVersion(
    "delta: filter on TIMESTAMP_NTZ column should fallback and return correct results",
    "3.4") {
    withTable("delta_ntz_filter") {
      spark.sql("CREATE TABLE delta_ntz_filter(id INT, ts TIMESTAMP_NTZ) USING DELTA")
      spark.sql("""INSERT INTO delta_ntz_filter VALUES
                  |(1, '2022-01-01 00:00:00'),
                  |(2, '2023-01-01 00:00:00'),
                  |(3, '2024-01-01 00:00:00')""".stripMargin)
      val df = runQueryAndCompare(
        "select id from delta_ntz_filter where ts > '2022-06-01 00:00:00'",
        noFallBack = false) { _ => }
      checkAnswer(df, Seq(Row(2), Row(3)))
    }
  }
}
