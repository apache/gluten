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

import org.apache.gluten.extension.DeltaPostTransformRules

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkVersionUtil

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
      .set("spark.sql.ansi.enabled", "false")
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

  // Counts files Delta will read for `df`. Driven by `PreparedDeltaFileIndex.inputFiles`, which
  // is the post-pruning, post-stats-skipping file set computed by `PrepareDeltaScan`. Useful for
  // asserting that Delta's file index actually pruned, regardless of what Gluten does later.
  private def deltaInputFileCount(df: org.apache.spark.sql.DataFrame): Int =
    df.inputFiles.length

  // Counts the partition directories selected by the executed scan after Gluten's rewrite.
  // Backed by `selectedPartitions` -> `relation.location.listFiles(partitionFilters, dataFilters)`,
  // which is the exact call site of issue #10511: pre-fix, physical-named partition filters
  // could not match Delta's logical partition schema, so this returned all directories. We use
  // `getPartitionArray` rather than `getPartitions` because the latter reflects post-coalesce
  // splits (Velox may merge small files into one split, hiding the per-partition count).
  private def selectedPartitionCount(df: org.apache.spark.sql.DataFrame): Int = {
    val scan = df.queryExecution.executedPlan.collect {
      case f: DeltaScanTransformer => f
    }.head
    scan.getPartitionArray.length
  }

  // Regression for issue #10511: with column mapping, a partition column filter must prune
  // partitions correctly. Pre-fix, Gluten rewrote partition filters to physical names, which
  // broke `PreparedDeltaFileIndex.matchingFiles` and silently returned all files.
  Seq("name", "id").foreach {
    mode =>
      testWithMinSparkVersion(
        s"column mapping mode = $mode with partition filter (single partition col)",
        "3.2") {
        withTable("delta_cm_part") {
          spark.sql(s"""
                       |create table delta_cm_part (id int, name string) using delta
                       |partitioned by (id)
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          // Use multiple inserts so each value lands in its own partition directory & file.
          spark.sql("insert into delta_cm_part values (1, \"v1\")")
          spark.sql("insert into delta_cm_part values (2, \"v2\")")
          spark.sql("insert into delta_cm_part values (3, \"v3\")")

          // Equality on partition column. 1 of 3 partitions matches.
          val df1 = runQueryAndCompare("select name from delta_cm_part where id = 2") { _ => }
          checkLengthAndPlan(df1, 1)
          checkAnswer(df1, Row("v2") :: Nil)
          assert(deltaInputFileCount(df1) == 1, "Delta should prune to 1 file")
          assert(selectedPartitionCount(df1) == 1, "native scan should see 1 split")

          // Range on partition column (the exact case from the bug report).
          val df2 = runQueryAndCompare("select name from delta_cm_part where id > 2") { _ => }
          checkLengthAndPlan(df2, 1)
          checkAnswer(df2, Row("v3") :: Nil)
          assert(deltaInputFileCount(df2) == 1)
          assert(selectedPartitionCount(df2) == 1)

          // IN list on partition column. 2 of 3 partitions match.
          val df3 =
            runQueryAndCompare("select name from delta_cm_part where id in (1, 3)") { _ => }
          checkLengthAndPlan(df3, 2)
          checkAnswer(df3, Row("v1") :: Row("v3") :: Nil)
          assert(deltaInputFileCount(df3) == 2)
          assert(selectedPartitionCount(df3) == 2)

          // No filter -- baseline: all 3 partitions read.
          val dfAll = runQueryAndCompare("select name from delta_cm_part") { _ => }
          assert(deltaInputFileCount(dfAll) == 3)
          assert(selectedPartitionCount(dfAll) == 3)
        }
      }

      testWithMinSparkVersion(
        s"column mapping mode = $mode with partition filter (multi partition col)",
        "3.2") {
        withTable("delta_cm_part_multi") {
          spark.sql(s"""
                       |create table delta_cm_part_multi
                       |  (id int, region string, name string)
                       |using delta partitioned by (region, id)
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          spark.sql("insert into delta_cm_part_multi values (1, \"us\", \"v1\")")
          spark.sql("insert into delta_cm_part_multi values (2, \"us\", \"v2\")")
          spark.sql("insert into delta_cm_part_multi values (1, \"eu\", \"v3\")")
          spark.sql("insert into delta_cm_part_multi values (2, \"eu\", \"v4\")")

          val df = runQueryAndCompare(
            "select name from delta_cm_part_multi where region = 'us' and id > 1") { _ => }
          checkLengthAndPlan(df, 1)
          checkAnswer(df, Row("v2") :: Nil)
          assert(deltaInputFileCount(df) == 1, "Delta should prune to 1 file with both filters")
          assert(selectedPartitionCount(df) == 1)

          // Filter on only one of two partition columns.
          val df2 = runQueryAndCompare(
            "select name from delta_cm_part_multi where region = 'eu'") { _ => }
          checkLengthAndPlan(df2, 2)
          checkAnswer(df2, Row("v3") :: Row("v4") :: Nil)
          assert(deltaInputFileCount(df2) == 2)
          assert(selectedPartitionCount(df2) == 2)
        }
      }

      testWithMinSparkVersion(
        s"column mapping mode = $mode with partition + data filter",
        "3.2") {
        withTable("delta_cm_part_data") {
          spark.sql(s"""
                       |create table delta_cm_part_data (id int, name string, age int)
                       |using delta partitioned by (id)
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          spark.sql("insert into delta_cm_part_data values (1, \"a\", 10), (1, \"b\", 20)")
          spark.sql("insert into delta_cm_part_data values (2, \"c\", 30), (2, \"d\", 40)")
          spark.sql("insert into delta_cm_part_data values (3, \"e\", 50), (3, \"f\", 60)")

          // Combined: partition pruning to id > 1 keeps 2 files; data stats-skipping on age >= 50
          // further drops the id=2 file (max age 40 < 50). Should leave 1 file.
          val df1 = runQueryAndCompare(
            "select name from delta_cm_part_data where id > 1 and age >= 50") { _ => }
          checkLengthAndPlan(df1, 2)
          checkAnswer(df1, Row("e") :: Row("f") :: Nil)
          assert(
            deltaInputFileCount(df1) == 1,
            "partition + stats-skipping should leave 1 file out of 3")

          // Data filter alone -- file-level stats skipping should resolve column names.
          // Only the id=2 file (age 30..40) matches age = 30.
          val df2 = runQueryAndCompare(
            "select name from delta_cm_part_data where age = 30") { _ => }
          checkLengthAndPlan(df2, 1)
          checkAnswer(df2, Row("c") :: Nil)
          assert(
            deltaInputFileCount(df2) == 1,
            "stats-based file skipping should leave 1 file out of 3")
        }
      }

      testWithMinSparkVersion(
        s"column mapping mode = $mode with IS [NOT] NULL on partition col",
        "3.2") {
        withTable("delta_cm_part_null") {
          spark.sql(s"""
                       |create table delta_cm_part_null (id int, name string)
                       |using delta partitioned by (id)
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          spark.sql("insert into delta_cm_part_null values (1, \"v1\")")
          spark.sql("insert into delta_cm_part_null values (2, \"v2\")")
          spark.sql("insert into delta_cm_part_null values (cast(null as int), \"vn\")")

          val df1 = runQueryAndCompare(
            "select name from delta_cm_part_null where id is null") { _ => }
          checkAnswer(df1, Row("vn") :: Nil)
          assert(deltaInputFileCount(df1) == 1)
          assert(selectedPartitionCount(df1) == 1)

          val df2 = runQueryAndCompare(
            "select name from delta_cm_part_null where id is not null") { _ => }
          checkAnswer(df2, Row("v1") :: Row("v2") :: Nil)
          assert(deltaInputFileCount(df2) == 2)
          assert(selectedPartitionCount(df2) == 2)
        }
      }

      testWithMinSparkVersion(
        s"column mapping mode = $mode partition filter survives column rename",
        "3.2") {
        withTable("delta_cm_part_rename") {
          spark.sql(s"""
                       |create table delta_cm_part_rename (id int, name string)
                       |using delta partitioned by (id)
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          spark.sql("insert into delta_cm_part_rename values (1, \"v1\")")
          spark.sql("insert into delta_cm_part_rename values (2, \"v2\")")
          spark.sql("insert into delta_cm_part_rename values (3, \"v3\")")
          // Rename the partition column. The physical name in storage stays the same; only the
          // logical name changes, so the logical-name-based partition filter must still resolve.
          spark.sql("alter table delta_cm_part_rename rename column id to pid")

          val df = runQueryAndCompare(
            "select name from delta_cm_part_rename where pid >= 2") { _ => }
          checkLengthAndPlan(df, 2)
          checkAnswer(df, Row("v2") :: Row("v3") :: Nil)
          assert(deltaInputFileCount(df) == 2)
          assert(selectedPartitionCount(df) == 2)
        }
      }

      testWithMinSparkVersion(
        s"column mapping mode = $mode data column rename + filter (file skipping)",
        "3.2") {
        withTable("delta_cm_data_rename") {
          spark.sql(s"""
                       |create table delta_cm_data_rename (id int, age int, name string)
                       |using delta
                       |tblproperties ("delta.columnMapping.mode" = "$mode")
                       |""".stripMargin)
          spark.sql("insert into delta_cm_data_rename values (1, 10, \"a\")")
          spark.sql("insert into delta_cm_data_rename values (2, 20, \"b\")")
          spark.sql("insert into delta_cm_data_rename values (3, 30, \"c\")")
          // Rename a data column. Filter pushdown must still match physical column in parquet,
          // and Delta's stats-based skipping must still resolve the logical name `years`.
          spark.sql("alter table delta_cm_data_rename rename column age to years")

          val df = runQueryAndCompare(
            "select name from delta_cm_data_rename where years = 20") { _ => }
          checkLengthAndPlan(df, 1)
          checkAnswer(df, Row("b") :: Nil)
          assert(
            deltaInputFileCount(df) == 1,
            "stats skipping on renamed data column should leave 1 file")
        }
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
        val df = spark.read.format("delta").load(path)
        val executedPlan = df.queryExecution.executedPlan
        if (SparkVersionUtil.gteSpark35) {
          assert(executedPlan.collect { case _: DeltaScanTransformer => true }.nonEmpty)
          val planText = executedPlan.toString()
          assert(!planText.contains("__delta_internal_is_row_deleted"))
          assert(!planText.contains("__delta_internal_row_index"))
        } else {
          assert(executedPlan.collect { case _: DeltaScanTransformer => true }.isEmpty)
        }
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
      withTable("delta_pf") {
        spark.sql(s"""
                     |create table delta_pf (id int, name string) using delta
                     |""".stripMargin)
        spark.sql(s"""
                     |insert into delta_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                     |""".stripMargin)
        runQueryAndCompare(
          "select id from delta_pf where name > 'v1'",
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

  // TIMESTAMP_NTZ was introduced in Spark 3.4 / Delta 2.4
  testWithMinSparkVersion(
    "delta: create table with TIMESTAMP_NTZ and return correct results",
    "3.4") {
    withTable("delta_ntz") {
      spark.sql("CREATE TABLE delta_ntz(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ) USING DELTA")
      spark.sql("""INSERT INTO delta_ntz VALUES
                  |('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456')""".stripMargin)
      val df = runQueryAndCompare("select * from delta_ntz") { _ => }
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

  testWithMinSparkVersion(
    "merge with column mapping handles struct field metadata correctly",
    "3.4") {
    withTable("merge_struct_source", "merge_struct_target") {
      spark.sql("""
                  |CREATE TABLE merge_struct_target(
                  |  key INT NOT NULL,
                  |  value INT NOT NULL,
                  |  cstruct STRUCT<foo: INT>)
                  |USING DELTA
                  |TBLPROPERTIES (
                  |  'delta.minReaderVersion' = '2',
                  |  'delta.minWriterVersion' = '5',
                  |  'delta.columnMapping.mode' = 'name')
        """.stripMargin)
      spark.sql("INSERT INTO merge_struct_target VALUES (0, 0, null)")
      spark.sql("INSERT INTO merge_struct_target VALUES (100, 100, named_struct('foo', 42))")

      spark.sql(
        "CREATE TABLE merge_struct_source (key INT NOT NULL, value INT NOT NULL) USING DELTA")
      spark.sql("INSERT INTO merge_struct_source VALUES (1, 1)")

      // MERGE with updateNotMatched to test CaseWhen else branch
      spark.sql("""
                  |MERGE INTO merge_struct_target AS target
                  |USING merge_struct_source AS source
                  |ON source.key = target.key
                  |WHEN MATCHED THEN
                  |  UPDATE SET target.value = source.value
                  |WHEN NOT MATCHED BY SOURCE AND target.key = 100 THEN
                  |  UPDATE SET target.value = 22
        """.stripMargin)

      val df = runQueryAndCompare(
        "SELECT key, value, cstruct FROM merge_struct_target ORDER BY key") { _ => }
      checkAnswer(df, Row(0, 0, null) :: Row(100, 22, Row(42)) :: Nil)
    }
  }

  testWithMinSparkVersion(
    "merge with column mapping handles array-of-struct field metadata correctly",
    "3.4") {
    withTable("merge_arraystruct_source", "merge_arraystruct_target") {
      spark.sql("""
                  |CREATE TABLE merge_arraystruct_target(
                  |  key INT NOT NULL,
                  |  tags ARRAY<STRUCT<label: STRING, score: INT>>)
                  |USING DELTA
                  |TBLPROPERTIES (
                  |  'delta.minReaderVersion' = '2',
                  |  'delta.minWriterVersion' = '5',
                  |  'delta.columnMapping.mode' = 'name')
        """.stripMargin)
      spark.sql("INSERT INTO merge_arraystruct_target VALUES (0, null)")
      spark.sql(
        "INSERT INTO merge_arraystruct_target VALUES " +
          "(100, array(named_struct('label', 'a', 'score', 10)))")
      spark.sql("CREATE TABLE merge_arraystruct_source (key INT NOT NULL) USING DELTA")
      spark.sql("INSERT INTO merge_arraystruct_source VALUES (1)")
      // MERGE that leaves the array-of-struct column unchanged via CaseWhen
      spark.sql("""
                  |MERGE INTO merge_arraystruct_target AS target
                  |USING merge_arraystruct_source AS source
                  |ON source.key = target.key
                  |WHEN NOT MATCHED BY SOURCE AND target.key = 100 THEN
                  |  UPDATE SET target.key = 101
        """.stripMargin)
      val df = runQueryAndCompare("SELECT key, tags FROM merge_arraystruct_target ORDER BY key") {
        _ =>
      }
      checkAnswer(df, Row(0, null) :: Row(101, Seq(Row("a", 10))) :: Nil)
    }
  }

  testWithMinSparkVersion(
    "merge with column mapping handles map-of-struct field metadata correctly",
    "3.4") {
    withTable("merge_mapstruct_source", "merge_mapstruct_target") {
      spark.sql("""
                  |CREATE TABLE merge_mapstruct_target(
                  |  key INT NOT NULL,
                  |  props MAP<STRING, STRUCT<val: INT>>)
                  |USING DELTA
                  |TBLPROPERTIES (
                  |  'delta.minReaderVersion' = '2',
                  |  'delta.minWriterVersion' = '5',
                  |  'delta.columnMapping.mode' = 'name')
        """.stripMargin)
      spark.sql("INSERT INTO merge_mapstruct_target VALUES (0, null)")
      spark.sql(
        "INSERT INTO merge_mapstruct_target VALUES " +
          "(100, map('x', named_struct('val', 99)))")
      spark.sql("CREATE TABLE merge_mapstruct_source (key INT NOT NULL) USING DELTA")
      spark.sql("INSERT INTO merge_mapstruct_source VALUES (1)")
      // MERGE that leaves the map-of-struct column unchanged via CaseWhen
      spark.sql("""
                  |MERGE INTO merge_mapstruct_target AS target
                  |USING merge_mapstruct_source AS source
                  |ON source.key = target.key
                  |WHEN NOT MATCHED BY SOURCE AND target.key = 100 THEN
                  |  UPDATE SET target.key = 101
        """.stripMargin)
      val df = runQueryAndCompare("SELECT key, props FROM merge_mapstruct_target ORDER BY key") {
        _ =>
      }
      checkAnswer(df, Row(0, null) :: Row(101, Map("x" -> Row(99))) :: Nil)
    }
  }

  testWithMinSparkVersion(
    "merge with column mapping handles nested struct-within-struct field metadata correctly",
    "3.4") {
    withTable("merge_nestedstruct_source", "merge_nestedstruct_target") {
      spark.sql("""
                  |CREATE TABLE merge_nestedstruct_target(
                  |  key INT NOT NULL,
                  |  nested STRUCT<outer_val: STRUCT<inner_val: INT>>)
                  |USING DELTA
                  |TBLPROPERTIES (
                  |  'delta.minReaderVersion' = '2',
                  |  'delta.minWriterVersion' = '5',
                  |  'delta.columnMapping.mode' = 'name')
        """.stripMargin)
      spark.sql("INSERT INTO merge_nestedstruct_target VALUES (0, null)")
      spark.sql(
        "INSERT INTO merge_nestedstruct_target VALUES " +
          "(100, named_struct('outer_val', named_struct('inner_val', 42)))")
      spark.sql("CREATE TABLE merge_nestedstruct_source (key INT NOT NULL) USING DELTA")
      spark.sql("INSERT INTO merge_nestedstruct_source VALUES (1)")
      spark.sql("""
                  |MERGE INTO merge_nestedstruct_target AS target
                  |USING merge_nestedstruct_source AS source
                  |ON source.key = target.key
                  |WHEN NOT MATCHED BY SOURCE AND target.key = 100 THEN
                  |  UPDATE SET target.key = 101
        """.stripMargin)
      val df = runQueryAndCompare(
        "SELECT key, nested FROM merge_nestedstruct_target ORDER BY key") { _ => }
      checkAnswer(df, Row(0, null) :: Row(101, Row(Row(42))) :: Nil)
    }
  }

  testWithMinSparkVersion(
    "merge with column mapping handles array with null struct elements correctly",
    "3.4") {
    withTable("merge_arraynull_source", "merge_arraynull_target") {
      spark.sql("""
                  |CREATE TABLE merge_arraynull_target(
                  |  key INT NOT NULL,
                  |  items ARRAY<STRUCT<name: STRING, qty: INT>>)
                  |USING DELTA
                  |TBLPROPERTIES (
                  |  'delta.minReaderVersion' = '2',
                  |  'delta.minWriterVersion' = '5',
                  |  'delta.columnMapping.mode' = 'name')
        """.stripMargin)
      spark.sql("INSERT INTO merge_arraynull_target VALUES (0, null)")
      spark.sql(
        "INSERT INTO merge_arraynull_target VALUES " +
          "(100, array(named_struct('name', 'a', 'qty', 1), null))")
      spark.sql("CREATE TABLE merge_arraynull_source (key INT NOT NULL) USING DELTA")
      spark.sql("INSERT INTO merge_arraynull_source VALUES (1)")
      spark.sql("""
                  |MERGE INTO merge_arraynull_target AS target
                  |USING merge_arraynull_source AS source
                  |ON source.key = target.key
                  |WHEN NOT MATCHED BY SOURCE AND target.key = 100 THEN
                  |  UPDATE SET target.key = 101
        """.stripMargin)
      val df = runQueryAndCompare("SELECT key, items FROM merge_arraynull_target ORDER BY key") {
        _ =>
      }
      checkAnswer(df, Row(0, null) :: Row(101, Seq(Row("a", 1), null)) :: Nil)
    }
  }

  test("post-transform rules are no-op on non-Delta plans") {
    withTempPath {
      p =>
        val path = p.getCanonicalPath
        spark.range(100).selectExpr("id", "id * 2 as value").write.parquet(path)
        val df = spark.read.parquet(path)
        val plan = df.queryExecution.executedPlan

        // Apply only the Delta-specific rules (skip RemoveTransitions which is generic)
        val deltaRules = DeltaPostTransformRules.rules.tail
        val transformed = deltaRules.foldLeft(plan)((p, rule) => rule(p))
        // No DeltaScanTransformer in the plan, so rules should return the same object (early-exit)
        assert(transformed eq plan, "Delta rules should return the exact same plan instance")
    }
  }

  test("Delta scan is offloaded to DeltaScanTransformer") {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath
        Seq(1, 2, 3, 4, 5).toDF("id").coalesce(1).write.format("delta").save(path)
        val df = spark.read.format("delta").load(path)
        val plan = df.queryExecution.executedPlan

        // Delta scan should be offloaded to DeltaScanTransformer
        val deltaScans = plan.collect { case s: DeltaScanTransformer => s }
        assert(deltaScans.nonEmpty, "Delta plan should contain DeltaScanTransformer")
    }
  }

  test("scanFilters returns consistent results on repeated access") {
    withTempPath {
      p =>
        import testImplicits._
        val path = p.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
        val df = spark.read.format("delta").load(path).where("id > 1")
        val plan = df.queryExecution.executedPlan
        val scans = plan.collect { case s: DeltaScanTransformer => s }

        assert(scans.nonEmpty, "Delta plan should contain DeltaScanTransformer")
        val scan = scans.head
        // scanFilters is now a lazy val; repeated calls should return the same instance
        val first = scan.scanFilters
        val second = scan.scanFilters
        val third = scan.scanFilters
        assert(first eq second, "scanFilters should return the same cached instance")
        assert(second eq third, "scanFilters should return the same cached instance")
    }
  }
}
