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
import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveTableScanExecTransformer}
import org.apache.spark.sql.hive.client.HiveClient

import scala.collection.immutable.Seq

class GlutenHiveSQLQuerySuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  testGluten("hive orc scan") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name STRING, favorite_color STRING)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test_1', 'red')")
      val df = spark.sql("select * from test_orc")
      checkAnswer(df, Seq(Row("test_1", "red")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  // copy from GLUTEN-4796, which only added to spark33
  testGluten("Add orc char type validation") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name char(10), id int)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test', 1)")
    }

    def testExecPlan(
        convertMetastoreOrc: String,
        charTypeFallbackEnabled: String,
        shouldFindTransformer: Boolean,
        transformerClass: Class[_ <: SparkPlan]
    ): Unit = {

      withSQLConf(
        "spark.sql.hive.convertMetastoreOrc" -> convertMetastoreOrc,
        GlutenConfig.VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK.key -> charTypeFallbackEnabled
      ) {
        val queries = Seq("select id from test_orc", "select name, id from test_orc")

        queries.foreach {
          query =>
            val executedPlan = getExecutedPlan(spark.sql(query))
            val planCondition = executedPlan.exists(_.find(transformerClass.isInstance).isDefined)

            if (shouldFindTransformer) {
              assert(planCondition)
            } else {
              assert(!planCondition)
            }
        }
      }
    }

    testExecPlan(
      "false",
      "true",
      shouldFindTransformer = false,
      classOf[HiveTableScanExecTransformer])
    testExecPlan(
      "false",
      "false",
      shouldFindTransformer = true,
      classOf[HiveTableScanExecTransformer])

    testExecPlan(
      "true",
      "true",
      shouldFindTransformer = false,
      classOf[FileSourceScanExecTransformer])
    testExecPlan(
      "true",
      "false",
      shouldFindTransformer = true,
      classOf[FileSourceScanExecTransformer])
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("orc.force.positional.evolution maps Hive ORC columns by position") {
    val hiveClient: HiveClient =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      withTempDir {
        dir =>
          val orcLoc = s"file:///$dir/test_orc_pos"
          withTable("test_orc_pos", "test_orc_pos_renamed") {
            // Write ORC files whose physical column names are c1, c2 (c1 = 1, c2 = 2).
            hiveClient.runSqlHive(
              s"create table test_orc_pos(c1 int, c2 int) stored as orc location '$orcLoc'")
            hiveClient.runSqlHive("insert into test_orc_pos select 1, 2")

            // A second table over the SAME files but with mismatched column names (x, y).
            // By name, x/y are not present in the files; only position mapping can read them.
            hiveClient.runSqlHive(
              s"create table test_orc_pos_renamed(x int, y int) stored as orc location '$orcLoc'")

            // orc.force.positional.evolution=true => read by position: x -> c1 (=1), y -> c2 (=2).
            withSQLConf("spark.hadoop.orc.force.positional.evolution" -> "true") {
              val df = sql("select x, y from test_orc_pos_renamed")
              checkAnswer(df, Seq(Row(1, 2)))
              checkOperatorMatch[HiveTableScanExecTransformer](df)
            }
          }
      }
    }
  }

  testGluten("ORC positional and Parquet name mapping can coexist in one Velox query") {
    val hiveClient: HiveClient =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

    withSQLConf(
      "spark.sql.hive.convertMetastoreOrc" -> "false",
      "spark.sql.hive.convertMetastoreParquet" -> "false",
      "spark.hadoop.orc.force.positional.evolution" -> "true"
    ) {
      withTempDir {
        dir =>
          val orcLoc = dir.toPath.resolve("test_orc_pos").toUri.toString
          val parquetLoc = dir.toPath.resolve("test_parquet_name").toUri.toString
          withTable(
            "test_orc_pos",
            "test_orc_pos_renamed",
            "test_parquet_name",
            "test_parquet_name_reordered") {
            hiveClient.runSqlHive(
              s"create table test_orc_pos(c1 int, c2 int) stored as orc location '$orcLoc'")
            hiveClient.runSqlHive("insert into test_orc_pos select 1, 2")
            hiveClient.runSqlHive(
              s"create table test_orc_pos_renamed(x int, y int) stored as orc location '$orcLoc'")

            hiveClient.runSqlHive(
              s"create table test_parquet_name(p int, q int) stored as parquet " +
                s"location '$parquetLoc'")
            hiveClient.runSqlHive("insert into test_parquet_name select 3, 4")
            hiveClient.runSqlHive(
              s"create table test_parquet_name_reordered(q int, p int) stored as parquet " +
                s"location '$parquetLoc'")

            val df = sql(
              "select o.x, o.y, p.q, p.p from test_orc_pos_renamed o " +
                "cross join test_parquet_name_reordered p")
            checkAnswer(df, Seq(Row(1, 2, 4, 3)))
            checkOperatorMatch[HiveTableScanExecTransformer](df)
          }
      }
    }
  }

  testGluten(
    "GLUTEN: Hive ORC files with _col* names read by position without positional flag") {
    // Regression for the case where two ORC tables must use OPPOSITE column
    // mapping modes in the same query: one with real column names (by name) and
    // one written by old Hive with placeholder _col* names (by position). The
    // native reader must decide the mode per file (matching vanilla Spark's
    // OrcUtils.requestedColumnIds), so a _col* file reads correctly even though
    // orc.force.positional.evolution is NOT set (ORC is read by name by
    // default). Without the fix the _col* columns would read back as NULL.
    val hiveClient: HiveClient =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      withTempDir {
        dir =>
          val colStarLoc = s"file:///$dir/test_orc_colstar"
          val namedLoc = s"file:///$dir/test_orc_named"
          withTable("test_orc_colstar", "test_orc_colstar_renamed", "test_orc_named") {
            // Naming the columns literally _col0/_col1 guarantees the physical
            // ORC field names are placeholders, independent of the Hive
            // version (mirrors Spark's SPARK-34897 setup).
            hiveClient.runSqlHive(
              s"create table test_orc_colstar(`_col0` int, `_col1` string) " +
                s"stored as orc location '$colStarLoc'")
            hiveClient.runSqlHive("insert into test_orc_colstar select 7, 'a'")

            // A second table over the SAME files but with real names. By name,
            // id/name are absent from the _col* files; only position mapping
            // can read them -- and it must happen WITHOUT the positional flag.
            hiveClient.runSqlHive(
              s"create table test_orc_colstar_renamed(id int, name string) " +
                s"stored as orc location '$colStarLoc'")

            // A table with real physical column names, read by name.
            hiveClient.runSqlHive(
              s"create table test_orc_named(uid int, label string) " +
                s"stored as orc location '$namedLoc'")
            hiveClient.runSqlHive("insert into test_orc_named select 7, 'b'")

            // No positional flag set. The _col* table read via real names must
            // still return the values (positional fallback).
            val colStar = sql("select id, name from test_orc_colstar_renamed")
            checkAnswer(colStar, Seq(Row(7, "a")))
            checkOperatorMatch[HiveTableScanExecTransformer](colStar)

            // The real-name table still reads correctly by name in the same
            // session (opposite mapping mode).
            val named = sql("select uid, label from test_orc_named")
            checkAnswer(named, Seq(Row(7, "b")))
            checkOperatorMatch[HiveTableScanExecTransformer](named)

            // Both in one query (the original failure folded the join to an
            // empty LocalTableScan). The join must return a non-empty result.
            val joined = sql(
              "select c.name, n.label from test_orc_colstar_renamed c " +
                "join test_orc_named n on c.id = n.uid")
            checkAnswer(joined, Seq(Row("a", "b")))
          }
      }
    }
  }

  test("GLUTEN-11062: Supports mixed input format for partitioned Hive table") {
    val hiveClient: HiveClient =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

    withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
      withTempDir {
        dir =>
          val parquetLoc = s"file:///$dir/test_parquet"
          val orcLoc = s"file:///$dir/test_orc"
          withTable("test_parquet", "test_orc") {
            hiveClient.runSqlHive(s"""create table test_parquet(id int)
                 partitioned by(pid int)
                 stored as parquet location '$parquetLoc'
                 """.stripMargin)
            hiveClient.runSqlHive("insert into test_parquet partition(pid=1) select 2")
            hiveClient.runSqlHive(s"""create table test_orc(id int)
                 partitioned by(pid int)
                 stored as orc location '$orcLoc'
                 """.stripMargin)
            hiveClient.runSqlHive("insert into test_orc partition(pid=2) select 2")
            hiveClient.runSqlHive(
              s"alter table test_parquet add partition (pid=2) location '$orcLoc/pid=2'")
            hiveClient.runSqlHive("alter table test_parquet partition(pid=2) SET FILEFORMAT orc")
            val df = sql("select pid, id from test_parquet order by pid")
            checkAnswer(df, Seq(Row(1, 2), Row(2, 2)))
            checkOperatorMatch[HiveTableScanExecTransformer](df)
          }
      }
    }
  }

  testGluten("avoid unnecessary filter binding for subfield during scan") {
    withSQLConf(
      "spark.sql.hive.convertMetastoreParquet" -> "false") {
      sql("DROP TABLE IF EXISTS test_subfield")
      sql(
        "CREATE TABLE test_subfield (name STRING, favorite_color STRING," +
          " label STRUCT<label_1:STRING, label_2:STRING>) USING hive OPTIONS(fileFormat 'parquet')")
      sql(
        "INSERT INTO test_subfield VALUES('test_1', 'red', named_struct('label_1', 'label-a'," +
          "'label_2', 'label-b'))")
      val df = spark.sql("select * from test_subfield where name='test_1'")
      checkAnswer(df, Seq(Row("test_1", "red", Row("label-a", "label-b"))))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_subfield"),
      ignoreIfNotExists = true,
      purge = false)
  }
}
