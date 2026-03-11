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
package org.apache.spark.sql.sources

import org.apache.gluten.GlutenColumnarWriteTestSupport
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.execution.SortExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.{CommandResultExec, GlutenImplicits, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.hadoop.fs.{Path, RawLocalFileSystem}

import java.io.{File, IOException}

class GlutenInsertSuite
  extends InsertSuite
  with GlutenSQLTestsBaseTrait
  with AdaptiveSparkPlanHelper
  with GlutenColumnarWriteTestSupport {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.leafNodeDefaultParallelism", "1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("""
                |CREATE TABLE source USING PARQUET AS
                |SELECT cast(id as int) as c1, cast(id % 5 as string) c2 FROM range(100)
                |""".stripMargin)

    spark.sql("INSERT INTO TABLE source SELECT 0, null")
    spark.sql("INSERT INTO TABLE source SELECT 0, ''")
  }

  override def afterAll(): Unit = {
    spark.sql("DROP TABLE source")
    super.afterAll()
  }

  private def checkWriteFilesAndGetChild(df: DataFrame): (SparkPlan, SparkPlan) = {
    val writeFiles = stripAQEPlan(
      df.queryExecution.executedPlan
        .asInstanceOf[CommandResultExec]
        .commandPhysicalPlan).children.head
    val child = checkWriteFilesAndGetChild(writeFiles)
    (writeFiles, child)
  }

  testGluten("insert partition table") {
    withTable("pt", "pt2") {
      spark.sql("CREATE TABLE pt (c1 int, c2 string) USING PARQUET PARTITIONED BY (pt string)")
      spark.sql("CREATE TABLE pt2 (c1 int, c2 string) USING PARQUET PARTITIONED BY (pt string)")

      var taskMetrics: OutputMetrics = null
      val taskListener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          taskMetrics = taskEnd.taskMetrics.outputMetrics
        }
      }

      var sqlMetrics: Map[String, SQLMetric] = null
      val queryListener = new QueryExecutionListener {
        override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
        override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
          qe.executedPlan match {
            case dataWritingCommandExec: DataWritingCommandExec =>
              sqlMetrics = dataWritingCommandExec.cmd.metrics
            case _ =>
          }
        }
      }
      spark.sparkContext.addSparkListener(taskListener)
      spark.listenerManager.register(queryListener)
      try {
        val df =
          spark.sql("INSERT INTO TABLE pt partition(pt='a') SELECT * FROM VALUES(1, 'a'),(2, 'b')")
        spark.sparkContext.listenerBus.waitUntilEmpty()
        checkWriteFilesAndGetChild(df)

        assert(taskMetrics.bytesWritten > 0)
        assert(taskMetrics.recordsWritten == 2)
        assert(sqlMetrics("numParts").value == 1)
        assert(sqlMetrics("numOutputRows").value == 2)
        assert(sqlMetrics("numOutputBytes").value > 0)
        assert(sqlMetrics("numFiles").value == 1)

        checkAnswer(spark.sql("SELECT * FROM pt"), Row(1, "a", "a") :: Row(2, "b", "a") :: Nil)
      } finally {
        spark.sparkContext.removeSparkListener(taskListener)
        spark.listenerManager.unregister(queryListener)
      }

      // check no fallback nodes
      val df2 = spark.sql("INSERT INTO TABLE pt2 SELECT * FROM pt")
      checkWriteFilesAndGetChild(df2)
      val fallbackSummary = GlutenImplicits
        .collectQueryExecutionFallbackSummary(spark, df2.queryExecution)
      assert(fallbackSummary.numFallbackNodes == 0)
    }
  }

  ignoreGluten("Cleanup staging files if job failed") {
    // Using a unique table name in this test. Sometimes, the table is not removed for some unknown
    // reason, which can cause test failure (location already exists) if other following tests have
    // the same table name.
    withTable("tbl") {
      spark.sql("CREATE TABLE tbl (c1 int, c2 string) USING PARQUET")
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl"))
      assert(new File(table.location).list().length == 0)

      intercept[Exception] {
        spark.sql(
          """
            |INSERT INTO TABLE tbl
            |SELECT id, assert_true(SPARK_PARTITION_ID() = 1) FROM range(1, 3, 1, 2)
            |""".stripMargin
        )
      }
      assert(new File(table.location).list().length == 0)
    }
  }

  private def validateDynamicPartitionWrite(
      df: DataFrame,
      expectedPartitionNames: Set[String]): Unit = {
    val (writeFiles, writeChild) = checkWriteFilesAndGetChild(df)
    assert(
      writeFiles
        .find(_.isInstanceOf[SortExecTransformer])
        .isEmpty)
    // all operators should be transformed
    assert(writeChild.find(!_.isInstanceOf[GlutenPlan]).isEmpty)

    val parts = spark.sessionState.catalog.listPartitionNames(TableIdentifier("pt")).toSet
    assert(parts == expectedPartitionNames)
  }

  testGluten("remove v1writes sort and project") {
    // Only string type has empty2null expression
    withTable("pt") {
      spark.sql("CREATE TABLE pt (c1 int) USING PARQUET PARTITIONED BY(p string)")

      val df = spark.sql(s"""
                            |INSERT OVERWRITE TABLE pt PARTITION(p)
                            |SELECT c1, c2 as p FROM source
                            |""".stripMargin)
      validateDynamicPartitionWrite(
        df,
        Set("p=0", "p=1", "p=2", "p=3", "p=4", "p=__HIVE_DEFAULT_PARTITION__"))

      // The partition column should never be empty
      checkAnswer(
        spark.sql("SELECT * FROM pt"),
        spark.sql("SELECT c1, if(c2 = '', null, c2) FROM source"))
    }
  }

  testGluten("remove v1writes sort") {
    // __HIVE_DEFAULT_PARTITION__ for other types are covered by other tests.
    Seq(
      ("p boolean", "coalesce(cast(c2 as boolean), false)", Set("p=false", "p=true")),
      ("p short", "coalesce(cast(c2 as short), 0s)", Set("p=0", "p=1", "p=2", "p=3", "p=4")),
      ("p int", "coalesce(cast(c2 as int), 0)", Set("p=0", "p=1", "p=2", "p=3", "p=4")),
      ("p long", "coalesce(cast(c2 as long), 0l)", Set("p=0", "p=1", "p=2", "p=3", "p=4")),
      (
        "p date",
        "if(c2 < 3, date '2023-01-01', date '2024-01-01')",
        Set("p=2023-01-01", "p=2024-01-01")),
      (
        "p int, p2 string",
        "if(cast(c2 as int) < 2, 0, 1), c2",
        Set(
          "p=0/p2=1",
          "p=0/p2=0",
          "p=1/p2=__HIVE_DEFAULT_PARTITION__",
          "p=1/p2=2",
          "p=1/p2=3",
          "p=1/p2=4"
        ))
    )
      .foreach {
        case (partitionType, partitionExpr, expectedPartitionNames) =>
          withTable("pt") {
            spark.sql(s"CREATE TABLE pt (c1 int) USING PARQUET PARTITIONED BY($partitionType)")

            val df = spark.sql(s"""
                                  |INSERT OVERWRITE TABLE pt
                                  |SELECT c1, $partitionExpr FROM source
                                  |""".stripMargin)
            validateDynamicPartitionWrite(df, expectedPartitionNames)
          }
      }
  }

  testGluten("do not remove non-v1writes sort and project") {
    withTable("t") {
      spark.sql("CREATE TABLE t (c1 int, c2 string) USING PARQUET")

      val df = spark.sql("INSERT OVERWRITE TABLE t SELECT c1, c2 FROM source SORT BY c1")
      val (writeFiles, _) = checkWriteFilesAndGetChild(df)
      assert(writeFiles.find(x => x.isInstanceOf[SortExecTransformer]).isDefined)
      checkAnswer(spark.sql("SELECT * FROM t"), spark.sql("SELECT * FROM source SORT BY c1"))
    }
  }

  testGluten("SPARK-35106: Throw exception when rename custom partition paths returns false") {
    withSQLConf(
      "fs.file.impl" -> classOf[
        GlutenRenameFromSparkStagingToFinalDirAlwaysTurnsFalseFilesystem].getName,
      "fs.file.impl.disable.cache" -> "true") {
      withTempPath {
        path =>
          withTable("t") {
            sql("""
                  |create table t(i int, part1 int, part2 int) using parquet
                  |partitioned by (part1, part2)
        """.stripMargin)

            sql(s"alter table t add partition(part1=1, part2=1) location '${path.getAbsolutePath}'")

            val e = intercept[IOException] {
              sql(s"insert into t partition(part1=1, part2=1) select 1")
            }
            assert(e.getMessage.contains("Failed to rename"))
          }
      }
    }
  }

  testGluten("Do not fallback write files if output columns contain Spark internal metadata") {
    withTable("t1", "t2") {
      spark.sql("CREATE TABLE t1 USING PARQUET AS SELECT id as c1, id % 3 as c2 FROM range(10)")
      spark.sql("CREATE TABLE t2 (c1 long, c2 long) USING PARQUET")
      val df = spark.sql("INSERT INTO TABLE t2 SELECT c2, count(*) FROM t1 GROUP BY c2")
      checkWriteFilesAndGetChild(df)
    }
  }

  testGluten("Add metadata white list to allow native write files") {
    withTable("t1", "t2") {
      spark.sql("""
                  |CREATE TABLE t1 (c1 long comment 'data column1', c2 long comment 'data column2')
                  |USING PARQUET
                  |""".stripMargin)
      spark.sql("INSERT INTO TABLE t1 VALUES(1, 1),(2, 2)")
      spark.sql("CREATE TABLE t2 (c1 long, c2 long) USING PARQUET")
      val df = spark.sql("INSERT INTO TABLE t2 SELECT * FROM t1")
      checkWriteFilesAndGetChild(df)
    }
  }

  testGluten("INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them") {
    import testImplicits._
    case class Config(sqlConf: Option[(String, String)], useDataFrames: Boolean = false)
    def runTest(dataSource: String, config: Config): Unit = {
      def insertIntoT(): Unit = {
        sql("insert into t(a, i) values('xyz', 42)")
      }
      def withTableT(f: => Unit): Unit = {
        sql(s"create table t(a string, i int) using $dataSource")
        withTable("t")(f)
      }
      // Positive tests:
      // Adding a column with a valid default value into a table containing existing data
      // returns null while it works successfully for newly added rows in Velox.
      withTableT {
        sql("alter table t add column (s string default concat('abc', 'def'))")
        insertIntoT()
        checkAnswer(spark.table("t"), Row("xyz", 42, "abcdef"))
        checkAnswer(sql("select i, s from t"), Row(42, "abcdef"))
        // Now alter the column to change the default value.
        // This still returns the previous value, not the new value.
        sql("alter table t alter column s set default concat('ghi', 'jkl')")
        checkAnswer(sql("select i, s from t"), Row(42, "abcdef"))
      }
      // Adding a column with a default value and then inserting explicit NULL values works.
      // Querying data back from the table differentiates between the explicit NULL values and
      // default values.
      withTableT {
        sql("alter table t add column (s string default concat('abc', 'def'))")
        insertIntoT()
        if (config.useDataFrames) {
          Seq((null, null, null)).toDF.write.insertInto("t")
        } else {
          sql("insert into t values(null, null, null)")
        }

        checkAnswer(spark.table("t"), Seq(Row("xyz", 42, "abcdef"), Row(null, null, null)))
        checkAnswer(sql("select i, s from t"), Seq(Row(42, "abcdef"), Row(null, null)))
      }
      // Adding two columns where only the first has a valid default value works successfully.
      // Querying data from the altered table returns the default value as well as NULL for the
      // second column.+
      withTableT {
        sql("alter table t add column (s string default concat('abc', 'def'))")
        insertIntoT()
        sql("alter table t add column (x string)")
        checkAnswer(spark.table("t"), Row("xyz", 42, "abcdef", null))
        checkAnswer(sql("select i, s, x from t"), Row(42, "abcdef", null))
      }
      // Test other supported data types.
      withTableT {
        sql(
          "alter table t add columns (" +
            "s boolean default true, " +
            "t byte default cast(null as byte), " +
            "u short default cast(42 as short), " +
            "v float default 0, " +
            "w double default 0, " +
            "x date default cast('2021-01-02' as date), " +
            "y timestamp default cast('2021-01-02 01:01:01' as timestamp), " +
            "z timestamp_ntz default cast('2021-01-02 01:01:01' as timestamp_ntz), " +
            "a1 timestamp_ltz default cast('2021-01-02 01:01:01' as timestamp_ltz), " +
            "a2 decimal(5, 2) default 123.45," +
            "a3 bigint default 43," +
            "a4 smallint default cast(5 as smallint)," +
            "a5 tinyint default cast(6 as tinyint))")
        insertIntoT()
        // Manually inspect the result row values rather than using the 'checkAnswer' helper method
        // in order to ensure the values' correctness while avoiding minor type incompatibilities.
        val result: Array[Row] =
          sql("select s, t, u, v, w, x, y, z, a1, a2, a3, a4, a5 from t").collect()
        for (row <- result) {
          assert(row.length == 13)
          assert(row(0) == true)
          assert(row(1) == null)
          assert(row(2) == 42)
          assert(row(3) == 0.0f)
          assert(row(4) == 0.0d)
          assert(row(5).toString == "2021-01-02")
          assert(row(6).toString == "2021-01-02 01:01:01.0")
          assert(row(7).toString.startsWith("2021-01-02"))
          assert(row(8).toString == "2021-01-02 01:01:01.0")
          assert(row(9).toString == "123.45")
          assert(row(10) == 43L)
          assert(row(11) == 5)
          assert(row(12) == 6)
        }
      }
    }

    // This represents one test configuration over a data source.
    case class TestCase(dataSource: String, configs: Seq[Config])
    // Run the test several times using each configuration.
    Seq(
      TestCase(
        dataSource = "csv",
        Seq(Config(None), Config(Some(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "false")))),
      TestCase(
        dataSource = "json",
        Seq(Config(None), Config(Some(SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key -> "false")))),
      TestCase(
        dataSource = "orc",
        Seq(Config(None), Config(Some(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false")))),
      TestCase(
        dataSource = "parquet",
        Seq(Config(None), Config(Some(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false"))))
    ).foreach {
      testCase: TestCase =>
        testCase.configs.foreach {
          config: Config =>
            // Run the test twice, once using SQL for the INSERT operations
            // and again using DataFrames.
            for (useDataFrames <- Seq(false, true)) {
              config.sqlConf
                .map {
                  kv: (String, String) =>
                    withSQLConf(kv) {
                      // Run the test with the pair of custom SQLConf values.
                      runTest(testCase.dataSource, config.copy(useDataFrames = useDataFrames))
                    }
                }
                .getOrElse {
                  // Run the test with default settings.
                  runTest(testCase.dataSource, config.copy(useDataFrames = useDataFrames))
                }
            }
        }
    }
  }

  // TODO: fix in Spark-4.0
  ignoreGluten("SPARK-39557 INSERT INTO statements with tables with array defaults") {
    withSQLConf("spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {
      import testImplicits._
      // Positive tests: array types are supported as default values.
      case class Config(dataSource: String, useDataFrames: Boolean = false)
      Seq(
        Config("parquet"),
        Config("parquet", useDataFrames = true),
        Config("orc"),
        Config("orc", useDataFrames = true)).foreach {
        config =>
          withTable("t") {
            sql(s"create table t(i boolean) using ${config.dataSource}")
            if (config.useDataFrames) {
              Seq(false).toDF.write.insertInto("t")
            } else {
              sql("insert into t select false")
            }
            sql("alter table t add column s array<int> default array(1, 2)")
            checkAnswer(spark.table("t"), Row(false, null))
            sql("insert into t(i) values (true)")
            checkAnswer(spark.table("t"), Seq(Row(false, null), Row(true, Seq(1, 2))))
          }
      }
      // Negative tests: provided array element types must match their corresponding DEFAULT
      // declarations, if applicable.
      val incompatibleDefault =
        "Failed to execute ALTER TABLE ADD COLUMNS command because the destination " +
          "table column `s` has a DEFAULT value"
      Seq(Config("parquet"), Config("parquet", useDataFrames = true)).foreach {
        config =>
          withTable("t") {
            sql(s"create table t(i boolean) using ${config.dataSource}")
            if (config.useDataFrames) {
              Seq(false).toDF.write.insertInto("t")
            } else {
              sql("insert into t select false")
            }
            assert(intercept[AnalysisException] {
              sql("alter table t add column s array<int> default array('abc', 'def')")
            }.getMessage.contains(incompatibleDefault))
          }
      }
    }
  }

  // TODO: fix in Spark-4.0
  ignoreGluten("SPARK-39557 INSERT INTO statements with tables with struct defaults") {
    withSQLConf("spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {

      import testImplicits._
      // Positive tests: struct types are supported as default values.
      case class Config(dataSource: String, useDataFrames: Boolean = false)
      Seq(
        Config("parquet"),
        Config("parquet", useDataFrames = true),
        Config("orc"),
        Config("orc", useDataFrames = true)).foreach {
        config =>
          withTable("t") {
            sql(s"create table t(i boolean) using ${config.dataSource}")
            if (config.useDataFrames) {
              Seq(false).toDF.write.insertInto("t")
            } else {
              sql("insert into t select false")
            }
            sql(
              "alter table t add column s struct<x boolean, y string> default struct(true, 'abc')")
            checkAnswer(spark.table("t"), Row(false, null))
            sql("insert into t(i) values (true)")
            checkAnswer(spark.table("t"), Seq(Row(false, null), Row(true, Row(true, "abc"))))
          }
      }

      // Negative tests: provided map element types must match their corresponding DEFAULT
      // declarations, if applicable.
      val incompatibleDefault =
        "Failed to execute ALTER TABLE ADD COLUMNS command because the destination " +
          "table column `s` has a DEFAULT value"
      Seq(Config("parquet"), Config("parquet", useDataFrames = true)).foreach {
        config =>
          withTable("t") {
            sql(s"create table t(i boolean) using ${config.dataSource}")
            if (config.useDataFrames) {
              Seq(false).toDF.write.insertInto("t")
            } else {
              sql("insert into t select false")
            }
            assert(intercept[AnalysisException] {
              sql("alter table t add column s struct<x boolean, y string> default struct(42, 56)")
            }.getMessage.contains(incompatibleDefault))
          }
      }
    }
  }

  ignoreGluten("SPARK-39557 INSERT INTO statements with tables with map defaults") {
    withSQLConf("spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {

      import testImplicits._
      // Positive tests: map types are supported as default values.
      case class Config(dataSource: String, useDataFrames: Boolean = false)
      Seq(
        Config("parquet"),
        Config("parquet", useDataFrames = true),
        Config("orc"),
        Config("orc", useDataFrames = true)).foreach {
        config =>
          withTable("t") {
            sql(s"create table t(i boolean) using ${config.dataSource}")
            if (config.useDataFrames) {
              Seq(false).toDF.write.insertInto("t")
            } else {
              sql("insert into t select false")
            }
            sql("alter table t add column s map<boolean, string> default map(true, 'abc')")
            checkAnswer(spark.table("t"), Row(false, null))
            sql("insert into t(i) select true")
            checkAnswer(spark.table("t"), Seq(Row(false, null), Row(true, Map(true -> "abc"))))
          }
          withTable("t") {
            sql(s"""
            create table t(
              i int,
              s struct<
                x array<
                  struct<a int, b int>>,
                y array<
                  map<boolean, string>>>
              default struct(
                array(
                  struct(1, 2)),
                array(
                  map(false, 'def', true, 'jkl'))))
              using ${config.dataSource}""")
            sql("insert into t select 1, default")
            sql("alter table t alter column s drop default")
            if (config.useDataFrames) {
              Seq((2, null)).toDF.write.insertInto("t")
            } else {
              sql("insert into t select 2, default")
            }
            sql("""
            alter table t alter column s
            set default struct(
              array(
                struct(3, 4)),
              array(
                map(false, 'mno', true, 'pqr')))""")
            sql("insert into t select 3, default")
            sql("""
            alter table t
            add column t array<
              map<boolean, string>>
            default array(
              map(true, 'xyz'))""")
            sql("insert into t(i, s) select 4, default")
            checkAnswer(
              spark.table("t"),
              Seq(
                Row(1, Row(Seq(Row(1, 2)), Seq(Map(false -> "def", true -> "jkl"))), null),
                Row(2, null, null),
                Row(3, Row(Seq(Row(3, 4)), Seq(Map(false -> "mno", true -> "pqr"))), null),
                Row(
                  4,
                  Row(Seq(Row(3, 4)), Seq(Map(false -> "mno", true -> "pqr"))),
                  Seq(Map(true -> "xyz")))
              )
            )
          }
      }
      // Negative tests: provided map element types must match their corresponding DEFAULT
      // declarations, if applicable.
      val incompatibleDefault =
        "Failed to execute ALTER TABLE ADD COLUMNS command because the destination " +
          "table column `s` has a DEFAULT value"
      Seq(Config("parquet"), Config("parquet", useDataFrames = true)).foreach {
        config =>
          withTable("t") {
            sql(s"create table t(i boolean) using ${config.dataSource}")
            if (config.useDataFrames) {
              Seq(false).toDF.write.insertInto("t")
            } else {
              sql("insert into t select false")
            }
            assert(intercept[AnalysisException] {
              sql("alter table t add column s map<boolean, string> default map(42, 56)")
            }.getMessage.contains(incompatibleDefault))
          }
      }
    }
  }
}

class GlutenRenameFromSparkStagingToFinalDirAlwaysTurnsFalseFilesystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    (!isSparkStagingDir(src) || isSparkStagingDir(dst)) && super.rename(src, dst)
  }

  private def isSparkStagingDir(path: Path): Boolean = {
    path.toString.contains("_temporary")
  }
}
