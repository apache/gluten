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

import org.apache.gluten.execution.{ProjectExecTransformer, SortExecTransformer}
import org.apache.gluten.execution.GlutenPlan

import org.apache.spark.SparkConf
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, CommandResultExec, GlutenImplicits, QueryExecution}
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
  with AdaptiveSparkPlanHelper {

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

  private def checkAndGetWriteFiles(df: DataFrame): ColumnarWriteFilesExec = {
    val writeFiles = stripAQEPlan(
      df.queryExecution.executedPlan
        .asInstanceOf[CommandResultExec]
        .commandPhysicalPlan).children.head
    assert(writeFiles.isInstanceOf[ColumnarWriteFilesExec])
    writeFiles.asInstanceOf[ColumnarWriteFilesExec]
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
        checkAndGetWriteFiles(df)

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
      checkAndGetWriteFiles(df2)
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
    val writeFiles = checkAndGetWriteFiles(df)
    assert(
      writeFiles
        .find(_.isInstanceOf[SortExecTransformer])
        .isEmpty)
    // all operators should be transformed
    assert(writeFiles.child.find(!_.isInstanceOf[GlutenPlan]).isEmpty)

    val parts = spark.sessionState.catalog.listPartitionNames(TableIdentifier("pt")).toSet
    assert(parts == expectedPartitionNames)
  }

  testGluten("offload empty2null when v1writes fallback") {
    withSQLConf((SQLConf.MAX_RECORDS_PER_FILE.key, "1000")) {
      withTable("pt") {
        spark.sql("CREATE TABLE pt (c1 int) USING PARQUET PARTITIONED BY(p string)")

        val df = spark.sql(s"""
                              |INSERT OVERWRITE TABLE pt PARTITION(p)
                              |SELECT c1, c2 as p FROM source
                              |""".stripMargin)

        val writeFiles = stripAQEPlan(
          df.queryExecution.executedPlan
            .asInstanceOf[CommandResultExec]
            .commandPhysicalPlan).children.head
        assert(!writeFiles.isInstanceOf[ColumnarWriteFilesExec])
        assert(writeFiles.exists(_.isInstanceOf[ProjectExecTransformer]))
        val projectExecTransformer = writeFiles
          .find(_.isInstanceOf[ProjectExecTransformer])
          .get
          .asInstanceOf[ProjectExecTransformer]
        projectExecTransformer.projectList.find(_.toString().contains("empty2null"))

        // The partition column should never be empty
        checkAnswer(
          spark.sql("SELECT * FROM pt"),
          spark.sql("SELECT c1, if(c2 = '', null, c2) FROM source"))
      }
    }
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
      val writeFiles = checkAndGetWriteFiles(df)
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
      checkAndGetWriteFiles(df)
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
      checkAndGetWriteFiles(df)
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
