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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.BatchScanExecTransformer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.execution.FileSourceScanLike
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.internal.SQLConf

import org.apache.hadoop.fs.Path

import java.io.FileNotFoundException

import scala.collection.mutable

class GlutenFileBasedDataSourceSuite extends FileBasedDataSourceSuite with GlutenSQLTestsTrait {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
  }

  // test data path is jar path, so failed, test code is same with spark
  testGluten("Option recursiveFileLookup: disable partition inferring") {
    val dataPath = getWorkspaceFilePath(
      "sql",
      "core",
      "src",
      "test",
      "resources").toString + "/" + "test-data/text-partitioned"

    val df = spark.read
      .format("binaryFile")
      .option("recursiveFileLookup", true)
      .load(dataPath)

    assert(!df.columns.contains("year"), "Expect partition inferring disabled")
    val fileList = df.select("path").collect().map(_.getString(0))

    val expectedFileList = Array(
      dataPath + "/year=2014/data.txt",
      dataPath + "/year=2015/data.txt"
    ).map(path => "file:" + new Path(path).toString)

    assert(fileList.toSet === expectedFileList.toSet)
  }

  testGluten("Spark native readers should respect spark.sql.caseSensitive - parquet") {
    withTempDir {
      dir =>
        val format = "parquet"
        val tableName = s"spark_25132_${format}_native"
        val tableDir = dir.getCanonicalPath + s"/$tableName"
        withTable(tableName) {
          val end = 5
          val data = spark.range(end).selectExpr("id as A", "id * 2 as b", "id * 3 as B")
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            data.write.format(format).mode("overwrite").save(tableDir)
          }
          sql(s"CREATE TABLE $tableName (a LONG, b LONG) USING $format LOCATION '$tableDir'")

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
            checkAnswer(sql(s"select a from $tableName"), data.select("A"))
            checkAnswer(sql(s"select A from $tableName"), data.select("A"))

            // TODO: gluten can catch exception in executor side, but cannot catch SparkException
            //  in Driver side
            // RuntimeException is triggered at executor side, which is then wrapped as
            // SparkException at driver side
            //          val e1 = intercept[SparkException] {
            //            sql(s"select b from $tableName").collect()
            //          }
            //
            //          assert(
            //            e1.getCause.isInstanceOf[RuntimeException] &&
            //              e1.getMessage.contains(
            //                """Found duplicate field(s) b in case-insensitive mode """))
            //          val e2 = intercept[SparkException] {
            //            sql(s"select B from $tableName").collect()
            //          }
            //          assert(
            //            e2.getCause.isInstanceOf[RuntimeException] &&
            //              e2.getMessage.contains(
            //                """Found duplicate field(s) b in case-insensitive mode"""))
          }

          withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
            checkAnswer(sql(s"select a from $tableName"), (0 until end).map(_ => Row(null)))
            checkAnswer(sql(s"select b from $tableName"), data.select("b"))
          }
        }
    }
  }

  testGluten("SPARK-22790,SPARK-27668: spark.sql.sources.compressionFactor takes effect") {
    Seq(1.0, 0.5).foreach {
      compressionFactor =>
        withSQLConf(
          SQLConf.FILE_COMPRESSION_FACTOR.key -> compressionFactor.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "350") {
          withTempPath {
            workDir =>
              // the file size is 504 bytes
              val workDirPath = workDir.getAbsolutePath
              val data1 = Seq(100, 200, 300, 400).toDF("count")
              data1.write.orc(workDirPath + "/data1")
              val df1FromFile = spark.read.orc(workDirPath + "/data1")
              val data2 = Seq(100, 200, 300, 400).toDF("count")
              data2.write.orc(workDirPath + "/data2")
              val df2FromFile = spark.read.orc(workDirPath + "/data2")
              val joinedDF = df1FromFile.join(df2FromFile, Seq("count"))
              if (compressionFactor == 0.5) {
                val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case bJoin: BroadcastHashJoinExec => bJoin
                }
                assert(bJoinExec.nonEmpty)
                val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case smJoin: SortMergeJoinExec => smJoin
                }
                assert(smJoinExec.isEmpty)
              } else {
                // compressionFactor is 1.0
                val bJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case bJoin: BroadcastHashJoinExec => bJoin
                }
                assert(bJoinExec.isEmpty)
                val smJoinExec = collect(joinedDF.queryExecution.executedPlan) {
                  case smJoin: SortMergeJoinExec => smJoin
                }
                assert(smJoinExec.nonEmpty)
              }
          }
        }
    }
  }

  testGluten("SPARK-25237 compute correct input metrics in FileScanRDD") {
    // TODO: Test CSV V2 as well after it implements [[SupportsReportStatistics]].
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "csv") {
      withTempPath {
        p =>
          val path = p.getAbsolutePath
          spark.range(1000).repartition(1).write.csv(path)
          val bytesReads = new mutable.ArrayBuffer[Long]()
          val bytesReadListener = new SparkListener() {
            override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
              bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
            }
          }
          sparkContext.addSparkListener(bytesReadListener)
          try {
            spark.read.csv(path).limit(1).collect()
            sparkContext.listenerBus.waitUntilEmpty()
            // plan is different, so metric is different
            assert(bytesReads.sum === 7864)
          } finally {
            sparkContext.removeSparkListener(bytesReadListener)
          }
      }
    }
  }

  testGluten("SPARK-41017: filter pushdown with nondeterministic predicates") {
    withTempPath {
      path =>
        val pathStr = path.getCanonicalPath
        spark.range(10).write.parquet(pathStr)
        Seq("parquet", "").foreach {
          useV1SourceList =>
            withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1SourceList) {
              val scan = spark.read.parquet(pathStr)
              val df = scan.where(rand() > 0.5 && $"id" > 5)
              val filters = df.queryExecution.executedPlan.collect {
                case f: FileSourceScanLike => f.dataFilters
                case b: BatchScanExec => b.scan.asInstanceOf[FileScan].dataFilters
                case b: BatchScanExecTransformer => b.scan.asInstanceOf[FileScan].dataFilters
              }.flatten
              assert(filters.contains(GreaterThan(scan.logicalPlan.output.head, Literal(5L))))
            }
        }
    }
  }

  Seq("orc", "parquet").foreach {
    format =>
      testQuietly(GLUTEN_TEST + s"Enabling/disabling ignoreMissingFiles using $format") {
        def testIgnoreMissingFiles(options: Map[String, String]): Unit = {
          withTempDir {
            dir =>
              val basePath = dir.getCanonicalPath

              Seq("0").toDF("a").write.format(format).save(new Path(basePath, "second").toString)
              Seq("1").toDF("a").write.format(format).save(new Path(basePath, "fourth").toString)

              val firstPath = new Path(basePath, "first")
              val thirdPath = new Path(basePath, "third")
              val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())
              Seq("2").toDF("a").write.format(format).save(firstPath.toString)
              Seq("3").toDF("a").write.format(format).save(thirdPath.toString)
              val files = Seq(firstPath, thirdPath).flatMap {
                p => fs.listStatus(p).filter(_.isFile).map(_.getPath)
              }

              val df = spark.read
                .options(options)
                .format(format)
                .load(
                  new Path(basePath, "first").toString,
                  new Path(basePath, "second").toString,
                  new Path(basePath, "third").toString,
                  new Path(basePath, "fourth").toString)

              // Make sure all data files are deleted and can't be opened.
              files.foreach(f => fs.delete(f, false))
              assert(fs.delete(thirdPath, true))
              for (f <- files) {
                intercept[FileNotFoundException](fs.open(f))
              }

              checkAnswer(df, Seq(Row("0"), Row("1")))
          }
        }

        // Test set ignoreMissingFiles via SQL Conf
        // Rewrite this test as error msg is different from velox and data Source reader options
        // is not supported.
        for {
          (ignore, options, sqlConf) <- Seq(
            // Set via SQL Conf: leave options empty
            ("true", Map.empty[String, String], "true"),
            ("false", Map.empty[String, String], "false")
          )
          sources <- Seq("", format)
        } {
          withSQLConf(
            SQLConf.USE_V1_SOURCE_LIST.key -> sources,
            SQLConf.IGNORE_MISSING_FILES.key -> sqlConf) {
            if (ignore.toBoolean) {
              testIgnoreMissingFiles(options)
            } else {
              val exception = intercept[SparkException] {
                testIgnoreMissingFiles(options)
              }
              assert(exception.getMessage().contains("No such file or directory"))
            }
          }
        }
      }
  }

}
