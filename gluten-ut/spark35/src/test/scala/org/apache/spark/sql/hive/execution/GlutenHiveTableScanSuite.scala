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

import org.apache.spark.sql.GlutenTestSetWithSystemPropertyTrait
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HivePartitionConverter
import org.apache.spark.tags.SlowHiveTest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, FileSplit, JobConf, TextInputFormat}

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@SlowHiveTest
class GlutenHiveTableScanSuite
  extends HiveTableScanSuite
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveComparisonTestSupport {

  override def testNameBlackList: Seq[String] = Seq(
    // Rewritten with a workspace-backed file because the upstream test resolves this resource
    // from the spark-hive tests jar in gluten-ut.
    "Spark-4077: timestamp query for null value"
  )

  testGluten("Hive recursive file listing excludes hidden directories") {
    withTempDir {
      dir =>
        Seq(
          "root-data",
          "visible/nested-data",
          "_temporary/attempt/uncommitted-data",
          ".backup/backup-data",
          "visible/_temporary/nested-uncommitted-data",
          "visible/.backup/nested-backup-data",
          "_hidden-file",
          ".hidden-file"
        ).foreach {
          relativePath =>
            val file = dir.toPath.resolve(relativePath)
            Files.createDirectories(file.getParent)
            Files.write(file, "1\n".getBytes(StandardCharsets.UTF_8))
        }

        Seq(false, true).foreach {
          recursive =>
            val conf = new Configuration(spark.sessionState.newHadoopConf())
            conf.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, recursive)
            // Ignore visible subdirectories in the non-recursive Hadoop control case.
            conf.setBoolean(
              "mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs",
              true)
            val job = new JobConf(conf)
            FileInputFormat.setInputPaths(job, new Path(dir.toURI))
            val inputFormat = new TextInputFormat
            inputFormat.configure(job)
            val hadoopFiles = inputFormat
              .getSplits(job, 1)
              .map(_.asInstanceOf[FileSplit].getPath.getName)
              .sorted
              .toSeq
            val expected = if (recursive) Seq("nested-data", "root-data") else Seq("root-data")
            assert(hadoopFiles == expected)

            val converterFiles = new HivePartitionConverter(conf, spark)
              .createFilePartition(dir.toURI)
              .flatMap(_.files)
              .map(file => new Path(file.filePath.toString).getName)
              .sorted
            assert(converterFiles == hadoopFiles, s"File listing differs with recursive=$recursive")
        }
    }
  }

  testGluten("Spark-4077: timestamp query for null value") {
    withTable("timestamp_query_null") {
      sql("""
            |CREATE TABLE timestamp_query_null (time TIMESTAMP,id INT)
            |ROW FORMAT DELIMITED
            |FIELDS TERMINATED BY ','
            |LINES TERMINATED BY '\n'
        """.stripMargin)
      val location = hiveResourcePath("data/files/issue-4077-data.txt").toFile.toURI

      sql(s"LOAD DATA LOCAL INPATH '$location' INTO TABLE timestamp_query_null")
      assert(
        sql("SELECT time FROM timestamp_query_null LIMIT 2").collect() ===
          Array(Row(java.sql.Timestamp.valueOf("2014-12-11 00:00:00")), Row(null)))
    }
  }
}
