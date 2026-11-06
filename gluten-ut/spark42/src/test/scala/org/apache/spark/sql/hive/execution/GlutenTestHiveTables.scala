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

import org.apache.spark.sql.hive.test.TestHive.sparkSession
import org.apache.spark.sql.hive.test.TestHive.sparkSession.TestTable
import org.apache.spark.sql.hive.test.TestHiveQueryExecution

import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import java.nio.file.Path

/** Wrappers around [[TestHive]] lazy-loaded table registrations. */
object GlutenTestHiveTables {
  def hiveResourcePath(resourceDir: Path, relativePath: String): Path = {
    relativePath.split('/').foldLeft(resourceDir) { case (path, child) => path.resolve(child) }
  }

  private def hiveDataFile(resourceDir: Path, fileName: String): String = {
    hiveResourcePath(resourceDir, fileName).toAbsolutePath.normalize.toString
  }

  implicit private class SqlCmd(sql: String) {
    def cmd: () => Unit = {
      () => new TestHiveQueryExecution(sql).executedPlan.executeCollect(): Unit
    }
  }

  def registerTestTable(testTable: TestTable): Unit = {
    sparkSession.registerTestTable(testTable)
  }

  def registerHiveQTestUtilsTables(resourceDir: Path): Unit = {
    def createTableSQL(tblName: String): String = {
      s"CREATE TABLE $tblName (key INT, value STRING) STORED AS textfile"
    }
    // The test tables that are defined in the Hive QTestUtil.
    // /itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
    // https://github.com/apache/hive/blob/branch-0.13/data/scripts/q_test_init.sql
    @transient
    val hiveQTestUtilTables: Seq[TestTable] = Seq(
      TestTable(
        "src",
        createTableSQL("src").cmd,
        s"""LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/kv1.txt")}'
           |INTO TABLE src""".stripMargin.cmd
      ),
      TestTable(
        "src1",
        createTableSQL("src1").cmd,
        s"""LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/kv3.txt")}'
           |INTO TABLE src1""".stripMargin.cmd
      ),
      TestTable(
        "srcpart",
        () => {
          s"${createTableSQL("srcpart")} PARTITIONED BY (ds STRING, hr STRING)".cmd.apply()
          for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
            s"""
               |LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/kv1.txt")}'
               |OVERWRITE INTO TABLE srcpart PARTITION (ds='$ds',hr='$hr')
          """.stripMargin.cmd.apply()
          }
        }
      ),
      TestTable(
        "srcpart1",
        () => {
          s"${createTableSQL("srcpart1")} PARTITIONED BY (ds STRING, hr INT)".cmd.apply()
          for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- 11 to 12) {
            s"""
               |LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/kv1.txt")}'
               |OVERWRITE INTO TABLE srcpart1 PARTITION (ds='$ds',hr='$hr')
          """.stripMargin.cmd.apply()
          }
        }
      ),
      TestTable(
        "src_thrift",
        () => {
          import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer
          import org.apache.hadoop.mapred.{SequenceFileInputFormat, SequenceFileOutputFormat}
          import org.apache.thrift.protocol.TBinaryProtocol

          s"""
             |CREATE TABLE src_thrift(fake INT)
             |ROW FORMAT SERDE '${classOf[ThriftDeserializer].getName}'
             |WITH SERDEPROPERTIES(
             |  'serialization.class'='org.apache.spark.sql.hive.test.Complex',
             |  'serialization.format'='${classOf[TBinaryProtocol].getName}'
             |)
             |STORED AS
             |INPUTFORMAT '${classOf[SequenceFileInputFormat[_, _]].getName}'
             |OUTPUTFORMAT '${classOf[SequenceFileOutputFormat[_, _]].getName}'
        """.stripMargin.cmd.apply()

          s"""
             |LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/complex.seq")}'
             |INTO TABLE src_thrift
        """.stripMargin.cmd.apply()
        }
      ),
      TestTable(
        "serdeins",
        s"""CREATE TABLE serdeins (key INT, value STRING)
           |ROW FORMAT SERDE '${classOf[LazySimpleSerDe].getCanonicalName}'
           |WITH SERDEPROPERTIES ('field.delim'='\\t')
         """.stripMargin.cmd,
        "INSERT OVERWRITE TABLE serdeins SELECT * FROM src".cmd
      ),
      TestTable(
        "episodes",
        s"""CREATE TABLE episodes (title STRING, air_date STRING, doctor INT)
           |STORED AS avro
           |TBLPROPERTIES (
           |  'avro.schema.literal'='{
           |    "type": "record",
           |    "name": "episodes",
           |    "namespace": "testing.hive.avro.serde",
           |    "fields": [
           |      {
           |          "name": "title",
           |          "type": "string",
           |          "doc": "episode title"
           |      },
           |      {
           |          "name": "air_date",
           |          "type": "string",
           |          "doc": "initial date"
           |      },
           |      {
           |          "name": "doctor",
           |          "type": "int",
           |          "doc": "main actor playing the Doctor in episode"
           |      }
           |    ]
           |  }'
           |)
         """.stripMargin.cmd,
        s"""
           |LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/episodes.avro")}'
           |INTO TABLE episodes
         """.stripMargin.cmd
      ),
      // THIS TABLE IS NOT THE SAME AS THE HIVE TEST TABLE episodes_partitioned AS DYNAMIC
      // PARTITIONING IS NOT YET SUPPORTED
      TestTable(
        "episodes_part",
        s"""CREATE TABLE episodes_part (title STRING, air_date STRING, doctor INT)
           |PARTITIONED BY (doctor_pt INT)
           |STORED AS avro
           |TBLPROPERTIES (
           |  'avro.schema.literal'='{
           |    "type": "record",
           |    "name": "episodes",
           |    "namespace": "testing.hive.avro.serde",
           |    "fields": [
           |      {
           |          "name": "title",
           |          "type": "string",
           |          "doc": "episode title"
           |      },
           |      {
           |          "name": "air_date",
           |          "type": "string",
           |          "doc": "initial date"
           |      },
           |      {
           |          "name": "doctor",
           |          "type": "int",
           |          "doc": "main actor playing the Doctor in episode"
           |      }
           |    ]
           |  }'
           |)
         """.stripMargin.cmd,
        // WORKAROUND: Required to pass schema to SerDe for partitioned tables.
        // TODO: Pass this automatically from the table to partitions.
        s"""
           |ALTER TABLE episodes_part SET SERDEPROPERTIES (
           |  'avro.schema.literal'='{
           |    "type": "record",
           |    "name": "episodes",
           |    "namespace": "testing.hive.avro.serde",
           |    "fields": [
           |      {
           |          "name": "title",
           |          "type": "string",
           |          "doc": "episode title"
           |      },
           |      {
           |          "name": "air_date",
           |          "type": "string",
           |          "doc": "initial date"
           |      },
           |      {
           |          "name": "doctor",
           |          "type": "int",
           |          "doc": "main actor playing the Doctor in episode"
           |      }
           |    ]
           |  }'
           |)
          """.stripMargin.cmd,
        s"""
          INSERT OVERWRITE TABLE episodes_part PARTITION (doctor_pt=1)
          SELECT title, air_date, doctor FROM episodes
        """.cmd
      ),
      TestTable(
        "src_json",
        s"""CREATE TABLE src_json (json STRING) STORED AS TEXTFILE
         """.stripMargin.cmd,
        s"""LOAD DATA LOCAL INPATH '${hiveDataFile(resourceDir, "data/files/json.txt")}'
           |INTO TABLE src_json""".stripMargin.cmd
      )
    )

    hiveQTestUtilTables.foreach(registerTestTable)
  }
}
