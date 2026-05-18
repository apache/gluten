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

import org.apache.spark.SparkFiles
import org.apache.spark.sql.GlutenTestSetWithSystemPropertyTrait
import org.apache.spark.sql.hive.HiveUtils.{builtinHiveVersion => hiveVersion}
import org.apache.spark.sql.hive.test.HiveTestJars
import org.apache.spark.tags.SlowHiveTest

import java.io.File

@SlowHiveTest
class GlutenHiveQuerySuite
  extends HiveQuerySuite
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveComparisonTestSupport {

  override def testNameBlackList: Seq[String] = Seq(
    "ADD FILE command",
    "ADD JAR command 2",
    "CREATE TEMPORARY FUNCTION",
    "SPARK-33084: Add jar support Ivy URI in SQL"
  )

  testGluten("ADD FILE command") {
    val testFile = hiveResourcePath("data/files/v1.txt").toFile.toURI
    sql(s"ADD FILE $testFile")

    val checkAddFileRDD = sparkContext.parallelize(1 to 2, 1).mapPartitions {
      _ => Iterator.single(new File(SparkFiles.get("v1.txt")).canRead)
    }

    assert(checkAddFileRDD.first())
    assert(sql("list files").filter(_.getString(0).contains("data/files/v1.txt")).count() > 0)
    assert(sql("list file").filter(_.getString(0).contains("data/files/v1.txt")).count() > 0)
    assert(sql(s"list file $testFile").count() == 1)
  }

  testGluten("ADD JAR command 2") {
    val testJar = HiveTestJars.getHiveHcatalogCoreJar().toURI
    val testData = hiveResourcePath("data/files/sample.json").toUri
    sql(s"ADD JAR $testJar")
    withTable("t1") {
      sql("""CREATE TABLE t1(a string, b string)
            |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'""".stripMargin)
      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE t1""")
      sql("select * from src join t1 on src.key = t1.a")
    }
    assert(
      sql("list jars")
        .filter(_.getString(0).contains(HiveTestJars.getHiveHcatalogCoreJar().getName))
        .count() > 0)
    assert(
      sql("list jar")
        .filter(_.getString(0).contains(HiveTestJars.getHiveHcatalogCoreJar().getName))
        .count() > 0)
    val testJar2 = hiveResourcePath("TestUDTF.jar").toFile.getCanonicalPath
    sql(s"ADD JAR $testJar2")
    assert(sql(s"list jar $testJar").count() == 1)
  }

  testGluten("SPARK-33084: Add jar support Ivy URI in SQL") {
    val testData = hiveResourcePath("data/files/sample.json").toUri
    withTable("t") {
      // Use transitive=false as it should be good enough to test the Ivy support in Hive ADD JAR.
      sql(
        s"ADD JAR ivy://org.apache.hive.hcatalog:hive-hcatalog-core:$hiveVersion" +
          "?transitive=false")
      sql("""CREATE TABLE t(a string, b string)
            |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'""".stripMargin)
      sql(s"""LOAD DATA LOCAL INPATH "$testData" INTO TABLE t""")
      sql("SELECT * FROM src JOIN t on src.key = t.a")
      assert(
        sql("LIST JARS")
          .filter(_.getString(0).contains(
            s"org.apache.hive.hcatalog_hive-hcatalog-core-$hiveVersion.jar"))
          .count() > 0)
      assert(
        sql("LIST JAR")
          .filter(_.getString(0).contains(
            s"org.apache.hive.hcatalog_hive-hcatalog-core-$hiveVersion.jar"))
          .count() > 0)
    }
  }

  testGluten("CREATE TEMPORARY FUNCTION") {
    val jarURL = hiveResourcePath("TestUDTF.jar").toUri.toURL
    sql(s"ADD JAR $jarURL")
    withUserDefinedFunction("udtf_count2" -> true) {
      sql("""CREATE TEMPORARY FUNCTION udtf_count2 AS
            |'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
            |""".stripMargin)
      assert(sql("DESCRIBE FUNCTION udtf_count2").count() > 1)
    }
  }
}
