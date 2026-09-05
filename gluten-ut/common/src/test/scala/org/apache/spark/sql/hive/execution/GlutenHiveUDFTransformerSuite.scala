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

import org.apache.gluten.expression.{GenericExpressionTransformer, UDFMappings}

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Generate
import org.apache.spark.sql.hive.{HiveGenericUDTF, HiveUDFTransformer}
import org.apache.spark.tags.SlowHiveTest

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files

@SlowHiveTest
abstract class GlutenHiveUDFTransformerSuiteBase
  extends SparkFunSuite {

  private var baseDir: File = _
  private var spark: SparkSession = _
  private var testUdtfJar: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (baseDir == null) {
      baseDir = Files.createTempDirectory(getClass.getSimpleName).toFile
    }
    if (testUdtfJar == null) {
      testUdtfJar = copyAndGetResourceFile("TestUDTF.jar", ".jar")
    }
    if (spark == null) {
      spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
      spark.sparkContext.setLogLevel("warn")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
      if (testUdtfJar != null) {
        testUdtfJar.delete()
        testUdtfJar = null
      }
      if (baseDir != null) {
        FileUtils.deleteDirectory(baseDir)
        baseDir = null
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterAll()
    }
  }

  private def sparkConf: SparkConf = {
    val warehouseDir = new File(baseDir, "spark-warehouse").getAbsolutePath
    val metastorePath = new File(baseDir, "metastore_db").getAbsolutePath

    new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.app.name", getClass.getSimpleName)
      .set("spark.ui.enabled", UI_ENABLED.defaultValueString)
      .set("spark.sql.warehouse.dir", warehouseDir)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastorePath;create=true")
  }

  test("HiveGenericUDTF is recognized and mapped by HiveUDFTransformer") {
    spark.sql(s"ADD JAR ${testUdtfJar.getCanonicalPath}")
    spark.sql(
      "CREATE TEMPORARY FUNCTION udtf_count2 " +
        "AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'")

    try {
      val analyzed = spark.sql("SELECT udtf_count2(a) FROM (SELECT 1 AS a) t")
        .queryExecution
        .analyzed
      val generate = analyzed
        .collectFirst {
          case generate: Generate if generate.generator.isInstanceOf[HiveGenericUDTF] =>
            generate
        }
        .getOrElse(fail(s"Expected HiveGenericUDTF in analyzed plan:\n$analyzed"))
      val udtf = generate.generator.asInstanceOf[HiveGenericUDTF]

      assert(HiveUDFTransformer.isHiveUDF(udtf))
      assert(
        HiveUDFTransformer.getHiveUDFNameAndClassName(udtf) ===
          ("udtf_count2", "org.apache.spark.sql.hive.execution.GenericUDTFCount2"))

      val previousMapping = UDFMappings.hiveUDFMap.get("udtf_count2")
      UDFMappings.hiveUDFMap.put("udtf_count2", "test_hive_generic_udtf")
      try {
        val transformer =
          HiveUDFTransformer.replaceWithExpressionTransformer(udtf, generate.child.output)
        assert(transformer.isInstanceOf[GenericExpressionTransformer])
        assert(transformer.substraitExprName === "test_hive_generic_udtf")
        assert(transformer.original eq udtf)
      } finally {
        previousMapping match {
          case Some(mapping) => UDFMappings.hiveUDFMap.put("udtf_count2", mapping)
          case None => UDFMappings.hiveUDFMap.remove("udtf_count2")
        }
      }
    } finally {
      spark.sql("DROP TEMPORARY FUNCTION IF EXISTS udtf_count2")
    }
  }
}
