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
package org.apache.spark.sql.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.GlutenStreamingTestConf
import org.apache.spark.sql.GlutenTestsCommonTrait

import java.io.{File, FileOutputStream}
import java.nio.file.Files

class GlutenStreamingQueryHashPartitionVerifySuite
  extends {
    private val sparkTestHomeInitialized: Unit =
      GlutenStreamingQueryHashPartitionVerifySuite.initSparkTestHome()
  }
  with StreamingQueryHashPartitionVerifySuite
  with GlutenTestsCommonTrait {

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    val warehousePath =
      conf
        .getOption("spark.sql.warehouse.dir")
        .getOrElse(System.getProperty("java.io.tmpdir") + "/spark-warehouse")
    GlutenStreamingTestConf.withFallbackToVanilla(
      GlutenSQLTestsBaseTrait.nativeSparkConf(conf, warehousePath))
  }
}

object GlutenStreamingQueryHashPartitionVerifySuite {
  private val ResourcePath = "structured-streaming/partition-tests"
  private val PartitionTestsRelativePath =
    "sql/core/src/test/resources/structured-streaming/partition-tests"

  private[streaming] def initSparkTestHome(): Unit = {
    val existingValidSparkTestHome =
      sys.props.get("spark.test.home").filter(isValidSparkTestHome)
    if (existingValidSparkTestHome.isEmpty) {
      val sparkTestHome = createSparkTestHomeFromResources()
        .orElse(localSparkTestHome().filter(isValidSparkTestHome))
        .orElse(sys.env.get("SPARK_HOME").filter(isValidSparkTestHome))

      sparkTestHome.foreach(path => System.setProperty("spark.test.home", path))
    }
  }

  private def localSparkTestHome(): Option[String] = {
    val userDir = sys.props.getOrElse("user.dir", ".")
    val moduleRelativeSparkTestHome =
      new File(userDir, "../common/src/test/resources/spark-home")
    val localSparkTestHome =
      new File(userDir, "src/test/resources/spark-home")

    if (moduleRelativeSparkTestHome.exists()) {
      Some(moduleRelativeSparkTestHome.getAbsolutePath)
    } else if (localSparkTestHome.exists()) {
      Some(localSparkTestHome.getAbsolutePath)
    } else {
      None
    }
  }

  private def createSparkTestHomeFromResources(): Option[String] = {
    val randomSchemasPath = s"$ResourcePath/randomSchemas"
    val rowsAndPartIdsPath = s"$ResourcePath/rowsAndPartIds"

    val rootDir = Files.createTempDirectory("gluten-spark-test-home").toFile
    val partitionTestsDir = new File(rootDir, PartitionTestsRelativePath)

    if (!partitionTestsDir.mkdirs() && !partitionTestsDir.exists()) {
      None
    } else {
      val copiedRandomSchemas =
        copyResource(randomSchemasPath, new File(partitionTestsDir, "randomSchemas"))
      val copiedRowsAndPartIds =
        copyResource(rowsAndPartIdsPath, new File(partitionTestsDir, "rowsAndPartIds"))

      if (copiedRandomSchemas && copiedRowsAndPartIds) {
        rootDir.deleteOnExit()
        Some(rootDir.getAbsolutePath).filter(isValidSparkTestHome)
      } else {
        None
      }
    }
  }

  private def isValidSparkTestHome(path: String): Boolean = {
    val partitionTestsDir = new File(path, PartitionTestsRelativePath)
    val randomSchemas = new File(partitionTestsDir, "randomSchemas")
    val rowsAndPartIds = new File(partitionTestsDir, "rowsAndPartIds")
    randomSchemas.exists() && rowsAndPartIds.exists()
  }

  private def copyResource(resourcePath: String, targetFile: File): Boolean = {
    val in = Option(getClass.getClassLoader.getResourceAsStream(resourcePath))
    in.exists {
      is =>
        val out = new FileOutputStream(targetFile)
        try {
          is.transferTo(out)
          true
        } finally {
          out.close()
          is.close()
        }
    }
  }
}
