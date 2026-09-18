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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{FileSourceScanExecTransformer, FilterExecTransformer}

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.io.File
import java.sql.Timestamp

import scala.reflect.ClassTag

class GlutenFileMetadataStructSuite extends FileMetadataStructSuite with GlutenSQLTestsBaseTrait {

  val schemaWithFilePathField: StructType = new StructType()
    .add(StructField("file_path", StringType))
    .add(StructField("age", IntegerType))
    .add(
      StructField(
        "info",
        new StructType()
          .add(StructField("id", LongType))
          .add(StructField("university", StringType))))

  private val METADATA_FILE_PATH = "_metadata.file_path"
  private val METADATA_FILE_NAME = "_metadata.file_name"
  private val METADATA_FILE_SIZE = "_metadata.file_size"
  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"

  private def getMetadataForFile(f: File): Map[String, Any] = {
    Map(
      METADATA_FILE_PATH -> f.toURI.toString,
      METADATA_FILE_NAME -> f.getName,
      METADATA_FILE_SIZE -> f.length(),
      METADATA_FILE_MODIFICATION_TIME -> new Timestamp(f.lastModified())
    )
  }

  private def metadataColumnsNativeTest(testName: String, fileSchema: StructType)(
      f: (DataFrame, Map[String, Any], Map[String, Any]) => Unit): Unit = {
    Seq("parquet").foreach {
      testFileFormat =>
        testGluten(s"metadata struct ($testFileFormat): " + testName) {
          withTempDir {
            dir =>
              import scala.collection.JavaConverters._

              // 1. create df0 and df1 and save under /data/f0 and /data/f1
              val df0 = spark.createDataFrame(data0.asJava, fileSchema)
              val f0 = new File(dir, "data/f0").getCanonicalPath
              df0.coalesce(1).write.format(testFileFormat).save(f0)

              val df1 = spark.createDataFrame(data1.asJava, fileSchema)
              val f1 = new File(dir, "data/f1 gluten").getCanonicalPath
              df1.coalesce(1).write.format(testFileFormat).save(f1)

              // 2. read both f0 and f1
              val df = spark.read
                .format(testFileFormat)
                .schema(fileSchema)
                .load(new File(dir, "data").getCanonicalPath + "/*")
              val realF0 = new File(dir, "data/f0")
                .listFiles()
                .filter(_.getName.endsWith(s".$testFileFormat"))
                .head
              val realF1 = new File(dir, "data/f1 gluten")
                .listFiles()
                .filter(_.getName.endsWith(s".$testFileFormat"))
                .head
              f(df, getMetadataForFile(realF0), getMetadataForFile(realF1))
          }
        }
    }
  }

  def checkOperatorMatch[T](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  metadataColumnsNativeTest(
    "plan check with metadata and user data select",
    schemaWithFilePathField) {
    (df, f0, f1) =>
      var dfWithMetadata = df.select(
        METADATA_FILE_NAME,
        METADATA_FILE_PATH,
        METADATA_FILE_SIZE,
        METADATA_FILE_MODIFICATION_TIME,
        "age")
      dfWithMetadata.collect
      if (BackendsApiManager.getSettings.supportNativeMetadataColumns()) {
        checkOperatorMatch[FileSourceScanExecTransformer](dfWithMetadata)
      } else {
        checkOperatorMatch[FileSourceScanExec](dfWithMetadata)
      }

      // would fallback
      dfWithMetadata = df.select(METADATA_FILE_PATH, "file_path")
      checkAnswer(
        dfWithMetadata,
        Seq(
          Row(f0(METADATA_FILE_PATH), "jack"),
          Row(f1(METADATA_FILE_PATH), "lily")
        )
      )
      checkOperatorMatch[FileSourceScanExec](dfWithMetadata)
  }

  metadataColumnsNativeTest("plan check with metadata filter", schemaWithFilePathField) {
    (df, f0, f1) =>
      var filterDF = df
        .select("file_path", "age", METADATA_FILE_NAME)
        .where(Column(METADATA_FILE_NAME) === f0((METADATA_FILE_NAME)))
      val ret = filterDF.collect
      assert(ret.size == 1)
      if (BackendsApiManager.getSettings.supportNativeMetadataColumns()) {
        checkOperatorMatch[FileSourceScanExecTransformer](filterDF)
      } else {
        checkOperatorMatch[FileSourceScanExec](filterDF)
      }
      checkOperatorMatch[FilterExecTransformer](filterDF)

      // case to check if file_path is URI string
      filterDF =
        df.select(METADATA_FILE_PATH).where(Column(METADATA_FILE_NAME) === f1((METADATA_FILE_NAME)))
      checkAnswer(
        filterDF,
        Seq(
          Row(f1(METADATA_FILE_PATH))
        )
      )
      if (BackendsApiManager.getSettings.supportNativeMetadataColumns()) {
        checkOperatorMatch[FileSourceScanExecTransformer](filterDF)
      } else {
        checkOperatorMatch[FileSourceScanExec](filterDF)
      }
      checkOperatorMatch[FilterExecTransformer](filterDF)
  }
}
