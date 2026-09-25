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
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FileSourceScanExecTransformer, FilterExecTransformer}

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
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
  private val METADATA_FILE_BLOCK_START = "_metadata.file_block_start"
  private val METADATA_FILE_BLOCK_LENGTH = "_metadata.file_block_length"

  private val metadataColumns = Seq(
    METADATA_FILE_PATH,
    METADATA_FILE_NAME,
    METADATA_FILE_SIZE,
    METADATA_FILE_MODIFICATION_TIME,
    METADATA_FILE_BLOCK_START,
    METADATA_FILE_BLOCK_LENGTH)

  private val schemaWithMetadataCollisions = StructType(
    metadataColumns.zipWithIndex.map {
      case (name, index) =>
        StructField(name.stripPrefix("_metadata."), if (index < 2) LongType else StringType)
    }).add("id", LongType)

  private val collisionRows0 = Seq(
    Row(10L, 20L, "size0", "time0", "start0", "length0", 0L),
    Row(11L, 21L, "size1", "time1", "start1", "length1", 1L))
  private val collisionRows1 = Seq(
    Row(12L, 22L, "size2", "time2", "start2", "length2", 2L))

  private def getMetadataForFile(f: File): Map[String, Any] = {
    Map(
      METADATA_FILE_PATH -> f.toURI.toString,
      METADATA_FILE_NAME -> f.getName,
      METADATA_FILE_SIZE -> f.length(),
      METADATA_FILE_MODIFICATION_TIME -> new Timestamp(f.lastModified()),
      METADATA_FILE_BLOCK_START -> 0L,
      METADATA_FILE_BLOCK_LENGTH -> f.length()
    )
  }

  private def metadataColumnsNativeTest(
      testName: String,
      fileSchema: StructType,
      firstFileRows: Seq[Row] = data0,
      secondFileRows: Seq[Row] = data1)(
      f: (DataFrame, Map[String, Any], Map[String, Any]) => Unit): Unit = {
    Seq("parquet").foreach {
      testFileFormat =>
        testGluten(s"metadata struct ($testFileFormat): " + testName) {
          withTempDir {
            dir =>
              import scala.collection.JavaConverters._

              // 1. create df0 and df1 and save under /data/f0 and /data/f1
              val df0 = spark.createDataFrame(firstFileRows.asJava, fileSchema)
              val f0 = new File(dir, "data/f0").getCanonicalPath
              df0.coalesce(1).write.format(testFileFormat).save(f0)

              val df1 = spark.createDataFrame(secondFileRows.asJava, fileSchema)
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
              assert(realF0.length() < 1024 * 1024 && realF1.length() < 1024 * 1024)
              withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1m") {
                f(df, getMetadataForFile(realF0), getMetadataForFile(realF1))
              }
          }
        }
    }
  }

  def checkOperatorMatch[T](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  private def checkMetadataAnswer(
      df: DataFrame,
      expected: Seq[Row],
      hasRowIndex: Boolean = false): Unit = {
    checkAnswer(df, expected)
    val settings = BackendsApiManager.getSettings
    if (
      settings.supportNativeMetadataColumns() &&
      (!hasRowIndex || settings.supportNativeRowIndexColumn())
    ) {
      checkOperatorMatch[FileSourceScanExecTransformer](df)
    } else {
      checkOperatorMatch[FileSourceScanExec](df)
    }
  }

  metadataColumns.zipWithIndex.foreach {
    case (metadataColumn, index) =>
      metadataColumnsNativeTest(
        s"incompatible same-name data field for $metadataColumn",
        schemaWithMetadataCollisions,
        collisionRows0,
        collisionRows1) {
        (df, f0, f1) =>
          val expected = Seq.fill(collisionRows0.size)(Row(f0(metadataColumn))) ++
            Seq.fill(collisionRows1.size)(Row(f1(metadataColumn)))
          checkMetadataAnswer(df.select(metadataColumn), expected)
          checkMetadataAnswer(df.select(Column(metadataColumn).as("aliased_metadata")), expected)

          val combined = df.select(
            Column(metadataColumn.stripPrefix("_metadata.")).as("user_value"),
            Column(metadataColumn).as("metadata_value"))
          checkAnswer(
            combined,
            collisionRows0.map(row => Row(row.get(index), f0(metadataColumn))) ++
              collisionRows1.map(row => Row(row.get(index), f1(metadataColumn))))
          checkOperatorMatch[FileSourceScanExec](combined)
      }
  }

  metadataColumnsNativeTest(
    "metadata constants with a regular sibling",
    schemaWithMetadataCollisions,
    collisionRows0,
    collisionRows1) {
    (df, f0, f1) =>
      val selected = df.select((metadataColumns.map(Column(_)) :+ Column("id")): _*)
      val expected = Seq((collisionRows0, f0), (collisionRows1, f1)).flatMap {
        case (rows, metadata) =>
          rows.map(row => Row.fromSeq(metadataColumns.map(metadata) :+ row.getLong(6)))
      }
      checkMetadataAnswer(selected, expected)
      checkMetadataAnswer(
        df.select(METADATA_FILE_NAME).distinct(),
        Seq(Row(f0(METADATA_FILE_NAME)), Row(f1(METADATA_FILE_NAME))))
  }

  metadataColumnsNativeTest(
    "whole metadata struct and struct alias with incompatible data fields",
    schemaWithMetadataCollisions,
    collisionRows0,
    collisionRows1) {
    (df, _, _) =>
      val expected = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        df.select("_metadata").collect().toSeq
      }
      checkMetadataAnswer(df.select("_metadata"), expected, hasRowIndex = true)
      checkMetadataAnswer(
        df.select(Column("_metadata").as("aliased_metadata")),
        expected,
        hasRowIndex = true)
  }

  metadataColumnsNativeTest(
    "metadata predicates do not use conflicting physical fields",
    schemaWithMetadataCollisions,
    collisionRows0,
    collisionRows1) {
    (df, f0, f1) =>
      Seq(METADATA_FILE_NAME, METADATA_FILE_SIZE, METADATA_FILE_MODIFICATION_TIME).foreach {
        metadataColumn =>
          val filtered = df
            .where(Column(metadataColumn) === f0(metadataColumn) && Column("id") > 0L)
            .select(METADATA_FILE_NAME, "id")
          val expected = Seq((collisionRows0, f0), (collisionRows1, f1))
            .filter { case (_, metadata) => metadata(metadataColumn) == f0(metadataColumn) }
            .flatMap {
              case (rows, metadata) =>
                rows.filter(_.getLong(6) > 0L).map {
                  row => Row(metadata(METADATA_FILE_NAME), row.getLong(6))
                }
            }
          checkMetadataAnswer(filtered, expected)
      }
      checkAnswer(
        df.where(Column(METADATA_FILE_NAME) === "missing.parquet").select(METADATA_FILE_NAME),
        Seq.empty)
  }

  metadataColumnsNativeTest(
    "metadata collisions respect case normalization",
    StructType(schemaWithMetadataCollisions.fields.map {
      case field if field.name == "file_name" => field.copy(name = "FILE_NAME")
      case field => field
    }),
    collisionRows0,
    collisionRows1
  ) {
    (df, f0, f1) =>
      Seq("false", "true").foreach {
        caseSensitive =>
          withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
            checkMetadataAnswer(
              df.select(METADATA_FILE_NAME),
              Seq.fill(collisionRows0.size)(Row(f0(METADATA_FILE_NAME))) ++
                Seq.fill(collisionRows1.size)(Row(f1(METADATA_FILE_NAME)))
            )
          }
      }
  }

  metadataColumnsNativeTest(
    "metadata with an explicit positional file schema",
    schemaWithMetadataCollisions,
    collisionRows0,
    collisionRows1) {
    (df, f0, f1) =>
      withSQLConf(GlutenConfig.VELOX_PARQUET_USE_COLUMN_NAMES -> "false") {
        checkMetadataAnswer(
          df.select(METADATA_FILE_NAME),
          Seq.fill(collisionRows0.size)(Row(f0(METADATA_FILE_NAME))) ++
            Seq.fill(collisionRows1.size)(Row(f1(METADATA_FILE_NAME)))
        )
        checkMetadataAnswer(
          df.select(METADATA_FILE_NAME, "id"),
          collisionRows0.map(row => Row(f0(METADATA_FILE_NAME), row.getLong(6))) ++
            collisionRows1.map(row => Row(f1(METADATA_FILE_NAME), row.getLong(6)))
        )
      }
  }

  testGluten("metadata constants with partition-only projection and pruning") {
    withTempPath {
      path =>
        spark
          .range(4)
          .selectExpr("id AS file_name", "cast(id % 2 as int) AS p")
          .coalesce(1)
          .write
          .partitionBy("p")
          .parquet(path.getCanonicalPath)
        val df = spark.read.parquet(path.getCanonicalPath)
        val expected = withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          df.select(METADATA_FILE_NAME, "p").collect().toSeq
        }
        checkMetadataAnswer(df.select(METADATA_FILE_NAME), expected.map(row => Row(row.get(0))))
        checkMetadataAnswer(df.select(METADATA_FILE_NAME, "p"), expected)
        checkMetadataAnswer(
          df.where(Column("p") === 1).select(METADATA_FILE_NAME, "p"),
          expected.filter(_.getInt(1) == 1))
    }
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
