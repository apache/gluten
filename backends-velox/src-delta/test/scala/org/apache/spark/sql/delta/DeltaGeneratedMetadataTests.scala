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
package org.apache.spark.sql.delta

import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}
import org.apache.gluten.execution.DeltaScanTransformer
import org.apache.gluten.extension.OffloadDeltaScan

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

/** The versioned suites supply only the Delta 3.3/4.0 relation-construction bridge. */
trait DeltaGeneratedMetadataTests {
  self: QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper =>

  import testImplicits._

  protected def generatedMetadataDataFrame(
      path: String,
      fields: Seq[StructField],
      filterType: Option[RowIndexFilterType] = None): DataFrame

  private val rowIndexField = DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD
  private val deletedField = DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD
  private val generatedFields = Seq(rowIndexField, deletedField)
  private val metadataRowIndexKey = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX.key

  private def generatedScans(df: DataFrame): Seq[DeltaScanTransformer] = {
    val plan = df.queryExecution.executedPlan
    val scans = collectWithSubqueries(plan) { case scan: DeltaScanTransformer => scan }
    assert(scans.nonEmpty, plan.treeString)
    assert(scans.forall(_.generatesDeletionVectorMetadata), plan.treeString)
    val generatedNames = generatedFields.map(_.name).toSet + "_tmp_metadata_row_index"
    assert(
      scans.forall(!_.filterExprs().exists(_.references.exists(
        attr => generatedNames.contains(attr.name)))),
      plan.treeString)
    scans
  }

  private def writeGeneratedMetadataFixture(
      path: String,
      values: Seq[Int],
      enableDvs: Boolean = false,
      append: Boolean = false): Unit = {
    withSQLConf(
      GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false",
      VeloxDeltaConfig.ENABLE_NATIVE_WRITE.key -> "false") {
      values.toDF("value").coalesce(1).sortWithinPartitions("value").write
        .format("delta")
        .mode(if (append) "append" else "error")
        .option("delta.enableDeletionVectors", enableDvs.toString)
        .option("parquet.block.size", "4096")
        .option("parquet.page.size", "1024")
        .option("parquet.enable.dictionary", "false")
        .save(path)
    }
  }

  for {
    fields <- Seq(Seq(rowIndexField), Seq(deletedField), generatedFields)
    (vectorized, batchAsRows) <- Seq((false, true), (true, false), (true, true))
  } {
    test(
      s"native DV-free generated metadata ${fields.map(_.name).mkString(",")}, " +
        s"vectorized=$vectorized, batchAsRows=$batchAsRows") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          writeGeneratedMetadataFixture(path, 0 until 23)
          withSQLConf(
            metadataRowIndexKey -> "false",
            "spark.sql.parquet.enableVectorizedReader" -> vectorized.toString,
            "spark.sql.codegen.maxFields" -> (if (batchAsRows) "0" else "100"),
            GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "7"
          ) {
            val df = generatedMetadataDataFrame(path, fields)
            generatedScans(df)
            checkAnswer(
              df,
              (0 until 23).map {
                value =>
                  Row.fromSeq(Seq(value) ++ fields.map {
                    case field if field.name == rowIndexField.name => value.toLong
                    case _ => 0.toByte
                  })
              })
          }
      }
    }
  }

  test("native Delta temporary row index and ordinary scans with metadata row index disabled") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        writeGeneratedMetadataFixture(path, 0 until 23)
        withSQLConf(metadataRowIndexKey -> "false") {
          val field = StructField("_tmp_metadata_row_index", LongType)
          val df = generatedMetadataDataFrame(path, Seq(field))
          generatedScans(df)
          checkAnswer(df, (0 until 23).map(value => Row(value, value.toLong)))
          val ordinary = spark.read.format("delta").load(path)
          assert(
            collectWithSubqueries(ordinary.queryExecution.executedPlan) {
              case scan: DeltaScanTransformer => scan
            }.nonEmpty)
          checkAnswer(ordinary, (0 until 23).map(Row(_)))
        }
    }
  }

  test("native generated metadata retains deleted rows through batches and row-group pruning") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        val deleted = Set(0, 1026, 2052, 4095, 5999)
        writeGeneratedMetadataFixture(path, 0 until 6000, enableDvs = true)
        withSQLConf(metadataRowIndexKey -> "true") {
          spark.sql(s"DELETE FROM delta.`$path` WHERE value IN (${deleted.mkString(",")})")
        }
        val log = DeltaLog.forTable(spark, new Path(path))
        val file = log.update().allFiles.collect().head
        assert(file.deletionVector != null)
        val footer = ParquetFileReader.readFooter(
          spark.sessionState.newHadoopConf(),
          new Path(log.dataPath, file.path),
          ParquetMetadataConverter.NO_FILTER)
        assert(footer.getBlocks.size() > 1, "fixture must have multiple Parquet row groups")

        withSQLConf(
          metadataRowIndexKey -> "false",
          GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "7") {
          val df = generatedMetadataDataFrame(path, generatedFields)
          generatedScans(df)
          def expected(value: Int): Row =
            Row(value, value.toLong, (if (deleted.contains(value)) 1 else 0).toByte)
          checkAnswer(df, (0 until 6000).map(expected))

          val pruned = df.filter("value >= 1024 AND value < 4096 AND value % 19 = 0")
          generatedScans(pruned)
          checkAnswer(pruned, (1024 until 4096).filter(_ % 19 == 0).map(expected))
          val marked = df.filter(s"${deletedField.name} = 1")
          generatedScans(marked)
          checkAnswer(marked, deleted.toSeq.map(expected))
          checkAnswer(
            df.filter(s"${rowIndexField.name} = 2052"),
            Seq(expected(2052)))
        }
    }
  }

  Seq(RowIndexFilterType.IF_CONTAINED, RowIndexFilterType.IF_NOT_CONTAINED).foreach {
    filterType =>
      test(s"native generated flags honor Tahoe $filterType and mixed DV-free files") {
        withTempDir {
          dir =>
            val path = dir.getCanonicalPath
            writeGeneratedMetadataFixture(path, 0 until 6, enableDvs = true)
            withSQLConf(metadataRowIndexKey -> "true") {
              spark.sql(s"DELETE FROM delta.`$path` WHERE value IN (0, 2)")
            }
            writeGeneratedMetadataFixture(path, 100 until 103, enableDvs = true, append = true)
            withSQLConf(
              metadataRowIndexKey -> "false",
              GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key -> "2") {
              val df = generatedMetadataDataFrame(path, generatedFields, Some(filterType))
              val scans = generatedScans(df)
              assert(scans.forall(_.relation.location.isInstanceOf[TahoeBatchFileIndex]))
              val expected = (0 until 6).map {
                value =>
                  val contained = value == 0 || value == 2
                  val drop =
                    if (filterType == RowIndexFilterType.IF_CONTAINED) contained else !contained
                  Row(value, value.toLong, (if (drop) 1 else 0).toByte)
              } ++ (100 until 103).map(value => Row(value, (value - 100).toLong, 0.toByte))
              checkAnswer(df, expected)
              val keep = df.filter(s"${deletedField.name} = 0")
              generatedScans(keep)
              checkAnswer(keep, expected.filter(_.getByte(2) == 0))
              assert(scans.map(_.metrics("dvPayloadReadAttempts").value).sum > 0)
            }
        }
      }
  }

  test("generated Delta metadata validates both output and required schema by exact name") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        writeGeneratedMetadataFixture(path, 0 until 3)
        withSQLConf(metadataRowIndexKey -> "false") {
          val scan = generatedScans(generatedMetadataDataFrame(path, generatedFields)).head
          val requiredOnly = scan.copy(output = scan.output.filterNot(_.name == rowIndexField.name))
          assert(requiredOnly.generatesDeletionVectorMetadata)
          assert(!requiredOnly.doValidate().ok())
          val outputOnly = scan.copy(
            requiredSchema =
              StructType(scan.requiredSchema.filterNot(_.name == rowIndexField.name)))
          assert(outputOnly.generatesDeletionVectorMetadata)
          assert(outputOnly.doValidate().ok())
          val invalid = scan.copy(requiredSchema = scan.requiredSchema
            .add(StructField(rowIndexField.name, StringType)))
          assert(!invalid.doValidate().ok())
        }
    }
  }

  test("native generated metadata preserves name column mapping") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        withSQLConf(
          "spark.databricks.delta.properties.defaults.columnMapping.mode" -> "name") {
          writeGeneratedMetadataFixture(path, 0 until 6, enableDvs = true)
        }
        withSQLConf(metadataRowIndexKey -> "true") {
          spark.sql(s"DELETE FROM delta.`$path` WHERE value IN (1, 4)")
        }
        withSQLConf(metadataRowIndexKey -> "false") {
          val df = generatedMetadataDataFrame(path, generatedFields)
          generatedScans(df)
          checkAnswer(
            df,
            (0 until 6).map(
              value =>
                Row(value, value.toLong, (if (value == 1 || value == 4) 1 else 0).toByte)))
        }
    }
  }

  test("generated metadata survives projections joining an ordinary native Delta scan") {
    withTempDir {
      dir =>
        val markedPath = new Path(dir.getCanonicalPath, "marked").toString
        val ordinaryPath = new Path(dir.getCanonicalPath, "ordinary").toString
        writeGeneratedMetadataFixture(markedPath, 0 until 6, enableDvs = true)
        writeGeneratedMetadataFixture(ordinaryPath, 0 until 6)
        withSQLConf(metadataRowIndexKey -> "true") {
          spark.sql(s"DELETE FROM delta.`$markedPath` WHERE value IN (0, 2)")
        }
        withSQLConf(metadataRowIndexKey -> "false") {
          val marked = generatedMetadataDataFrame(markedPath, generatedFields).as("marked")
          val ordinary = spark.read.format("delta").load(ordinaryPath).as("ordinary")
          val joined = marked
            .join(ordinary, marked("value") === ordinary("value"))
            .select(marked("value"), marked(rowIndexField.name), marked(deletedField.name))
          val scans = collectWithSubqueries(joined.queryExecution.executedPlan) {
            case scan: DeltaScanTransformer => scan
          }
          assert(scans.exists(_.generatesDeletionVectorMetadata))
          assert(scans.exists(!_.generatesDeletionVectorMetadata))
          val expected = (0 until 6).map {
            value => Row(value, value.toLong, (if (value == 0 || value == 2) 1 else 0).toByte)
          }
          checkAnswer(joined, expected)
          checkAnswer(
            joined.filter(s"${deletedField.name} = 1"),
            expected.filter(_.getByte(2) == 1))
        }
    }
  }

  test("native generated row indexes accumulate DVs while retaining the DML escape hatch") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        writeGeneratedMetadataFixture(path, 0 until 8, enableDvs = true)
        withSQLConf(metadataRowIndexKey -> "true") {
          spark.sql(s"DELETE FROM delta.`$path` WHERE value IN (0, 1)")
        }
        withSQLConf(metadataRowIndexKey -> "false") {
          val plans = DeltaTestUtils.withAllPlansCaptured(spark) {
            spark.sql(s"DELETE FROM delta.`$path` WHERE value IN (2, 3)").collect()
          }.map(_.executedPlan)
          assert(
            plans.exists {
              plan =>
                collectWithSubqueries(plan) {
                  case scan: DeltaScanTransformer if scan.generatesDeletionVectorMetadata =>
                    scan
                }.nonEmpty
            },
            plans.map(_.treeString).mkString("\n")
          )
          val files = DeltaLog.forTable(spark, new Path(path)).update().allFiles.collect()
          assert(files.flatMap(file => Option(file.deletionVector).map(_.cardinality)).sum == 4L)

          withSQLConf(VeloxDeltaConfig.ENABLE_NATIVE_DML_ROW_INDEX_SCAN.key -> "false") {
            val fallbackPlans = DeltaTestUtils.withAllPlansCaptured(spark) {
              spark.sql(s"DELETE FROM delta.`$path` WHERE value = 4").collect()
            }.map(_.executedPlan)
            assert(
              !fallbackPlans.exists {
                plan =>
                  collectWithSubqueries(plan) {
                    case scan: DeltaScanTransformer => scan
                  }.nonEmpty
              },
              fallbackPlans.map(_.treeString).mkString("\n")
            )
          }
          val remaining = spark.read.format("delta").load(path)
          generatedScans(remaining)
          checkAnswer(remaining, (5 until 8).map(Row(_)))
        }
    }
  }

  test("DV-bearing scans without generated deleted-row output retain explicit fallback") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        writeGeneratedMetadataFixture(path, 0 until 6, enableDvs = true)
        withSQLConf(metadataRowIndexKey -> "true") {
          spark.sql(s"DELETE FROM delta.`$path` WHERE value = 0")
        }
        withSQLConf(metadataRowIndexKey -> "false", "spark.gluten.enabled" -> "false") {
          val plan =
            generatedMetadataDataFrame(path, generatedFields).queryExecution.executedPlan
          val scan =
            collectWithSubqueries(plan) { case scan: FileSourceScanExec => scan }.head
          val withoutFlags = scan.copy(
            output = scan.output.filterNot(_.name == deletedField.name),
            requiredSchema =
              StructType(scan.requiredSchema.filterNot(_.name == deletedField.name)))
          assert(OffloadDeltaScan(enableNativeDmlRowIndexScan =
            true).offload(withoutFlags) eq withoutFlags)
        }
    }
  }
}
