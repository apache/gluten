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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil

import org.apache.arrow.vector.types.{TimeUnit => ArrowTimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.{TimestampLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.json4s.jackson.JsonMethods.parse

import java.io.File
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import scala.collection.JavaConverters._

class GlutenDeltaTimestampNtzWriteSuite extends GlutenQueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite", "true")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.optimizeWrite.enabled", "false")
      .set("spark.databricks.delta.properties.defaults.writePartitionColumnsToParquet", "false")
      .set(SQLConf.ANSI_ENABLED.key, "false")
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key, "0")
      .set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "TIMESTAMP_MICROS")
      .set(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key, "CORRECTED")
      .set(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key, "2")

  private val timestampPair = new StructType()
    .add("ntz", TimestampNTZType)
    .add("ltz", TimestampType)
  private val mixedSchema = new StructType()
    .add("id", LongType)
    .add("ntz", TimestampNTZType)
    .add("ltz", TimestampType)
    .add("nested", timestampPair)
    .add("items", ArrayType(timestampPair))
    .add("mapping", MapType(TimestampNTZType, timestampPair))

  private val times = Seq(
    "1500-01-01T00:00:00.123456",
    "1969-12-31T23:59:59.999999",
    "1970-01-01T00:00:00.000001",
    "2024-03-10T02:30:00.123456",
    "2024-11-03T01:30:00.123456",
    "2024-11-03T01:30:00.123457"
  ).map(LocalDateTime.parse) :+ null

  private def mixedRows: Seq[Row] = times.zipWithIndex.map {
    case (null, i) => Row(i.toLong, null, null, null, null, null)
    case (ntz, i) =>
      val ltz = Timestamp.from(ntz.toInstant(ZoneOffset.UTC))
      val pair = Row(ntz, ltz)
      Row(i.toLong, ntz, ltz, pair, Seq(pair, null), Map(ntz -> pair, ntz.plusSeconds(1) -> null))
  }

  private def dataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)

  private def files(path: String): Seq[AddFile] =
    DeltaLog.forTable(spark, path).update().allFiles.collect().toSeq

  private def write(
      data: DataFrame,
      path: String,
      partitioned: Boolean = false,
      append: Boolean = false): Unit = {
    val writer = data.coalesce(1).write.format("delta").mode(if (append) "append" else "error")
    if (partitioned) {
      writer.partitionBy("part")
    }
    writer.save(path)
  }

  private def checkNativeWrite(
      path: String,
      reference: String,
      schema: StructType,
      expected: Seq[Row],
      timestampLeaves: Int,
      copies: Int = 1): Unit =
    // Both writers use the corrected calendar; native files have no Spark rebase metadata.
    withSQLConf(
      GlutenConfig.GLUTEN_ENABLED.key -> "false",
      SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> "CORRECTED") {
      val referenceFiles = files(reference)
      val byPartition = referenceFiles.map(file => file.partitionValues -> file).toMap
      assert(byPartition.size == referenceFiles.size)
      val writtenFiles = files(path)
      assert(writtenFiles.size == referenceFiles.size * copies)
      assert(writtenFiles.map(_.partitionValues).toSet == byPartition.keySet)
      writtenFiles.foreach {
        file =>
          val referenceFile = byPartition(file.partitionValues)
          assert(new Path(file.path).getParent == new Path(referenceFile.path).getParent, file.path)
          val physicalPath = DeltaFileOperations.absolutePath(path, file.path)
          val reader = ParquetFileReader.open(
            HadoopInputFile.fromPath(physicalPath, spark.sessionState.newHadoopConf()))
          try {
            val footer = reader.getFooter
            assert(
              footer.getFileMetaData.getCreatedBy.startsWith("parquet-cpp-velox"),
              s"${file.path}: ${footer.getFileMetaData}")
            val parquetSchema = footer.getFileMetaData.getSchema
            val timestamps = parquetSchema.getColumns.asScala.filter {
              _.getPrimitiveType.getLogicalTypeAnnotation.isInstanceOf[
                TimestampLogicalTypeAnnotation]
            }
            assert(timestamps.size == timestampLeaves, parquetSchema)
            timestamps.foreach {
              column =>
                val primitive = column.getPrimitiveType
                assert(primitive.getPrimitiveTypeName == PrimitiveTypeName.INT64, column)
                assert(
                  primitive.getLogicalTypeAnnotation ==
                    LogicalTypeAnnotation.timestampType(
                      column.getPath.last == "ltz",
                      TimeUnit.MICROS),
                  column)
            }
            assert(
              footer.getBlocks.asScala.map(_.getRowCount).sum == file.numLogicalRecords.get,
              file.stats)
          } finally {
            reader.close()
          }
          // Neither these readers nor the reference writer use Gluten.
          val actual = spark.read.parquet(physicalPath.toString)
          val dataSchema = StructType(schema.filterNot(f => file.partitionValues.contains(f.name)))
          assert(actual.schema.sameType(dataSchema), actual.schema)
          checkAnswer(
            actual,
            spark.read.parquet(
              DeltaFileOperations.absolutePath(reference, referenceFile.path).toString))
          assert(parse(file.stats).values == parse(referenceFile.stats).values, file.stats)
      }
      checkAnswer(spark.read.format("delta").load(path), expected)
    }

  test("Arrow schemas preserve the native NTZ logical type at every depth") {
    val schema = SparkArrowUtil.toArrowSchema(mixedSchema, "America/Los_Angeles")
    def timestamps(field: Field): Seq[Field] =
      (if (field.getType.isInstanceOf[ArrowType.Timestamp]) Seq(field) else Seq.empty) ++
        field.getChildren.asScala.flatMap(timestamps)
    val timestampFields = schema.getFields.asScala.flatMap(timestamps)
    assert(timestampFields.size == 9)
    timestampFields.foreach {
      field =>
        val metadata = field.getMetadata.asScala
        if (field.getName == "ltz") {
          assert(field.getType == new ArrowType.Timestamp(ArrowTimeUnit.MICROSECOND, "UTC"))
          assert(!metadata.contains("velox.logical_timestamp"))
        } else {
          assert(field.getType == new ArrowType.Timestamp(ArrowTimeUnit.MICROSECOND, null))
          assert(metadata.get("velox.logical_timestamp").contains("utc"), field)
        }
    }
    assert(SparkArrowUtil.fromArrowSchema(schema) == mixedSchema)
  }

  test("native statistics can project ordinary fields from a mixed NTZ struct") {
    assume(BackendTestUtils.isVeloxBackendLoaded())
    withTempDir {
      dir =>
        val source = new File(dir, "source").getCanonicalPath
        val ntz = times(4)
        val ltz = Timestamp.from(ntz.toInstant(ZoneOffset.UTC))
        val schema = new StructType().add(
          "mixed",
          timestampPair.add("plain", new StructType().add("ltz", TimestampType)))
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          dataFrame(
            Seq(Row(Row(ntz, ltz, Row(ltz))), Row(Row(ntz, null, Row(null))), Row(null)),
            schema).write.parquet(source)
        }
        for (validateNtz <- Seq(false, true)) {
          withSQLConf(
            "spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation" ->
              validateNtz.toString) {
            val input = spark.read.parquet(source)
            val projected = input.selectExpr(
              "mixed.ltz AS direct",
              "mixed.plain.ltz AS deep",
              "mixed.ntz",
              "isnull(mixed.ltz)",
              "isnotnull(mixed.plain.ltz)")
            def hasNativeProject(data: DataFrame): Boolean = {
              val plan = data.queryExecution.executedPlan
              plan.exists {
                node =>
                  node.getClass.getSimpleName == "ProjectExecTransformer" &&
                  node.outputSet == plan.outputSet
              }
            }
            assert(
              hasNativeProject(projected) == !validateNtz,
              projected.queryExecution.executedPlan.treeString)
            checkAnswer(
              projected,
              Seq(
                Row(ltz, ltz, ntz, false, true),
                Row(null, null, ntz, true, false),
                Row(null, null, null, true, false)))
            assert(!hasNativeProject(input.selectExpr("to_json(mixed)")))
          }
        }
    }
  }

  for (
    timeZone <- Seq("UTC", "America/Los_Angeles", "Asia/Kathmandu");
    nativeInput <- Seq(false, true)
  ) {
    test(s"native NTZ Delta data and statistics: timezone=$timeZone, nativeInput=$nativeInput") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        withTempDir {
          dir =>
            val path = new File(dir, "native").getCanonicalPath
            val reference = new File(dir, "reference").getCanonicalPath
            val source = new File(dir, "source").getCanonicalPath
            val expected = mixedRows
            val rows = dataFrame(expected, mixedSchema)
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              write(rows, reference)
              if (nativeInput) {
                rows.write.parquet(source)
              }
            }
            val input = if (nativeInput) {
              val scan = spark.read.parquet(source)
              assert(
                scan.queryExecution.executedPlan.exists(
                  _.getClass.getSimpleName == "FileSourceScanExecTransformer"),
                scan.queryExecution.executedPlan.treeString)
              scan
            } else {
              rows
            }
            write(input, path)
            write(input, path, append = true)
            checkNativeWrite(path, reference, mixedSchema, expected ++ expected, 9, copies = 2)
        }
      }
    }
  }

  for (
    (timestampType, rebaseMode) <-
      Seq(("INT96", "LEGACY"), ("TIMESTAMP_MILLIS", "EXCEPTION"), ("TIMESTAMP_MICROS", "CORRECTED"))
  ) {
    test(s"native NTZ ignores LTZ encoding: $timestampType / $rebaseMode") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSQLConf(
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> timestampType,
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> rebaseMode,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles"
      ) {
        withTempDir {
          dir =>
            val path = new File(dir, "native").getCanonicalPath
            val reference = new File(dir, "reference").getCanonicalPath
            val schema = new StructType().add("id", LongType).add("ntz", TimestampNTZType)
            val expected = times.zipWithIndex.map { case (ntz, i) => Row(i.toLong, ntz) }
            val input = dataFrame(expected, schema)
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              write(input, reference)
            }
            write(input, path)
            checkNativeWrite(path, reference, schema, expected, 1)
        }
      }
    }
  }

  for (
    partitionType <- Seq(IntegerType, TimestampNTZType, TimestampType);
    includeNested <- Seq(false, true)
  ) {
    test(s"native NTZ Delta partitioned files: type=$partitionType, nested=$includeNested") {
      assume(BackendTestUtils.isVeloxBackendLoaded())
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        withTempDir {
          dir =>
            val path = new File(dir, "native").getCanonicalPath
            val reference = new File(dir, "reference").getCanonicalPath
            val dataSchema =
              if (includeNested) mixedSchema else StructType(mixedSchema.fields.take(3))
            val rows =
              if (includeNested) mixedRows else mixedRows.map(row => Row.fromSeq(row.toSeq.take(3)))
            val schema = dataSchema.add("part", partitionType)
            val partitions = Seq(times(1), times(4), times(5), null)
            val expected = (rows ++ rows).zipWithIndex.map {
              case (row, i) =>
                val time = partitions(i % partitions.size)
                val partition = partitionType match {
                  case IntegerType => Int.box(i % 2)
                  case TimestampNTZType => time
                  case TimestampType =>
                    Option(time).map(t => Timestamp.from(t.toInstant(ZoneOffset.UTC))).orNull
                  case other => fail(s"Unexpected partition type: $other")
                }
                Row.fromSeq(row.toSeq.updated(0, i.toLong) :+ partition)
            }
            val input = dataFrame(expected, schema)
            withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
              write(input, reference, partitioned = true)
            }
            write(input, path, partitioned = true)
            checkNativeWrite(path, reference, schema, expected, if (includeNested) 9 else 2)
        }
      }
    }
  }
}
