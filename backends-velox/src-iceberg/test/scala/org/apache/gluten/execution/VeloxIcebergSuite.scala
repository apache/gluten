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
package org.apache.gluten.execution

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.utils.VeloxFileSystemValidationJniWrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.utils.GlutenSuiteUtils

import org.apache.iceberg.{FileScanTask, MetadataColumns, UpdateSchema}
import org.apache.iceberg.expressions.Literal
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.{ResolvingFileIO, StorageCredential}
import org.apache.iceberg.spark.{ScanTaskSetManager, Spark3Util, SparkCatalog, SparkSQLProperties, SparkTableCache}
import org.apache.iceberg.spark.source.{GlutenIcebergSourceUtil, SparkTable}
import org.apache.iceberg.types.{Type, Types}

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

class VeloxIcebergSuite extends IcebergSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.ansi.enabled", "false")

  test("iceberg metadata file paths and row positions offload in a plain batch scan") {
    withTable("iceberg_metadata_tb") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("CREATE TABLE iceberg_metadata_tb (id BIGINT) USING iceberg")
        spark.range(20).coalesce(1).writeTo("iceberg_metadata_tb").append()
      }

      runQueryAndCompare("SELECT _file FROM iceberg_metadata_tb") {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 1)
          assert(df.collect().forall(_.getString(0).endsWith(".parquet")))
      }
      runQueryAndCompare("SELECT _file, _pos FROM iceberg_metadata_tb") {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 1)
          val scan = getExecutedPlan(df).collect { case scan: IcebergScanTransformer => scan }.head
          assert(scan.scan.getClass.getSimpleName == "SparkBatchQueryScan")
          assert(scan.output.filter(_.name == "_file").forall(_.isMetadataCol))
          val rows = df.collect()
          assert(rows.map(_.getString(0)).distinct.length == 1)
          assert(rows.forall(_.getString(0).endsWith(".parquet")))
          assert(rows.map(_.getLong(1)).sorted.toSeq == (0L until 20L))
      }
      runQueryAndCompare("SELECT _pos FROM iceberg_metadata_tb") {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 1)
          checkAnswer(df, (0L until 20L).map(Row(_)))
      }
    }
  }

  test("iceberg metadata columns offload after schema evolution") {
    withTable("iceberg_metadata_evolved_tb") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("CREATE TABLE iceberg_metadata_evolved_tb (id BIGINT) USING iceberg")
        spark.range(20).coalesce(1).writeTo("iceberg_metadata_evolved_tb").append()
        spark.sql("ALTER TABLE iceberg_metadata_evolved_tb ADD COLUMN data STRING")
      }

      runQueryAndCompare("SELECT id, data, _file, _pos FROM iceberg_metadata_evolved_tb") {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 1)
          val rows = df.collect()
          assert(rows.map(_.getLong(0)).sorted.toSeq == (0L until 20L))
          assert(rows.forall(row => row.isNullAt(1) && row.getLong(0) == row.getLong(3)))
          assert(rows.map(_.getString(2)).distinct.length == 1)
      }
    }
  }

  test("iceberg metadata row positions are file-relative across splits and filters") {
    withTable("iceberg_metadata_split_tb") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_metadata_split_tb (id BIGINT, data STRING)
                    |USING iceberg
                    |TBLPROPERTIES (
                    |  'write.parquet.row-group-size-bytes' = '1024',
                    |  'write.parquet.compression-codec' = 'uncompressed',
                    |  'write.distribution-mode' = 'none',
                    |  'read.split.target-size' = '1024',
                    |  'read.split.open-file-cost' = '0')
                    |""".stripMargin)
        spark.range(0, 2000, 1, 1)
          .selectExpr("id", "sha2(cast(id as string), 256) AS data")
          .writeTo("iceberg_metadata_split_tb")
          .append()
      }

      Seq(
        "" -> (0L until 2000L),
        "WHERE id >= 1000" -> (1000L until 2000L),
        "WHERE _pos >= 1000 AND _pos < 1500" -> (1000L until 1500L),
        "WHERE id % 3 = 0" -> (0L until 2000L by 3)
      ).foreach {
        case (filter, expectedIds) =>
          runQueryAndCompare(
            s"SELECT id, _file, _pos FROM iceberg_metadata_split_tb $filter") {
            df =>
              checkGlutenPlanCount[IcebergScanTransformer](df, 1)
              val scans = getExecutedPlan(df).collect { case scan: IcebergScanTransformer => scan }
              assert(scans.size == 1)
              assert(
                scans.head.inputRDD.partitions.length > 1,
                "The single data file must be read using multiple splits")
              val rows = df.collect()
              assert(rows.map(_.getString(1)).distinct.length == 1)
              assert(rows.map(_.getLong(0)).sorted.toSeq == expectedIds)
              assert(rows.forall(row => !row.isNullAt(2)))
              assert(rows.forall(row => row.getLong(0) == row.getLong(2)))
          }
      }
    }
  }

  test("iceberg metadata ORC row positions fall back but file paths offload") {
    withTable("iceberg_metadata_orc_tb") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_metadata_orc_tb (id BIGINT) USING iceberg
                    |TBLPROPERTIES ('write.format.default' = 'orc')
                    |""".stripMargin)
        spark.range(20).coalesce(1).writeTo("iceberg_metadata_orc_tb").append()
      }

      runQueryAndCompare("SELECT _file FROM iceberg_metadata_orc_tb") {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 1)
          assert(df.collect().forall(_.getString(0).endsWith(".orc")))
      }
      runQueryAndCompare(
        "SELECT _file, _pos FROM iceberg_metadata_orc_tb",
        noFallBack = false) {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 0)
          val scans = getExecutedPlan(df).collect { case scan: BatchScanExec => scan }
          assert(scans.size == 1)
          assert(FallbackTags.get(scans.head.logicalLink.get).reason()
            .contains("Iceberg row position reads with ORC files are not supported"))
          assert(df.collect().map(_.getLong(1)).sorted.toSeq == (0L until 20L))
      }
    }
  }

  for (
    (dataType, value) <- Seq(
      "TIMESTAMP" -> "TIMESTAMP '2000-01-01 00:00:00'",
      "STRUCT<ts: TIMESTAMP>" -> "named_struct('ts', TIMESTAMP '2000-01-01 00:00:00')")
  ) {
    test(s"iceberg ORC timestamps fall back when projected out or dropped - $dataType") {
      withTable("iceberg_orc_timestamp_tb") {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.sql(s"""
                       |CREATE TABLE iceberg_orc_timestamp_tb (id INT, value $dataType) USING iceberg
                       |TBLPROPERTIES ('write.format.default' = 'orc')
                       |""".stripMargin)
          spark.sql(s"INSERT INTO iceberg_orc_timestamp_tb VALUES (1, $value)")
        }
        for (dropColumn <- Seq(false, true)) {
          if (dropColumn) {
            spark.sql("ALTER TABLE iceberg_orc_timestamp_tb DROP COLUMN value")
          }
          runQueryAndCompare(
            "SELECT id, _file FROM iceberg_orc_timestamp_tb",
            noFallBack = false) {
            df =>
              checkGlutenPlanCount[IcebergScanTransformer](df, 0)
              val scans = getExecutedPlan(df).collect { case scan: BatchScanExec => scan }
              assert(scans.size == 1)
              assert(FallbackTags.get(scans.head.logicalLink.get).reason()
                .contains("Iceberg ORC timestamp-with-zone reads are not supported"))
              assert(df.collect().map(_.getInt(0)).toSeq == Seq(1))
          }
        }
      }
    }
  }

  test("iceberg mixed file formats fall back before split generation") {
    withTable("iceberg_mixed_formats_tb") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("CREATE TABLE iceberg_mixed_formats_tb (id BIGINT) USING iceberg")
        spark.range(0, 3).coalesce(1).writeTo("iceberg_mixed_formats_tb").append()
        spark.sql("""
                    |ALTER TABLE iceberg_mixed_formats_tb
                    |SET TBLPROPERTIES ('write.format.default' = 'orc')
                    |""".stripMargin)
        spark.range(3, 6).coalesce(1).writeTo("iceberg_mixed_formats_tb").append()
      }
      runQueryAndCompare("SELECT id, _file FROM iceberg_mixed_formats_tb", noFallBack = false) {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 0)
          val scans = getExecutedPlan(df).collect { case scan: BatchScanExec => scan }
          assert(scans.size == 1)
          assert(FallbackTags.get(scans.head.logicalLink.get).reason()
            .contains("Iceberg scans with mixed file formats are not supported"))
          val rows = df.collect()
          assert(rows.map(_.getLong(0)).sorted.toSeq == (0L until 6L))
          assert(rows.exists(_.getString(1).endsWith(".parquet")))
          assert(rows.exists(_.getString(1).endsWith(".orc")))
      }
      runQueryAndCompare("SELECT id, _file FROM iceberg_mixed_formats_tb WHERE id >= 3") {
        df =>
          checkGlutenPlanCount[IcebergScanTransformer](df, 1)
          assert(df.collect().map(_.getLong(0)).sorted.toSeq == (3L until 6L))
      }
    }
  }

  test("iceberg partitioned scans with fresh expression IDs have the same result") {
    withTable("iceberg_canonical_scan_tb") {
      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        SparkSQLProperties.PRESERVE_DATA_GROUPING -> "true") {
        spark.sql("""
                    |CREATE TABLE iceberg_canonical_scan_tb (id INT, dep STRING) USING iceberg
                    |PARTITIONED BY (dep)
                    |""".stripMargin)
        spark.sql("INSERT INTO iceberg_canonical_scan_tb VALUES (1, 'hr'), (2, 'hardware')")
        val df = spark.sql("SELECT * FROM iceberg_canonical_scan_tb")
        val scan = getExecutedPlan(df).collect { case scan: IcebergScanTransformer => scan }.head
        assert(scan.keyGroupedPartitioning.nonEmpty)
        val newOutput = scan.output.map(_.newInstance())
        val attributes = AttributeMap(scan.output.zip(newOutput))
        val equivalent = scan.copy(
          output = newOutput,
          keyGroupedPartitioning = scan.keyGroupedPartitioning.map(_.map(_.transform {
            case attribute: AttributeReference => attributes.getOrElse(attribute, attribute)
          }))
        )
        assert(scan.sameResult(equivalent))
      }
    }
  }

  for (format <- Seq("parquet", "orc")) {
    test(
      s"iceberg metadata file path keeps its Spark metadata flag in copy-on-write MERGE - $format") {
      withTable("iceberg_metadata_merge_tb") {
        withSQLConf("spark.sql.adaptive.enabled" -> "false") {
          spark.sql(s"""
                       |CREATE TABLE iceberg_metadata_merge_tb (id INT, data STRING) USING iceberg
                       |TBLPROPERTIES (
                       |  'write.merge.mode' = 'copy-on-write',
                       |  'write.format.default' = '$format')
                       |""".stripMargin)
          spark.sql("INSERT INTO iceberg_metadata_merge_tb VALUES (1, 'a'), (2, 'b'), (3, 'c')")

          val scans = new ConcurrentLinkedQueue[IcebergScanTransformer]()
          val scanSeen = new CountDownLatch(1)
          val listener = new QueryExecutionListener {
            override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
              qe.executedPlan.collect {
                case scan: IcebergScanTransformer
                    if scan.scan.getClass.getSimpleName == "SparkCopyOnWriteScan" => scan
              }.foreach {
                scan =>
                  scans.add(scan)
                  scanSeen.countDown()
              }
            }

            override def onFailure(
                funcName: String,
                qe: QueryExecution,
                exception: Exception): Unit = {}
          }
          try {
            spark.listenerManager.register(listener)
            spark.sql("""
                        |MERGE INTO iceberg_metadata_merge_tb t
                        |USING (SELECT * FROM VALUES (2, 'updated'), (4, 'new') AS s(id, data)) s
                        |ON t.id = s.id
                        |WHEN MATCHED THEN UPDATE SET data = s.data
                        |WHEN NOT MATCHED THEN INSERT *
                        |""".stripMargin).collect()
            assert(
              scanSeen.await(10, TimeUnit.SECONDS),
              "MERGE must use a native copy-on-write scan")
            val metadata = scans.asScala.flatMap(_.output).filter {
              _.name == MetadataColumns.FILE_PATH.name()
            }.toSeq
            assert(metadata.nonEmpty)
            assert(metadata.forall(_.isMetadataCol))
          } finally {
            spark.listenerManager.unregister(listener)
          }
          checkAnswer(
            spark.sql("SELECT * FROM iceberg_metadata_merge_tb"),
            Seq(Row(1, "a"), Row(2, "updated"), Row(3, "c"), Row(4, "new")))
        }
      }
    }
  }

  private def withCredentialTable(catalogName: String)(
      f: (String, ResolvingFileIO) => Unit): Unit = {
    withTempDir {
      dir =>
        withSQLConf(
          s"spark.sql.catalog.$catalogName" -> classOf[SparkCatalog].getName,
          s"spark.sql.catalog.$catalogName.type" -> "hadoop",
          s"spark.sql.catalog.$catalogName.warehouse" -> dir.toURI.toString,
          s"spark.sql.catalog.$catalogName.io-impl" -> classOf[ResolvingFileIO].getName,
          s"spark.sql.catalog.$catalogName.cache-enabled" -> "false"
        ) {
          val catalog = spark.sessionState.catalogManager.catalog(catalogName)
            .asInstanceOf[SparkCatalog].icebergCatalog().asInstanceOf[HadoopCatalog]
          val tableName = s"$catalogName.default.t"
          try {
            withTable(tableName) {
              withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
                spark.sql(s"CREATE TABLE $tableName (id INT) USING iceberg")
                spark.sql(s"INSERT INTO $tableName VALUES (1), (2)")
              }
              f(
                tableName,
                Spark3Util.loadIcebergTable(spark, tableName).io()
                  .asInstanceOf[ResolvingFileIO])
            }
          } finally {
            try {
              GlutenSuiteUtils.waitUntilEmpty(spark.sparkContext)
            } finally {
              catalog.close()
            }
          }
        }
    }
  }

  test("iceberg vended FileIO credentials fall back") {
    withCredentialTable("vended_credentials") {
      (tableName, io) =>
        val originalProperties = io.properties().asScala.toMap
        val keys = Map("s3.access-key-id" -> "test-key", "s3.secret-access-key" -> "test-secret")
        // These are the FileIO inputs supplied by REST catalogs. Exercise the real scan path
        // without duplicating Iceberg's REST client/server implementation.
        val cases = Seq(
          ("S3 key pair", keys, Seq.empty[StorageCredential]),
          ("S3 session credentials", keys + ("s3.session-token" -> "test-token"), Seq.empty),
          (
            "storage-credentials",
            Map.empty[String, String],
            Seq(StorageCredential.create("s3://bucket/table", keys.asJava))),
          (
            "credential refresh",
            Map("client.refresh-credentials-endpoint" -> "credentials"),
            Seq.empty)
        )
        cases.foreach {
          case (name, properties, credentials) =>
            withClue(name) {
              io.initialize((originalProperties ++ properties).asJava)
              io.setCredentials(credentials.asJava)
              runQueryAndCompare(s"SELECT id FROM $tableName", noFallBack = false) {
                df =>
                  checkAnswer(df, Seq(Row(1), Row(2)))
                  checkGlutenPlanCount[IcebergScanTransformer](df, 0)
                  val scans = getExecutedPlan(df).collect { case scan: BatchScanExec => scan }
                  assert(scans.size == 1)
                  val reason = FallbackTags.get(scans.head.logicalLink.get).reason()
                  assert(reason.contains("catalog-vended credentials"))
                  assert(!reason.contains("test-secret") && !reason.contains("test-token"))
              }
            }
        }
    }
  }

  test("iceberg staged scans with vended FileIO credentials fall back") {
    withCredentialTable("staged_vended_credentials") {
      (tableName, io) =>
        io.initialize((io.properties().asScala.toMap ++ Map(
          "s3.access-key-id" -> "test-key",
          "s3.secret-access-key" -> "test-secret")).asJava)
        val table = Spark3Util.loadIcebergTable(spark, tableName)
        val taskSetId = "vended-credentials"
        val taskSetManager = ScanTaskSetManager.get()
        val tableCache = SparkTableCache.get()
        val tasks = table.newScan().planFiles()
        try {
          // Register the table under the task set ID and load by that ID, which resolves to a
          // staged scan on both Iceberg 1.10 (needs the "scan-task-set-id" option) and Iceberg
          // 1.11+ (removed SparkReadOptions.SCAN_TASK_SET_ID, resolves cached tables directly).
          tableCache.add(taskSetId, table)
          taskSetManager.stageTasks(table, taskSetId, tasks.asScala.toList.asJava)
          compareDfResultsAgainstVanillaSpark(
            () =>
              spark.read
                .format("iceberg")
                .option("scan-task-set-id", taskSetId)
                .load(taskSetId),
            noFallBack = false,
            customCheck = df => {
              checkAnswer(df, Seq(Row(1), Row(2)))
              checkGlutenPlanCount[IcebergScanTransformer](df, 0)
              val scans = getExecutedPlan(df).collect { case scan: BatchScanExec => scan }
              assert(scans.size == 1)
              assert(GlutenIcebergSourceUtil.isSparkStagedScan(scans.head.scan))
              val reason = FallbackTags.get(scans.head.logicalLink.get).reason()
              assert(reason.contains("catalog-vended credentials"))
              assert(!reason.contains("test-secret"))
            }
          )
        } finally {
          tableCache.remove(taskSetId)
          taskSetManager.removeTasks[FileScanTask](table, taskSetId)
          tasks.close()
        }
    }
  }

  test("iceberg catalog authentication and storage settings still offload") {
    withCredentialTable("catalog_settings") {
      (tableName, io) =>
        io.initialize((io.properties().asScala.toMap ++ Map(
          "credential" -> "catalog-secret",
          "token" -> "catalog-token",
          "s3.endpoint" -> "http://unused",
          "client.region" -> "us-east-1")).asJava)
        runQueryAndCompare(s"SELECT id FROM $tableName") {
          df =>
            checkAnswer(df, Seq(Row(1), Row(2)))
            checkGlutenPlanCount[IcebergScanTransformer](df, 1)
        }
    }
  }

  test("iceberg parquet split uses name mapping for projected columns") {
    withTable("iceberg_parquet_name_mapping") {
      withSQLConf(VeloxConfig.PARQUET_USE_COLUMN_NAMES.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_parquet_name_mapping (
                    |  id BIGINT,
                    |  amount DECIMAL(12, 2),
                    |  note STRING
                    |)
                    |USING iceberg
                    |TBLPROPERTIES ('write.format.default' = 'parquet')
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO iceberg_parquet_name_mapping
                    |VALUES (CAST(1 AS BIGINT), CAST(10.50 AS DECIMAL(12, 2)), 'a')
                    |""".stripMargin)

        runQueryAndCompare("SELECT amount FROM iceberg_parquet_name_mapping") {
          df =>
            checkAnswer(df, Seq(Row(BigDecimal("10.50"))))
            checkGlutenPlan[IcebergScanTransformer](df)
        }
      }
    }
  }

  test("iceberg v3 initial default for an added column") {
    withTable("iceberg_v3_initial_default") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        spark.sql("""
                    |CREATE TABLE iceberg_v3_initial_default (id INT)
                    |USING iceberg
                    |TBLPROPERTIES ('format-version' = '3')
                    |""".stripMargin)
        spark.sql("INSERT INTO iceberg_v3_initial_default VALUES (1), (2)")

        val catalog = spark.sessionState.catalogManager
          .catalog("spark_catalog")
          .asInstanceOf[TableCatalog]
        val updateSchema = catalog
          .loadTable(Identifier.of(Array("default"), "iceberg_v3_initial_default"))
          .asInstanceOf[SparkTable]
          .table()
          .updateSchema()
        classOf[UpdateSchema]
          .getMethod(
            "addColumn",
            classOf[String],
            classOf[Type],
            classOf[Literal[_]])
          .invoke(updateSchema, "country", Types.StringType.get(), Literal.of("IN"))
        updateSchema.commit()
        spark.catalog.refreshTable("iceberg_v3_initial_default")
      }

      runQueryAndCompare(
        "SELECT id, country FROM iceberg_v3_initial_default ORDER BY id") {
        df =>
          checkAnswer(df, Seq(Row(1, "IN"), Row(2, "IN")))
          checkGlutenPlan[IcebergScanTransformer](df)
      }
    }
  }

  test("iceberg root paths on an unsupported scheme fail native scheme validation") {
    // See https://github.com/apache/gluten/issues/12712: IcebergScanTransformer used to always
    // report Seq.empty root paths, so VeloxBackend.validateScanExec's scheme check was silently
    // skipped for every Iceberg scan, regardless of the scan's actual filesystem.
    withTable("iceberg_root_paths_scheme_tb") {
      spark.sql("""
                  |CREATE TABLE iceberg_root_paths_scheme_tb (id INT)
                  |USING iceberg
                  |""".stripMargin)
      spark.sql("INSERT INTO iceberg_root_paths_scheme_tb VALUES (1), (2)")

      runQueryAndCompare("SELECT * FROM iceberg_root_paths_scheme_tb") {
        df =>
          val scans = getExecutedPlan(df).collect { case i: IcebergScanTransformer => i }
          assert(scans.size == 1)
          val rootPaths = scans.head.getRootPathsInternal
          assert(rootPaths.nonEmpty)
          // Confirm these are real table root paths (registered as `file` scheme), which
          // VeloxBackendSettings.distinctRootPaths always excludes from scheme validation (the
          // local filesystem is always registered) -- so a real root path from this suite
          // passes validation, as expected for a supported local table.
          assert(
            VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
              rootPaths.toArray))

          // Since this suite only exercises the local `file` scheme, which is always
          // considered supported, separately confirm -- with a clean, synthetic URI, not
          // derived from the real paths above -- that Velox's own native filesystem check (the
          // same one VeloxBackend.validateScanExec relies on to decide whether to fall back)
          // correctly rejects a genuinely unsupported scheme.
          assert(
            !VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
              Array("unsupported-test-scheme://bucket/path/file.parquet")),
            "expected an unsupported scheme to fail native filesystem validation"
          )
      }
    }
  }
}
