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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.uniform.ParquetIcebergCompatV2Utils
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.tags.ExtendedSQLTest

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS

import java.io.File
import java.net.URI
import java.util.Locale

import scala.io.Source
import scala.util.control.NonFatal

@ExtendedSQLTest
class DeltaUniFormIcebergSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils {

  private val icebergSparkExtension =
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  private val icebergRequiredClasses = Seq(
    "org.apache.iceberg.spark.source.IcebergSource",
    icebergSparkExtension,
    "org.apache.iceberg.hive.TestHiveMetastore")

  private var metastore: AnyRef = _

  override protected def beforeAll(): Unit = {
    if (hasIcebergTestClasspath) {
      super.beforeAll()
    }
  }

  override protected def beforeEach(): Unit = {
    if (hasIcebergTestClasspath) {
      super.beforeEach()
    }
  }

  override protected def afterEach(): Unit = {
    if (hasIcebergTestClasspath) {
      super.afterEach()
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    if (!hasIcebergTestClasspath) {
      conf
    } else {
      val hiveConf = startMetastore()
      val currentExtensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "")
      val extensions =
        if (currentExtensions.isEmpty) icebergSparkExtension
        else s"$currentExtensions,$icebergSparkExtension"
      conf
        .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, extensions)
        .set(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
        .set("spark.sql.catalog.uniform_hive", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.uniform_hive.type", "hive")
        .set("spark.sql.catalog.uniform_hive.uri", hiveConf.get(METASTOREURIS.varname))
        .set(s"spark.hadoop.${METASTOREURIS.varname}", hiveConf.get(METASTOREURIS.varname))
    }
  }

  override def afterAll(): Unit = {
    try {
      if (hasIcebergTestClasspath) {
        super.afterAll()
      }
    } finally {
      stopMetastore()
    }
  }

  private def stopMetastore(): Unit = {
    if (metastore != null) {
      metastore.getClass.getMethod("stop").invoke(metastore)
      metastore = null
    }
  }

  private def startMetastore(): HiveConf = synchronized {
    if (metastore == null) {
      val metastoreClass = loadClass("org.apache.iceberg.hive.TestHiveMetastore")
      metastore = metastoreClass.getConstructor().newInstance().asInstanceOf[AnyRef]
      metastoreClass.getMethod("start").invoke(metastore)
    }
    metastore.getClass.getMethod("hiveConf").invoke(metastore).asInstanceOf[HiveConf]
  }

  test("write UniForm Iceberg Delta table and read through Iceberg") {
    requireIcebergTestClasspath()

    withTempDir {
      tableDir =>
        val tablePath = tableDir.getCanonicalPath
        val tableName = s"uniform_delta_${math.abs(System.nanoTime())}"
        val icebergTableName = s"uniform_hive.default.$tableName"
        withTable(tableName) {
          sql(s"""
                 |CREATE TABLE $tableName (
                 |  id BIGINT,
                 |  name STRING,
                 |  ts TIMESTAMP,
                 |  payload STRUCT<
                 |    items: ARRAY<STRUCT<sku: STRING, qty: INT>>,
                 |    attrs: MAP<STRING, STRING>>,
                 |  part INT)
                 |USING DELTA
                 |PARTITIONED BY (part)
                 |LOCATION '$tablePath'
                 |TBLPROPERTIES (
                 |  'delta.columnMapping.mode' = 'name',
                 |  'delta.minReaderVersion' = '2',
                 |  'delta.minWriterVersion' = '7',
                 |  'delta.enableIcebergCompatV2' = 'true',
                 |  'delta.universalFormat.enabledFormats' = 'iceberg')
                 |""".stripMargin)

          sql(s"""
                 |INSERT INTO $tableName VALUES
                 |  (1, 'alice', TIMESTAMP '2024-01-01 00:00:00',
                 |   named_struct(
                 |     'items', array(named_struct('sku', 'a-1', 'qty', 2)),
                 |     'attrs', map('source', 'gluten')),
                 |   0),
                 |  (2, 'bob', TIMESTAMP '2024-01-02 00:00:00',
                 |   named_struct(
                 |     'items', array(named_struct('sku', 'b-1', 'qty', 3)),
                 |     'attrs', map('source', 'velox')),
                 |   1)
                 |""".stripMargin)

          val dataFile = listFiles(tableDir).find(_.getName.endsWith(".parquet")).getOrElse {
            fail(s"No Parquet data file found under $tablePath")
          }
          val parquetFooter =
            ParquetIcebergCompatV2Utils.getParquetFooter(dataFile.getCanonicalPath)
          assert(ParquetIcebergCompatV2Utils.isParquetIcebergCompatV2(parquetFooter))

          val latestDeltaVersion = sql(s"DESCRIBE HISTORY $tableName")
            .select("version")
            .collect()
            .map(_.getLong(0))
            .max
          val metadataFile =
            waitForIcebergMetadata(tableDir, tableName, icebergTableName, latestDeltaVersion)
          val metadata = readFile(metadataFile)
          assert(metadata.contains(deltaVersionProperty(latestDeltaVersion)))
          assert(metadata.contains("delta-timestamp"))
          assert(!metadata.contains("\"current-snapshot-id\" : -1"))

          checkAnswer(
            spark.read
              .format("iceberg")
              .load(icebergTableName)
              .select("id", "name", "part")
              .orderBy("id"),
            Seq(Row(1L, "alice", 0), Row(2L, "bob", 1)))
        }
    }
  }

  test("UniForm Iceberg rejects active deletion vectors") {
    requireIcebergTestClasspath()

    withTempDir {
      tableDir =>
        val tablePath = tableDir.getCanonicalPath
        withDeletionVectorsEnabled() {
          sql(s"""
                 |CREATE TABLE delta.`$tablePath` (id BIGINT)
                 |USING DELTA
                 |TBLPROPERTIES ('${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'true')
                 |""".stripMargin)
          sql(s"INSERT INTO delta.`$tablePath` VALUES (1), (2)")
          sql(s"DELETE FROM delta.`$tablePath` WHERE id = 1")
        }

        val error = intercept[Throwable] {
          sql(s"""
                 |ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES (
                 |  'delta.columnMapping.mode' = 'name',
                 |  'delta.minReaderVersion' = '2',
                 |  'delta.minWriterVersion' = '7',
                 |  'delta.enableIcebergCompatV2' = 'true',
                 |  'delta.universalFormat.enabledFormats' = 'iceberg')
                 |""".stripMargin)
        }
        val message = Option(error.getMessage).getOrElse("").toLowerCase(Locale.ROOT)
        assert(
          message.contains("deletion vector") ||
            message.contains("iceberg") ||
            message.contains("uniform"))
    }
  }

  private def requireIcebergTestClasspath(): Unit = {
    assume(
      hasIcebergTestClasspath,
      s"requires ${icebergRequiredClasses.mkString(", ")} on the test classpath"
    )
  }

  private def hasIcebergTestClasspath: Boolean =
    icebergRequiredClasses.forall(hasClass)

  private def hasClass(className: String): Boolean = {
    try {
      loadClass(className)
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def loadClass(className: String): Class[_] =
    Class.forName(className, false, Thread.currentThread().getContextClassLoader)

  private def waitForIcebergMetadata(
      tableDir: File,
      deltaTableName: String,
      icebergTableName: String,
      expectedDeltaVersion: Long): File = {
    val deadline = System.nanoTime() + 30L * 1000L * 1000L * 1000L
    var files = listIcebergMetadataFiles(tableDir, deltaTableName)
    var metadataLocation = getIcebergMetadataLocation(icebergTableName)
    var convertedMetadata = findConvertedMetadata(files, metadataLocation, expectedDeltaVersion)
    while (convertedMetadata.isEmpty && System.nanoTime() < deadline) {
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      Thread.sleep(500)
      files = listIcebergMetadataFiles(tableDir, deltaTableName)
      metadataLocation = getIcebergMetadataLocation(icebergTableName)
      convertedMetadata = findConvertedMetadata(files, metadataLocation, expectedDeltaVersion)
    }
    convertedMetadata.getOrElse {
      fail(
        s"No Iceberg metadata file for Delta version $expectedDeltaVersion generated under " +
          s"${new File(tableDir, "metadata")}. " +
          s"${icebergMetadataDebug(tableDir, deltaTableName, icebergTableName)}")
    }
  }

  private def findConvertedMetadata(
      files: Seq[File],
      metadataLocation: Option[String],
      expectedDeltaVersion: Long): Option[File] = {
    val candidates = files ++ metadataLocation.map(metadataLocationToFile)
    candidates.reverse.find {
      file =>
        file.isFile && {
          val metadata = readFile(file)
          metadata.contains(deltaVersionProperty(expectedDeltaVersion)) &&
          metadata.contains("delta-timestamp") &&
          !metadata.contains("\"current-snapshot-id\" : -1")
        }
    }
  }

  private def deltaVersionProperty(version: Long): String =
    "\"" + "delta-version" + "\" : \"" + version + "\""

  private def getIcebergMetadataLocation(tableName: String): Option[String] = {
    try {
      spark
        .sql(s"SHOW TBLPROPERTIES $tableName")
        .collect()
        .find(row => row.getString(0) == IcebergConstants.ICEBERG_TBLPROP_METADATA_LOCATION)
        .map(_.getString(1))
        .filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }
  }

  private def metadataLocationToFile(location: String): File = {
    val uri = new URI(location)
    if (uri.getScheme == null) new File(location) else new File(uri)
  }

  private def icebergMetadataDebug(
      tableDir: File,
      deltaTableName: String,
      icebergTableName: String): String = {
    val deltaProperties = spark
      .sql(s"SHOW TBLPROPERTIES $deltaTableName")
      .collect()
      .map(row => s"${row.getString(0)}=${row.getString(1)}")
      .sorted
      .mkString("[", ", ", "]")
    val icebergProperties =
      try {
        spark
          .sql(s"SHOW TBLPROPERTIES $icebergTableName")
          .collect()
          .map(row => s"${row.getString(0)}=${row.getString(1)}")
          .sorted
          .mkString("[", ", ", "]")
      } catch {
        case NonFatal(e) => s"unavailable: ${e.getMessage}"
      }
    val nearbyMetadata = listIcebergMetadataFiles(tableDir, deltaTableName)
      .map(_.getCanonicalPath)
      .sorted
      .mkString("[", ", ", "]")
    s"Delta table properties: $deltaProperties. " +
      s"Iceberg table properties: $icebergProperties. Nearby metadata files: $nearbyMetadata"
  }

  private def listIcebergMetadataFiles(tableDir: File, tableName: String): Seq[File] = {
    val deltaMetadataDir = new File(tableDir, "metadata")
    val metadataFiles =
      Option(deltaMetadataDir.listFiles())
        .getOrElse(Array.empty)
        .filter(file => file.isFile && file.getName.endsWith(".metadata.json")) ++
        Option(tableDir.getParentFile)
          .map(listFiles)
          .getOrElse(Seq.empty)
          .filter(
            file =>
              file.isFile &&
                file.getName.endsWith(".metadata.json") &&
                file.getCanonicalPath.contains(tableName))

    metadataFiles
      .groupBy(_.getCanonicalPath)
      .values
      .map(_.head)
      .toSeq
      .sortBy(_.getName)
  }

  private def listFiles(root: File): Seq[File] = {
    Option(root.listFiles()).getOrElse(Array.empty).toSeq.flatMap {
      file => if (file.isDirectory) listFiles(file) else Seq(file)
    }
  }

  private def readFile(file: File): String = {
    val source = Source.fromFile(file, "UTF-8")
    try source.mkString
    finally source.close()
  }
}
