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
package org.apache.gluten.utils

import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.hive.execution.HiveFileFormat

import java.util.Properties

import scala.collection.JavaConverters._

/**
 * Accessors for the Hive `TableDesc` a [[HiveFileFormat]] writes to. `HiveFileFormat` keeps its
 * `FileSinkDesc` in a private constructor parameter and `TableDesc` lives in hive-exec, so both are
 * reached through reflection.
 */
object HiveFileFormatUtil {
  val PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

  private def tableInfo(format: HiveFileFormat): AnyRef = {
    val fileSinkConfField = format.getClass.getDeclaredField("fileSinkConf")
    fileSinkConfField.setAccessible(true)
    val fileSinkConf = fileSinkConfField.get(format)
    val tableInfoField = fileSinkConf.getClass.getDeclaredField("tableInfo")
    tableInfoField.setAccessible(true)
    tableInfoField.get(fileSinkConf)
  }

  /** The Hive output format class name, e.g. [[PARQUET_OUTPUT_FORMAT]]. */
  def outputFileFormatClassName(format: HiveFileFormat): String = {
    val info = tableInfo(format)
    info.getClass.getMethod("getOutputFileFormatClassName").invoke(info).asInstanceOf[String]
  }

  /**
   * The Hive table properties, including TBLPROPERTIES such as `parquet.compression`. Vanilla Spark
   * copies these into the job configuration (`Utilities.copyTableJobPropertiesToConf`) and resolves
   * the Parquet compression codec from them in `HiveOptions.getHiveWriteCompression`, so the native
   * writer has to see them as well to produce the same files. Empty for every other [[FileFormat]].
   */
  def tableProperties(format: FileFormat): Map[String, String] = format match {
    case hive: HiveFileFormat =>
      val info = tableInfo(hive)
      val properties =
        info.getClass.getMethod("getProperties").invoke(info).asInstanceOf[Properties]
      properties.stringPropertyNames().asScala.map(k => k -> properties.getProperty(k)).toMap
    case _ => Map.empty
  }
}
