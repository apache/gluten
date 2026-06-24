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
package org.apache.spark.sql.execution.datasources.velox

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

class VeloxParquetWriterInjects extends VeloxFormatWriterInjects {
  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): java.util.Map[String, String] = {
    // pass options to native so that velox can take user-specified conf to write parquet,
    // i.e., compression, block size, block rows.
    val sparkOptions = new mutable.HashMap[String, String]()
    sparkOptions.put(SQLConf.PARQUET_COMPRESSION.key, compressionCodec)
    sparkOptions.put(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      SQLConf.get.writeLegacyParquetFormat.toString)
    val blockSize = options.getOrElse(
      GlutenConfig.PARQUET_BLOCK_SIZE,
      GlutenConfig.get.columnarParquetWriteBlockSize.toString)
    sparkOptions.put(GlutenConfig.PARQUET_BLOCK_SIZE, blockSize)
    val blockRows = options.getOrElse(
      GlutenConfig.PARQUET_BLOCK_ROWS,
      GlutenConfig.get.columnarParquetWriteBlockRows.toString)
    sparkOptions.put(GlutenConfig.PARQUET_BLOCK_ROWS, blockRows)
    sparkOptions.put(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      options.getOrElse(
        SQLConf.SESSION_LOCAL_TIMEZONE.key,
        SQLConf.SESSION_LOCAL_TIMEZONE.defaultValueString))
    options
      .get(GlutenConfig.PARQUET_GZIP_WINDOW_SIZE)
      .foreach(sparkOptions.put(GlutenConfig.PARQUET_GZIP_WINDOW_SIZE, _))

    Seq(
      GlutenConfig.PARQUET_ZSTD_COMPRESSION_LEVEL,
      GlutenConfig.PARQUET_DATAPAGE_SIZE,
      GlutenConfig.PARQUET_ENABLE_DICTIONARY,
      GlutenConfig.PARQUET_WRITER_VERSION
    ).foreach(key => options.get(key).foreach(sparkOptions.put(key, _)))
    sparkOptions.asJava
  }

  override val formatName: String = "parquet"
}
