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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile

/** Shim layer for ParquetFooterReader to maintain compatibility across different Spark versions. */
object ParquetFooterReaderShim {

  /** @since Spark 4.1 */
  def readFooter(
      configuration: Configuration,
      fileStatus: FileStatus,
      filter: ParquetMetadataConverter.MetadataFilter): ParquetMetadata = {
    ParquetFooterReader.readFooter(HadoopInputFile.fromStatus(fileStatus, configuration), filter)
  }

  /** @since Spark 4.1 */
  def readFooter(
      configuration: Configuration,
      file: Path,
      filter: ParquetMetadataConverter.MetadataFilter): ParquetMetadata = {
    ParquetFooterReader.readFooter(HadoopInputFile.fromPath(file, configuration), filter)
  }
}
