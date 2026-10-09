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

import org.apache.spark.SparkFunSuite

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Files
import org.apache.iceberg.parquet.GlutenParquetUtil
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageTypeParser

import java.nio.file.{Files => NioFiles}
import java.util.Collections

class IcebergParquetFieldIdSuite extends SparkFunSuite {
  Seq(
    "optional int32 id; optional binary data (UTF8);" -> false,
    "optional int32 id = 1; optional binary data (UTF8) = 2;" -> true,
    "optional group profile { optional binary name (UTF8); }" -> false,
    "optional group profile = 1 { optional binary name (UTF8) = 2; }" -> true,
    "optional group items (LIST) = 1 { repeated group list { optional int32 element = 2; } }" -> true
  ).foreach {
    case (fields, expected) =>
      test(s"physical Parquet field IDs: $fields") {
        val file = NioFiles.createTempFile("iceberg-field-ids", ".parquet").toFile
        try {
          val writer = new ParquetFileWriter(
            new Configuration(),
            MessageTypeParser.parseMessageType(s"message table { $fields }"),
            new Path(file.toURI),
            ParquetFileWriter.Mode.OVERWRITE)
          try {
            writer.start()
            writer.end(Collections.emptyMap[String, String]())
          } finally {
            writer.close()
          }
          assert(GlutenParquetUtil.hasFieldIds(Files.localInput(file)) == expected)
        } finally {
          NioFiles.deleteIfExists(file.toPath)
        }
      }
  }
}
