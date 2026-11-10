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

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.hadoop.fs.Path

import java.nio.file.Files

class FileSourceScanOptionsSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = ""
  override protected val fileFormat: String = "csv"

  private lazy val dir = Files.createTempDirectory("csv-options").toFile

  override def afterAll(): Unit = {
    try JavaUtils.deleteRecursively(dir)
    finally super.afterAll()
  }

  private def scan(options: Map[String, String]): FileSourceScanExecTransformer = {
    val schema = StructType(Seq(StructField("a", StringType, nullable = true)))
    val relation = HadoopFsRelation(
      new InMemoryFileIndex(spark, Seq(new Path(dir.toURI)), Map.empty, None),
      StructType(Nil),
      schema,
      None,
      new CSVFileFormat(),
      CaseInsensitiveMap(options))(spark)
    val attr = AttributeReference("a", StringType)()
    FileSourceScanExecTransformer(relation, None, Seq(attr), schema, Nil, None, None, Nil, None)
  }

  private def scanProperties(options: Map[String, String]): Map[String, String] =
    scan(options).getProperties

  test("csv sep option maps to the native field delimiter") {
    assert(scanProperties(Map("sep" -> "\t"))("field_delimiter") == "\t")
    // Spark decodes an escaped delimiter, so a backslash followed by t is a tab, and two
    // backslashes are one.
    assert(scanProperties(Map("sep" -> "\\t"))("field_delimiter") == "\t")
    assert(scanProperties(Map("sep" -> "\\\\"))("field_delimiter") == "\\")
  }

  test("sep wins over delimiter when both are set, like Spark") {
    assert(scanProperties(Map("sep" -> "\t", "delimiter" -> ";"))("field_delimiter") == "\t")
    assert(scanProperties(Map("delimiter" -> ";"))("field_delimiter") == ";")
    assert(scanProperties(Map.empty)("field_delimiter") == ",")
  }

  test("a field delimiter the native reader cannot split on falls back") {
    // A multi-character delimiter, a non-ASCII one, and an empty one.
    Seq("||", 0xa7.toChar.toString, "").foreach {
      delimiter =>
        val result = scan(Map("sep" -> delimiter)).doValidate()
        assert(!result.ok())
        assert(result.reason().contains("single ASCII character field delimiter"), result.reason())
    }
    // Spark rejects a null delimiter rather than defaulting to a comma, so this falls back too.
    val result = scan(Map("sep" -> null)).doValidate()
    assert(!result.ok())
    assert(result.reason().contains("Cannot decode the CSV field delimiter"), result.reason())
  }

  test("header accepts case-insensitive booleans like Spark") {
    assert(scanProperties(Map("header" -> "TRUE"))("header") == "1")
    assert(scanProperties(Map("header" -> null))("header") == "0")
  }
}
