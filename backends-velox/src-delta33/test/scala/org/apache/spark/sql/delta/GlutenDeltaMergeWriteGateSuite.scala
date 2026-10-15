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

import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, IntegerType, StringType, StructType, TimestampNTZType}

import org.scalatest.funsuite.AnyFunSuite

class GlutenDeltaMergeWriteGateSuite extends AnyFunSuite {

  private val schema = new StructType().add("id", IntegerType).add("p", StringType)

  private def metadata(
      schema: StructType = schema,
      partitionColumns: Seq[String] = Nil,
      configuration: Map[String, String] = Map.empty): Metadata =
    Metadata(
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      configuration = configuration)

  private def conf(settings: (String, String)*): SQLConf = {
    val conf = new SQLConf
    settings.foreach { case (key, value) => conf.setConfString(key, value) }
    conf
  }

  private def reason(
      md: Metadata = metadata(),
      protocol: Protocol = Protocol(1, 2),
      writeSchema: StructType = schema,
      sqlConf: SQLConf = conf()): Option[String] =
    GlutenDeltaMergeWriteGate.fallbackReason(md, protocol, writeSchema, sqlConf)

  private def withColumn(dataType: DataType): StructType = schema.add("c", dataType)

  test("a plain table may be written natively") {
    assert(reason().isEmpty)
    assert(reason(metadata(partitionColumns = Seq("p"))).isEmpty)
  }

  test("column mapping falls back in both name and id mode") {
    for (mode <- Seq("name", "id")) {
      val md = metadata(configuration = Map("delta.columnMapping.mode" -> mode))
      assert(reason(md).exists(_.contains(mode)), mode)
    }
  }

  test("IcebergCompat falls back") {
    val md = metadata(configuration = Map("delta.enableIcebergCompatV2" -> "true"))
    assert(reason(md).exists(_.contains("IcebergCompat")))
  }

  test("materializePartitionColumns falls back") {
    val protocol = Action
      .fromJson(
        """{"protocol":{"minReaderVersion":1,"minWriterVersion":7,""" +
          """"writerFeatures":["materializePartitionColumns"]}}""")
      .asInstanceOf[Protocol]
    assert(reason(protocol = protocol).exists(_.contains("materializePartitionColumns")))
  }

  test("FLOAT and DOUBLE partition columns fall back") {
    for (dataType <- Seq(FloatType, DoubleType)) {
      val md = metadata(withColumn(dataType), partitionColumns = Seq("c"))
      assert(reason(md).exists(_.contains("partition")), dataType)
      // A non-partition column of the same type is fine.
      assert(reason(metadata(withColumn(dataType))).isEmpty, dataType)
    }
  }

  test("TIMESTAMP_NTZ anywhere in the table or the written data falls back") {
    val nested = withColumn(ArrayType(new StructType().add("ts", TimestampNTZType)))
    assert(reason(metadata(nested)).exists(_.contains("TIMESTAMP_NTZ")))
    // Schema evolution can add the column before the table metadata has it.
    assert(reason(writeSchema = withColumn(TimestampNTZType)).exists(_.contains("TIMESTAMP_NTZ")))
  }

  test("nested upper-case field names fall back unless analysis is case sensitive") {
    val nested = withColumn(new StructType().add("Value", IntegerType))
    assert(reason(metadata(nested)).exists(_.contains("upper-case")))
    assert(reason(writeSchema = nested).exists(_.contains("upper-case")))
    assert(reason(metadata(nested), sqlConf = conf(SQLConf.CASE_SENSITIVE.key -> "true")).isEmpty)
    // Top-level names keep their case in native files.
    assert(reason(metadata(new StructType().add("Id", IntegerType))).isEmpty)
  }

  test("writer settings the native writer does not honour fall back") {
    assert(reason(sqlConf = conf("spark.databricks.delta.stats.collect" -> "false")).isDefined)
    assert(
      reason(sqlConf = conf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key -> "2")).isDefined)
    val maxRecords = conf(SQLConf.MAX_RECORDS_PER_FILE.key -> "10")
    assert(reason(sqlConf = maxRecords).isDefined)
    assert(reason(metadata(partitionColumns = Seq("p")), sqlConf = maxRecords).isEmpty)
  }

  test("the merge scope is thread-local and restored on exit") {
    assert(!GlutenDeltaMergeWriteGate.inMergeScope)
    intercept[IllegalStateException] {
      GlutenDeltaMergeWriteGate.withMergeScope {
        assert(GlutenDeltaMergeWriteGate.inMergeScope)
        var otherThread = true
        val thread = new Thread(() => otherThread = GlutenDeltaMergeWriteGate.inMergeScope)
        thread.start()
        thread.join()
        assert(!otherThread)
        throw new IllegalStateException("fail inside the scope")
      }
    }
    assert(!GlutenDeltaMergeWriteGate.inMergeScope)
  }
}
