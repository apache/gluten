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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil}
import org.apache.spark.util.SparkVersionUtil

import org.apache.arrow.vector.types.{TimeUnit => ArrowTimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class SparkArrowUtilSuite extends AnyFunSuite {
  test("Arrow schemas preserve the native NTZ logical type at every depth") {
    val timestampPair = new StructType()
      .add("ntz", TimestampNTZType)
      .add("ltz", TimestampType)
    val mixedSchema = new StructType()
      .add("id", LongType)
      .add("ntz", TimestampNTZType)
      .add("ltz", TimestampType)
      .add("nested", timestampPair)
      .add("items", ArrayType(timestampPair))
      .add("mapping", MapType(TimestampNTZType, timestampPair))
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

  test("file schemas preserve nested field names and NTZ tags with either Arrow offset width") {
    val schema =
      StructType.fromDDL("items array<struct<EventTime:timestamp_ntz,Labels:map<string,binary>>>")
    for (enabled <- Seq(false, true)) {
      val conf = new SQLConf
      conf.setConf(SQLConf.CASE_SENSITIVE, false)
      conf.setConfString("spark.sql.execution.arrow.useLargeVarTypes", enabled.toString)
      val fileSchema = SQLConf.withExistingConf(conf) {
        SparkSchemaUtil.toArrowSchemaForWrite(schema)
      }
      assert(SparkArrowUtil.fromArrowSchema(fileSchema) == schema)
      val fields = fileSchema.getFields.get(0).getChildren.get(0).getChildren
      assert(fields.get(0).getMetadata.get("velox.logical_timestamp") == "utc")
      val entries = fields.get(1).getChildren.get(0).getChildren
      val large = enabled && SparkVersionUtil.gteSpark35
      assert(entries.get(0).getType == (if (large) ArrowType.LargeUtf8.INSTANCE
                                        else ArrowType.Utf8.INSTANCE))
      assert(entries.get(1).getType == (if (large) ArrowType.LargeBinary.INSTANCE
                                        else ArrowType.Binary.INSTANCE))
    }
  }
}
