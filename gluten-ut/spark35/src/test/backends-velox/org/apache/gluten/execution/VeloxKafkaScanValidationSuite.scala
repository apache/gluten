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

import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.KafkaReadFormat

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

import org.apache.hadoop.conf.Configuration

/** Schema validation must also work without Kafka jars, a broker, or a Kafka-enabled native build. */
class VeloxKafkaScanValidationSuite extends SparkFunSuite {
  private val kafkaFields = Array(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType))

  private def validate(fields: Array[StructField]): ValidationResult = {
    VeloxBackendSettings.validateScanExec(
      KafkaReadFormat,
      fields,
      StructType(fields),
      Seq.empty,
      Map.empty,
      new Configuration(false),
      Set(KafkaReadFormat))
  }

  test("native Kafka scan accepts the Spark Kafka schema and pruned columns") {
    assert(validate(kafkaFields).ok())
    assert(validate(Array(kafkaFields(4), kafkaFields(1))).ok())
    assert(validate(Array.empty[StructField]).ok())
  }

  test("native Kafka scan rejects headers, unknown columns, and incompatible types") {
    val headers = StructField(
      "headers",
      ArrayType(StructType(Seq(StructField("key", StringType), StructField("value", BinaryType)))))
    Seq(headers, StructField("unknown", StringType), StructField("value", StringType)).foreach {
      field =>
        val result = validate(kafkaFields :+ field)
        assert(!result.ok())
        assert(result.reason().contains(s"Unsupported Kafka column ${field.name}"))
    }
  }
}
