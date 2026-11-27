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

  private def validate(
      fields: Array[StructField],
      properties: Map[String, String] = Map.empty): ValidationResult = {
    VeloxBackendSettings.validateScanExec(
      KafkaReadFormat,
      fields,
      StructType(fields),
      Seq.empty,
      properties,
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

  test("native Kafka scan accepts supported executor parameters") {
    val executorParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "spark-kafka-test-executor",
      "client.id" -> "spark-kafka-test-client",
      "isolation.level" -> "read_uncommitted",
      "security.protocol" -> "PLAINTEXT",
      "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "auto.offset.reset" -> "none",
      "enable.auto.commit" -> "false",
      "receive.buffer.bytes" -> "65536"
    )
    Seq(
      Map.empty[String, String],
      executorParams,
      executorParams - "security.protocol",
      executorParams.updated("isolation.level", "read_committed"),
      executorParams.updated("metadata.max.age.ms", "1"),
      executorParams.updated("metadata.max.age.ms", "86400000")
    ).foreach {
      properties => assert(validate(kafkaFields, properties).ok())
    }
  }

  test("native Kafka scan rejects unsupported parameters without exposing their values") {
    val secret = "kafka-test-secret-must-not-appear"
    val unsupportedParams = Seq(
      "security.protocol" -> "SSL",
      "security.protocol" -> "SASL_PLAINTEXT",
      "security.protocol" -> "SASL_SSL",
      "security.protocol" -> secret,
      "sasl.jaas.config" -> s"""LoginModule required password="$secret";""",
      "ssl.truststore.location" -> s"/private/$secret",
      "fetch.max.wait.ms" -> "500",
      "request.timeout.ms" -> "10000",
      "default.api.timeout.ms" -> "10000",
      "custom.kafka.property" -> secret,
      "metadata.max.age.ms" -> "0",
      "metadata.max.age.ms" -> "-1",
      "metadata.max.age.ms" -> "86400001",
      "metadata.max.age.ms" -> secret,
      "receive.buffer.bytes" -> "32768",
      "receive.buffer.bytes" -> "-1"
    )
    unsupportedParams.foreach {
      case (key, value) =>
        val result = validate(kafkaFields, Map(key -> value))
        assert(!result.ok(), s"Expected fallback for $key")
        assert(result.reason().contains(key))
        assert(!result.reason().contains(value))
        assert(!result.reason().contains(secret))
    }
  }
}
