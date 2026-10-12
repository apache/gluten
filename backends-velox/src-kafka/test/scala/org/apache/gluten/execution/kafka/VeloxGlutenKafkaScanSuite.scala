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
package org.apache.gluten.execution.kafka

import org.apache.gluten.execution.{MicroBatchScanExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.apache.spark.sql.streaming.Trigger

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.util.{Collections, Properties}

import scala.concurrent.duration.DurationInt

/**
 * Kafka streaming read tests for the Velox backend. Requires the native library to be built with
 * ENABLE_KAFKA and a running Kafka broker, reachable at localhost:9092 by default or at the address
 * given by the KAFKA_BOOTSTRAP_SERVERS environment variable.
 */
class VeloxGlutenKafkaScanSuite
  extends VeloxWholeStageTransformerSuite
  with GlutenKafkaScanSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected val kafkaBootstrapServers: String =
    sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
  }

  private def withTopic(topicName: String)(func: => Unit): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    val adminClient = AdminClient.create(props)
    try {
      if (adminClient.listTopics().names().get().contains(topicName)) {
        adminClient.deleteTopics(Collections.singletonList(topicName)).all().get()
      }
      adminClient
        .createTopics(Collections.singletonList(new NewTopic(topicName, 1, 1.toShort)))
        .all()
        .get()
      func
    } finally {
      try {
        if (adminClient.listTopics().names().get().contains(topicName)) {
          adminClient.deleteTopics(Collections.singletonList(topicName)).all().get()
        }
      } catch {
        case _: Exception => logWarning(s"Delete topic $topicName failed.")
      }
      adminClient.close()
    }
  }

  /**
   * Writes ids 0 until 1000 to a new topic, streams them into a parquet table with the given Kafka
   * source options, and checks the table contents and whether the scan was offloaded.
   */
  private def checkStreamingRead(
      topic: String,
      sourceOptions: Map[String, String],
      expectOffloaded: Boolean): Unit = {
    withTempDir {
      dir =>
        val tableName = s"${topic}_sink"
        withTable(tableName) {
          withTopic(topic) {
            spark.sql(s"""
                         |CREATE EXTERNAL TABLE $tableName (
                         |    id int
                         |) USING parquet
                         |LOCATION '${dir.getCanonicalPath}'
                         |""".stripMargin)

            spark
              .range(1000)
              .selectExpr("cast(id as string) as value")
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("topic", topic)
              .save()

            val streamQuery = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")
              .options(sourceOptions)
              .load()
              .selectExpr("cast(cast(value as string) as int) as id")
              .writeStream
              .format("parquet")
              .option("checkpointLocation", dir.getCanonicalPath + "/_checkpoint")
              .trigger(Trigger.ProcessingTime("1 seconds"))
              .start(dir.getCanonicalPath)

            try {
              eventually(timeout(60.seconds), interval(5.seconds)) {
                val scans = streamQuery
                  .asInstanceOf[StreamingQueryWrapper]
                  .streamingQuery
                  .lastExecution
                  .executedPlan
                  .collect { case p: MicroBatchScanExecTransformer => p }
                assert(scans.size == (if (expectOffloaded) 1 else 0))

                val result =
                  spark.sql(s"SELECT count(id), min(id), max(id) FROM $tableName").collect()
                assert(result.length == 1)
                assert(result.head.getLong(0) == 1000)
                assert(result.head.getInt(1) == 0)
                assert(result.head.getInt(2) == 999)
              }
            } finally {
              streamQuery.stop()
            }
          }
        }
    }
  }

  test("kafka streaming read offloads to native and returns consistent data") {
    checkStreamingRead("velox_kafka_read", Map.empty, expectOffloaded = true)
  }

  test("kafka streaming read with headers falls back") {
    checkStreamingRead(
      "velox_kafka_read_headers",
      Map("includeHeaders" -> "true"),
      expectOffloaded = false)
  }
}
