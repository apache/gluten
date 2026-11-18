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
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.apache.spark.sql.streaming.Trigger

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.time.Duration
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.TimeUnit

/**
 * Requires ENABLE_KAFKA in the native build and a broker at KAFKA_BOOTSTRAP_SERVERS (default:
 * localhost:9092). Run with the kafka Maven profile.
 */
class VeloxGlutenKafkaScanSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private val kafkaBootstrapServers =
    sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
  }

  private def withTopic(func: String => Unit): Unit = {
    val topic = "velox_kafka_" + UUID.randomUUID().toString.replace("-", "")
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    val adminClient = AdminClient.create(props)
    try {
      adminClient
        .createTopics(Collections.singletonList(new NewTopic(topic, 2, 1.toShort)))
        .all()
        .get(30, TimeUnit.SECONDS)
      try {
        func(topic)
      } finally {
        adminClient.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS)
      }
    } finally {
      adminClient.close(Duration.ofSeconds(5))
    }
  }

  private def checkStreamingRead(includeHeaders: Boolean): Unit = {
    withTopic {
      topic =>
        withTempDir {
          dir =>
            spark
              .range(1000)
              .selectExpr("cast(id as string) as value")
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("topic", topic)
              .save()

            val stream = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")
              .option("includeHeaders", includeHeaders.toString)
              .load()
            // Keep headers in the fallback query so column pruning cannot hide the unsupported type.
            val result = if (includeHeaders) {
              stream.selectExpr("cast(cast(value as string) as int) as id", "headers")
            } else {
              stream.selectExpr("cast(cast(value as string) as int) as id")
            }
            val streamQuery = result.writeStream
              .format("parquet")
              .option("checkpointLocation", dir.getCanonicalPath + "/checkpoint")
              .trigger(Trigger.AvailableNow())
              .start(dir.getCanonicalPath + "/data")

            try {
              assert(streamQuery.awaitTermination(60000), "Kafka micro-batch did not finish")
              val scans = streamQuery
                .asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
                .lastExecution
                .executedPlan
                .collect { case p: MicroBatchScanExecTransformer => p }
              assert(scans.size == (if (includeHeaders) 0 else 1))
              checkAnswer(
                spark.read.parquet(dir.getCanonicalPath + "/data").select("id"),
                (0 until 1000).map(Row(_)))
            } finally {
              streamQuery.stop()
            }
        }
    }
  }

  test("kafka micro-batch scan offloads and returns every record") {
    checkStreamingRead(includeHeaders = false)
  }

  test("kafka micro-batch scan with headers falls back and returns every record") {
    checkStreamingRead(includeHeaders = true)
  }
}
