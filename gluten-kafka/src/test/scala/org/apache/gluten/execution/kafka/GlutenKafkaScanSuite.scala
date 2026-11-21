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

import org.apache.gluten.execution.{MicroBatchScanExecTransformer, WholeStageTransformerSuite}

import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

trait GlutenKafkaScanSuite extends WholeStageTransformerSuite {
  protected val kafkaBootstrapServers: String

  test("test MicroBatchScanExecTransformer not fallback") {
    withTempDir(
      dir => {
        val table_name = "kafka_table"
        withTable(s"$table_name") {
          val df = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", table_name)
            .load()
            .withColumn("sp", split(col("value").cast("string"), ","))
            .withColumn("id", col("sp").getItem(0).cast("int"))
            .withColumn("name", col("sp").getItem(1).cast("string"))
            .drop(col("sp"))
            .drop(col("value"))
            .drop(col("key"))
            .drop(col("topic"))
            .drop(col("partition"))
            .drop(col("offset"))
            .drop(col("timestamp"))
            .drop(col("timestampType"))

          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $table_name (
                       |    id long,
                       |    name string
                       |)USING Parquet
                       |LOCATION '${dir.getCanonicalPath}'
                       |""".stripMargin)

          spark
            .range(0, 20)
            .selectExpr(
              "concat(id,',', id) as value"
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("topic", table_name)
            .save()

          val streamQuery = df.writeStream
            .format("parquet")
            .option("checkpointLocation", dir.getCanonicalPath + "/_checkpoint")
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .start(dir.getCanonicalPath)

          eventually(timeout(60.seconds), interval(5.seconds)) {
            val size = KafkaTestUtils
              .executedPlan(streamQuery)
              .collect { case p: MicroBatchScanExecTransformer => p }
            assert(size.size == 1)
            streamQuery.awaitTermination(1000)
          }
        }
      })
  }
}
