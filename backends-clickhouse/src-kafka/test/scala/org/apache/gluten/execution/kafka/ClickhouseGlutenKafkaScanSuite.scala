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

import org.apache.gluten.backendsapi.clickhouse.RuntimeSettings
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger

import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class ClickhouseGlutenKafkaScanSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with GlutenKafkaScanSuite {

  override protected val fileFormat: String = "parquet"

  protected val kafkaBootstrapServers: String = "localhost:9092"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, spark35.toString)
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set(RuntimeSettings.MERGE_AFTER_INSERT.key, "false")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .set("spark.sql.warehouse.dir", warehouse)
  }

  def withTopic(topicName: String)(func: => Unit): Unit = {
    val bootstrapServers = kafkaBootstrapServers
    val numPartitions = 1
    val replicationFactor: Short = 1

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(props)

    if (adminClient.listTopics().names().get().contains(topicName)) {
      adminClient.deleteTopics(List(topicName).asJava)
    }

    try {
      val newTopic = new NewTopic(topicName, numPartitions, replicationFactor)
      adminClient.createTopics(List(newTopic).asJava).all().get()
      func
    } catch {
      case t: Throwable =>
        throw t
    } finally {
      try {
        if (adminClient.listTopics().names().get().contains(topicName)) {
          adminClient.deleteTopics(List(topicName).asJava)
        }
      } catch {
        case e: Exception =>
          logWarning(s"Delete topic $topicName failed.")
      }

      adminClient.close()
    }
  }

  // TODO: after rebase-25.12, failed with spark35 (0 did not equal 100000), fix later
  testWithSpecifiedSparkVersion("GLUTEN-9681: test kafka data consistency", "3.3") {
    withTempDir(
      dir => {
        val table_name = "data_consistency"
        withTable(s"$table_name") {
          withTopic(table_name) {
            spark.sql(s"""
                         |CREATE EXTERNAL TABLE $table_name (
                         |    id int
                         |)USING Delta
                         |LOCATION '${dir.getCanonicalPath}'
                         |""".stripMargin)

            spark
              .range(100000)
              .selectExpr("cast(id as string) as value")
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("topic", table_name)
              .save()

            val streamQuery = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("subscribe", table_name)
              .option("startingOffsets", "earliest")
              .load()
              .withColumn("sp", col("value").cast("string"))
              .withColumn("id", col("sp").cast("int"))
              .drop(col("sp"))
              .drop(col("value"))
              .drop(col("key"))
              .drop(col("topic"))
              .drop(col("partition"))
              .drop(col("offset"))
              .drop(col("timestamp"))
              .drop(col("timestampType"))
              .writeStream
              .format("delta")
              .option("checkpointLocation", dir.getCanonicalPath + "/_checkpoint")
              .trigger(Trigger.ProcessingTime("5 seconds"))
              .start(dir.getCanonicalPath)

            eventually(timeout(60.seconds), interval(5.seconds)) {
              val result = spark.sql(s"select count(*) from $table_name").collect()
              assert(result.length == 1)
              assert(result.apply(0).get(0) == 100000)
              streamQuery.awaitTermination(10000)
            }

            streamQuery.stop()
          }
        }
      })
  }
}
