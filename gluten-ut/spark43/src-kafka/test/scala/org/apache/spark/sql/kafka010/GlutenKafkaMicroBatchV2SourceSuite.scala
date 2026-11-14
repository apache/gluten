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
package org.apache.spark.sql.kafka010

import org.apache.gluten.execution.MicroBatchScanExecTransformer

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.MicroBatchScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.streaming.runtime.{SerializedOffset, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger

import org.apache.kafka.common.TopicPartition

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

class GlutenKafkaMicroBatchV2SourceSuite
  extends KafkaMicroBatchV2SourceSuite
  with GlutenKafkaSourceTest {

  import testImplicits._

  testGluten("Kafka stream-stream self-join preserves output and a single source offset") {
    // Preserve Spark's self-join coverage without its unsupported StreamingQueryProgress row
    // counts. Spark 4.3 ProgressReporter cannot attribute rows from native micro-batch scans.
    Seq(false, true).foreach {
      reuseExchange =>
        withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> reuseExchange.toString) {
          val topic = newTopic()
          testUtils.createTopic(topic, partitions = 2)
          require(testUtils.getLatestOffsets(Set(topic)).size == 2)

          val values = spark.readStream
            .format("kafka")
            .option("subscribe", topic)
            .option("kafka.bootstrap.servers", testUtils.brokerAddress)
            .option("kafka.metadata.max.age.ms", "1")
            .load()
            .selectExpr(
              "CAST(CAST(value AS STRING) AS INT) AS value",
              "CAST(CAST(value AS STRING) AS INT) % 5 AS key")

          testStream(values.join(values, "key"))(
            makeSureGetOffsetCalled,
            AddKafkaData(Set(topic), 1, 2),
            CheckAnswer((1, 1, 1), (2, 2, 2)),
            AddKafkaData(Set(topic), 6, 3),
            CheckAnswer((1, 1, 1), (2, 2, 2), (3, 3, 3), (1, 6, 1), (1, 1, 6), (1, 6, 6)),
            AssertOnQuery {
              query =>
                assert(query.availableOffsets.iterator.size == 1)
                val expectedOffset = query.availableOffsets.iterator.next()._2.json
                val loggedOffsets = query.offsetLog.getLatest
                  .getOrElse(fail("No Kafka offset log entry found"))
                  ._2.offsets
                // Spark stores one checkpoint entry per logical source occurrence. Both sides
                // of this self-join must record the same offset for the single shared source.
                assert(loggedOffsets.size == 2)
                assert(loggedOffsets.forall(_.exists(_.json == expectedOffset)))
                assertKafkaScan(query, requireScan = true)
                val plan = query.lastExecution.executedPlan
                assert(plan.exists(_.isInstanceOf[MicroBatchScanExecTransformer]))
                val reusedExchanges = plan.collect { case exchange: ReusedExchangeExec => exchange }
                assert(reusedExchanges.size == (if (reuseExchange) 1 else 0))
                true
            }
          )
        }
    }
  }

  testGluten("Kafka micro-batches consume records through a native scan") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    val values = (0 until 12).map(_.toString)
    testUtils.sendMessages(topic, values.toArray)

    val input = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")

    testStream(input)(
      // Records were sent before testStream, so CheckAnswer has no AddData offset to await.
      makeSureGetOffsetCalled,
      CheckAnswer(values: _*),
      AssertOnQuery {
        query =>
          assertKafkaScan(query, requireScan = true)
          assert(
            query.lastExecution.executedPlan.exists(_.isInstanceOf[MicroBatchScanExecTransformer]))
          true
      }
    )
  }

  testGluten("Kafka retained headers use Spark fallback and preserve their values") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    val headerValue = "header-value".getBytes(UTF_8)
    testUtils.sendMessage(
      new RecordBuilder(topic, "value")
        .headers(Seq("header-key" -> headerValue))
        .build())

    val input = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .load()
      .selectExpr("CAST(value AS STRING)", "headers")

    testStream(input)(
      // Wait for the preloaded record before checking its value and retained headers.
      makeSureGetOffsetCalled,
      CheckAnswer(Row("value", Seq(Row("header-key", headerValue)))),
      AssertOnQuery {
        query =>
          assertKafkaScan(query, requireScan = true)
          val plan = query.lastExecution.executedPlan
          assert(!plan.exists(_.isInstanceOf[MicroBatchScanExecTransformer]))
          assert(plan.exists {
            case scan: MicroBatchScanExec =>
              MicroBatchScanExecTransformer.supportsBatchScan(scan.scan)
            case _ => false
          })
          true
      }
    )
  }

  testGluten("Kafka AvailableNow scans write Parquet and restart from the same checkpoint") {
    val topic = newTopic()
    val topicPartition = new TopicPartition(topic, 0)
    testUtils.createTopic(topic, partitions = 1)

    withTempDir {
      dir =>
        val outputPath = new File(dir, "parquet").getCanonicalPath
        val checkpointPath = new File(dir, "checkpoint").getCanonicalPath
        val input = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", testUtils.brokerAddress)
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("maxOffsetsPerTrigger", 2)
          .load()
          .selectExpr("CAST(value AS STRING)")

        def runAvailableNow(startOffset: Long, endOffset: Long): Unit = {
          val query = input.writeStream
            .format("parquet")
            .outputMode("append")
            .option("checkpointLocation", checkpointPath)
            .trigger(Trigger.AvailableNow())
            .start(outputPath)
          try {
            assert(query.awaitTermination(streamingTimeout.toMillis), "AvailableNow timed out")
            val execution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery
            assertKafkaScan(execution, requireScan = true)
            assert(
              execution.lastExecution.executedPlan
                .exists(_.isInstanceOf[MicroBatchScanExecTransformer]))

            // Offset progress remains meaningful even when Spark cannot count native scan rows.
            val batchEnds = query.recentProgress
              .flatMap(_.sources)
              .map {
                source =>
                  KafkaSourceOffset(SerializedOffset(source.endOffset))
                    .partitionToOffsets(topicPartition)
              }
              .distinct
              .toSeq
            assert(batchEnds.nonEmpty, "AvailableNow reported no Kafka offset progress")
            assert(batchEnds.last == endOffset)
            val batchSizes = (Seq(startOffset) ++ batchEnds).sliding(2).map {
              offsets => offsets.last - offsets.head
            }.toSeq
            assert(batchSizes.forall(size => size >= 0 && size <= 2))
            assert(batchSizes.count(_ > 0) == (endOffset - startOffset + 1) / 2)
          } finally {
            query.stop()
          }
        }

        val firstValues = (0 until 7).map(_.toString)
        testUtils.sendMessages(topic, firstValues.toArray, Some(0))
        runAvailableNow(startOffset = 0, endOffset = 7)
        checkAnswer(spark.read.parquet(outputPath), firstValues.map(Row(_)))

        val nextValues = (7 until 12).map(_.toString)
        testUtils.sendMessages(topic, nextValues.toArray, Some(0))
        runAvailableNow(startOffset = 7, endOffset = 12)
        checkAnswer(spark.read.parquet(outputPath), (firstValues ++ nextValues).map(Row(_)))
    }
  }
}
