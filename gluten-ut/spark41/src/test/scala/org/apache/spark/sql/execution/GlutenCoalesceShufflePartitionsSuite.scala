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
package org.apache.spark.sql.execution

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{GlutenTestsCommonTrait, QueryTest, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

class GlutenCoalesceShufflePartitionsSuite
  extends CoalesceShufflePartitionsSuite
  with GlutenTestsCommonTrait {

  override protected def afterAll(): Unit = {}

  override def withSparkSession(
      f: SparkSession => Unit,
      targetPostShuffleInputSize: Int,
      minNumPostShufflePartitions: Option[Int],
      enableIOEncryption: Boolean = false): Unit = {
    val sparkConf =
      new SparkConf(false)
        .setMaster("local[*]")
        .setAppName("test")
        .set(UI_ENABLED, false)
        .set(IO_ENCRYPTION_ENABLED, enableIOEncryption)
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, targetPostShuffleInputSize.toString)
        .set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "true")
        // Gluten config
        .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "5g")
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
    }

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark)
    finally {
      spark.stop()
    }
  }

  Seq(Some(5), None).foreach {
    minNumPostShufflePartitions =>
      val testNameNote = minNumPostShufflePartitions match {
        case Some(numPartitions) => "(minNumPostShufflePartitions: " + numPartitions + ")"
        case None => ""
      }

      // Ported from vanilla spark with targetPostShuffleInputSize changed.
      testGluten(s"determining the number of reducers: aggregate operator$testNameNote") {
        val test: SparkSession => Unit = {
          spark: SparkSession =>
            val df =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 20 as key", "id as value")
            val agg = df.groupBy("key").count()

            // Check the answer first.
            QueryTest.checkAnswer(agg, spark.range(0, 20).selectExpr("id", "50 as cnt").collect())

            // Then, let's look at the number of post-shuffle partitions estimated
            // by the ExchangeCoordinator.
            val finalPlan = stripAQEPlan(agg.queryExecution.executedPlan)
            val shuffleReads = finalPlan.collect { case r @ CoalescedShuffleRead() => r }

            minNumPostShufflePartitions match {
              case Some(numPartitions) =>
                assert(shuffleReads.isEmpty)
              case None =>
                assert(shuffleReads.length === 1)
                shuffleReads.foreach(read => assert(read.outputPartitioning.numPartitions === 3))
            }
        }
        // Change the original value 2000 to 2500 for gluten. The test depends on the calculation
        // for bytesByPartitionId in MapOutputStatistics. Gluten has a different statistic result.
        // See ShufflePartitionsUtil.coalescePartitions & GlutenColumnarShuffleWriter's mapStatus.
        withSparkSession(test, 2500, minNumPostShufflePartitions)
      }

      testGluten(s"determining the number of reducers: join operator$testNameNote") {
        val test: SparkSession => Unit = {
          spark: SparkSession =>
            val df1 =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 500 as key1", "id as value1")
            val df2 =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 500 as key2", "id as value2")

            val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

            // Check the answer first.
            val expectedAnswer =
              spark
                .range(0, 1000)
                .selectExpr("id % 500 as key", "id as value")
                .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
            QueryTest.checkAnswer(join, expectedAnswer.collect())

            // Then, let's look at the number of post-shuffle partitions estimated
            // by the ExchangeCoordinator.
            val finalPlan = stripAQEPlan(join.queryExecution.executedPlan)
            val shuffleReads = finalPlan.collect { case r @ CoalescedShuffleRead() => r }

            minNumPostShufflePartitions match {
              case Some(numPartitions) =>
                assert(shuffleReads.isEmpty)

              case None =>
                assert(shuffleReads.length === 2)
                shuffleReads.foreach(read => assert(read.outputPartitioning.numPartitions === 2))
            }
        }
        // Change the original value 16384 to 20000 for gluten. The test depends on the calculation
        // for bytesByPartitionId in MapOutputStatistics. Gluten has a different statistic result.
        // See ShufflePartitionsUtil.coalescePartitions & GlutenColumnarShuffleWriter's mapStatus.
        withSparkSession(test, 20000, minNumPostShufflePartitions)
      }

      testGluten(s"determining the number of reducers: complex query 1$testNameNote") {
        val test: (SparkSession) => Unit = {
          spark: SparkSession =>
            val df1 =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 500 as key1", "id as value1")
                .groupBy("key1")
                .count()
                .toDF("key1", "cnt1")
            val df2 =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 500 as key2", "id as value2")
                .groupBy("key2")
                .count()
                .toDF("key2", "cnt2")

            val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("cnt2"))

            // Check the answer first.
            val expectedAnswer =
              spark
                .range(0, 500)
                .selectExpr("id", "2 as cnt")
            QueryTest.checkAnswer(join, expectedAnswer.collect())

            // Then, let's look at the number of post-shuffle partitions estimated
            // by the ExchangeCoordinator.
            val finalPlan = stripAQEPlan(join.queryExecution.executedPlan)
            val shuffleReads = finalPlan.collect { case r @ CoalescedShuffleRead() => r }

            minNumPostShufflePartitions match {
              case Some(numPartitions) =>
                assert(shuffleReads.isEmpty)

              case None =>
                assert(shuffleReads.length === 2)
                shuffleReads.foreach(read => assert(read.outputPartitioning.numPartitions === 2))
            }
        }

        // Change the original value 16384 to 20000 for gluten. The test depends on the calculation
        // for bytesByPartitionId in MapOutputStatistics. Gluten has a different statistic result.
        // See ShufflePartitionsUtil.coalescePartitions & GlutenColumnarShuffleWriter's mapStatus.
        withSparkSession(test, 20000, minNumPostShufflePartitions)
      }

      testGluten(s"determining the number of reducers: complex query 2$testNameNote") {
        val test: (SparkSession) => Unit = {
          spark: SparkSession =>
            val df1 =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 500 as key1", "id as value1")
                .groupBy("key1")
                .count()
                .toDF("key1", "cnt1")
            val df2 =
              spark
                .range(0, 1000, 1, numInputPartitions)
                .selectExpr("id % 500 as key2", "id as value2")

            val join =
              df1
                .join(df2, col("key1") === col("key2"))
                .select(col("key1"), col("cnt1"), col("value2"))

            // Check the answer first.
            val expectedAnswer =
              spark
                .range(0, 1000)
                .selectExpr("id % 500 as key", "2 as cnt", "id as value")
            QueryTest.checkAnswer(join, expectedAnswer.collect())

            // Then, let's look at the number of post-shuffle partitions estimated
            // by the ExchangeCoordinator.
            val finalPlan = stripAQEPlan(join.queryExecution.executedPlan)
            val shuffleReads = finalPlan.collect { case r @ CoalescedShuffleRead() => r }

            minNumPostShufflePartitions match {
              case Some(numPartitions) =>
                assert(shuffleReads.isEmpty)

              case None =>
                assert(shuffleReads.length === 2)
                shuffleReads.foreach(read => assert(read.outputPartitioning.numPartitions === 3))
            }
        }

        // Change the original value 12000 to 16000 for gluten. The test depends on the calculation
        // for bytesByPartitionId in MapOutputStatistics. Gluten has a different statistic result.
        // See ShufflePartitionsUtil.coalescePartitions & GlutenColumnarShuffleWriter's mapStatus.
        withSparkSession(test, 16000, minNumPostShufflePartitions)
      }

      testGluten(s"determining the number of reducers: plan already partitioned$testNameNote") {
        val test: SparkSession => Unit = {
          spark: SparkSession =>
            try {
              spark.range(1000).write.bucketBy(30, "id").saveAsTable("t")
              // `df1` is hash partitioned by `id`.
              val df1 = spark.read.table("t")
              val df2 =
                spark
                  .range(0, 1000, 1, numInputPartitions)
                  .selectExpr("id % 500 as key2", "id as value2")

              val join = df1.join(df2, col("id") === col("key2")).select(col("id"), col("value2"))

              // Check the answer first.
              val expectedAnswer = spark
                .range(0, 500)
                .selectExpr("id % 500", "id as value")
                .union(spark.range(500, 1000).selectExpr("id % 500", "id as value"))
              QueryTest.checkAnswer(join, expectedAnswer.collect())

              // Then, let's make sure we do not reduce number of post shuffle partitions.
              val finalPlan = join.queryExecution.executedPlan
                .asInstanceOf[AdaptiveSparkPlanExec]
                .executedPlan
              val shuffleReads = finalPlan.collect { case r @ CoalescedShuffleRead() => r }
              assert(shuffleReads.length === 0)
            } finally {
              spark.sql("drop table t")
            }
        }
        withSparkSession(test, 12000, minNumPostShufflePartitions)
      }
  }
}
