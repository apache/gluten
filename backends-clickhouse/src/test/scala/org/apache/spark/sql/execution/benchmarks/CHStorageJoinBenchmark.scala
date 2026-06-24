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
package org.apache.spark.sql.execution.benchmarks

import org.apache.gluten.utils.IteratorUtil
import org.apache.gluten.vectorized.{BlockOutputStream, CHBlockWriterJniWrapper, CHStreamReader}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{RemoveTopColumnarToRow, SparkPlan}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.joins.ClickHouseBuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.CHShuffleReadStreamFactory

import java.io.ByteArrayOutputStream

object CHStorageJoinBenchmark extends SqlBasedBenchmark with CHSqlBasedBenchmark {

  protected lazy val appName = "CHStorageJoinBenchmark"
  protected lazy val thrdNum = "2"
  protected lazy val memorySize = "4G"
  protected lazy val offheapSize = "4G"

  override def getSparkSession: SparkSession = {

    val conf = getSparkConf
      .set("spark.driver.maxResultSize", "0")
    SparkSession.builder.config(conf).getOrCreate()
  }
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    // /home/chang/test/tpch/parquet/s100/supplier * 200
    val (parquetDir, scanSchema, executedCnt) =
      if (mainArgs.isEmpty) {
        ("/data/tpch-data/parquet/lineitem", "l_orderkey,l_receiptdate", 5)
      } else {
        (mainArgs(0), mainArgs(1), mainArgs(2).toInt)
      }

    val chParquet = spark.sql(s"""
                                 |select $scanSchema from parquet.`$parquetDir`
                                 |
                                 |""".stripMargin)
    val rowCount = chParquet.count()
    val benchmark =
      new Benchmark("CHStorageJoinBenchmark", rowCount, outputPerIteration = false, output = output)
    benchmark.addCase("mapPartitions with CHBlockWriterJniWrapper", executedCnt) {
      _ => createBroadcastRelation1(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("map with CHBlockWriterJniWrapper", executedCnt) {
      _ => createBroadcastRelation2(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }

    benchmark.addCase("mapPartitions with compressed lz4 BlockOutputStream", executedCnt) {
      _ => createBroadcastRelation4(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("mapPartitionsInternal with compressed lz4 BlockOutputStream", executedCnt) {
      _ => createBroadcastRelation5(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("map with compressed lz4 BlockOutputStream", executedCnt) {
      _ => createBroadcastRelation6(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.run()
  }

  def createBroadcastRelation1(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitions {
        iter =>
          var _numRows: Long = 0
          // Use for reading bytes array from block
          val blockNativeWriter = new CHBlockWriterJniWrapper()
          while (iter.hasNext) {
            val batch = iter.next
            blockNativeWriter.write(batch)
            _numRows += batch.numRows
          }
          Iterator((_numRows, blockNativeWriter.collectAsByteArray()))
          // Iterator((_numRows, new Array[Byte](0)))
      }
      .collect
    val count0 = countsAndBytes.map(_._1).sum
    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    val allBytes = batches.flatten
    val count1 = iterateBatch(allBytes, compressed = false)
    require(count0 == count1, s"count0: $count0, count1: $count1")
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      allBytes,
      count0,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation2(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .map {
        cb =>
          val blockNativeWriter = new CHBlockWriterJniWrapper()
          blockNativeWriter.write(cb)
          (cb.numRows, blockNativeWriter.collectAsByteArray())
      }
      .collect
    val count0 = countsAndBytes.map(_._1).sum
    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    val allBytes = batches.flatten
    val count1 = iterateBatch(allBytes, compressed = false)
    require(count0 == count1, s"count0: $count0, count1: $count1")
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      allBytes,
      count0,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation4(child: SparkPlan): ClickHouseBuildSideRelation = {
    val dataSize = SQLMetrics.createSizeMetric(spark.sparkContext, "size of files read")

    val countsAndBytes = child
      .executeColumnar()
      .mapPartitions(iter => CHExecUtil.toBytes(dataSize, iter))
      .collect()

    val count0 = countsAndBytes.map(_._1).sum
    val batches = countsAndBytes.map(_._2)
    val rawSize = dataSize.value
    val allBytes = batches.flatten
    val count1 = iterateBatch(allBytes, compressed = true)
    require(count0 == count1, s"count0: $count0, count1: $count1")
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      allBytes,
      count0,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation5(child: SparkPlan): ClickHouseBuildSideRelation = {
    val dataSize = SQLMetrics.createSizeMetric(spark.sparkContext, "size of files read")

    val countsAndBytes = child
      .executeColumnar()
      .mapPartitionsInternal(iter => CHExecUtil.toBytes(dataSize, iter))
      .collect()

    val count0 = countsAndBytes.map(_._1).sum
    val batches = countsAndBytes.map(_._2)
    val rawSize = dataSize.value
    val allBytes = batches.flatten
    val count1 = iterateBatch(allBytes, compressed = true)
    require(count0 == count1, s"count0: $count0, count1: $count1")
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      allBytes,
      count0,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation6(child: SparkPlan): ClickHouseBuildSideRelation = {
    val dataSize = SQLMetrics.createSizeMetric(spark.sparkContext, "size of files read")

    val countsAndBytes = child
      .executeColumnar()
      .map {
        batch =>
          val bos = new ByteArrayOutputStream()
          val buffer = new Array[Byte](4 << 10) // 4K
          val dout =
            new BlockOutputStream(bos, buffer, dataSize, true, "lz4", Int.MinValue, buffer.length)
          dout.write(batch)
          dout.flush()
          dout.close()
          (batch.numRows, bos.toByteArray)
      }
      .collect()

    val count0 = countsAndBytes.map(_._1).sum
    val batches = countsAndBytes.map(_._2)
    val rawSize = dataSize.value
    val allBytes = batches.flatten
    val count1 = iterateBatch(batches.flatten, compressed = true)
    require(count0 == count1, s"count0: $count0, count1: $count1")
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      allBytes,
      count0,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def iterateBatch(array: Array[Byte], compressed: Boolean): Int = {
    val blockReader = new CHStreamReader(CHShuffleReadStreamFactory.create(array, compressed))
    val broadcastIter: Iterator[ColumnarBatch] = IteratorUtil.createBatchIterator(blockReader)
    broadcastIter.foldLeft(0) {
      case (acc, batch) =>
        acc + batch.numRows
    }
  }
}

/** NoopBroadcastMode does nothing. */
case object NoopBroadcastMode extends BroadcastMode {
  override def transform(rows: Array[InternalRow]): Any =
    throw new UnsupportedOperationException("NoOPBroadcastMode.transform")
  override def transform(rows: Iterator[InternalRow], sizeHint: Option[Long]): Any =
    throw new UnsupportedOperationException("NoOPBroadcastMode.transform")
  override def canonicalized: BroadcastMode =
    throw new UnsupportedOperationException("NoOPBroadcastMode.canonicalized")
}
