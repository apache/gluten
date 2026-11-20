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
package org.apache.spark.shuffle

import org.apache.gluten.backendsapi.bolt.BoltMetricsApi
import org.apache.gluten.config.HashShuffleWriterType
import org.apache.gluten.proto.ShuffleWriterInfo
import org.apache.gluten.vectorized.NativePartitioning

import org.apache.spark.{HashPartitioner, SparkConf, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.mockito.Mockito.{mock, when}

class BoltCelebornShuffleWriterSuite extends SparkFunSuite with SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "1g")
    .set("spark.gluten.sql.columnar.backend.lib", "bolt")

  for (compress <- Seq(false, true)) {
    test(s"Celeborn serializes native writer options when shuffle compression is $compress") {
      val conf = SparkEnv.get.conf
      val original = conf.getOption("spark.shuffle.compress")
      val originalCodec = conf.getOption("spark.io.compression.codec")
      conf.set("spark.shuffle.compress", compress.toString)
      conf.set("spark.io.compression.codec", "lz4")
      try {
        TaskResources.runUnsafe {
          val dependency =
            mock(classOf[ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch]])
          when(dependency.partitioner).thenReturn(new HashPartitioner(2))
          when(dependency.nativePartitioning)
            .thenReturn(new NativePartitioning("hash", 2, null, null))
          when(dependency.metrics).thenReturn(new BoltMetricsApi()
            .genColumnarShuffleExchangeMetrics(spark.sparkContext, HashShuffleWriterType))
          val handle = mock(classOf[CelebornShuffleHandle[Int, ColumnarBatch, ColumnarBatch]])
          when(handle.dependency).thenReturn(dependency)
          val writer = new BoltCelebornColumnarShuffleWriter[Int, ColumnarBatch](
            0,
            handle,
            TaskContext.get(),
            new CelebornConf(),
            mock(classOf[ShuffleClient]),
            mock(classOf[ShuffleWriteMetricsReporter]))
          val method = classOf[BoltCelebornColumnarShuffleWriter[_, _]]
            .getDeclaredMethods.find(_.getName.endsWith("getShuffleWriterInfo")).get
          method.setAccessible(true)
          val info = method.invoke(writer).asInstanceOf[ShuffleWriterInfo]
          assert(info.getCompressionCodec == (if (compress) "lz4" else "uncompressed"))
          assert(info.getWriterType == "celeborn")
        }
      } finally {
        originalCodec match {
          case Some(value) => conf.set("spark.io.compression.codec", value)
          case None => conf.remove("spark.io.compression.codec")
        }
        original match {
          case Some(value) => conf.set("spark.shuffle.compress", value)
          case None => conf.remove("spark.shuffle.compress")
        }
      }
    }
  }
}
