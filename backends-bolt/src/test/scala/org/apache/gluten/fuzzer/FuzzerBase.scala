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
package org.apache.gluten.fuzzer

import org.apache.gluten.benchmarks.RandomParquetDataGenerator
import org.apache.gluten.execution.BoltWholeStageTransformerSuite
import org.apache.gluten.fuzzer.FuzzerResult.{Failed, OOM, Successful, TestResult}
import org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget

import org.apache.spark.SparkConf

abstract class FuzzerBase extends BoltWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  protected val dataGenerator = RandomParquetDataGenerator(System.currentTimeMillis())

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.maxResultSize", "4g")
  }

  protected def defaultRunner(f: () => Unit): () => TestResult =
    () => {
      try {
        f()
        Successful(dataGenerator.getSeed)
      } catch {
        case oom: java.lang.OutOfMemoryError =>
          logError(
            s"Cannot verify test with seed ${dataGenerator.getSeed} " +
              s"as the required execution memory is too large",
            oom)
          OOM(dataGenerator.getSeed)
        case oom: ThrowOnOomMemoryTarget.OutOfMemoryException =>
          logError(s"Out of memory while running test with seed: ${dataGenerator.getSeed}", oom)
          OOM(dataGenerator.getSeed)
        case t: Throwable =>
          val rootCause = getRootCause(t).getMessage
          if (
            rootCause != null && rootCause.contains(
              classOf[ThrowOnOomMemoryTarget.OutOfMemoryException].getName)
          ) {
            logError(s"Out of memory while running test with seed: ${dataGenerator.getSeed}", t)
            OOM(dataGenerator.getSeed)
          } else {
            logError(s"Failed to run test with seed: ${dataGenerator.getSeed}", t)
            Failed(dataGenerator.getSeed)
          }
      }
    }

  protected def repeat(iterations: Int, testName: String, runTest: () => TestResult): Unit = {
    val result = (0 until iterations)
      .map {
        i =>
          val seed = dataGenerator.getSeed
          logWarning(
            s"==============================> " +
              s"Started iteration $i (seed: $seed)")
          sparkContext.setJobDescription(s"'$testName' with seed: $seed")
          val result = runTest()
          dataGenerator.reFake(System.currentTimeMillis())
          result
      }
    val oom = result.filter(_.isInstanceOf[OOM]).map(_.getSeed)
    if (oom.nonEmpty) {
      logError(s"Out of memory while running test '$testName' with seed: ${oom.mkString(", ")}")
    }
    val failed = result.filter(_.isInstanceOf[Failed]).map(_.getSeed)
    assert(failed.isEmpty, s"Failed to run test '$testName' with seed: ${failed.mkString(",")}")
  }

  def getRootCause(e: Throwable): Throwable = {
    if (e.getCause == null) {
      return e
    }
    getRootCause(e.getCause)
  }
}
