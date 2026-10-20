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
package org.apache.spark.util

import org.apache.spark.SparkConf

import org.scalatest.funsuite.AnyFunSuite

class SparkResourceUtilSuite extends AnyFunSuite {

  test("getTaskSlots floors at one when task cpus exceed executor cores") {
    // spark.task.cpus > executor cores is an invalid config that Spark rejects later, but Gluten
    // reads task slots during plugin init and divides by the result. Returning 0 here makes that
    // division throw ArithmeticException before Spark's validateTaskCpusLargeEnough can report the
    // real misconfiguration.
    val conf = new SparkConf(false)
      .set("spark.master", "local[1]")
      .set("spark.task.cpus", "2")
    assert(SparkResourceUtil.getTaskSlots(conf) == 1)
  }

  test("getTaskSlots fails fast when task cpus is zero") {
    // spark.task.cpus is read via raw conf.getInt, which bypasses Spark's checkValue(_ > 0) (a
    // check that only exists on Spark >= 4.2), so a zero value must not reach the division. Fail
    // fast with a clear message instead of an opaque "/ by zero" ArithmeticException.
    val conf = new SparkConf(false)
      .set("spark.master", "local[8]")
      .set("spark.task.cpus", "0")
    val e = intercept[IllegalArgumentException](SparkResourceUtil.getTaskSlots(conf))
    assert(e.getMessage.contains("spark.task.cpus should be positive"))
  }

  test("getTaskSlots fails fast when task cpus is negative") {
    // A negative spark.task.cpus would otherwise divide to a negative slot count, silently yielding
    // negative per-task off-heap budgets. Fail fast rather than propagate the bad value.
    val conf = new SparkConf(false)
      .set("spark.master", "local[8]")
      .set("spark.task.cpus", "-2")
    val e = intercept[IllegalArgumentException](SparkResourceUtil.getTaskSlots(conf))
    assert(e.getMessage.contains("spark.task.cpus should be positive"))
  }

  test("getTaskSlots divides executor cores by task cpus") {
    val conf = new SparkConf(false)
      .set("spark.master", "local[8]")
      .set("spark.task.cpus", "2")
    assert(SparkResourceUtil.getTaskSlots(conf) == 4)
  }

  test("getTaskSlots returns one slot per core when task cpus defaults to one") {
    val conf = new SparkConf(false).set("spark.master", "local[8]")
    assert(SparkResourceUtil.getTaskSlots(conf) == 8)
  }

  test("getExecutorMemorySize reads a bare spark.executor.memory as MiB") {
    // Spark defines spark.executor.memory as bytesConf(ByteUnit.MiB), so a value without a size
    // suffix means MiB. Reading it with SparkConf#getSizeAsBytes would treat 8192 as 8192 bytes.
    val conf = new SparkConf(false).set("spark.executor.memory", "8192")
    assert(SparkResourceUtil.getExecutorMemorySize(conf) == 8192L * 1024 * 1024)
  }

  test("getExecutorMemorySize honours a size suffix on spark.executor.memory") {
    val conf = new SparkConf(false).set("spark.executor.memory", "8g")
    assert(SparkResourceUtil.getExecutorMemorySize(conf) == 8L * 1024 * 1024 * 1024)
  }

  test("getExecutorMemorySize falls back to the Spark default when unset") {
    // spark.executor.memory defaults to 1g in Spark.
    val conf = new SparkConf(false)
    assert(SparkResourceUtil.getExecutorMemorySize(conf) == 1024L * 1024 * 1024)
  }

  test("getExecutorMemorySize rejects an executor memory that overflows on conversion") {
    // The MiB-to-byte conversion multiplies by 2^20, so a suffix-less byte count large enough to
    // overflow must fail rather than wrap to a negative budget.
    val conf = new SparkConf(false).set("spark.executor.memory", "9000000000000")
    val e = intercept[IllegalArgumentException](SparkResourceUtil.getExecutorMemorySize(conf))
    assert(e.getMessage.contains("exceeds Long.MAX_VALUE"))
  }

  test("getExecutorMemorySize rejects a negative executor memory") {
    // Spark's typed entry carries no positivity check, and the conversion would pass a negative
    // through, so guard it here rather than propagate a negative budget.
    val conf = new SparkConf(false).set("spark.executor.memory", "-8192")
    val e = intercept[IllegalArgumentException](SparkResourceUtil.getExecutorMemorySize(conf))
    assert(e.getMessage.contains("spark.executor.memory should not be negative"))
  }

  test("getMemoryOverheadSize honours a size suffix on the minimum overhead") {
    // spark.executor.minMemoryOverhead is a size string, so reading it with conf.getLong throws
    // NumberFormatException on any value carrying a unit. VeloxListenerApi#onDriverStart calls this
    // unconditionally, so that would abort driver startup on a value Spark itself accepts.
    val conf = new SparkConf(false)
      .set("spark.executor.memory", "1g")
      .set("spark.executor.minMemoryOverhead", "512m")
    assert(SparkResourceUtil.getMemoryOverheadSize(conf) == 512L * 1024 * 1024)
  }

  test("getMemoryOverheadSize falls back to the 384m minimum overhead") {
    val conf = new SparkConf(false).set("spark.executor.memory", "1g")
    assert(SparkResourceUtil.getMemoryOverheadSize(conf) == 384L * 1024 * 1024)
  }

  test("getMemoryOverheadSize prefers the factor when it exceeds the minimum") {
    // 8g * 0.1 = 819 MiB, above the 384 MiB floor.
    val conf = new SparkConf(false)
      .set("spark.executor.memory", "8g")
      .set("spark.executor.minMemoryOverhead", "512m")
    assert(SparkResourceUtil.getMemoryOverheadSize(conf) == 819L * 1024 * 1024)
  }

  test("getMemoryOverheadSize reads a suffix-less minimum overhead as MiB") {
    // The value users are most likely to already have. It read the same before the switch to
    // getSizeAsMb, so nothing pinned it; a future accessor change could silently reinterpret it.
    val conf = new SparkConf(false)
      .set("spark.executor.memory", "1g")
      .set("spark.executor.minMemoryOverhead", "512")
    assert(SparkResourceUtil.getMemoryOverheadSize(conf) == 512L * 1024 * 1024)
  }

  test("getMemoryOverheadSize rejects an overhead that overflows on conversion") {
    // spark.executor.memoryOverhead is MiB-declared and carries no magnitude check, so the
    // MiB-to-byte conversion must raise rather than wrap to a negative budget.
    val conf = new SparkConf(false)
      .set("spark.executor.memory", "1g")
      .set("spark.executor.memoryOverhead", "9000000000000")
    val e = intercept[IllegalArgumentException](SparkResourceUtil.getMemoryOverheadSize(conf))
    assert(e.getMessage.contains("exceeds Long.MAX_VALUE"))
  }

  test("getMemoryOverheadSize rejects a negative explicit overhead") {
    // An explicit spark.executor.memoryOverhead skips the floor below, so guard the conversion.
    val conf = new SparkConf(false)
      .set("spark.executor.memory", "1g")
      .set("spark.executor.memoryOverhead", "-1")
    val e = intercept[IllegalArgumentException](SparkResourceUtil.getMemoryOverheadSize(conf))
    assert(e.getMessage.contains("should not be negative"))
  }
}
