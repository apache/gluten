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
package org.apache.spark.sql.execution.benchmark

private[spark] case class RateMicroBatchSample(
    batchId: Long,
    startedAtMs: Long,
    durationMs: Long,
    rows: Long)

private[spark] case class RateMicroBatchBenchmarkResult(
    rows: Long,
    batches: Int,
    elapsedMs: Double,
    rowsPerSecond: Double,
    meanBatchMs: Double,
    p95BatchMs: Long)

private[spark] object RateMicroBatchBenchmarkResult {
  def summarize(
      samples: Seq[RateMicroBatchSample],
      startMs: Long,
      endMs: Long,
      elapsedMs: Double): RateMicroBatchBenchmarkResult = {
    require(endMs > startMs, "The measurement window must have positive duration")
    require(
      elapsedMs > 0 && !elapsedMs.isInfinity && !elapsedMs.isNaN,
      "Elapsed time must be positive and finite")
    require(
      samples.forall(s => s.batchId >= 0 && s.durationMs >= 0 && s.rows >= 0),
      "Batch IDs, durations and sink row counts must be nonnegative")

    // Exclude warm-up and batches crossing either boundary. Never prorate a batch's rows.
    val measured = samples.filter {
      sample =>
        sample.startedAtMs >= startMs && sample.startedAtMs < endMs &&
        sample.durationMs <= endMs - sample.startedAtMs
    }
    require(measured.nonEmpty, "No complete measured batches; increase the measurement duration")
    require(
      measured.map(_.batchId).sliding(2).forall(ids => ids.size < 2 || ids(1) == ids(0) + 1),
      "Measured batch progress is missing, duplicated or reordered")
    val rows = measured.foldLeft(0L)((sum, sample) => Math.addExact(sum, sample.rows))
    require(rows > 0, "No measured rows; increase the rate or measurement duration")
    val durations = measured.map(_.durationMs).sorted
    RateMicroBatchBenchmarkResult(
      rows,
      measured.size,
      elapsedMs,
      rows * 1000.0 / elapsedMs,
      durations.map(_.toDouble).sum / durations.size,
      durations(math.ceil(durations.size * 0.95).toInt - 1))
  }
}
