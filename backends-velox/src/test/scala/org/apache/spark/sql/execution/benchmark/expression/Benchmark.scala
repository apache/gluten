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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.spark.benchmark
import org.apache.spark.util.Utils

import java.io.OutputStream
import java.nio.file.Files

final private[benchmark] class Benchmark(
    evaluator: ExpressionEvaluator,
    options: RunOptions,
    output: Option[OutputStream] = None,
    profiler: Option[Profiler] = None)
  extends benchmark.Benchmark(
    evaluator.name,
    evaluator.numRows,
    options.minNumIters,
    options.warmup,
    options.minTime,
    output = output) {
  def registerCases(): Unit = {
    register("vanilla")(evaluator.runVanilla())
    register("native")(evaluator.runNative())
  }
  private[expression] def register(engine: String)(action: => Unit): Unit = profiler match {
    case None => super.addCase(engine)(_ => action)
    case Some(controller) =>
      val path = controller.profileRoot
        .resolve(evaluator.name)
        .resolve(engine)
        .resolve("profile.collapsed")
      var elapsed = 0L
      super.addTimerCase(engine) {
        timer =>
          if (timer.iteration <= 0) {
            Files.createDirectories(path.getParent)
            require(!Files.exists(path), s"Profile already exists: $path")
            elapsed = 0L
          }
          val measured = timer.iteration >= 0
          // Sample the whole measured phase, including gaps, without resetting the sampling clock.
          // A failed start must not authorize cleanup of another profiler session.
          if (timer.iteration == 0) controller.start()
          var stopAfter = true
          try
            Utils.tryWithSafeFinally {
              timer.startTiming()
              Utils.tryWithSafeFinally(action)(timer.stopTiming())
              if (measured) {
                if (options.minTime.length > 0) elapsed += timer.totalTime()
                stopAfter = timer.iteration + 1 >= options.minNumIters &&
                  elapsed >= options.minTime.toNanos
              }
            } {
              if (measured && stopAfter) {
                controller.stop()
                controller.dump(path)
              }
            }
          catch {
            case e: InterruptedException =>
              Thread.currentThread().interrupt()
              throw e
          }
      }
  }

}
