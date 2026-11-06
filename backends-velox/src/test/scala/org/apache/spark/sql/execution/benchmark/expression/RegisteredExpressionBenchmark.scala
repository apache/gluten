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

import org.apache.gluten.test.MockVeloxBackend

import org.apache.spark.benchmark.{BenchmarkBase => SparkBenchmarkBase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.GlutenImplicits.withSQLConf
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import java.nio.file.Files

import scala.util.control.NonFatal

/** Selects catalog cases and owns their Spark session and resource scopes. */
object RegisteredExpressionBenchmark extends SparkBenchmarkBase {
  override def runBenchmarkSuite(args: Array[String]): Unit = {
    // BenchmarkBase.main has no finally; close its file even when parsing or execution fails.
    Utils.tryWithSafeFinally {
      val options = RunOptions.parse(args)
      val catalog =
        Catalog.load() ++ Catalog.loadAggregates()
      if (options.list) {
        System.out.println(options.listing(catalog)) // scalastyle:ignore println
      } else {
        val cases = options.selected(catalog)
        require(cases.nonEmpty, "No selected cases")
        run(cases, options.runtime)
      }
    } {
      val stream = output
      output = None
      stream.foreach(_.close())
    }
  }

  private[benchmark] def run(cases: Seq[Catalog.CaseDef], options: RunOptions): Unit = {
    if (cases.isEmpty) return
    val input = options.input
    BenchmarkBlackhole.requireEnabled(true)
    val profiler = options.profiler.map(new Profiler(_))
    try
      Utils.tryWithSafeFinally {
        val conf = MockVeloxBackend
          .mockPluginContext()
          .conf()
          .setMaster("local[1]")
          .setAppName("ExpressionBenchmark")
          .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
          .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
          .set("spark.shuffle.compress", "false")
          .set("spark.memory.offHeap.enabled", "true")
          .set("spark.memory.offHeap.size", "8g")
          .set("spark.ui.enabled", "false")
        (RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf).foreach {
          case (key, value) => conf.set(key, value)
        }
        input.shuffleDir.foreach {
          path =>
            Files.createDirectories(path)
            require(
              Files.isWritable(path),
              s"Shuffle directory is not writable: $path")
            conf.set("spark.gluten.benchmark.shuffleDir", path.toString)
        }
        val spark = SparkSession.builder().config(conf)
          .withExtensions(new ExpressionBenchmarkExtensions).getOrCreate()
        Utils.tryWithSafeFinally {
          cases.foreach {
            scenario =>
              val resolved = scenario.copy(
                mode = input.aggregateMode.orElse(scenario.mode),
                maps = input.maps.orElse(scenario.maps),
                reducers = input.reducers.orElse(scenario.reducers),
                keyCardinality = input.keyCardinality.orElse(scenario.keyCardinality)
              )
              val context = Data.Context(
                input.rowsFor(resolved),
                resolved.keyCardinality,
                input.seed,
                resolved.maps.getOrElse(3))
              runCase(spark, resolved, context, options, profiler)
          }
        } {
          spark.stop()
        }
      } {
        profiler.foreach(_.stop())
      }
    catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw e
    }
  }

  // scalastyle:off println
  private def runCase(
      spark: SparkSession,
      scenario: Catalog.CaseDef,
      context: Data.Context,
      options: RunOptions,
      profiler: Option[Profiler]): Unit =
    withSQLConf((RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf): _*) {
      val plan = new ExpressionPlan(spark, scenario, context, Data.compile(scenario))
      try {
        val validation = plan.validate()
        if (!validation.ok()) {
          System.err.println("SKIPPED " + scenario.id + ": " + validation.reason())
        } else {
          val evaluator = new ExpressionEvaluator(plan, options.batchSize)
          val benchmark = new Benchmark(evaluator, options, output, profiler)
          TaskResources.runUnsafe {
            System.err.println(scenario.id + ": " + scenario.description)
            System.err.println(s"rows=${context.rows}, batchSize=${options.batchSize}")
            if (plan.aggregate || plan.window) {
              val mode = if (plan.window) "window" else scenario.mode.getOrElse("partial-final")
              System.err.println(
                s"mode=$mode, maps=${plan.partitions} (requested=${context.inputPartitions}), " +
                  s"reducers=1 (requested=${scenario.reducers.getOrElse(1)}), " +
                  s"keyCardinality=${context.keys}")
            }
            benchmark.registerCases()
            runBenchmark(scenario.id)(benchmark.run())
          }
        }
      } catch {
        case NonFatal(cause) =>
          throw new IllegalArgumentException(
            s"${scenario.sourceLocation} case=${scenario.id}: ${cause.getMessage}",
            cause)
      }
    }
  // scalastyle:on println
}
