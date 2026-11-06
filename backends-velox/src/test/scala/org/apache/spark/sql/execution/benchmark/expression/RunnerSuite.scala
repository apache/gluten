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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.GlutenImplicits.withSQLConf
import org.apache.spark.sql.execution.benchmark.expression.Catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class RunnerSuite extends SparkFunSuite {
  private def checkRegistration(): Unit = {
    val events = ArrayBuffer.empty[String]
    val scenario = CaseDef(
      "registration/shared",
      Seq.empty,
      "1",
      "Registration",
      SourceLocation("registration", 1, 1))
    val evaluator = new ExpressionEvaluator(
      new ExpressionPlan(null, scenario, Data.Context(1), Data.compile(scenario))) {
      override def outputSchema = new StructType
      override def runVanilla(): Unit = { events += "vanilla" }
      override def runNative(): Unit = { events += "native" }
    }
    val benchmark = new Benchmark(
      evaluator,
      RunOptions(Duration.Zero, Duration.Zero))
    assert(events.isEmpty)
    benchmark.registerCases()
    assert(events.isEmpty)
    benchmark.run()
    val execution = events
    assert(execution.takeWhile(_ == "vanilla").size >= 2)
    assert(execution.dropWhile(_ == "vanilla").size >= 2)
    assert(execution.dropWhile(_ == "vanilla").forall(_ == "native"))
  }

  test("direct run uses supplied case order and input definitions without catalog selection") {
    assume(!SPARK_VERSION.startsWith("3."), "Runner requires Spark 4")
    val directory = Files.createTempDirectory("expression-direct-run")
    val log = directory.resolve("child.log")
    val builder = new ProcessBuilder(blackholeJavaCommand :+ getClass.getName: _*)
      .directory(directory.toFile)
      .redirectErrorStream(true)
      .redirectOutput(log.toFile)
    builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    val process = builder.start()
    try {
      assert(process.waitFor(90, TimeUnit.SECONDS), "Direct-run test timed out")
      assert(process.exitValue() == 0, new String(Files.readAllBytes(log), UTF_8))
    } finally {
      if (process.isAlive) process.destroyForcibly().waitFor()
      Utils.deleteRecursively(directory.toFile)
    }
  }

  private[expression] def checkDirectRun(): Unit = {
    checkRegistration()
    val second = CaseDef(
      "memory/second",
      Seq(Binding(Seq("right_input"), "standard.long", Seq.empty)),
      "right_input + 1",
      "Supplied long input",
      SourceLocation("memory", 1, 1))
    val first = CaseDef(
      "memory/first",
      Seq(Binding(Seq("left_input"), "standard.string", Seq("length" -> LongArgument(3)))),
      "rtrim(left_input)",
      "Supplied string input",
      SourceLocation("memory", 2, 1)
    )
    val options = RunOptions(
      Duration.Zero,
      Duration.Zero,
      input = InputOptions(rows = Some(7), batchSize = 3, seed = -7L))
    val bytes = new ByteArrayOutputStream
    val out = new PrintStream(bytes)
    val oldOut = System.out
    val oldErr = System.err
    val errors = new PrintStream(new ByteArrayOutputStream)
    val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
    def assertClean(): Unit = {
      assert(!TaskResources.inSparkTask())
      assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
      assert(SparkSession.getActiveSession.isEmpty && SparkSession.getDefaultSession.isEmpty)
    }
    try {
      System.setOut(out)
      System.setErr(errors)
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
        RegisteredExpressionBenchmark.run(Seq(second, first), options)
        assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
      }
      val invalidAggregate =
        second.copy(sql = "sum(missing_column)", groupingKeys = Some(Seq.empty))
      val planningFailure = intercept[IllegalArgumentException] {
        RegisteredExpressionBenchmark.run(Seq(invalidAggregate), options)
      }
      assert(planningFailure.getMessage.contains(invalidAggregate.id))
      assert(planningFailure.getMessage.contains(invalidAggregate.sourceLocation.toString))
      assert(planningFailure.getCause != null)
      assertClean()
      val text = bytes.toString(UTF_8.name())
      val results = text.linesIterator.filter(_.startsWith("memory/")).toSeq
      assert(results.map(_.takeWhile(_ != ' ').stripSuffix(":")) == Seq(second.id, first.id))
      val engines =
        text.linesIterator
          .filter(l => l.startsWith("vanilla") || l.startsWith("native"))
          .map(_.takeWhile(_ != ' '))
          .toSeq
      assert(engines == Seq("vanilla", "native", "vanilla", "native"))
      assertClean()

      bytes.reset()
      val failure = withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
        val error = intercept[IllegalArgumentException] {
          RegisteredExpressionBenchmark.run(Seq(first, second.copy(sql = "missing + 1")), options)
        }
        assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
        error
      }
      assert(failure.getMessage.contains("memory/second") && failure.getCause != null)
      val partial = bytes.toString(UTF_8.name())
      assert(partial.linesIterator.exists(_.startsWith(first.id)))
      assert(!partial.linesIterator.exists(_.startsWith(second.id)))
      assertClean()

      Seq(false, true).foreach {
        interrupted =>
          val primary =
            if (interrupted) new InterruptedException("Spark statistics")
            else new IllegalStateException("Spark statistics")
          val console = new PrintStream(new ByteArrayOutputStream) {
            override def println(value: Any): Unit = { // scalastyle:ignore println
              if (String.valueOf(value).contains("Stopped after")) throw primary
            }
          }
          try
            Console.withOut(console) {
              withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
                val thrown = intercept[Exception] {
                  RegisteredExpressionBenchmark.run(Seq(first), options)
                }
                if (interrupted) assert(thrown eq primary)
                else {
                  assert(thrown.isInstanceOf[IllegalArgumentException])
                  assert(thrown.getCause eq primary)
                }
                assert(thrown.getSuppressed.isEmpty && primary.getSuppressed.isEmpty)
                assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
              }
              assert(Thread.currentThread().isInterrupted == interrupted)
              assertClean()
            }
          finally {
            Thread.interrupted()
            console.close()
          }
      }

    } finally {
      System.setOut(oldOut)
      System.setErr(oldErr)
      out.close()
      errors.close()
    }
  }

  private def blackholeJavaCommand: Seq[String] = Seq(
    new File(System.getProperty("java.home"), "bin/java").getPath,
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Dspark.testing=true",
    "-Dgluten.expressionBenchmark.diagnostics=true",
    "-Xmx2g",
    "-XX:+UnlockExperimentalVMOptions",
    "-XX:CompileCommand=blackhole," + classOf[BenchmarkBlackhole].getName + "::consume",
    "-cp",
    System.getProperty("java.class.path")
  )

  test("runner writes Spark output and propagates profiler loading failures") {
    assume(!SPARK_VERSION.startsWith("3."), "Runner requires Spark 4")
    val directory = Files.createTempDirectory("expression-output")
    val stdout = directory.resolve("stdout.log")
    val stderr = directory.resolve("stderr.log")
    val command = blackholeJavaCommand ++ Seq(
      RegisteredExpressionBenchmark.getClass.getName.stripSuffix("$"),
      "--cases",
      "trim/standard-string,sum/global",
      "--rows",
      "1000",
      "--warmup-seconds",
      "1",
      "--measurement-seconds",
      "1"
    )
    def run(args: Seq[String], files: Boolean): (Int, String, String) = {
      val builder = new ProcessBuilder(args: _*).directory(directory.toFile)
        .redirectOutput(stdout.toFile).redirectError(stderr.toFile)
      builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
      builder.environment().remove("SPARK_GENERATE_BENCHMARK_FILES")
      if (files) builder.environment().put("SPARK_GENERATE_BENCHMARK_FILES", "1")
      val process = builder.start()
      try {
        assert(process.waitFor(90, TimeUnit.SECONDS), "Benchmark timed out")
        (
          process.exitValue(),
          new String(Files.readAllBytes(stdout), UTF_8),
          new String(Files.readAllBytes(stderr), UTF_8))
      } finally if (process.isAlive) process.destroyForcibly().waitFor()
    }
    try {
      Seq(false, true).foreach {
        files =>
          val (exit, output, errors) = run(command, files)
          assert(exit == 0, errors)
          assert(output.contains("trim/standard-string:") && output.contains("sum/global:"))
          Seq(
            "Best Time(ms)",
            "Avg Time(ms)",
            "Stdev(ms)",
            "Rate(M/s)",
            "Per Row(ns)",
            "Relative",
            "vanilla",
            "native").foreach(label => assert(output.contains(label)))
          assert(Files.exists(directory.resolve("benchmarks")) == files)
          if (files) {
            val reports = Files.list(directory.resolve("benchmarks"))
            try {
              val report = new String(Files.readAllBytes(reports.findFirst().get()), UTF_8)
              assert(report.contains("sum/global") && report.contains("vanilla") &&
                report.contains("native"))
            } finally reports.close()
          }
      }
      val profiler = Files.createDirectory(directory.resolve("profiler"))
      val (exit, _, errors) = run(command ++ Seq("--async-profiler", profiler.toString), false)
      assert(exit != 0 && errors.contains("UnsatisfiedLinkError"), errors)
      assert(!errors.contains("SKIPPED"))
    } finally Utils.deleteRecursively(directory.toFile)
  }

}

object RunnerSuite {
  def main(args: Array[String]): Unit = new RunnerSuite().checkDirectRun()
}
