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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import java.io.{ByteArrayOutputStream, IOException, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.concurrent.duration._

class RunOptionsSuite extends SparkFunSuite {
  private def writeString(path: Path, text: String): Unit = {
    Files.write(path, text.getBytes(UTF_8))
  }

  private val catalog = Catalog.load()
  private def parse(args: String*): RunOptions.Parsed =
    RunOptions.parse(args.toArray)

  test("case rows fills only an unspecified row count") {
    val scenario = catalog.head.copy(rows = Some(2000000))
    val input = parse("trim").runtime.input
    assert(input.rowsFor(scenario) == 2000000)
    assert(input.rowsFor(scenario.copy(rows = None)) == 4000000)
    assert(parse("trim", "--rows", "4000000").runtime.input.rowsFor(scenario) == 4000000)
  }

  test("benchmark SQL configuration disables unsupported ANSI mode") {
    assert(RunOptions.sqlConf.toMap.get(SQLConf.ANSI_ENABLED.key).contains("false"))
    assert(!RunOptions.sqlConf.exists(_._1.endsWith("expression.dedup_non_deterministic")))
  }
  test("empty run and CLI listing need neither Spark nor compiler blackholes nor profiling") {
    val active = SparkSession.getActiveSession
    val default = SparkSession.getDefaultSession
    val directory = Files.createTempDirectory("empty-run")
    val output = directory.resolve("profiles")
    val options = RunOptions(
      Duration.Zero,
      Duration.Zero,
      profiler = Some(ProfilerOptions(directory.resolve("missing-profiler"), output = output)))
    val bytes = new ByteArrayOutputStream
    val out = new PrintStream(bytes)
    val oldOut = System.out
    try {
      System.setOut(out)
      RegisteredExpressionBenchmark.run(Seq.empty, options)
      assert(bytes.size() == 0 && !Files.exists(output))
      RegisteredExpressionBenchmark.run(Seq.empty, options.copy(profiler = None))
      RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--list", "trim"))
      assert(bytes.toString(UTF_8.name()).contains("trim/standard-string"))
      assert(SparkSession.getActiveSession == active && SparkSession.getDefaultSession == default)
      assert(!Files.exists(output))
    } finally {
      System.setOut(oldOut)
      out.close()
      Utils.deleteRecursively(directory.toFile)
    }
  }
  test("suite closes and clears standard output on success and parse failure") {
    Seq(false, true).foreach {
      invalid =>
        var closed = 0
        val stream = new ByteArrayOutputStream {
          override def close(): Unit = { closed += 1 }
        }
        assert(RegisteredExpressionBenchmark.output.isEmpty)
        RegisteredExpressionBenchmark.output = Some(stream)
        try {
          if (invalid) {
            intercept[IllegalArgumentException] {
              RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--unknown"))
            }
          } else RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--list", "trim"))
          assert(closed == 1 && RegisteredExpressionBenchmark.output.isEmpty)
        } finally RegisteredExpressionBenchmark.output = None
    }
  }
  test("suite preserves a primary parse error when closing standard output fails") {
    val cleanup = new IOException("output close failure")
    RegisteredExpressionBenchmark.output = Some(new ByteArrayOutputStream {
      override def close(): Unit = throw cleanup
    })
    try {
      val error = intercept[IllegalArgumentException] {
        RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--unknown"))
      }
      assert(error.getMessage.contains("Unknown --unknown"))
      assert(error.getSuppressed.toSeq == Seq(cleanup))
      assert(RegisteredExpressionBenchmark.output.isEmpty)
    } finally RegisteredExpressionBenchmark.output = None
  }
  test("runtime timing permits zero but enforces nonnegative durations and at least two samples") {
    val options = RunOptions(Duration.Zero, Duration.Zero)
    assert(options.minNumIters == 2)
    assert(options.copy(minNumIters = 3).minNumIters == 3)
    Seq(-1, 0, 1).foreach(n => intercept[IllegalArgumentException](options.copy(minNumIters = n)))
    intercept[IllegalArgumentException](options.copy(warmup = -1.nanos))
    intercept[IllegalArgumentException](options.copy(minTime = -1.nanos))
  }
  test("selection preserves first occurrence and catalog order with slash-safe globs") {
    val substring = catalog.filter(_.id.startsWith("substring/"))
    assert(parse("substring,substring").selected(catalog) == substring)
    val chosen = parse("--cases", "substring/*,rtrim/l1?-half-even,substring/*")
      .selected(catalog)
    assert(chosen == substring ++ catalog.filter(c => c.id.matches("rtrim/l1.-half-even")))
    assertThrows[IllegalArgumentException](parse("--cases", "*").selected(catalog))
    assertThrows[IllegalArgumentException](parse("--cases", "trim/*,missing/*").selected(catalog))
  }
  test("empty unknown conflicting selectors and list execution flags fail") {
    Seq(
      Seq.empty,
      Seq("rtrim,,trim"),
      Seq("missing"),
      Seq("--cases", ""),
      Seq("trim", "--cases", "trim/*"),
      Seq("--list", "--rows", "1"),
      Seq("--list", "--config", "missing.json"),
      Seq("trim", "--profile-event", "cpu"),
      Seq("trim", "--profile-output", "x"),
      Seq("trim", "--unknown", "1")
    ).foreach(args => assertThrows[IllegalArgumentException](parse(args: _*).selected(catalog)))
    Seq("blackhole", "observable").foreach {
      value =>
        val error = intercept[IllegalArgumentException](parse("trim", "--consumer", value))
        assert(error.getMessage.contains("Unknown --consumer"))
    }
  }
  test("defaults and input sizes are strict without forcing iteration counts") {
    val options = parse("trim")
    val input = options.runtime.input
    assert(input.rows.isEmpty && input.rowsFor(catalog.head) == 4000000 && input.batchSize == 10240)
    assert(input.seed == 20260912L && input.keyCardinality.isEmpty)
    assert(options.runtime.warmup.toSeconds == 10 && options.runtime.minTime.toSeconds == 60)
    assert(options.runtime.minNumIters == 2)
    Seq("--rows", "--batch-size").foreach {
      flag =>
        Seq("0", "-1", "2147483648", "1.5").foreach {
          value => assertThrows[IllegalArgumentException](parse("trim", flag, value))
        }
    }
    assert(parse("trim", "--seed", Long.MinValue.toString).runtime.input.seed == Long.MinValue)
    assertThrows[IllegalArgumentException](parse("trim", "--key-cardinality", "0"))
    assertThrows[IllegalArgumentException](parse("trim", "--seed", "9223372036854775808"))
  }
  test("explicit duration pair uses 2 plus 10 seconds without forcing iteration counts") {
    val options = parse("trim", "--warmup-seconds", "2", "--measurement-seconds", "10")
    assert(options.runtime.warmup.toSeconds == 2 && options.runtime.minTime.toSeconds == 10)
    assert(options.runtime.minNumIters == 2)
    val max = parse(
      "trim",
      "--warmup-seconds",
      Int.MaxValue.toString,
      "--measurement-seconds",
      Int.MaxValue.toString)
    assert(max.runtime.warmup.toNanos == Int.MaxValue.toLong * 1000000000L)
    assert(max.runtime.minTime.toNanos == Int.MaxValue.toLong * 1000000000L)
  }
  test("duration overrides validate each source and replace whole timing groups") {
    val cwd = Files.createTempDirectory("duration-cwd")
    val dir = Files.createDirectory(cwd.resolve("config"))
    val file = dir.resolve("run.json")
    def fromFile(args: String*): RunOptions =
      RunOptions.parse(Array("--config", "config/run.json") ++ args, cwd).runtime
    def write(fields: String): Unit =
      writeString(file, s"""{"functions":["trim"],$fields}""")
    try {
      write(""""warmupSeconds":2,"measurementSeconds":10""")
      val unchanged = fromFile("--rows", "17")
      assert(unchanged.warmup.toSeconds == 2 && unchanged.minTime.toSeconds == 10)
      val replaced = fromFile("--warmup-seconds", "3", "--measurement-seconds", "11")
      assert(replaced.warmup.toSeconds == 3 && replaced.minTime.toSeconds == 11)
      assertThrows[IllegalArgumentException](fromFile("--warmup-seconds", "3"))
      assertThrows[IllegalArgumentException](fromFile("--measurement-seconds", "11"))
      Seq(
        """"warmupSeconds":2""",
        """"measurementSeconds":10""",
        """"warmupSeconds":0,"measurementSeconds":10""",
        """"warmupSeconds":2,"measurementSeconds":2147483648""",
        """"warmupSeconds":2.0,"measurementSeconds":10""",
        """"warmupSeconds":"2","measurementSeconds":10"""
      ).foreach {
        fields =>
          write(fields)
          assertThrows[IllegalArgumentException](fromFile())
          assertThrows[IllegalArgumentException](
            fromFile("--warmup-seconds", "2", "--measurement-seconds", "10"))
      }
    } finally Utils.deleteRecursively(cwd.toFile)
  }
  test("strict JSON types keys UTF8 integers and trailing tokens") {
    val file = Files.createTempFile("expression-options", ".json")
    try {
      Seq(
        "{}",
        "[]",
        "{\"functions\":[\"trim\"],\"rows\":1.0}",
        "{\"functions\":[\"trim\"],\"rows\":2147483648}",
        "{\"functions\":[\"trim\"],\"seed\":9223372036854775808}",
        "{\"functions\":[\"trim\"],\"rows\":1,\"rows\":2}",
        "{\"functions\":[\"trim\"],\"sql\":\"trim(x)\"}",
        "{\"functions\":[1]}",
        "{\"functions\":[]}",
        "{\"functions\":[\"trim\"],\"cases\":[\"trim/*\"]}",
        "{\"functions\":[\"trim\"]} {}"
      ).foreach {
        json =>
          writeString(file, json)
          assertThrows[IllegalArgumentException](parse("--config", file.toString))
      }
      Seq("blackhole", "observable").foreach {
        value =>
          writeString(file, s"""{"functions":["trim"],"consumer":"$value"}""")
          val error = intercept[IllegalArgumentException](
            parse(
              "--config",
              file.toString,
              "--warmup-seconds",
              "2",
              "--measurement-seconds",
              "10"))
          assert(error.getMessage.contains("Unknown config key"))
      }
      val invalidUtf8 = """{"x":1}""".getBytes(UTF_8)
      invalidUtf8(2) = -1
      Files.write(file, invalidUtf8)
      assertThrows[IllegalArgumentException](parse("--config", file.toString))
    } finally Files.delete(file)
  }
  test("each scalar source is strict before valid overrides and selector replacement") {
    val file = Files.createTempFile("scalar-config-values", ".json")
    try {
      Seq(
        ("rows", "0", "--rows"),
        ("batchSize", "2147483648", "--batch-size"),
        ("rows", "false", "--rows"),
        ("seed", "9223372036854775808", "--seed"),
        ("keyCardinality", "-1", "--key-cardinality")
      ).foreach {
        case (key, value, flag) =>
          writeString(file, s"""{"cases":["trim/*"],"$key":$value}""")
          assertThrows[IllegalArgumentException](
            parse("--config", file.toString, "trim", flag, "17"))
      }
      writeString(
        file,
        s"""{"cases":[" trim/* "],"seed":${Long.MinValue},""" +
          s""""keyCardinality":${Long.MaxValue},"rows":2}""")
      val options = parse("--config", file.toString, "substring", "--rows", "3")
      assert(options.cases.isEmpty && options.functions == Seq("substring"))
      val input = options.runtime.input
      assert(input.seed == Long.MinValue && input.keyCardinality.contains(Long.MaxValue))
      assert(options.runtime.input.rows.contains(3))
      Seq("--rows", "--seed").foreach {
        flag =>
          Seq("+1", " 1", "1e2").foreach {
            value => assertThrows[IllegalArgumentException](parse("trim", flag, value))
          }
          assertThrows[IllegalArgumentException](parse("trim", flag, "1", flag, "2"))
          assertThrows[IllegalArgumentException](parse("trim", flag))
      }
      assert(parse("trim", "--rows", "001").runtime.input.rows.contains(1))
      assert(parse("trim", "--seed", "-0").runtime.input.seed == 0)
    } finally Files.delete(file)
  }
  test("CLI scalars and whole selectors override JSON and paths keep their own origin") {
    val cwd = Files.createTempDirectory("expression-cwd")
    val dir = Files.createDirectory(cwd.resolve("config"))
    val config = dir.resolve("run.json")
    try {
      writeString(
        config,
        """{"functions":["trim"],"rows":19,""" +
          """"asyncProfiler":"ap","profileOutput":"out"}""")
      val fromJson = RunOptions.parse(Array("--config", "config/run.json"), cwd)
      assert(fromJson.runtime.profiler.get.home == dir.resolve("ap"))
      assert(fromJson.runtime.profiler.get.output == dir.resolve("out"))
      val overrideJson = RunOptions.parse(
        Array(
          "--config",
          "config/run.json",
          "--cases",
          "rtrim/l13-half-even",
          "--rows",
          "17",
          "--async-profiler",
          "cli-ap",
          "--profile-output",
          "cli-out"),
        cwd
      )
      assert(overrideJson.selected(catalog).map(_.id) == Seq("rtrim/l13-half-even"))
      assert(overrideJson.runtime.input.rows.contains(17))
      assert(overrideJson.runtime.profiler.get.home == cwd.resolve("cli-ap"))
      assert(overrideJson.runtime.profiler.get.output == cwd.resolve("cli-out"))
    } finally Utils.deleteRecursively(cwd.toFile)
  }

  test("shuffle directory is optional and rejects empty paths") {
    assert(parse("trim").runtime.input.maps.isEmpty)
    assert(parse("trim", "--input-partitions", "8").runtime.input.maps.contains(8))
    Seq("0", "-1", "1.5", "2147483648").foreach {
      value => assertThrows[IllegalArgumentException](parse("trim", "--input-partitions", value))
    }
    assert(parse("trim").runtime.input.shuffleDir.isEmpty)
    val shuffleDir = "/dev/shm/gluten-benchmark"
    assert(
      parse(
        "trim",
        "--shuffle-dir",
        shuffleDir).runtime.input.shuffleDir.get.toString == shuffleDir)
    assertThrows[IllegalArgumentException](parse("trim", "--shuffle-dir", " "))
  }

}
