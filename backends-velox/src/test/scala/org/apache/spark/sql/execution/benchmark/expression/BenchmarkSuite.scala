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

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.benchmark.{Benchmark => SparkBenchmark}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, GenericInternalRow, LeafExpression, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.benchmark.expression.Catalog._
import org.apache.spark.sql.execution.benchmark.expression.Data.Context
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import org.apache.xbean.asm9.{ClassReader, ClassVisitor, MethodVisitor, Opcodes}
import org.codehaus.commons.compiler.util.reflect.ByteArrayClassLoader
import org.mockito.Mockito

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class BenchmarkSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", classOf[ExpressionBenchmarkExtensions].getName)

  // Generated consumers, repeated execution, measurement and profiling.
  private val marker = "org/apache/spark/sql/execution/benchmark/expression/BenchmarkBlackhole"

  private def metricCase(id: String, description: String, sql: String = "input"): CaseDef =
    CaseDef(s"metric/$id", Seq.empty, sql, description, SourceLocation("metric", 1, 1))

  private def prepareTerminal(
      expression: Expression,
      attributes: Seq[Attribute]) = {
    val predicate = Predicate.create(
      ConsumeCodegenResult(expression),
      attributes)
    predicate.initialize(0)
    predicate
  }

  private def calls(generated: Class[_]): Seq[(String, String, String)] = {
    // Janino does not expose generated classes as classpath resources.
    val field = classOf[ByteArrayClassLoader].getDeclaredField("classes")
    field.setAccessible(true)
    val classes =
      field.get(generated.getClassLoader).asInstanceOf[JMap[String, Array[Byte]]]
    val result = ArrayBuffer.empty[(String, String, String)]
    new ClassReader(classes.get(generated.getName)).accept(
      new ClassVisitor(Opcodes.ASM9) {
        override def visitMethod(
            access: Int,
            name: String,
            descriptor: String,
            signature: String,
            exceptions: Array[String]): MethodVisitor = new MethodVisitor(Opcodes.ASM9) {
          override def visitMethodInsn(
              opcode: Int,
              owner: String,
              name: String,
              descriptor: String,
              isInterface: Boolean): Unit = { result += ((owner, name, descriptor)) }
        }
      },
      0
    )
    result.toVector
  }

  test("metric: generated results use matching blackhole overloads for values and nulls") {
    withSQLConf(RunOptions.sqlConf: _*) {
      Seq(
        (BooleanType, true, "Z"),
        (ByteType, 7.toByte, "B"),
        (ShortType, 8.toShort, "S"),
        (IntegerType, 9, "I"),
        (LongType, 10L, "J"),
        (FloatType, 1.5f, "F"),
        (DoubleType, 2.5d, "D"),
        (DateType, 20, "I"),
        (YearMonthIntervalType(), 14, "I"),
        (TimestampType, 30L, "J"),
        (BinaryType, Array[Byte](1, 2), "[B"),
        (StringType, UTF8String.fromString("value"), "Ljava/lang/Object;JI"),
        (DecimalType(20, 0), Decimal(123L, 20, 0), "Ljava/lang/Object;")
      ).foreach {
        case (dataType, value, descriptor) =>
          val predicate =
            prepareTerminal(
              BoundReference(0, dataType, nullable = true),
              Seq.empty)
          assert(predicate.eval(InternalRow(value)))
          assert(predicate.eval(InternalRow(null)))
          val instructions = calls(predicate.getClass)
          assert(
            instructions.filter(_._1 == marker) ==
              Seq((marker, "consume", s"(Z$descriptor)V")),
            dataType.toString)
          assert(!instructions.exists {
            case (_, name, _) =>
              Set("copy", "getBytes", "toUnscaledLong", "toJavaBigDecimal", "toDouble", "valueOf")
                .contains(name)
          })
          assert(!instructions.exists { case (owner, _, _) => owner.contains("BoxesRunTime") })

      }
    }
  }
  test("independent scalar execution retains owned results after task cleanup") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val schema = new StructType().add("bytes", BinaryType).add("number", DecimalType(20, 0))
      var generated = 0
      val data = Data.Plan(
        schema,
        (_, i, _, _) => {
          generated += 1
          InternalRow(Array(i.toByte), Decimal(i, 20, 0))
        })
      val expected = ArrayBuffer.empty[InternalRow]
      val actual = ArrayBuffer.empty[InternalRow]
      val evaluator = ExpressionEvaluator(
        spark,
        metricCase("snapshots", "Nested snapshots", "named_struct('b', bytes, 'd', number)"),
        Context(5),
        2,
        data = Some(data),
        vanillaCollector = Some(row => expected += row),
        nativeCollector = Some(row => actual += row)
      )
      val snapshot = TaskResources.runUnsafe {
        evaluator.runVanilla()
        val snapshot = expected.toVector
        evaluator.runVanilla()
        assert(expected.size == 10)
        expected.clear()
        evaluator.runVanilla()
        assert(evaluator.outputSchema == new StructType().add(
          "__gluten_expression_benchmark_result",
          new StructType().add("b", BinaryType).add("d", DecimalType(20, 0)),
          false))
        evaluator.runNative()
        ExpressionResultChecks.verify(evaluator, expected.toVector, actual.toVector)()
        evaluator.runNative()
        assert(actual.size == 10)
        assert(generated == 5)
        snapshot
      }
      Seq(snapshot, expected.toVector, actual.take(5).toVector).foreach {
        rows =>
          assert(rows.size == 5)
          rows.zipWithIndex.foreach {
            case (row, i) =>
              val value = row.getStruct(0, 2)
              assert(value.getBinary(0).sameElements(Array(i.toByte)))
              assert(value.getDecimal(1, 20, 0) == Decimal(i.toLong, 20, 0))
          }
      }
    }
  }

  test("JVM execution initializes state each round and propagates failures") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val collected = ArrayBuffer.empty[InternalRow]
      val scenario = metricCase(
        "state",
        "Persistent evaluator",
        "monotonically_increasing_id()")
      val evaluator = new ExpressionEvaluator(
        new ExpressionPlan(spark, scenario, Context(5), Data.compile(scenario)),
        vanillaCollector = Some(row => collected += row))
      TaskResources.runUnsafe {
        (0 until 2).foreach {
          _ =>
            collected.clear()
            evaluator.runVanilla()
            assert(collected.map(_.getLong(0)).toVector == Vector(0L, 1L, 2L, 3L, 4L))
        }
      }
      TaskResources.runUnsafe {
        val owner = TaskContext.get()
        var failed: Option[TaskContext] = None
        var completed = false
        val evaluator = ExpressionEvaluator(
          spark,
          metricCase("collector-failure", "Collector failure", "id + 1"),
          Context(5),
          2,
          data = Some(Data.Plan(
            new StructType().add("id", LongType),
            (_, i, _, _) => InternalRow(i.toLong))),
          vanillaCollector = Some(
            _ => {
              val task = TaskContext.get()
              assert(task ne owner)
              failed = Some(task)
              task.addTaskCompletionListener[Unit](_ => completed = true)
              throw new IllegalStateException("collector failure")
            })
        )
        assert(intercept[IllegalStateException](evaluator.runVanilla())
          .getMessage == "collector failure")
        assert(completed)
        assert(failed.exists(task => task.isFailed() && task.isCompleted()))
        assert(TaskContext.get() eq owner)
      }
    }
  }

  private val cpuStart = "start,event=cpu,interval=1ms,cstack=dwarf,threads"
  private val measuredCommands = Seq(cpuStart, "stop")

  private def withProfile(options: RunOptions = RunOptions(Duration.Zero, Duration.Zero))(
      f: (Benchmark, Profiler, ArrayBuffer[String], Path) => Unit): Unit = {
    val root = Files.createTempDirectory("expression-shared-profiler")
    val commands = ArrayBuffer.empty[String]
    val profiler =
      Mockito.spy(new Profiler(ProfilerOptions(root, output = root)))
    Mockito
      .doReturn(
        (command: String) => {
          commands += command
          ""
        },
        Array.empty[Object]: _*)
      .when(profiler)
      .execute
    val scenario = metricCase("profile", "Profile")
    val benchmark =
      new Benchmark(
        new ExpressionEvaluator(
          new ExpressionPlan(null, scenario, Context(3), Data.compile(scenario)),
          options.batchSize),
        options,
        profiler = Some(profiler))
    try
      withSQLConf(RunOptions.sqlConf: _*) {
        TaskResources.runUnsafe {
          f(
            benchmark,
            profiler,
            commands,
            profiler.profileRoot
              .resolve(scenario.id)
              .resolve("vanilla")
              .resolve("profile.collapsed"))
        }
      }
    finally Utils.deleteRecursively(root.toFile)
  }

  test("metric: explicit warmup and Spark-owned measurement: 3 / 5.millis") {
    val minNumIters = 3
    val minTime = 5.millis
    withProfile(RunOptions(Duration.Zero, minTime, minNumIters)) {
      (benchmark, profiler, commands, path) =>
        val iterations = ArrayBuffer.empty[Int]
        var measuredMillis = -1L
        var timer = Option.empty[SparkBenchmark.Timer]
        // Exercise registration without requiring native expression preparation.
        benchmark.register("vanilla") {
          iterations += timer.get.iteration
          if (timer.get.iteration < 0) assert(commands.isEmpty && !profiler.owned)
          else assert(profiler.owned && !Files.exists(path))
          Thread.sleep(1)
        }
        val callback = benchmark.benchmarks.last
        def runIteration(current: SparkBenchmark.Timer): Unit = {
          timer = Some(current)
          callback.fn(current)
          if (current.iteration < 0) {
            assert(commands.isEmpty && !profiler.owned && !Files.exists(path))
          } else assert(profiler.owned == !Files.exists(path))
        }
        val console = new PrintStream(new ByteArrayOutputStream) {
          override def println(value: Any): Unit = { // scalastyle:ignore println
            if (String.valueOf(value).contains("Stopped after")) {
              assert(!profiler.owned && commands.last == "collapsed,total")
              assert(Files.isReadable(path))
              val elapsed = "Stopped after [0-9]+ iterations, ([0-9]+) ms".r
              measuredMillis = elapsed.findFirstMatchIn(String.valueOf(value)).get.group(1).toLong
            }
          }
        }
        try
          Console.withOut(console) {
            // A real warmup deadline can expire before its first callback.
            (0 until 2).foreach(_ => runIteration(new SparkBenchmark.Timer(-1)))
            benchmark.measure(3L, callback.numIters)(runIteration)
          }
        finally console.close()
        val measured = iterations.filter(_ >= 0)
        assert(iterations.take(2).toSeq == Seq(-1, -1))
        assert(measured.toSeq == measured.indices)
        assert(measured.size >= minNumIters && measuredMillis >= minTime.toMillis)
        assert(commands.toSeq == measuredCommands :+ "collapsed,total")
        assert(Files.size(path) == 0)
    }
  }
  test("metric: profiling completes both engines across cases") {
    withProfile() {
      (_, profiler, commands, _) =>
        Seq("first", "second").foreach {
          id =>
            val scenario = metricCase(id, "Profile registration")
            val benchmark = new Benchmark(
              new ExpressionEvaluator(
                new ExpressionPlan(null, scenario, Context(0), Data.compile(scenario))),
              RunOptions(Duration.Zero, Duration.Zero),
              profiler = Some(profiler)
            )
            Seq("vanilla", "native").foreach(engine => benchmark.register(engine)(()))
            benchmark.run()
            Seq("vanilla", "native").foreach {
              engine =>
                assert(Files.isReadable(profiler.profileRoot.resolve(scenario.id)
                  .resolve(engine).resolve("profile.collapsed")))
            }
        }
        assert(commands.toSeq == Seq.fill(4)(measuredCommands :+ "collapsed,total").flatten)
        assert(!profiler.owned)
    }
  }
  test("metric: complex terminal does not serialize the generated result") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val reads = new AtomicInteger
      val array = new GenericArrayData(Array(3, 1, 2)) {
        override def getInt(ordinal: Int): Int = {
          reads.incrementAndGet()
          super.getInt(ordinal)
        }
      }
      val row = new GenericInternalRow(Array[Any](3)) {
        override def getInt(ordinal: Int): Int = {
          reads.incrementAndGet()
          super.getInt(ordinal)
        }
      }
      val map = new ArrayBasedMapData(array, array)
      Seq(
        (array, ArrayType(IntegerType, false)),
        (map, MapType(IntegerType, IntegerType, false)),
        (row, new StructType().add("value", IntegerType))).foreach {
        case (value, dataType) =>
          val evaluations = new AtomicInteger
          def expression: Expression = MetricTestResult(value, dataType, evaluations)
          val metric = prepareTerminal(expression, Seq.empty)
          val inputs = Array.fill(5)(new UnsafeRow(0))
          assert(evaluations.get() == 0)
          inputs.foreach(metric.eval)
          assert(evaluations.get() == 5)
          inputs.foreach(metric.eval)
          assert(evaluations.get() == 10)
          assert(reads.get() == 0, "The terminal must not add an UnsafeProjection writer")
          assert(metric.eval(InternalRow.empty))
          assert(
            calls(metric.getClass).filter(_._1 == marker) ==
              Seq((marker, "consume", "(ZLjava/lang/Object;)V")))
          val nullable =
            prepareTerminal(
              BoundReference(0, dataType, nullable = true),
              Seq.empty)
          assert(nullable.eval(InternalRow(null)))
      }
    }
  }

}
private case class MetricTestResult(
    value: Any,
    override val dataType: DataType,
    evaluated: AtomicInteger)
  extends LeafExpression
  with CodegenFallback {
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = {
    evaluated.incrementAndGet()
    value
  }
}
