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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.ColumnarBatchOutIterator
import org.apache.gluten.vectorized.ExpressionBenchmarkJni

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources

import java.nio.file.{Files, Paths}
import java.nio.file.Path

/** Validate the real shuffle boundary before benchmarking aggregate state transport. */
class AggregateShuffleBenchmarkSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", classOf[ExpressionBenchmarkExtensions].getName)
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.shuffle.compress", "false")

  private val schema = new StructType().add("k", LongType).add("v", LongType)
  private lazy val catalog = Catalog.load(root = "expression-benchmark/aggregate", aggregate = true)

  private def caseDef(id: String): Catalog.CaseDef = catalog.find(_.id == id).get

  test("standalone aggregate disables spilling with the default spill strategy") {
    withSQLConf((RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf :+
      ("spark.gluten.sql.columnar.backend.velox.spillStrategy" -> "auto")): _*) {
      val evaluator = ExpressionEvaluator(spark, caseDef("sum/global"), Data.Context(4), 2)
      TaskResources.runUnsafe {
        evaluator.runNative()
      }
    }
  }

  test("aggregate planning does not materialize input rows") {
    val scenario = caseDef("sum/grouped")
    val data = Data.Plan(schema, (_, _, _, _) => fail("Planning must not generate input rows"))
    withSQLConf(RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf: _*) {
      assert(new ExpressionPlan(spark, scenario, Data.Context(5), data).validate().ok())
    }
  }

  test("aggregate JNI rejects malformed plans and invalid evaluator handles") {
    TaskResources.runUnsafe {
      val runtime = Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "aggregate JNI test")
      val evaluator = new ExpressionBenchmarkJni(runtime)
      intercept[RuntimeException](evaluator.compile(
        Array(Array.emptyByteArray),
        2,
        false,
        false,
        false,
        1,
        2,
        1024L,
        false,
        ""))
      intercept[RuntimeException](evaluator.execute(
        -1L,
        Array.empty[Array[Long]],
        new Array[Long](2)))
    }
  }

  test("Spark aggregate codegen preserves two generated stages") {
    Seq("sum/global", "sum/grouped").foreach {
      id =>
        val scenario = caseDef(id)
        withAggregate(
          scenario,
          Data.Context(31, Some(3), inputPartitions = 4),
          5) {
          (evaluator, verify) =>
            assert(evaluator.metadata("vanillaCodegenStages") == "2")
            var count = 0
            verify((_, _) => count += 1)
            assert(count == (if (scenario.groupingKeys.get.isEmpty) 1 else 3))
        }
    }
  }

  test("aggregate preparation freezes query constants for both engines") {
    val scenario = caseDef("sum/global")
      .copy(sql = "sum(unix_micros(current_timestamp()))")
    withAggregate(scenario, Data.Context(3), 2) {
      (evaluator, verify) =>
        var expected = Vector.empty[Long]
        verify((_, row) => expected :+= row.getLong(0))
        var actual = Vector.empty[Long]
        verify((_, row) => actual :+= row.getLong(0))
        assert(expected.size == 1 && actual == expected)
    }
  }

  test("disabled whole-stage codegen uses Spark aggregate iterator") {
    withSQLConf("spark.sql.codegen.wholeStage" -> "false") {
      Seq("sum/grouped", "sum/projected-plus-one", "sum/projected-null").foreach {
        id =>
          withAggregate(caseDef(id), Data.Context(24, Some(3)), 2) {
            (evaluator, verify) =>
              assert(evaluator.metadata("vanillaCodegenStages") == "0")
              verify((_, _) => ())
          }
      }
    }
  }

  test("bitmap construction has independent value cardinality and computed group keys") {
    val scenario = caseDef("bitmap_construct_agg/long-g256")
    withAggregate(
      scenario,
      Data.Context(1025, inputPartitions = 7),
      31) {
      (evaluator, verify) =>
        var groups = 0
        verify {
          (_, row) =>
            val key = row.getLong(0).toInt
            val expected = Array.fill[Byte](4096)(0)
            (key until 1025 by 256).foreach {
              bit => expected(bit / 8) = (expected(bit / 8) | (1 << (bit % 8))).toByte
            }
            assert(row.getBinary(1).sameElements(expected))
            groups += 1
        }
        assert(groups == 256)
    }
  }

  test("input partition count handles one partition and more partitions than rows") {
    for (partitions <- Seq(1, 2, 8); layout <- Seq("global", "grouped")) {
      val scenario = caseDef(s"sum/$layout")
      withAggregate(
        scenario,
        Data.Context(5, Some(2), inputPartitions = partitions),
        2) {
        (evaluator, verify) =>
          var groups = 0
          verify {
            (_, row) =>
              if (layout == "global") assert(row.getLong(0) == 10L)
              else assert(row.getLong(1) == (if (row.getLong(0) == 0) 6L else 4L))
              groups += 1
          }
          assert(groups == (if (layout == "global") 1 else 2))
          assert(evaluator.metadata("inputPartitions") == partitions.toString)
      }
    }
  }

  test("evaluator micro pipeline runs without Spark jobs and serializes every iteration") {
    val group = "aggregate-micro-no-jobs"
    spark.sparkContext.setJobGroup(group, group)
    val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
    try {
      catalog.filter(c => Set("sum/grouped", "bitmap_construct_agg/long-g256")(c.id)).foreach {
        scenario =>
          withAggregate(
            scenario,
            Data.Context(24, Some(3)),
            2) {
            (evaluator, verify) =>
              var count = 0
              verify((_, _) => count += 1)
              assert(count == (if (scenario.groupingKeys.get.isEmpty) 1 else 3))
              (0 until 2).foreach(_ => verify((_, _) => ()))
              assert(evaluator.metadata("native.serializedBytes").toLong > 0)
              assert(evaluator.metadata("vanilla.serializedBytes").toLong > 0)
          }
      }
      assert(spark.sparkContext.statusTracker.getJobIdsForGroup(group).isEmpty)
      assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
    } finally spark.sparkContext.clearJobGroup()
  }

  test("native shuffle write failure releases state and permits retry") {
    val scenario = caseDef("sum/grouped")
    val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
    withAggregate(
      scenario,
      Data.Context(24, Some(3)),
      2) {
      (evaluator, verify) =>
        verify((_, _) => ())
        val retainedBytes = evaluator.metadata("native.retainedPoolBytes")
        val directory = Paths.get(evaluator.metadata("shuffleDirectory"))
        Files.delete(directory.resolve("native-0"))
        val obstruction = Files.createDirectory(directory.resolve("native-0"))
        val marker = Files.createFile(obstruction.resolve("keep"))
        try {
          intercept[GlutenException] {
            evaluator.runNative()
          }
          assert(Files.exists(marker))
          assert(evaluator.metadata("native.retainedPoolBytes") == retainedBytes)
        } finally {
          Files.deleteIfExists(marker)
          Files.deleteIfExists(obstruction)
        }
        evaluator.runNative()
        verify((_, _) => ())
        assert(evaluator.metadata("native.retainedPoolBytes") == retainedBytes)
        val remaining = Files.list(directory)
        try assert(remaining.count() == 6)
        finally remaining.close()
    }
    assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
  }

  test("shared aggregate runner compares known null and empty results") {
    for (function <- Seq("sum", "avg", "count")) {
      val rows = Seq(
        InternalRow(1L, 2L),
        InternalRow(1L, 4L),
        InternalRow(2L, null),
        InternalRow(null, 8L)
      )
      val scenario = caseDef(s"$function/grouped")
      val data = Data.Plan(schema, (_, i, _, _) => rows(i.toInt))
      withAggregate(
        scenario,
        Data.Context(4),
        2,
        data) {
        (evaluator, verify) =>
          var results = Set.empty[(Option[Long], Any)]
          verify {
            (_, row) =>
              def read(i: Int): Option[Long] = if (row.isNullAt(i)) None else Some(row.getLong(i))
              results += ((
                read(0),
                if (row.isNullAt(1)) null
                else row.get(1, if (function == "avg") DoubleType else LongType)))
          }
          val expected = function match {
            case "sum" => Set((Some(1L), 6L), (Some(2L), null), (None, 8L))
            case "avg" => Set((Some(1L), 3.0), (Some(2L), null), (None, 8.0))
            case "count" => Set((Some(1L), 2L), (Some(2L), 0L), (None, 1L))
          }
          assert(results == expected)
      }
      withAggregate(
        caseDef(s"$function/global"),
        Data.Context(rows.size),
        2,
        data) {
        (_, verify) =>
          verify {
            (i, row) =>
              assert(i == 0)
              function match {
                case "sum" => assert(row.getLong(0) == 14L)
                case "avg" => assert(math.abs(row.getDouble(0) - 14.0 / 3) < 1e-12)
                case "count" => assert(row.getLong(0) == 3L)
              }
          }
      }
      Seq(s"$function/global", s"$function/grouped").foreach {
        id =>
          val empty = caseDef(id)
          withAggregate(
            empty,
            Data.Context(0),
            2,
            data) {
            (evaluator, verify) =>
              var count = 0
              verify {
                (_, row) =>
                  count += 1
                  if (function == "count") assert(row.getLong(0) == 0L)
                  else assert(row.isNullAt(0))
              }
              assert(count == (if (empty.groupingKeys.get.isEmpty) 1 else 0))
          }
      }
    }
  }

  test("early close and collector failures release execution state and allow retry") {
    Seq(
      caseDef("sum/grouped"),
      Catalog.load().find(_.id == "window/row-number").get).foreach {
      scenario =>
        withSQLConf(RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf: _*) {
          var failConsumer = false
          var batches = 0
          var earlyClose: Option[Boolean] = None
          val evaluator = new ExpressionEvaluator(
            new ExpressionPlan(
              spark,
              scenario,
              Data.Context(20000, Some(20000)),
              Data.compile(scenario)),
            127,
            vanillaCollector = Some(
              _ => {
                if (failConsumer) throw new IllegalStateException("injected collector failure")
              }),
            nativeCollector = None
          ) {
            override protected lazy val nativeExecutor = new NativeExecutor(
              plan,
              nativeInputs,
              batchSize,
              () => shuffleDir,
              Some(
                _ => {
                  batches += 1
                  if (failConsumer) throw new IllegalStateException("injected collector failure")
                })) {
              override protected def consume(output: ColumnarBatchOutIterator): Unit =
                earlyClose match {
                  case None => super.consume(output)
                  case Some(readBatch) =>
                    try {
                      if (readBatch) {
                        assert(output.hasNext)
                        output.next().close()
                      }
                    } finally output.close()
                }
            }
          }
          TaskResources.runUnsafe {
            assert(batches == 0)
            evaluator.runVanilla()
            evaluator.runNative()
            assert(batches > 1)
            val retained = evaluator.metadata("native.retainedPoolBytes")
            Seq(false, true).foreach {
              readBatch =>
                earlyClose = Some(readBatch)
                try evaluator.runNative()
                finally earlyClose = None
                assert(evaluator.metadata("native.retainedPoolBytes") == retained)
                evaluator.runNative()
            }
            failConsumer = true
            intercept[IllegalStateException](evaluator.runVanilla())
            intercept[IllegalStateException](evaluator.runNative())
            val files = Files.list(Paths.get(evaluator.metadata("shuffleDirectory")))
            try assert(files.count() == 6)
            finally files.close()
            assert(evaluator.metadata("native.retainedPoolBytes") == retained)
            failConsumer = false
            evaluator.runVanilla()
            evaluator.runNative()
            assert(evaluator.metadata("native.retainedPoolBytes") == retained)
          }
        }
    }
  }

  test("window preserves known results across empty inputs and repeated iterations") {
    val scenario = Catalog.load().find(_.id == "window/global-row-number").get
    Seq(0, 1, 17).foreach {
      count =>
        withAggregate(scenario, Data.Context(count, inputPartitions = 3), 2) {
          (evaluator, verify) =>
            (0 until 3).foreach {
              _ =>
                var rows = 0
                verify {
                  (i, row) =>
                    assert(row.getInt(0) == i + 1)
                    rows += 1
                }
                assert(rows == count)
                assert(evaluator.metadata("mode") == "window")
            }
        }
    }
  }

  test("window sliding frame includes following rows across batches") {
    val scenario = Catalog.load().find(_.id == "window/sum-sliding").get
      .copy(sql = "sum(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)")
    withAggregate(scenario, Data.Context(4, inputPartitions = 3), 1) {
      (_, verify) =>
        val expected = Seq(1L, 3L, 5L, 6L)
        verify((i, row) => assert(row.getLong(0) == expected(i.toInt)))
    }
  }

  private def withAggregate(
      scenario: Catalog.CaseDef,
      context: Data.Context,
      batchSize: Int,
      data: Data.Plan = null)(
      test: (ExpressionEvaluator, ((Long, InternalRow) => Any) => Unit) => Unit): Unit = {
    var directory: Path = null
    try withSQLConf(RunOptions.sqlConf ++ ExpressionEvaluator.aggregateSqlConf: _*) {
        ExpressionResultChecks.withCollected(
          (vanilla, native) =>
            ExpressionEvaluator(
              spark,
              scenario,
              context,
              batchSize,
              Option(data),
              Some(vanilla),
              Some(native))) {
          (evaluator, verify) =>
            directory = Paths.get(evaluator.metadata("shuffleDirectory"))
            test(evaluator, verify)
            assert(Files.exists(directory))
        }
      }
    finally if (directory != null) assert(!Files.exists(directory))
  }
}
