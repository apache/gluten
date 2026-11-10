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
package org.apache.gluten.expressions

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BasicScanExecTransformer, BatchScanExecTransformer, BatchScanExecTransformerBase, FileSourceScanExecTransformer, FilterExecTransformerBase, GlutenQueryComparisonTest, ProjectExecTransformer}
import org.apache.gluten.expression.{ExpressionConverter, ExpressionMappings}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

class GlutenExpressionMappingSuite
  extends GlutenQueryComparisonTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    }
    conf
  }

  test("test expression blacklist") {
    val names = ExpressionMappings.expressionsMap.values.toSet
    assert(names.contains("regexp_replace"))
    assert(names.contains("regexp_extract"))

    withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "regexp_replace,regexp_extract,add") {
      val names = ExpressionMappings.expressionsMap.values.toSet
      assert(!names.contains("regexp_replace"))
      assert(!names.contains("regexp_extract"))
      assert(names.contains("regexp_extract_all"))
      assert(!names.contains("add"))
      spark.sql("CREATE TABLE t USING PARQUET AS SELECT 1 as c")
      withTable("t") {
        val df = spark.sql("SELECT c + 1 FROM t")
        checkAnswer(df, Row(2))
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExecTransformer]).isEmpty)
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExec]).isDefined)
      }
    }
  }

  test("test blacklisting regexp expressions") {
    val names = ExpressionMappings.expressionsMap.values.toSet
    assert(names.contains("rlike"))
    assert(names.contains("regexp_replace"))
    assert(names.contains("regexp_extract"))
    assert(names.contains("regexp_extract_all"))
    assert(names.contains("split"))

    withSQLConf(
      GlutenConfig.EXPRESSION_BLACK_LIST.key -> "",
      GlutenConfig.FALLBACK_REGEXP_EXPRESSIONS.key -> "true") {
      val names = ExpressionMappings.expressionsMap.values.toSet
      assert(!names.contains("rlike"))
      assert(!names.contains("regexp_replace"))
      assert(!names.contains("regexp_extract"))
      assert(!names.contains("regexp_extract_all"))
      assert(!names.contains("split"))

      spark.sql("CREATE TABLE t USING PARQUET AS SELECT 'abc100' as c")
      withTable("t") {
        val df = spark.sql("SELECT regexp_replace(c, '(\\d+)', 'something')  FROM t")
        assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[ProjectExecTransformer]).isEmpty)
      }
    }
  }

  test("GLUTEN-7213: Check fallback reason with CheckOverflowInTableInsert") {
    withTable("t1", "t2") {
      sql("create table t1 (a float) using parquet")
      sql("insert into t1 values(1.1)")
      sql("create table t2 (b decimal(10,4)) using parquet")

      val msg =
        "CheckOverflowInTableInsert is used in ANSI mode, but Gluten does not support ANSI mode."
      import org.apache.spark.sql.execution.GlutenImplicits._
      val fallbackSummary = sql("insert overwrite t2 select * from t1").fallbackSummary()
      assert(fallbackSummary.fallbackNodeToReason.flatMap(_.values).exists(_.contains(msg)))
    }
  }

  private def hasNonFoldableIn(expression: Expression): Boolean = {
    expression.exists {
      case in: In => in.list.exists(!_.foldable)
      case _ => false
    }
  }

  test("rewrite preserves IN and NOT IN results with nulls") {
    val value = BoundReference(0, IntegerType, nullable = true)
    val option = BoundReference(1, IntegerType, nullable = true)
    val lists = Seq(
      Seq(Literal(1), Literal(2), option),
      Seq(option),
      Seq(Literal.create(null, IntegerType), option),
      Seq(Literal(1), Add(option, Literal(1))))
    for (list <- lists; negate <- Seq(false, true)) {
      val in = In(value, list)
      val expression = if (negate) Not(in) else in
      val rewritten = ExpressionConverter.rewriteIn(expression)
      assert(!hasNonFoldableIn(rewritten))
      for (v <- Seq(null, 1, 2, 3); o <- Seq(null, 1, 2, 3)) {
        val row = InternalRow(v, o)
        assert(rewritten.eval(row) == expression.eval(row))
      }
      assert(ExpressionConverter.rewriteIn(rewritten) == rewritten)
    }
  }

  test("literal-only and struct IN expressions are unchanged") {
    val value = BoundReference(0, IntegerType, nullable = true)
    val literalIn = In(value, Seq(Literal(1), Literal(2)))
    assert(ExpressionConverter.rewriteIn(literalIn) == literalIn)
    val structType = new StructType().add("a", IntegerType)
    val structIn = In(
      BoundReference(0, structType, nullable = true),
      Seq(BoundReference(1, structType, nullable = true)))
    assert(ExpressionConverter.rewriteIn(structIn) == structIn)
  }

  test("conversion accepts dynamic IN and preserves unsupported cases") {
    val value = AttributeReference("a", IntegerType, nullable = true)()
    val option = AttributeReference("b", IntegerType, nullable = true)()
    val in = In(value, Seq(Literal(1), option))
    assert(ExpressionConverter.canReplaceWithExpressionTransformer(in, Seq(value, option)))
    assert(ExpressionConverter.canReplaceWithExpressionTransformer(Not(in), Seq(value, option)))
    withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "equal") {
      assert(!ExpressionConverter.canReplaceWithExpressionTransformer(in, Seq(value, option)))
    }

    val structType = new StructType().add("a", IntegerType)
    val structValue = AttributeReference("s1", structType, nullable = true)()
    val structOption = AttributeReference("s2", structType, nullable = true)()
    assert(!ExpressionConverter.canReplaceWithExpressionTransformer(
      In(structValue, Seq(structOption)),
      Seq(structValue, structOption)))

    // Expansion duplicates the value expression; nondeterministic inputs must fall back.
    val doubleOption = AttributeReference("d", DoubleType, nullable = true)()
    val randomIn = In(Rand(Literal(0L)), Seq(Literal(1.0), doubleOption))
    assert(ExpressionConverter.rewriteIn(randomIn) == randomIn)
    assert(!ExpressionConverter.canReplaceWithExpressionTransformer(randomIn, Seq(doubleOption)))
  }

  for (format <- Seq("parquet", "orc"); useV1 <- Seq(true, false)) {
    val version = if (useV1) "V1" else "V2"
    test(s"convert non-foldable IN in $version $format filters and projections") {
      withTempPath {
        path =>
          withTempView("rewrite_in") {
            withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) format else "")) {
              withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
                sql("""SELECT * FROM VALUES
                      |(1, 9), (2, NULL), (3, 3), (4, 3), (5, 8), (NULL, 3)
                      |AS t(a, b)""".stripMargin)
                  .write.format(format).save(path.getCanonicalPath)
              }
              spark.read.format(format).load(path.getCanonicalPath)
                .createOrReplaceTempView("rewrite_in")
              val predicates = Seq(
                "a IN (1, 2, b)",
                "a NOT IN (1, 2, b)",
                "a IN (b, b + 1)",
                "a NOT IN (b, b + 1)",
                "a IN (1, b + 1)",
                "a NOT IN (1, CASE WHEN b > 2 THEN b + 1 ELSE b END)",
                "a IN (1, NULL, b)"
              )
              predicates.foreach {
                predicate =>
                  runQueryAndCompare(s"SELECT * FROM rewrite_in WHERE $predicate") {
                    df =>
                      val scans = collect(df.queryExecution.executedPlan) {
                        case scan: BasicScanExecTransformer => scan
                      }
                      assert(scans.size == 1)
                      val scan = scans.head
                      assert(if (useV1) scan.isInstanceOf[FileSourceScanExecTransformer]
                      else scan.isInstanceOf[BatchScanExecTransformerBase])
                      val originalFilters = scan.scanFilters
                      assert(originalFilters.exists(hasNonFoldableIn))
                      if (
                        BackendsApiManager.getSparkPlanExecApiInstance
                          .supportPushDownFilterToScan(scan)
                      ) {
                        assert(scan.filterExprs().exists(hasNonFoldableIn))
                        assert(scan.filterExprs().forall(
                          ExpressionConverter.canReplaceWithExpressionTransformer(_, scan.output)))

                        // Initial predicates must survive the conversion-based support check.
                        val initial: BasicScanExecTransformer = if (useV1) {
                          scan.asInstanceOf[FileSourceScanExecTransformer].copy(pushDownFilters =
                            None)
                        } else {
                          scan.asInstanceOf[BatchScanExecTransformer].copy(pushDownFilters = None)
                        }
                        val rewrittenFilters = originalFilters.map(ExpressionConverter.rewriteIn)
                        assert(initial.filterExprs().toSet == originalFilters.toSet)

                        // Model an override merging the rewritten outer filter with raw scan
                        // predicates, including a dynamic IN supplied only through pushdown.
                        val a = scan.output.find(_.name == "a").get
                        val b = scan.output.find(_.name == "b").get
                        val option = CaseWhen(
                          Seq(GreaterThan(b, Literal(2)) -> Add(b, Literal(1))),
                          Some(b))
                        val injected = Not(In(a, Seq(Literal(1), option)))
                        val combined = initial.withNewPushdownFilters(
                          rewrittenFilters ++ originalFilters ++ Seq(injected))
                        val nativeFilters = combined.filterExprs()
                        assert(nativeFilters.contains(injected))
                        assert(nativeFilters.forall(
                          ExpressionConverter.canReplaceWithExpressionTransformer(
                            _,
                            combined.output)))
                        assert(combined.doValidate().ok())
                        assert(combined.scanFilters == originalFilters)
                        if (!useV1) {
                          assert(combined.asInstanceOf[BatchScanExecTransformerBase]
                            .scan.asInstanceOf[FileScan].dataFilters == originalFilters)
                        }
                      } else {
                        // ClickHouse V1 Parquet scans only accept Spark's translatable filters.
                        // The dynamic IN must instead be converted by the native outer filter.
                        assert(!scan.filterExprs().exists(hasNonFoldableIn))
                        val filters = collect(df.queryExecution.executedPlan) {
                          case filter: FilterExecTransformerBase => filter
                        }
                        assert(filters.exists(filter => hasNonFoldableIn(filter.cond)))
                      }
                  }
                  runQueryAndCompare(s"SELECT a, b, $predicate AS matches FROM rewrite_in") {
                    df =>
                      val projects = collect(df.queryExecution.executedPlan) {
                        case project: ProjectExecTransformer => project
                      }
                      assert(projects.exists(_.projectList.exists(hasNonFoldableIn)))
                  }
              }
              val scanConfig = if (useV1) GlutenConfig.COLUMNAR_FILESCAN_ENABLED
              else GlutenConfig.COLUMNAR_BATCHSCAN_ENABLED
              withSQLConf(scanConfig.key -> "false") {
                runQueryAndCompare(
                  "SELECT * FROM rewrite_in WHERE a NOT IN (1, 2, b)",
                  noFallBack = false) {
                  df =>
                    assert(collect(df.queryExecution.executedPlan) {
                      case scan: BatchScanExec if !useV1 => scan
                      case scan: FileSourceScanExec if useV1 => scan
                    }.nonEmpty)
                }
              }
            }
          }
      }
    }
  }
}
