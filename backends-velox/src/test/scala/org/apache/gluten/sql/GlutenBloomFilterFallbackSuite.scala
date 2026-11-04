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
package org.apache.gluten.sql

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FilterExecTransformerBase, HashAggregateExecBaseTransformer, WholeStageTransformerSuite}
import org.apache.gluten.expression.VeloxBloomFilterMightContain
import org.apache.gluten.expression.aggregate.VeloxBloomFilterAggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.sketch.BloomFilter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

/**
 * Regression tests for https://github.com/apache/gluten/issues/12013 and
 * https://github.com/apache/gluten/issues/13489.
 *
 * Verifies that the bloom-filter producer (`bloom_filter_agg`) and consumer (`might_contain`)
 * always stay on a consistent serialized byte format.
 *
 * `BloomFilterMightContainJointRewriteRule.Logical` rewrites verified `(might_contain,
 * bloom_filter_agg)` scalar-subquery pairs with supported integral input types to their Velox forms
 * at Pre-CBO, and tags unpaired or unsupported expressions with `KEEP_VANILLA_BLOOM_FILTER_TAG` so
 * they stay on vanilla Spark. For runtime bloom filters injected later by Spark's
 * `InjectRuntimeFilter` (GLUTEN-12013), the physical `BloomFilterMightContainJointRewriteRule`
 * registered at `injectPre` rewrites both untagged sides before `HeuristicApplier` captures the
 * plan that `ExpandFallbackPolicy` reverts to, so a whole-stage fallback can no longer strip the
 * rewrite from one stage and leave it mismatched against another.
 */
class GlutenBloomFilterFallbackSuite extends WholeStageTransformerSuite {
  protected val resourcePath: String = null
  protected val fileFormat: String = null

  import testImplicits._

  private val funcIdBloomFilterAgg = FunctionIdentifier("bloom_filter_agg")
  private val funcIdMightContain = FunctionIdentifier("might_contain")

  // GLUTEN-13179: Spark 4.2's FunctionRegistry rejects the 1-part FunctionIdentifiers registered
  // in beforeAll (they must be fully qualified), which aborts the suite. Ignore every test on
  // Spark 4.2 only; with no runnable tests ScalaTest also skips beforeAll/afterAll. Other Spark
  // versions run the suite unchanged.
  override def tags: Map[String, Set[String]] = {
    val inherited = super.tags
    if (matchSparkVersion(Some("4.2"), Some("4.2"))) {
      inherited ++ testNames.map {
        name => name -> (inherited.getOrElse(name, Set.empty[String]) + "org.scalatest.Ignore")
      }.toMap
    } else {
      inherited
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.functionRegistry.registerFunction(
      funcIdBloomFilterAgg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      args =>
        args.size match {
          case 1 => new BloomFilterAggregate(args(0))
          case 2 => new BloomFilterAggregate(args(0), args(1))
          case 3 => new BloomFilterAggregate(args(0), args(1), args(2))
          case _ => throw new IllegalArgumentException("bloom_filter_agg requires 1-3 arguments")
        }
    )
    spark.sessionState.functionRegistry.registerFunction(
      funcIdMightContain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      args => BloomFilterMightContain(args(0), args(1)))
  }

  override def afterAll(): Unit = {
    spark.sessionState.functionRegistry.dropFunction(funcIdBloomFilterAgg)
    spark.sessionState.functionRegistry.dropFunction(funcIdMightContain)
    super.afterAll()
  }

  private val veloxBloomFilterMaxNumBits = 4194304L

  // GLUTEN-12013: only filter stage falls back (threshold=2).
  // bloom_filter_agg subquery runs natively and produces Velox-format bytes; the filter stage
  // falls back via ExpandFallbackPolicy.  Because the rewrite is applied at injectPre it is part
  // of the plan the reversion falls back to, so the JVM filter still reads Velox-format bytes.
  test("GLUTEN-12013: bloom_filter_agg whole-stage fallback does not corrupt bloom filter bytes") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_test"
      val numEstimatedItems = 5000000L
      val sqlString =
        s"""
           |SELECT col positive_membership_test
           |FROM $table
           |WHERE might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimatedItems as long),
           |              cast($veloxBloomFilterMaxNumBits as long))
           |             FROM $table), col)
           |""".stripMargin
      withTempView(table) {
        (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
          .toDF("col")
          .createOrReplaceTempView(table)
        // Threshold=2: FilterExec fallback cost=2 triggers whole-stage fallback; agg cost=1
        // does not, so Stage 0 runs natively.  ANSI off keeps agg cost at 1 on Spark 4.0+.
        withSQLConf(
          GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "2",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val df = spark.sql(sqlString)
          // Must not throw: java.io.IOException: Unexpected Bloom filter version number.
          assert(df.collect().length == 200003)
          // Verify the rewrite survived the whole-stage fallback: VeloxBloomFilterMightContain
          // must still be present even though Stage 1 executes inside a FallbackNode.
          assert(
            df.queryExecution.executedPlan.toString.contains("velox_might_contain"),
            s"Expected velox_might_contain to survive whole-stage fallback. Plan:\n" +
              s"${df.queryExecution.executedPlan}"
          )
        }
      }
    }
  }

  // GLUTEN-12013: both stages fall back (threshold=1).
  // Stage 0's inherent transition cost of 1 meets the threshold so ExpandFallbackPolicy
  // promotes it to a whole-stage fallback too.  The injectPre rule has already rewritten both
  // sides to Velox variants before ExpandFallbackPolicy captures its snapshot.  Even in JVM
  // row-mode, VeloxBloomFilterAggregate produces Velox-format bytes (via JNI) and
  // VeloxBloomFilterMightContain consumes them -- both sides are consistent.
  test("GLUTEN-12013: bloom_filter_agg whole-stage fallback when both stages fall back") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_test"
      val numEstimatedItems = 5000000L
      val sqlString =
        s"""
           |SELECT col positive_membership_test
           |FROM $table
           |WHERE might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimatedItems as long),
           |              cast($veloxBloomFilterMaxNumBits as long))
           |             FROM $table), col)
           |""".stripMargin
      withTempView(table) {
        (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
          .toDF("col")
          .createOrReplaceTempView(table)
        // Threshold=1: both stages fall back; both use Velox variants via JNI.
        withSQLConf(
          GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val df = spark.sql(sqlString)
          // Must not throw: java.io.IOException: Unexpected Bloom filter version number.
          assert(df.collect().length == 200003)
          // Verify the rewrite survived on both sides.
          assert(
            df.queryExecution.executedPlan.toString.contains("velox_might_contain"),
            s"Expected velox_might_contain to survive whole-stage fallback. Plan:\n" +
              s"${df.queryExecution.executedPlan}"
          )
        }
      }
    }
  }

  // GLUTEN-12013: DataFrame.stat.bloomFilter() must not be affected by the rewrite rule.
  // The rule must only rewrite BloomFilterAggregate inside a BloomFilterMightContain subquery.
  // A standalone BloomFilterAggregate (as used here) must remain vanilla so that the collected
  // bytes are in Spark-native format and BloomFilter.readFrom() succeeds.
  test("GLUTEN-12013: DataFrame.stat.bloomFilter() produces Spark-readable bytes") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_stat_test"
      withTempView(table) {
        (1L to 1000L).toDF("col").createOrReplaceTempView(table)
        // Must not throw: java.io.IOException: Unexpected Bloom filter version number
        val bf = spark.table(table).stat.bloomFilter("col", 1000L, 0.01)
        // Bloom filters have no false negatives: every inserted value must be present.
        assert(bf.mightContainLong(500L), "Expected 500 to be in bloom filter")
      }
    }
  }

  // GLUTEN-12013: native bloom filter disabled -- early-exit path of the rewrite rule.
  // When spark.gluten.sql.native.bloomFilter=false the rule returns the plan unchanged.
  // BloomFilterAggregate / BloomFilterMightContain remain as vanilla Spark expressions and
  // produce/consume consistent Spark-format bytes.
  test(
    "GLUTEN-12013: native bloom filter disabled skips rewrite and produces correct results") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_test"
      val numEstimatedItems = 5000000L
      val sqlString =
        s"""
           |SELECT col positive_membership_test
           |FROM $table
           |WHERE might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimatedItems as long),
           |              cast($veloxBloomFilterMaxNumBits as long))
           |             FROM $table), col)
           |""".stripMargin
      withTempView(table) {
        (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
          .toDF("col")
          .createOrReplaceTempView(table)
        withSQLConf(
          GlutenConfig.COLUMNAR_NATIVE_BLOOMFILTER_ENABLED.key -> "false",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val df = spark.sql(sqlString)
          assert(df.collect().length == 200003)
          // Verify the rule early-exited: plan must NOT contain Velox variants.
          assert(
            !df.queryExecution.executedPlan.toString.contains("velox_might_contain"),
            "Expected vanilla BloomFilterMightContain when native bloom filter is disabled"
          )
        }
      }
    }
  }

  // GLUTEN-12013: verify that the bloom-filter subquery uses VeloxBloomFilterAggregate even when
  // the aggregate node executes in JVM mode (ObjectHashAggregateExec).
  //
  // When hash-aggregate offloading is disabled, the subquery runs as
  // ObjectHashAggregateExec(VeloxBloomFilterAggregate).  VeloxBloomFilterAggregate.eval() calls
  // serialize(buffer) directly without a cardinality guard, so it always produces Velox-format
  // (version=1) bytes.  VeloxBloomFilterMightContain on the outer side reads those bytes correctly.
  test("GLUTEN-12013: VeloxBloomFilterAggregate in JVM subquery produces correct Velox bytes") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_test"
      val numEstimatedItems = 5000000L
      val sqlString =
        s"""
           |SELECT col positive_membership_test
           |FROM $table
           |WHERE might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimatedItems as long),
           |              cast($veloxBloomFilterMaxNumBits as long))
           |             FROM $table), col)
           |""".stripMargin
      withTempView(table) {
        (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 200000L))
          .toDF("col")
          .createOrReplaceTempView(table)
        // Disable hash-aggregate offloading: the bloom_filter_agg subquery executes in JVM mode as
        // ObjectHashAggregateExec(VeloxBloomFilterAggregate).  This mirrors the q59 golden shape.
        withSQLConf(
          GlutenConfig.COLUMNAR_HASHAGG_ENABLED.key -> "false",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val df = spark.sql(sqlString)
          // Must not throw: java.io.IOException: Unexpected Bloom filter version number.
          // VeloxBloomFilterAggregate.eval() produces Velox-format bytes even in JVM mode.
          val result = df.collect()
          assert(result.length == 200003, s"Expected 200003 rows, got ${result.length}")

          // Directly verify the subquery's aggregate function class at runtime.
          // ObjectHashAggregateExec(VeloxBloomFilterAggregate) must be present -- NOT vanilla
          // BloomFilterAggregate -- so we know the physical rewrite actually happened.
          val subqueryVeloxAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
            case agg: ObjectHashAggregateExec
                if agg.aggregateExpressions.exists(
                  _.aggregateFunction.isInstanceOf[VeloxBloomFilterAggregate]) =>
              agg
          }
          assert(
            subqueryVeloxAggs.nonEmpty,
            "Expected ObjectHashAggregateExec(VeloxBloomFilterAggregate) in the bloom-filter " +
              "subquery. Actual subquery aggs: " +
              collectWithSubqueries(df.queryExecution.executedPlan) {
                case agg: ObjectHashAggregateExec => agg
              }.map(
                a =>
                  a.aggregateExpressions
                    .map(_.aggregateFunction.getClass.getSimpleName)
                    .mkString(","))
                .mkString("; ")
          )
        }
      }
    }
  }

  // GLUTEN-12013 follow-up: runtime bloom filters injected by Spark's InjectRuntimeFilter must
  // stay native.  These expressions only appear after Spark's optimizer has run, so they are only
  // visible to a physical rule; BloomFilterMightContainJointRewriteRule rewrites them along with
  // every other bloom filter.  Without that rewrite, vanilla might_contain/bloom_filter_agg have no
  // Substrait mapping and the consuming FilterExec plus the producing aggregate fall back to the
  // JVM with R2C/C2R transitions (the regression originally visible in the TPC-DS q59 golden).
  test("GLUTEN-12013: runtime bloom filter keeps FilterExecTransformer native") {
    withTable("bf_fact", "bf_dim") {
      spark
        .range(0, 10000)
        .selectExpr("id as key", "id % 100 as payload")
        .write
        .saveAsTable("bf_fact")
      spark
        .range(0, 100)
        .selectExpr("id as key", "id % 10 as f")
        .write
        .saveAsTable("bf_dim")
      withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ANSI_ENABLED.key -> "false"
      ) {
        val df = spark.sql(
          "SELECT * FROM bf_fact JOIN bf_dim ON bf_fact.key = bf_dim.key WHERE bf_dim.f = 5")
        assert(
          df.queryExecution.optimizedPlan.toString.contains("might_contain"),
          "Precondition failed: InjectRuntimeFilter did not inject a bloom filter"
        )
        assert(df.collect().length == 10)
        // The consumer side must be a native FilterExecTransformer evaluating
        // velox_might_contain -- not a fallen-back JVM FilterExec.
        val nativeBloomFilters = collectWithSubqueries(df.queryExecution.executedPlan) {
          case f: FilterExecTransformerBase
              if f.cond.exists(_.isInstanceOf[VeloxBloomFilterMightContain]) =>
            f
        }
        assert(
          nativeBloomFilters.nonEmpty,
          "Expected a native FilterExecTransformer with velox_might_contain; the runtime " +
            s"bloom filter fell back to JVM. Plan:\n${df.queryExecution.executedPlan}"
        )
        // The producer side must use VeloxBloomFilterAggregate so the bytes are version=1.
        val veloxAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case agg: BaseAggregateExec
              if agg.aggregateExpressions.exists(
                _.aggregateFunction.isInstanceOf[VeloxBloomFilterAggregate]) =>
            agg
        }
        assert(
          veloxAggs.nonEmpty,
          "Expected VeloxBloomFilterAggregate in the runtime-filter subquery. Plan:\n" +
            s"${df.queryExecution.executedPlan}"
        )
      }
    }
  }

  // GLUTEN-12013 follow-up: closes the whole-stage reversion gap for runtime bloom filters.
  // ExpandFallbackPolicy's whole-stage fallback can revert the runtime bloom filter's partial
  // and final aggregation stages independently, since they are separate physical operators
  // across a shuffle boundary. If only one side reverts, a native-Velox stage would otherwise
  // pair with a vanilla-reverted stage. Registering the rewrite at injectPre closes this: the
  // rewrite is already part of the plan the reversion falls back to, so both sides keep their
  // Velox forms. This is only safe now that VeloxBloomFilterAggregate's JVM-side buffer sizing
  // agrees with the native aggregate's (GLUTEN-12613): before that fix, a reverted final stage
  // merging with a still-native partial stage silently corrupted the filter -- forcing a
  // capacity mismatch here (without the fix) would drop rows instead of crashing.
  test("GLUTEN-12013: runtime bloom filter survives whole-stage reversion of one stage") {
    withTable("bf_fact", "bf_dim") {
      spark
        .range(0, 10000)
        .selectExpr("id as key", "id % 100 as payload")
        .write
        .saveAsTable("bf_fact")
      spark
        .range(0, 100)
        .selectExpr("id as key", "id % 10 as f")
        .write
        .saveAsTable("bf_dim")
      // Threshold=1 forces ExpandFallbackPolicy to revert the runtime bloom filter's final
      // aggregation stage (its own transition cost already meets the threshold) while the
      // partial aggregation stage, in a separate shuffle-bounded physical operator, stays
      // native. Without the injectFinal re-rewrite, this reverted final stage would be vanilla
      // ObjectHashAggregateExec(BloomFilterAggregate), disagreeing on both byte format and (pre
      // GLUTEN-12613) capacity with the still-native partial stage.
      withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1",
        SQLConf.ANSI_ENABLED.key -> "false"
      ) {
        val df = spark.sql(
          "SELECT * FROM bf_fact JOIN bf_dim ON bf_fact.key = bf_dim.key WHERE bf_dim.f = 5")
        assert(
          df.queryExecution.optimizedPlan.toString.contains("might_contain"),
          "Precondition failed: InjectRuntimeFilter did not inject a bloom filter"
        )
        // Must return all 10 matching rows, not a silently-corrupted subset.
        val result = df.collect()
        assert(
          result.length == 10,
          s"Expected 10 rows (reversion must not corrupt the bloom filter), got " +
            s"${result.length}. Plan:\n${df.queryExecution.executedPlan}"
        )
        // Every aggregate function touching the runtime bloom filter must be the Velox variant,
        // regardless of whether its own stage executes natively or was reverted to the JVM.
        val vanillaAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case agg: BaseAggregateExec
              if agg.aggregateExpressions.exists(
                _.aggregateFunction.isInstanceOf[BloomFilterAggregate]) =>
            agg
        }
        assert(
          vanillaAggs.isEmpty,
          "Expected no vanilla BloomFilterAggregate after reversion; injectFinal re-rewrite " +
            s"should have restored the Velox variant. Plan:\n${df.queryExecution.executedPlan}"
        )
      }
    }
  }

  // SPARK-54336 / GLUTEN-12013: a might_contain whose value argument is a literal (not a column),
  // fed by a nested scalar subquery.  This mirrors Spark's upstream
  // `BloomFilterAggregateQuerySuite."SPARK-54336"`.  Because the rewrite is unconditional, both the
  // inner bloom_filter_agg and the outer might_contain become Velox forms together, so the byte
  // formats agree.  The failure mode being guarded against is a partial rewrite: if only the outer
  // side became velox_might_contain (version=1) while the inner aggregate stayed vanilla
  // bloom_filter_agg and emitted version=0 bytes, deserialization would fail with
  // (kBloomFilterV1 == version, 1 vs. 0).
  //
  // Gated to Spark 4.0+: the query exercises the exact `MergeScalarSubqueries` path that Spark's
  // own SPARK-54336 fixes, and that fix only exists in Spark 4.0.2+/4.1.  On earlier Spark the
  // analyzer/optimizer throws `UnresolvedException` (dataType on an unresolved ScalarSubquery)
  // before Gluten's rule runs, so there is nothing for this test to verify there.
  testWithMinSparkVersion(
    "GLUTEN-12013: might_contain with a literal value keeps both sides consistent (SPARK-54336)",
    "4.0") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_lit_test"
      withTempView(table) {
        // Single non-null row [0]; the bloom filter therefore contains 0L.
        Seq(0L).toDF("col").createOrReplaceTempView(table)
        val sqlString =
          s"""
             |SELECT
             |  (SELECT
             |    first(might_contain(
             |      (SELECT bloom_filter_agg(col) FROM $table),
             |      0L
             |    ))
             |  FROM $table)
             |FROM $table
             |""".stripMargin
        // Codegen-off, matching the upstream BloomFilterAggregateQuerySuiteCGOff variant that
        // originally surfaced this crash.
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
          SQLConf.ANSI_ENABLED.key -> "false"
        ) {
          val df = spark.sql(sqlString)
          // Producer and consumer must not end up on different byte formats.  Either both sides
          // are rewritten to their Velox forms, or neither is; a plan containing
          // velox_might_contain without velox_bloom_filter_agg is the broken state.
          val plan = df.queryExecution.executedPlan.toString
          assert(
            !plan.contains("velox_might_contain") || plan.contains("velox_bloom_filter_agg"),
            s"Mismatched bloom filter byte formats: velox_might_contain without a matching " +
              s"velox_bloom_filter_agg. Plan:\n${df.queryExecution.executedPlan}"
          )
          // Must not throw kBloomFilterV1 == version (1 vs. 0); 0L was inserted, so it is present.
          checkAnswer(df, Row(true))
        }
      }
    }
  }

  // Gated to Spark 3.5+: Spark's BloomFilterAggregate only supports StringType starting in
  // Spark 3.5 (SPARK-43726); on Spark 3.4 CheckAnalysis rejects bloom_filter_agg(string) with
  // DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE before Optimizer/Gluten rules run.
  testWithMinSparkVersion(
    "GLUTEN-13489: bloom_filter_agg on string column falls back to vanilla Spark",
    "3.5") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_str_test"
      withTempView(table) {
        Seq("a", "b", "c").toDF("s").createOrReplaceTempView(table)
        val aggDf = spark.sql(s"SELECT bloom_filter_agg(s) AS bf FROM $table")
        val bytes = aggDf.collect().head.getAs[Array[Byte]]("bf")
        assert(!aggDf.queryExecution.executedPlan.toString.contains("velox_bloom_filter_agg"))
        val bf = BloomFilter.readFrom(new ByteArrayInputStream(bytes))
        assert(bf.mightContainString("a"))
        assert(!bf.mightContainString("z"))

        val df = spark.sql(
          s"""
             |SELECT might_contain((SELECT bloom_filter_agg(s) FROM $table), xxhash64(s))
             |FROM $table
             |""".stripMargin)
        checkAnswer(df, Seq(Row(false), Row(false), Row(false)))
        val plan = df.queryExecution.executedPlan.toString
        assert(!plan.contains("velox_might_contain"))
        assert(!plan.contains("velox_bloom_filter_agg"))
      }
    }
  }

  test("GLUTEN-13489: bloom_filter_agg on nullable column filters out nulls and runs natively") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_nullable_test"
      withTempView(table) {
        Seq[java.lang.Long](1L, null, 2L, 3L).toDF("col").createOrReplaceTempView(table)
        val df = spark.sql(
          s"""
             |SELECT col
             |FROM $table
             |WHERE might_contain((SELECT bloom_filter_agg(col) FROM $table), col)
             |""".stripMargin)
        checkAnswer(df, Seq(Row(1L), Row(2L), Row(3L)))
        val nativeAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case h: HashAggregateExecBaseTransformer => h
        }
        assert(nativeAggs.size == 2, df.queryExecution.executedPlan.toString)

        val filteredDf = spark.sql(
          s"""
             |SELECT col
             |FROM $table
             |WHERE might_contain(
             |  (SELECT bloom_filter_agg(col) FILTER (WHERE col IS NULL OR col > 1L) FROM $table),
             |  col)
             |""".stripMargin)
        checkAnswer(filteredDf, Seq(Row(2L), Row(3L)))
        val filteredNativeAggs = collectWithSubqueries(filteredDf.queryExecution.executedPlan) {
          case h: HashAggregateExecBaseTransformer => h
        }
        assert(filteredNativeAggs.size == 2, filteredDf.queryExecution.executedPlan.toString)
      }

      val allNullTable = "bloom_filter_all_null_test"
      withTempView(allNullTable) {
        Seq[java.lang.Long](null, null).toDF("col").createOrReplaceTempView(allNullTable)
        val df = spark.sql(
          s"SELECT might_contain((SELECT bloom_filter_agg(col) FROM $allNullTable), 1L)")
        checkAnswer(df, Seq(Row(null)))
        val allNullNativeAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case h: HashAggregateExecBaseTransformer => h
        }
        assert(allNullNativeAggs.size == 2, df.queryExecution.executedPlan.toString)
      }
    }
  }

  test("GLUTEN-13489: might_contain with non-bloom_filter_agg first arg stays on vanilla Spark") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_const_bytes_test"
      val bytesTable = "bloom_filter_bytes_holder"
      withTempView(table, bytesTable) {
        Seq(201L, 999L).toDF("col").createOrReplaceTempView(table)
        val bf = BloomFilter.create(1000, 8000)
        bf.putLong(201L)
        val out = new ByteArrayOutputStream()
        bf.writeTo(out)
        val sparkBfHex = out.toByteArray.map("%02X".format(_)).mkString

        val literalDf = spark.sql(
          s"SELECT col, might_contain(X'$sparkBfHex', col) FROM $table")
        checkAnswer(literalDf, Seq(Row(201L, true), Row(999L, false)))
        assert(!literalDf.queryExecution.executedPlan.toString.contains("velox_might_contain"))

        Seq(out.toByteArray).toDF("bf").createOrReplaceTempView(bytesTable)
        val nonAggSubqueryDf = spark.sql(
          s"""
             |SELECT col, might_contain((SELECT bf FROM $bytesTable LIMIT 1), col)
             |FROM $table
             |""".stripMargin)
        checkAnswer(nonAggSubqueryDf, Seq(Row(201L, true), Row(999L, false)))
        assert(
          !nonAggSubqueryDf.queryExecution.executedPlan.toString.contains("velox_might_contain"))

        val otherAggSubqueryDf = spark.sql(
          s"""
             |SELECT col, might_contain((SELECT first(bf) FROM $bytesTable), col)
             |FROM $table
             |""".stripMargin)
        checkAnswer(otherAggSubqueryDf, Seq(Row(201L, true), Row(999L, false)))
        assert(
          !otherAggSubqueryDf.queryExecution.executedPlan.toString.contains("velox_might_contain"))

        val groupByAttrSubqueryDf = spark.sql(
          s"""
             |SELECT col, might_contain((SELECT bf FROM $bytesTable GROUP BY bf), col)
             |FROM $table
             |""".stripMargin)
        checkAnswer(groupByAttrSubqueryDf, Seq(Row(201L, true), Row(999L, false)))
        assert(
          !groupByAttrSubqueryDf.queryExecution.executedPlan.toString
            .contains("velox_might_contain"))

        val groupByAliasSubqueryDf = spark.sql(
          s"""
             |SELECT col, might_contain(
             |  (SELECT coalesce(bf, bf) AS bf_alias FROM $bytesTable GROUP BY bf), col)
             |FROM $table
             |""".stripMargin)
        checkAnswer(groupByAliasSubqueryDf, Seq(Row(201L, true), Row(999L, false)))
        assert(
          !groupByAliasSubqueryDf.queryExecution.executedPlan.toString
            .contains("velox_might_contain"))

        val cteFilterInSubqueryDf = spark.sql(
          s"""
             |WITH bf_cte AS (
             |  SELECT bloom_filter_agg(col) AS bf FROM $table WHERE col = 201L
             |)
             |SELECT col, might_contain((SELECT bf FROM bf_cte WHERE bf IS NOT NULL), col)
             |FROM $table
             |""".stripMargin)
        checkAnswer(cteFilterInSubqueryDf, Seq(Row(201L, true), Row(999L, false)))
        assert(
          !cteFilterInSubqueryDf.queryExecution.executedPlan.toString
            .contains("velox_might_contain"))
        assert(
          !cteFilterInSubqueryDf.queryExecution.executedPlan.toString
            .contains("velox_bloom_filter_agg"))

        val ctePairedSubqueryDf = spark.sql(
          s"""
             |WITH bf_cte AS (
             |  SELECT bloom_filter_agg(col) AS bf FROM $table WHERE col = 201L
             |)
             |SELECT col, might_contain((SELECT bf FROM bf_cte), col)
             |FROM $table
             |""".stripMargin)
        checkAnswer(ctePairedSubqueryDf, Seq(Row(201L, true), Row(999L, false)))
        assert(
          ctePairedSubqueryDf.queryExecution.executedPlan.toString.contains("velox_might_contain"))
        assert(
          ctePairedSubqueryDf.queryExecution.executedPlan.toString
            .contains("velox_bloom_filter_agg"))

        val cteInInSubqueryDf = spark.sql(
          s"""
             |WITH filtered_cte AS (
             |  SELECT col FROM $table WHERE might_contain(X'$sparkBfHex', col)
             |)
             |SELECT col FROM $table WHERE col IN (SELECT col FROM filtered_cte)
             |""".stripMargin)
        checkAnswer(cteInInSubqueryDf, Seq(Row(201L)))
        assert(
          !cteInInSubqueryDf.queryExecution.executedPlan.toString.contains("velox_might_contain"))
      }
    }
  }

  test("GLUTEN-13489: standalone SQL bloom_filter_agg produces Spark-readable bytes") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_standalone_sql_test"
      withTempView(table) {
        (1L to 100L).toDF("col").createOrReplaceTempView(table)
        val df = spark.sql(s"SELECT bloom_filter_agg(col) AS bf FROM $table")
        val bytes = df.collect().head.getAs[Array[Byte]]("bf")
        assert(!df.queryExecution.executedPlan.toString.contains("velox_bloom_filter_agg"))
        val bf = BloomFilter.readFrom(new ByteArrayInputStream(bytes))
        assert(bf.mightContainLong(50L))
        assert(!bf.mightContainLong(500L))
      }
    }
  }

  // Gated to Spark 3.5+: Spark's BloomFilterAggregate only supports ByteType, ShortType, and
  // IntegerType starting in Spark 3.5 (SPARK-41982); on Spark 3.4 only LongType is accepted.
  testWithMinSparkVersion(
    "GLUTEN-13489: bloom_filter_agg on byte, short, and int columns offloads natively",
    "3.5") {
    if (BackendsApiManager.getSettings.requireBloomFilterAggMightContainJointFallback()) {
      val table = "bloom_filter_small_ints_test"
      withTempView(table) {
        Seq((1.toByte, 10.toShort, 100), (2.toByte, 20.toShort, 200), (3.toByte, 30.toShort, 300))
          .toDF("b", "s", "i")
          .createOrReplaceTempView(table)
        Seq(
          ("b", Seq(Row(2.toByte), Row(3.toByte))),
          ("s", Seq(Row(20.toShort), Row(30.toShort))),
          ("i", Seq(Row(200), Row(300)))
        ).foreach {
          case (colName, expectedRows) =>
            val df = spark.sql(
              s"""
                 |SELECT $colName
                 |FROM $table
                 |WHERE might_contain(
                 |  (SELECT bloom_filter_agg($colName) FROM $table WHERE b > 1),
                 |  cast($colName as long))
                 |""".stripMargin)
            checkAnswer(df, expectedRows)
            val nativeAggs = collectWithSubqueries(df.queryExecution.executedPlan) {
              case h: HashAggregateExecBaseTransformer => h
            }
            assert(
              nativeAggs.size == 2,
              s"Expected native HashAggregateExecBaseTransformer for column $colName, plan:\n" +
                s"${df.queryExecution.executedPlan}"
            )
        }
      }
    }
  }
}
