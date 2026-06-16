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
package org.apache.gluten.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException

/**
 * Reproduction for the native crash fixed in cpp/velox/substrait/SubstraitToVeloxExpr.cc
 * (SubstraitVeloxExprConverter::toVeloxExpr).
 *
 * Delta rewrites a MERGE/UPDATE that assigns to a field nested under an array (e.g. `value.a` where
 * `value` is `array<struct<a: int>>`) into a field reference whose path descends through the array
 * element. While resolving that reference Gluten's converter did
 * `inputColumnType = asRowType(childAt(idx))`; for the array child `asRowType()` returns null, and
 * the next loop iteration dereferenced the null RowType, crashing the whole forked JVM with a
 * SIGSEGV in `toVeloxExpr(FieldReference, ...)`. Because a SIGSEGV is not a catchable C++
 * exception, plan validation could not fall back. The crash was observed in the Delta Spark UT
 * pipeline running Delta's `MergeIntoNestedDataSQLPathBasedSuite` test "nested data support -
 * analysis error - updating array type".
 *
 * Updating a field under an array is unsupported, so Delta is expected to raise an
 * AnalysisException; the query must never crash the JVM. With the fix the converter throws a
 * VeloxUserError that SubstraitToVeloxPlanValidator catches, Gluten falls back, and the query fails
 * cleanly with that AnalysisException.
 *
 * NOTE: this only reaches the native converter (and so the SIGSEGV on an unfixed build) when Gluten
 * actually offloads the rewritten MERGE expression, which happens with the Delta 4.x plan used by
 * the Spark 4.0/4.1 `-Pdelta` CI jobs. On Delta 3.3.x (the Spark 3.x jobs / a typical local run)
 * the statement is rejected during analysis before the converter runs, so this test passes there
 * without exercising the crash. It is the Spark-level companion to
 * cpp/velox/tests/SubstraitVeloxExprConverterTest.cc, which reproduces the crash directly.
 */
class VeloxDeltaNestedFieldArraySuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      // Gluten blanket-falls-back when ANSI mode is on (Spark 4.x default); disable it so the
      // MERGE update expression is actually offloaded and reaches the native converter.
      .set("spark.sql.ansi.enabled", "false")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  testWithMinSparkVersion(
    "delta: merge update of a field nested under an array does not crash natively",
    "3.2") {
    withTable("merge_array_src", "merge_array_tgt") {
      spark.sql(
        "CREATE TABLE merge_array_tgt (key STRING, value ARRAY<STRUCT<a: INT>>) USING delta")
      spark.sql("INSERT INTO merge_array_tgt VALUES ('A', array(named_struct('a', 1)))")
      spark.sql(
        "CREATE TABLE merge_array_src (key STRING, value ARRAY<STRUCT<a: INT>>) USING delta")
      spark.sql("INSERT INTO merge_array_src VALUES ('A', array(named_struct('a', 0)))")

      // Updating `value.a` (a field inside the array) is unsupported. The converter must surface a
      // catchable error so Gluten falls back and Delta can report the analysis error, rather than
      // dereferencing a null RowType and crashing the JVM with a SIGSEGV.
      val e = intercept[AnalysisException] {
        spark.sql("""
                    |MERGE INTO merge_array_tgt t
                    |USING merge_array_src s
                    |ON s.key = t.key
                    |WHEN MATCHED THEN UPDATE SET value.a = 2
                    |""".stripMargin)
      }
      assert(e.getMessage.contains("Updating nested fields is only supported for StructType"))
    }
  }
}
