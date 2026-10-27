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
package org.apache.spark.sql.connector

import org.apache.gluten.execution.FilterExecTransformerBase

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.connector.catalog.{BufferedRows, InMemoryEnhancedPartitionFilterTable}
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class GlutenDataSourceV2EnhancedPartitionFilterSuite
  extends DataSourceV2EnhancedPartitionFilterSuite
  with GlutenSQLTestsTrait {

  // The vanilla "case 9" tests (excluded in VeloxTestSettings) require a Spark FilterExec for the
  // partition filter that the source returns in the first pass, but Gluten can offload that
  // post-scan filter to a native FilterExecTransformer. These variants keep the answer and
  // pushdown checks and accept either filter.
  testGluten(
    "case 9: partition filter pushed but returned in first pass is not re-pushed second pass") {
    withTable(partFilterTableName) {
      sql(
        s"CREATE TABLE $partFilterTableName (part_col string, data string) USING $v2Source " +
          "PARTITIONED BY (part_col) " +
          "TBLPROPERTIES('return-accepted-partition-predicates' = 'true')")
      sql(s"INSERT INTO $partFilterTableName VALUES ('a', 'x'), ('b', 'y'), ('c', 'z')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE part_col = 'b'")
      checkAnswer(df, Seq(Row("b", "y")))
      assertNoSecondPassPartitionPredicates(df)
      assertScannedPartitionKeys(df, Set("b"))
      assertPostScanFilter(df)
    }
  }

  testGluten(
    "nested identity partition: case 9 partition filter pushed but returned in first pass " +
      "is not re-pushed second pass") {
    withTable(partFilterTableName) {
      sql(
        s"CREATE TABLE $partFilterTableName " +
          s"(s struct<tz: string, x: int>, data string) USING $v2Source " +
          "PARTITIONED BY (s.tz) " +
          "TBLPROPERTIES('return-accepted-partition-predicates' = 'true')")
      sql(
        s"INSERT INTO $partFilterTableName VALUES " +
          "(named_struct('tz', 'LA', 'x', 1), 'a'), " +
          "(named_struct('tz', 'NY', 'x', 2), 'b')")

      val df = sql(s"SELECT * FROM $partFilterTableName WHERE s.tz = 'LA'")
      checkAnswer(df, Seq(Row(Row("LA", 1), "a")))
      assertNoSecondPassPartitionPredicates(df)
      assertScannedPartitionKeys(df, Set("LA"))
      assertPostScanFilter(df)
    }
  }

  private def batchScan(df: DataFrame): BatchScanExec = {
    stripAQEPlan(df.queryExecution.executedPlan)
      .collectFirst { case b: BatchScanExec => b }
      .getOrElse(fail("Expected BatchScanExec in plan"))
  }

  private def assertNoSecondPassPartitionPredicates(df: DataFrame): Unit = {
    val predicates = batchScan(df).batch match {
      case s: InMemoryEnhancedPartitionFilterTable#InMemoryEnhancedPartitionFilterBatchScan =>
        s.getPushedPartitionPredicates
      case _ => Seq.empty
    }
    assert(predicates.isEmpty, s"Expected no pushed partition predicates, got $predicates")
  }

  private def assertScannedPartitionKeys(df: DataFrame, expectedKeys: Set[String]): Unit = {
    val partitions = batchScan(df).batch.planInputPartitions()
    assert(partitions.length === expectedKeys.size)
    assert(partitions.map(_.asInstanceOf[BufferedRows].keyString()).toSet === expectedKeys)
  }

  private def assertPostScanFilter(df: DataFrame): Unit = {
    val expected = df.queryExecution.analyzed
      .collectFirst { case f: Filter => f.condition }
      .getOrElse(fail("Expected a Filter in the analyzed plan"))
    val hasReturnedPredicate = stripAQEPlan(df.queryExecution.executedPlan).exists {
      case f: FilterExec =>
        splitConjunctivePredicates(f.condition).exists(_.semanticEquals(expected))
      case f: FilterExecTransformerBase =>
        splitConjunctivePredicates(f.cond).exists(_.semanticEquals(expected))
      case _ => false
    }
    assert(
      hasReturnedPredicate,
      "Partition filter returned in first pass should remain in the post-scan filter")
  }
}
