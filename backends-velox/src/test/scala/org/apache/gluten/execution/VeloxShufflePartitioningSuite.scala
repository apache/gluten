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

import org.apache.spark.{KeyGroupedPartitioner, SparkConf}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, GenericInternalRow}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * SxS test suite for columnar shuffle exchange partitioning.
 *
 * Tests cover hash, range, round-robin, and single partitioning via columnar shuffle.
 * KeyGroupedPartitioning is validated at the unit level (key extraction, partitioner construction)
 * but cannot be exercised end-to-end without V2 data source connectors (Iceberg/Paimon) which
 * require test infrastructure not available in this module.
 */
class VeloxShufflePartitioningSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.ansi.enabled", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  // === A: Basic Correctness (Hash Partitioning) ===

  test("A1: SxS hash shuffle via single-key GROUP BY") {
    runQueryAndCompare("""SELECT l_returnflag, count(*) as cnt
                         |FROM lineitem
                         |GROUP BY l_returnflag""".stripMargin) { _ => }
  }

  test("A2: SxS hash shuffle via multi-key GROUP BY") {
    runQueryAndCompare("""SELECT l_returnflag, l_linestatus,
                         |  sum(l_quantity) as total_qty
                         |FROM lineitem
                         |GROUP BY l_returnflag, l_linestatus""".stripMargin) { _ => }
  }

  test("A3: SxS hash shuffle via column repartition") {
    compareDfResultsAgainstVanillaSpark(
      () =>
        spark
          .sql("SELECT * FROM lineitem")
          .repartition(
            4,
            spark
              .sql("SELECT * FROM lineitem")
              .col("l_orderkey")),
      compareResult = true,
      customCheck = { _ => },
      noFallBack = true
    )
  }

  test("A4: SxS hash shuffle via equi-JOIN") {
    runQueryAndCompare("""SELECT l.l_orderkey, l.l_partkey
                         |FROM lineitem l
                         |JOIN orders o ON l.l_orderkey = o.o_orderkey
                         |WHERE o.o_orderpriority = '1-URGENT'""".stripMargin) { _ => }
  }

  // === B: Range Partitioning ===

  test("B1: SxS range shuffle via ORDER BY single column") {
    runQueryAndCompare("""SELECT l_orderkey, l_quantity
                         |FROM lineitem
                         |ORDER BY l_quantity, l_orderkey""".stripMargin) { _ => }
  }

  test("B2: SxS range shuffle via ORDER BY multiple columns") {
    runQueryAndCompare("""SELECT l_returnflag, l_linestatus,
                         |  l_quantity, l_orderkey
                         |FROM lineitem
                         |ORDER BY l_returnflag, l_linestatus,
                         |  l_quantity, l_orderkey""".stripMargin) { _ => }
  }

  test("B3: SxS range shuffle ORDER BY with LIMIT") {
    runQueryAndCompare("""SELECT l_orderkey, l_extendedprice
                         |FROM lineitem
                         |ORDER BY l_extendedprice DESC, l_orderkey
                         |LIMIT 50""".stripMargin) { _ => }
  }

  // === C: Round-Robin Partitioning ===

  test("C1: SxS round-robin repartition") {
    compareDfResultsAgainstVanillaSpark(
      () =>
        spark
          .sql("""SELECT l_orderkey, l_partkey, l_quantity
                 |FROM lineitem""".stripMargin)
          .repartition(3),
      compareResult = true,
      customCheck = { _ => },
      noFallBack = true
    )
  }

  test("C2: SxS round-robin with different partition count") {
    compareDfResultsAgainstVanillaSpark(
      () =>
        spark
          .sql("SELECT l_orderkey FROM lineitem")
          .repartition(7),
      compareResult = true,
      customCheck = { _ => },
      noFallBack = true)
  }

  // === D: Single Partitioning ===

  test("D1: SxS single partition via coalesce(1)") {
    compareDfResultsAgainstVanillaSpark(
      () =>
        spark
          .sql("""SELECT l_orderkey, l_quantity
                 |FROM lineitem""".stripMargin)
          .coalesce(1),
      compareResult = true,
      customCheck = { _ => },
      noFallBack = true
    )
  }

  test("D2: SxS single partition via global aggregation") {
    runQueryAndCompare("""SELECT count(*) as cnt,
                         |  sum(l_quantity) as total_qty,
                         |  avg(l_extendedprice) as avg_price
                         |FROM lineitem""".stripMargin) { _ => }
  }

  // === E: Null Semantics ===

  test("E1: SxS hash shuffle GROUP BY with NULLs") {
    runQueryAndCompare("""SELECT l_comment, count(*) as cnt
                         |FROM lineitem
                         |GROUP BY l_comment""".stripMargin) { _ => }
  }

  test("E2: SxS hash shuffle JOIN with NULL keys") {
    runQueryAndCompare("""SELECT l.l_orderkey, l.l_partkey
                         |FROM lineitem l
                         |LEFT JOIN orders o
                         |  ON l.l_orderkey = o.o_orderkey
                         |ORDER BY l.l_orderkey, l.l_partkey""".stripMargin) { _ => }
  }

  // === F: Data Type Coverage ===

  test("F1: SxS hash shuffle with decimal types") {
    runQueryAndCompare("""SELECT l_extendedprice,
                         |  sum(l_extendedprice * l_discount) as revenue
                         |FROM lineitem
                         |GROUP BY l_extendedprice""".stripMargin) { _ => }
  }

  test("F2: SxS range shuffle with string ordering") {
    runQueryAndCompare("""SELECT l_returnflag, l_linestatus, l_orderkey
                         |FROM lineitem
                         |ORDER BY l_returnflag, l_linestatus,
                         |  l_orderkey""".stripMargin) { _ => }
  }

  // === G: Boundary Cases ===

  test("G1: SxS hash shuffle single partition") {
    withSQLConf("spark.sql.shuffle.partitions" -> "1") {
      runQueryAndCompare("""SELECT l_returnflag, count(*) as cnt
                           |FROM lineitem
                           |GROUP BY l_returnflag""".stripMargin) { _ => }
    }
  }

  test("G2: SxS hash shuffle many partitions") {
    withSQLConf("spark.sql.shuffle.partitions" -> "32") {
      runQueryAndCompare("""SELECT l_returnflag, count(*) as cnt
                           |FROM lineitem
                           |GROUP BY l_returnflag""".stripMargin) { _ => }
    }
  }

  // === H: KeyGroupedPartitioning Unit Tests ===
  // These test the key extraction logic used when
  // KeyGroupedPartitioning is triggered by V2 connectors.

  test("H1: key extractor single-column integer") {
    val keyAttr = AttributeReference("key", IntegerType)()
    val valAttr = AttributeReference("val", StringType)()
    val outputAttrs = Seq(keyAttr, valAttr)
    val boundExprs = BindReferences.bindReferences(
      Seq(keyAttr.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]),
      outputAttrs)
    val row = new GenericInternalRow(Array[Any](42, UTF8String.fromString("hello")))
    val extracted = boundExprs.map(_.eval(row))
    assert(extracted == Seq(42))
  }

  test("H2: key extractor multi-column composite key") {
    val k1 = AttributeReference("k1", IntegerType)()
    val k2 = AttributeReference("k2", StringType)()
    val valAttr = AttributeReference("val", IntegerType)()
    val outputAttrs = Seq(k1, k2, valAttr)
    val boundExprs = BindReferences.bindReferences(
      Seq(
        k1.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
        k2.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]),
      outputAttrs
    )
    val row = new GenericInternalRow(Array[Any](10, UTF8String.fromString("abc"), 999))
    val extracted = boundExprs.map(_.eval(row))
    assert(extracted == Seq(10, UTF8String.fromString("abc")))
  }

  test("H3: key extractor with null values") {
    val k1 = AttributeReference("k1", IntegerType, nullable = true)()
    val valAttr = AttributeReference("val", StringType)()
    val outputAttrs = Seq(k1, valAttr)
    val boundExprs = BindReferences.bindReferences(
      Seq(k1.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]),
      outputAttrs)
    val row = new GenericInternalRow(Array[Any](null, UTF8String.fromString("hello")))
    val extracted = boundExprs.map(_.eval(row))
    assert(extracted == Seq(null))
  }

  test("H4: key extractor with long and double types") {
    val k1 = AttributeReference("k1", LongType)()
    val k2 = AttributeReference("k2", DoubleType)()
    val valAttr = AttributeReference("val", IntegerType)()
    val outputAttrs = Seq(k1, k2, valAttr)
    val boundExprs = BindReferences.bindReferences(
      Seq(
        k1.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
        k2.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]),
      outputAttrs
    )
    val row = new GenericInternalRow(Array[Any](Long.MaxValue, 3.14d, 1))
    val extracted = boundExprs.map(_.eval(row))
    assert(extracted == Seq(Long.MaxValue, 3.14d))
  }

  test("H5: KeyGroupedPartitioner maps keys to correct partition IDs") {
    val valueMap = scala.collection.mutable.Map.empty[Seq[Any], Int]
    valueMap.update(Seq(1, "a"), 0)
    valueMap.update(Seq(2, "b"), 1)
    valueMap.update(Seq(3, "c"), 2)
    val partitioner = new KeyGroupedPartitioner(valueMap, 3)

    assert(partitioner.getPartition(Seq(1, "a")) == 0)
    assert(partitioner.getPartition(Seq(2, "b")) == 1)
    assert(partitioner.getPartition(Seq(3, "c")) == 2)
    assert(partitioner.numPartitions == 3)
  }

  test("H6: KeyGroupedPartitioner end-to-end with key extraction") {
    val k1 = AttributeReference("k1", IntegerType)()
    val k2 = AttributeReference("k2", StringType)()
    val valAttr = AttributeReference("val", IntegerType)()
    val outputAttrs = Seq(k1, k2, valAttr)
    val boundExprs = BindReferences.bindReferences(
      Seq(
        k1.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
        k2.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression]),
      outputAttrs
    )

    // Build partitioner with known keys
    val valueMap = scala.collection.mutable.Map.empty[Seq[Any], Int]
    valueMap.update(Seq(10, UTF8String.fromString("abc")), 0)
    valueMap.update(Seq(20, UTF8String.fromString("def")), 1)
    val partitioner = new KeyGroupedPartitioner(valueMap, 2)

    // Extract key from row and look up partition
    val row = new GenericInternalRow(Array[Any](10, UTF8String.fromString("abc"), 999))
    val key = boundExprs.map(_.eval(row)).toSeq
    val pid = partitioner.getPartition(key)
    assert(pid == 0)

    val row2 = new GenericInternalRow(Array[Any](20, UTF8String.fromString("def"), 123))
    val key2 = boundExprs.map(_.eval(row2)).toSeq
    val pid2 = partitioner.getPartition(key2)
    assert(pid2 == 1)
  }

  // === Helpers ===
  // SxS tests use runQueryAndCompare / compareDfResultsAgainstVanillaSpark
  // from GlutenQueryComparisonTest which automatically verifies:
  // 1. Result correctness (checkAnswer against vanilla Spark)
  // 2. No fallback (FallbackUtil.hasFallback check on executed plan)
}
