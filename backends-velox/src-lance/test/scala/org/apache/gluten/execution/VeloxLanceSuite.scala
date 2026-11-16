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

import org.apache.gluten.extension.LanceArrowBatchScan

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, ColumnarToRowExec, InputAdapter, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types._

import org.lance.spark.read.LanceScan

import scala.collection.JavaConverters._

/**
 * Exercises the read-only Lance offload: a Lance scan emits Arrow batches
 * ([[LanceArrowBatchScan]]), which Gluten offloads to Velox, while scans that cannot emit them (a
 * pushed aggregation) or whose types Velox does not import stay on vanilla Spark.
 *
 * Requires the lance-spark runtime (and its native library) on the classpath; see
 * docs/backends/velox/get-started/VeloxLance.md for the supported platforms.
 */
class VeloxLanceSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      // Spark 4.x defaults to ANSI mode, which Velox does not offload yet.
      .set("spark.sql.ansi.enabled", "false")
      // Path-based reads use the DataSource directly; the catalog is registered for parity with
      // lance-spark's own tests and any namespace-qualified access.
      .set("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
  }

  /** Writes a small Lance dataset and registers it as a temp view. */
  private def withLanceView(
      view: String,
      rows: Int,
      writeOptions: Map[String, String] = Map.empty)(f: => Unit): Unit = {
    val data = spark
      .range(rows)
      .selectExpr(
        "cast(id as int) as id",
        "cast(id * 2 as int) as v",
        "concat('n', cast(id as string)) as name")
    withLanceView(view, data, writeOptions, Map.empty)(f)
  }

  /** Writes `data` as a Lance dataset and registers it as a temp view. */
  private def withLanceView(
      view: String,
      data: DataFrame,
      writeOptions: Map[String, String],
      readOptions: Map[String, String])(f: => Unit): Unit = {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/$view.lance"
        data.write
          .format("lance")
          .options(writeOptions)
          .option("path", uri)
          .save()

        spark.read
          .format("lance")
          .options(readOptions)
          .option("path", uri)
          .load()
          .createOrReplaceTempView(view)
        try f
        finally spark.catalog.dropTempView(view)
    }
  }

  /** Field metadata that makes lance-spark write an array column as a FixedSizeList (a vector). */
  private def vectorMetadata(dim: Int): Metadata =
    new MetadataBuilder().putLong("arrow.fixed-size-list.size", dim).build()

  /**
   * Row `i` of the vector dataset (see [[withLanceVectorView]]). Every tenth row, from row 3 on,
   * has a null `vec`, a null `doc`, a null vector inside `doc`, and a null `multi`, respectively.
   */
  private def vectorRow(i: Int): Row = {
    val f = i.toFloat
    Row(
      i,
      if (i % 10 == 3) null else Seq(f, f + 1, f + 2, f + 3),
      Seq(i * 10.0, i * 10.0 + 1),
      if (i % 10 == 5) null else Row(s"d$i", if (i % 10 == 6) null else Seq(-f, -f - 1, -f - 2)),
      if (i % 10 == 7) null else Seq.tabulate(i % 3)(j => Seq(f * 100 + j, f * 100 + j + 0.5f))
    )
  }

  /**
   * Writes `rows` rows of FixedSizeList vector columns as a Lance dataset: `vec` (float, size 4),
   * `dvec` (double, size 2), a vector inside the struct `doc`, and the list of vectors `multi`.
   */
  private def withLanceVectorView(
      view: String,
      rows: Int = 10,
      writeOptions: Map[String, String] = Map.empty,
      readOptions: Map[String, String] = Map.empty)(f: => Unit): Unit = {
    // The element metadata of `multi` makes each element a FixedSizeList of size 2.
    val multiMetadata =
      new MetadataBuilder().putString("_lance.element", vectorMetadata(2).json).build()
    val schema = new StructType()
      .add("id", IntegerType)
      .add("vec", ArrayType(FloatType, containsNull = false), nullable = true, vectorMetadata(4))
      .add("dvec", ArrayType(DoubleType, containsNull = false), nullable = true, vectorMetadata(2))
      .add(
        "doc",
        new StructType()
          .add("label", StringType)
          .add(
            "emb",
            ArrayType(FloatType, containsNull = false),
            nullable = true,
            vectorMetadata(3)))
      .add(
        "multi",
        ArrayType(ArrayType(FloatType, containsNull = false), containsNull = false),
        nullable = true,
        multiMetadata)
    val data = spark.createDataFrame((0 until rows).map(vectorRow).asJava, schema)
    withLanceView(view, data, writeOptions, readOptions)(f)
  }

  private def isLanceScan(plan: SparkPlan): Boolean = plan match {
    case scan: BatchScanExec => scan.scan.isInstanceOf[LanceScan]
    case InputAdapter(child) => isLanceScan(child)
    case _ => false
  }

  private def lanceScans(df: DataFrame): Seq[BatchScanExec] =
    getExecutedPlan(df).collect {
      case scan: BatchScanExec if scan.scan.isInstanceOf[LanceScan] => scan
    }

  /** Asserts that the query reads its Lance scans as Arrow batches. */
  private def assertArrowLanceScan(df: DataFrame): Unit = {
    val scans = lanceScans(df)
    assert(
      scans.nonEmpty && scans.forall(LanceArrowBatchScan.isArrowBatchScan),
      s"Expected Lance scans reading Arrow batches, got:\n${df.queryExecution.executedPlan}"
    )
  }

  /** Asserts that the query reads its Lance scans without Arrow batches, as vanilla Spark does. */
  private def assertVanillaLanceScan(df: DataFrame): Unit = {
    val scans = lanceScans(df)
    assert(
      scans.nonEmpty && !scans.exists(LanceArrowBatchScan.isArrowBatchScan),
      s"Expected Lance scans without Arrow batches, got:\n${df.queryExecution.executedPlan}"
    )
    assert(
      !getExecutedPlan(df).exists {
        case offload: OffloadArrowDataExec => isLanceScan(offload.child)
        case _ => false
      },
      s"Expected no OffloadArrowDataExec over a Lance scan, got:\n${df.queryExecution.executedPlan}"
    )
  }

  /** The operators that directly consume the Lance scan (looking through codegen InputAdapters). */
  private def lanceScanParents(df: DataFrame): Seq[SparkPlan] =
    getExecutedPlan(df)
      .filterNot(_.isInstanceOf[InputAdapter])
      .filter(_.children.exists(isLanceScan))

  /** Asserts that Velox reads the Lance scan's batches directly, with no row conversion between. */
  private def assertScanFeedsVelox(df: DataFrame): Unit = {
    val parents = lanceScanParents(df)
    assert(
      parents.nonEmpty && parents.forall(_.isInstanceOf[OffloadArrowDataExec]),
      s"Expected OffloadArrowDataExec over the Lance scan, got:\n${df.queryExecution.executedPlan}"
    )
  }

  test("lance scan reads Arrow batches") {
    withLanceView("lance_basic", 100) {
      runQueryAndCompare("select id, v, name from lance_basic") {
        df =>
          assertArrowLanceScan(df)
          // With no Velox consumer, the Arrow Java batches are converted to rows directly by
          // vanilla ColumnarToRowExec, with no round trip through Velox.
          val parents = lanceScanParents(df)
          assert(
            parents.nonEmpty && parents.forall(_.isInstanceOf[ColumnarToRowExec]),
            s"Expected ColumnarToRowExec over the Lance scan, got:\n${df.queryExecution.executedPlan}"
          )
      }
    }
  }

  test("lance scan feeds Velox operators without a row conversion") {
    withLanceView("lance_velox_agg", 100) {
      // A grouped aggregation is not pushed into Lance, so it runs as a Velox aggregate on top of
      // the scan: Arrow Java batches -> OffloadArrowDataExec -> ArrowColumnarToVeloxColumnarExec.
      runQueryAndCompare(
        "select id % 10 as k, sum(v) as s, count(*) as c from lance_velox_agg group by id % 10") {
        df =>
          val plan = df.queryExecution.executedPlan
          assertArrowLanceScan(df)
          checkGlutenPlan[HashAggregateExecBaseTransformer](df)
          val parents = lanceScanParents(df)
          assert(
            parents.nonEmpty && parents.forall(_.isInstanceOf[OffloadArrowDataExec]),
            s"Expected OffloadArrowDataExec over the Lance scan, got:\n$plan")
          assert(
            getExecutedPlan(df).exists {
              case p: ArrowColumnarToVeloxColumnarExec =>
                p.child.isInstanceOf[OffloadArrowDataExec] && isLanceScan(p.child.children.head)
              case _ => false
            },
            s"Expected ArrowColumnarToVeloxColumnarExec(OffloadArrowDataExec(scan)), got:\n$plan"
          )
          assert(
            !getExecutedPlan(df).exists {
              case c: ColumnarToRowExec => isLanceScan(c.child)
              case _ => false
            },
            s"Lance scan output must not be converted to rows before Velox, got:\n$plan"
          )
      }
    }
  }

  test("lance scan offloads with projection and pushed filter") {
    withLanceView("lance_filtered", 100) {
      runQueryAndCompare("select id, v from lance_filtered where id < 50") {
        df =>
          assertArrowLanceScan(df)
          checkAnswer(df, (0 until 50).map(i => Row(i, i * 2)))
      }
    }
  }

  test("lance scan reads a multi-fragment dataset") {
    withLanceView("lance_multi", 1000, Map("max_row_per_file" -> "100")) {
      runQueryAndCompare("select id, v, name from lance_multi") {
        df =>
          assertArrowLanceScan(df)
          assert(df.rdd.getNumPartitions > 1, "Expected one partition per Lance fragment")
      }
      // LIMIT stops consuming before the fragments are exhausted, so open scans are released by
      // the task-completion listener rather than on exhaustion.
      val limited = spark.sql("select id from lance_multi limit 5")
      assert(limited.collect().length == 5)
    }
  }

  test("lance scan falls back when an aggregation is pushed down") {
    withLanceView("lance_agg", 100) {
      // A filtered COUNT(*) is pushed into the Lance scan, which then emits no Arrow batches, so the
      // scan must stay as vanilla Spark reads it.
      val df: DataFrame = spark.sql("select count(*) from lance_agg where id < 50")
      checkSparkPlan[BatchScanExec](df)
      assertVanillaLanceScan(df)
      checkAnswer(df, Seq(Row(50L)))
    }
  }

  test("lance scan offloads TIMESTAMP_NTZ and null columns") {
    val data = spark
      .range(20)
      .selectExpr(
        "cast(id as int) as id",
        "cast(timestamp_seconds(id + 1700000000) as timestamp_ntz) as ts",
        "null as n")
    withLanceView("lance_ntz_null", data, Map.empty, Map.empty) {
      runQueryAndCompare("select id, ts, n from lance_ntz_null where id < 10") {
        df =>
          assertArrowLanceScan(df)
          assert(df.collect().length == 10)
      }
      // Velox sorts the TIMESTAMP_NTZ values and counts the null column.
      runQueryAndCompare(
        "select id % 3 as k, max(ts) as t, count(n) as c from lance_ntz_null group by id % 3 " +
          "order by t") {
        df =>
          assertArrowLanceScan(df)
          assertScanFeedsVelox(df)
      }
    }
  }

  test("lance scan offloads vector columns") {
    withLanceVectorView("lance_vectors") {
      runQueryAndCompare("select id, vec, dvec from lance_vectors") {
        df =>
          assertArrowLanceScan(df)
          checkAnswer(df, (0 until 10).map(vectorRow).map(r => Row(r(0), r(1), r(2))))
      }
    }
  }

  test("lance scan offloads nested vector columns") {
    withLanceVectorView("lance_nested_vectors") {
      runQueryAndCompare("select id, doc, multi from lance_nested_vectors where id >= 2") {
        df =>
          assertArrowLanceScan(df)
          checkAnswer(df, (2 until 10).map(vectorRow).map(r => Row(r(0), r(3), r(4))))
      }
    }
  }

  test("lance vector columns feed Velox operators") {
    withLanceVectorView("lance_vectors_velox") {
      // Velox evaluates the element accesses and the aggregation over the imported vectors; null
      // vectors, structs, and lists must stay null rather than expose their child slots.
      runQueryAndCompare(
        """select id % 2 as k, count(vec) as c, sum(vec[0]) as s, sum(dvec[1]) as d,
          |  count(doc) as docs, count(doc.emb) as embs, sum(doc.emb[2]) as e,
          |  count(multi) as m, sum(multi[0][1]) as mm
          |from lance_vectors_velox group by id % 2""".stripMargin) {
        df =>
          assertArrowLanceScan(df)
          checkGlutenPlan[HashAggregateExecBaseTransformer](df)
          assertScanFeedsVelox(df)
      }
    }
  }

  test("lance vector columns pass through a Velox filter, shuffle and sort") {
    withLanceVectorView("lance_vectors_shuffle") {
      // `id % 3` is not pushed into Lance, so Velox filters the imported vectors, shuffles and
      // sorts them, and converts them back to rows.
      runQueryAndCompare(
        "select id, vec, doc, multi from lance_vectors_shuffle where id % 3 != 1 order by id") {
        df =>
          assertArrowLanceScan(df)
          assertScanFeedsVelox(df)
          checkGlutenPlan[FilterExecTransformerBase](df)
          checkGlutenPlan[ColumnarShuffleExchangeExec](df)
          checkGlutenPlan[SortExecTransformer](df)
          checkAnswer(
            df,
            (0 until 10).filter(_ % 3 != 1).map(vectorRow).map(r => Row(r(0), r(1), r(3), r(4))))
      }
    }
  }

  test("lance scan reads vector batches that are all null or empty") {
    withLanceVectorView("lance_vectors_edge") {
      // Single-row batches whose vector, struct, or list is null, and a scan with no rows.
      Seq(
        "select id, vec from lance_vectors_edge where id = 3" -> Seq(Row(3, null)),
        "select id, doc from lance_vectors_edge where id = 5" -> Seq(Row(5, null)),
        "select id, doc from lance_vectors_edge where id = 6" -> Seq(Row(6, Row("d6", null))),
        "select id, multi from lance_vectors_edge where id = 7" -> Seq(Row(7, null)),
        "select id, vec, doc, multi from lance_vectors_edge where id < 0" -> Seq.empty[Row]
      ).foreach {
        case (query, expected) =>
          runQueryAndCompare(query) {
            df =>
              assertArrowLanceScan(df)
              checkAnswer(df, expected)
          }
      }
    }
  }

  test("lance scan reads vectors across fragments and batches") {
    withLanceVectorView(
      "lance_vectors_multi",
      rows = 1000,
      writeOptions = Map("max_row_per_file" -> "300"),
      readOptions = Map("batch_size" -> "64")) {
      runQueryAndCompare("select * from lance_vectors_multi") {
        df =>
          assertArrowLanceScan(df)
          assert(df.rdd.getNumPartitions > 1, "Expected one partition per Lance fragment")
          checkAnswer(df, (0 until 1000).map(vectorRow))
      }
      runQueryAndCompare(
        """select count(vec), sum(vec[0]), sum(vec[3]), sum(doc.emb[1]), sum(multi[1][0])
          |from lance_vectors_multi""".stripMargin) {
        df =>
          assertArrowLanceScan(df)
          checkGlutenPlan[HashAggregateExecBaseTransformer](df)
          assertScanFeedsVelox(df)
      }
    }
  }
}
