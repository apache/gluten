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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, ColumnarToRowExec, InputAdapter, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types._

import org.lance.spark.utils.BlobUtils

import java.sql.Date

import scala.collection.JavaConverters._

/**
 * Exercises the read-only Lance offload: a columnar Lance scan is handed to Velox through the Arrow
 * C stream ([[LanceScanTransformer]]), while scans that the export path cannot serve (pushed
 * aggregation, full-text query, columns it cannot export) fall back to vanilla Spark.
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

  /** Writes `rows` to a Lance dataset; field metadata in `schema` selects Lance's Arrow types. */
  private def withLanceData(
      schema: StructType,
      rows: Seq[Row],
      writeOptions: Map[String, String] = Map.empty)(f: DataFrame => Unit): Unit = {
    withTempPath {
      path =>
        val uri = s"${path.getCanonicalPath}/data.lance"
        spark
          .createDataFrame(rows.asJava, schema)
          .write
          .format("lance")
          .options(writeOptions)
          .option("path", uri)
          .save()
        f(spark.read.format("lance").option("path", uri).load())
    }
  }

  private def assertFallback(df: DataFrame): Unit = {
    checkSparkPlan[BatchScanExec](df)
    assert(
      !getExecutedPlan(df).exists(_.isInstanceOf[LanceScanTransformer]),
      s"Lance scan must not be offloaded, got:\n${df.queryExecution.executedPlan}")
  }

  private def isLanceScan(plan: SparkPlan): Boolean = plan match {
    case _: LanceScanTransformer => true
    case InputAdapter(child) => isLanceScan(child)
    case _ => false
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

  test("lance scan offloads to LanceScanTransformer") {
    withLanceView("lance_basic", 100) {
      runQueryAndCompare("select id, v, name from lance_basic") {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
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
          checkGlutenPlan[LanceScanTransformer](df)
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
          checkGlutenPlan[LanceScanTransformer](df)
          checkAnswer(df, (0 until 50).map(i => Row(i, i * 2)))
      }
    }
  }

  test("lance scan reads a multi-fragment dataset") {
    withLanceView("lance_multi", 1000, Map("max_row_per_file" -> "100")) {
      runQueryAndCompare("select id, v, name from lance_multi") {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
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
      // A filtered COUNT(*) is pushed into the Lance scan; the Arrow C stream export cannot serve a
      // pushed aggregation, so the offload guard must leave it on a vanilla BatchScanExec.
      val df: DataFrame = spark.sql("select count(*) from lance_agg where id < 50")
      checkSparkPlan[BatchScanExec](df)
      assert(
        !getExecutedPlan(df).exists(_.isInstanceOf[LanceScanTransformer]),
        "Lance scan with a pushed aggregation must not be offloaded")
      checkAnswer(df, Seq(Row(50L)))
    }
  }

  test("lance scan offloads vector columns") {
    withLanceVectorView("lance_vectors") {
      runQueryAndCompare("select id, vec, dvec from lance_vectors") {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
          checkAnswer(df, (0 until 10).map(vectorRow).map(r => Row(r(0), r(1), r(2))))
      }
    }
  }

  test("lance scan offloads nested vector columns") {
    withLanceVectorView("lance_nested_vectors") {
      runQueryAndCompare("select id, doc, multi from lance_nested_vectors where id >= 2") {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
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
          checkGlutenPlan[LanceScanTransformer](df)
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
          checkGlutenPlan[LanceScanTransformer](df)
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
              checkGlutenPlan[LanceScanTransformer](df)
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
          checkGlutenPlan[LanceScanTransformer](df)
          assert(df.rdd.getNumPartitions > 1, "Expected one partition per Lance fragment")
          checkAnswer(df, (0 until 1000).map(vectorRow))
      }
      runQueryAndCompare(
        """select count(vec), sum(vec[0]), sum(vec[3]), sum(doc.emb[1]), sum(multi[1][0])
          |from lance_vectors_multi""".stripMargin) {
        df =>
          checkGlutenPlan[LanceScanTransformer](df)
          checkGlutenPlan[HashAggregateExecBaseTransformer](df)
          assertScanFeedsVelox(df)
      }
    }
  }

  test("lance scan falls back on other columns it cannot export") {
    def marked(name: String, dataType: DataType, key: String, value: Any): StructField = {
      val metadata = value match {
        case v: Long => new MetadataBuilder().putLong(key, v)
        case v: String => new MetadataBuilder().putString(key, v)
      }
      StructField(name, dataType, nullable = true, metadata.build())
    }
    // Float16 vectors are a FixedSizeList of Float16, which lance-spark writes on Spark 4.0+ only.
    val float16Vector = StructField(
      "f",
      ArrayType(FloatType, containsNull = false),
      nullable = true,
      new MetadataBuilder()
        .putLong("arrow.fixed-size-list.size", 2)
        .putString("arrow.float16", "true")
        .build()
    )
    val cases = Seq(
      // Arrow types ArrowColumnVector has no accessor for, also after a vector is read as a list.
      marked("b", BinaryType, "arrow.fixed-size-binary.byte-width", 2L) -> Array[Byte](1, 2),
      marked("d", DateType, "arrow.date-millisecond", "true") -> Date.valueOf("2024-01-02"),
      // A column the native scan returns in its stored form, not the declared one.
      marked("j", StringType, "ARROW:extension:name", "arrow.json") -> """{"a":1}"""
    ) ++ (if (isSparkVersionGE("4.0")) Seq(float16Vector -> Seq(0.5f, 1.5f)) else Nil)
    cases.foreach {
      case (field, value) =>
        val rows = Seq(Row(0, value), Row(1, null))
        withLanceData(new StructType().add("id", IntegerType).add(field), rows) {
          df =>
            assertFallback(df)
            checkAnswer(df, rows)
            // The scan's columns decide: without this one it still offloads.
            checkGlutenPlan[LanceScanTransformer](df.select("id"))
        }
    }

    // Blob columns are also returned in their stored form, alongside `_rowaddr`. lance-spark reads
    // them back as references (v1) or descriptions (v2, file format 2.2) rather than the written
    // bytes, so only the row count is checked.
    Seq(
      (marked("blob", BinaryType, "lance-encoding:blob", "true"), Map.empty[String, String], false),
      (
        marked("blob", BinaryType, "ARROW:extension:name", "lance.blob.v2"),
        Map("file_format_version" -> "2.2"),
        true)
    ).foreach {
      case (field, options, v2) =>
        withLanceData(
          new StructType().add("id", IntegerType).add(field),
          Seq(Row(0, Array[Byte](3))),
          options) {
          df =>
            assert(BlobUtils.isBlobV2SparkField(df.schema("blob")) == v2)
            assertFallback(df)
            assert(df.collect().length == 1)
        }
    }
  }

  test("lance scan falls back on metadata columns and on reading no columns") {
    val schema = new StructType().add("id", IntegerType).add("name", StringType)
    withLanceData(schema, Seq(Row(0, "a"), Row(1, "b"))) {
      df =>
        // lance-spark leaves these out of the native scan and adds them itself.
        Seq(
          "_rowid",
          "_rowaddr",
          "_fragid",
          "_row_created_at_version",
          "_row_last_updated_at_version").foreach {
          column =>
            compareDfResultsAgainstVanillaSpark(
              () => df.select(column, "id"),
              customCheck = assertFallback,
              noFallBack = false)
        }
        // With no columns to read, the native scan returns `_rowid`.
        compareDfResultsAgainstVanillaSpark(
          () => df.selectExpr("1 as one"),
          customCheck = assertFallback,
          noFallBack = false)
    }
  }
}
