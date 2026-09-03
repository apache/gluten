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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarBuildSideRelation, SerializedHashTableBroadcastRelation}
import org.apache.spark.sql.execution.joins.BuildSideRelation

/**
 * Covers the multi-threaded shape of the broadcast hash table build, which the tests in
 * [[VeloxHashJoinSuite]] do not reach: their build sides are small enough that
 * 'broadcast.build.targetBytesPerThread' resolves to a single build thread, and a single-threaded
 * build produces one row container, hence one row section on the wire, hence nothing for the
 * reading side to decode or insert in parallel.
 *
 * This suite pins that config low enough that even a unit-test sized build side fans out, so that
 * the paths that only exist above one thread are exercised:
 *   - several row containers on the driver, serialized as several independent row sections;
 *   - a deserialization that decodes those sections concurrently and inserts them into the slot
 *     array by bucket range;
 *   - the on-heap relation layout, which the existing driver-side tests skip by forcing off-heap.
 */
class VeloxDriverSideBroadcastBuildSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    // 'buildThreads' is ceil(buildSideBytes / this), capped at the core count. 4 KiB per thread
    // makes the few-hundred-row build sides below fan out over several threads. This is a static
    // conf, so it has to be set here rather than per test.
    .set(VeloxConfig.COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_TARGET_BYTES.key, "4096")
    // Threads alone are not enough: the build side is split across them a batch at a time, each
    // thread's batches become one row container, and each non-empty container becomes one row
    // section on the wire. With the default batch size the whole build side is a single batch, so
    // eight of nine threads would get nothing and the serialized form would carry one section --
    // leaving exactly the parallel paths this suite exists for untested. A small batch size is
    // what turns the build side into enough batches to go around.
    .set(GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key, "32")
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    VeloxBroadcastBuildSideCache.cleanAll()
  }

  override protected def afterEach(): Unit = {
    try {
      VeloxBroadcastBuildSideCache.cleanAll()
    } finally {
      super.afterEach()
    }
  }

  private def broadcastExchanges(df: DataFrame): Seq[ColumnarBroadcastExchangeExec] =
    collectWithSubqueries(df.queryExecution.executedPlan) {
      case exchange: ColumnarBroadcastExchangeExec => exchange
    }

  private def broadcastRelations(df: DataFrame): Seq[BuildSideRelation] =
    broadcastExchanges(df).map(_.executeBroadcast[BuildSideRelation]().value)

  /**
   * Asserts that the build really did fan out, or the parallel paths went untested. Both halves
   * matter: the thread count decides how many row containers the build may use, and the batch count
   * decides how many of them actually receive rows, which is what ends up as the number of
   * independently decodable row sections on the wire.
   */
  private def assertFannedOut(df: DataFrame): Unit = {
    val exchanges = broadcastExchanges(df)
    assert(exchanges.nonEmpty, "Expected a columnar broadcast exchange")
    exchanges.foreach {
      exchange =>
        val buildThreads = exchange.metrics("buildThreads").value
        assert(
          buildThreads > 1,
          s"Expected the build side to fan out over several threads, got $buildThreads. Check " +
            s"${VeloxConfig.COLUMNAR_VELOX_BROADCAST_HASH_TABLE_BUILD_TARGET_BYTES.key} and the " +
            s"host's core count."
        )
    }
    // Only the on-heap relation exposes its batches; the off-heap one keeps them private. Both
    // are produced by the same serializer from the same data under the same batch size, so
    // checking the reachable one is enough to know the batching is right.
    rawBuildSideRelations(df).foreach {
      case columnar: ColumnarBuildSideRelation =>
        assert(
          columnar.batches.length > 1,
          s"Expected the build side to arrive as several serialized batches so that more than " +
            s"one of the build threads gets rows, got ${columnar.batches.length}. Check " +
            s"${GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key}."
        )
      case _ =>
    }
  }

  /**
   * The relation holding the still-serialized build side. For a driver-side build that is the raw
   * relation the serialized hash table was built from, which it keeps for driver-only uses.
   */
  private def rawBuildSideRelations(df: DataFrame): Seq[BuildSideRelation] =
    broadcastRelations(df).map {
      case serialized: SerializedHashTableBroadcastRelation =>
        serialized.getSerializedHashTable.buildSideRelation
      case other => other
    }

  private def withBuildTables(body: => Unit): Unit = {
    withTable("bcast_fact", "bcast_dim") {
      // Wide-ish string payload so that the build side is big enough in bytes to fan out, and so
      // that the row sections carry out-of-line strings: those are copied into the row
      // container's own allocator, which is what lets the decode use a private memory pool.
      spark
        .range(0, 4000)
        .selectExpr("id as k", "id % 600 as fk", "cast(id as string) as tag")
        .write
        .saveAsTable("bcast_fact")
      spark
        .range(0, 600)
        .selectExpr(
          "id as k",
          "concat('dimension_name_padded_out_to_some_length_', cast(id as string)) as name",
          "cast(id % 7 as int) as bucket")
        .write
        .saveAsTable("bcast_dim")
      body
    }
  }

  private val joinQuery =
    """
      |SELECT /*+ BROADCAST(d) */ f.k, f.tag, d.name, d.bucket
      |FROM bcast_fact f
      |JOIN bcast_dim d
      |ON f.fk = d.k
      |ORDER BY f.k
      |""".stripMargin

  private def runWithBuildSide(driverSide: Boolean, offHeap: Boolean)(
      customCheck: DataFrame => Unit): Unit = {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "10MB"),
      ("spark.sql.adaptive.enabled", "false"),
      (VeloxConfig.VELOX_DRIVER_SIDE_BROADCAST_HASH_TABLE_BUILD.key, driverSide.toString),
      (VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key, offHeap.toString),
      // Lets the reading side partition the slot array by bucket range instead of falling back to
      // a single serial insert, which is what a table this small would otherwise do.
      (VeloxConfig.VELOX_MIN_TABLE_ROWS_FOR_PARALLEL_JOIN_BUILD.key, "0")
    ) {
      runQueryAndCompare(joinQuery)(customCheck)
    }
  }

  for (offHeap <- Seq(false, true)) {
    val layout = if (offHeap) "off-heap" else "on-heap"

    test(s"driver-side build over several threads matches vanilla ($layout relation)") {
      withBuildTables {
        runWithBuildSide(driverSide = true, offHeap = offHeap) {
          df =>
            assertFannedOut(df)
            val relations = broadcastRelations(df)
            assert(relations.size == 1, s"Expected one broadcast relation, got ${relations.size}")
            assert(
              relations.head.isInstanceOf[SerializedHashTableBroadcastRelation],
              s"Expected the driver-side build to produce a " +
                s"SerializedHashTableBroadcastRelation, got ${relations.head.getClass.getName}"
            )
            val serialized =
              relations.head.asInstanceOf[SerializedHashTableBroadcastRelation]
            assert(serialized.getSerializedHashTable.sizeInBytes > 0)
            assert(serialized.getSerializedHashTable.numRows == 600)
        }
      }
    }

    test(s"executor-side build over several threads matches vanilla ($layout relation)") {
      withBuildTables {
        runWithBuildSide(driverSide = false, offHeap = offHeap) {
          df =>
            assertFannedOut(df)
            val relations = broadcastRelations(df)
            assert(relations.size == 1, s"Expected one broadcast relation, got ${relations.size}")
            assert(
              !relations.head.isInstanceOf[SerializedHashTableBroadcastRelation],
              "Expected the raw build side relation when the driver-side build is disabled"
            )
        }
      }
    }
  }

  test("driver-side and executor-side builds agree row for row") {
    withBuildTables {
      def collectRows(driverSide: Boolean): Seq[org.apache.spark.sql.Row] = {
        var rows: Seq[org.apache.spark.sql.Row] = Nil
        runWithBuildSide(driverSide, offHeap = false)(df => rows = df.collect().toSeq)
        VeloxBroadcastBuildSideCache.cleanAll()
        rows
      }

      val driverSideRows = collectRows(driverSide = true)
      val executorSideRows = collectRows(driverSide = false)
      assert(driverSideRows.nonEmpty, "The join produced no rows, so this proves nothing")
      assert(
        driverSideRows == executorSideRows,
        s"Driver-side build produced ${driverSideRows.size} rows, executor-side build produced " +
          s"${executorSideRows.size}"
      )
    }
  }

  test("driver-side build over several threads keeps null and unmatched keys straight") {
    withTable("null_fact", "null_dim") {
      // Nulls on both sides, plus fact keys with no match, so that the hashers take their
      // null-aware path and the probe has to miss as well as hit.
      spark
        .range(0, 4000)
        .selectExpr(
          "id as id",
          "case when id % 13 = 0 then null else id % 900 end as fk",
          "cast(id as string) as tag")
        .write
        .saveAsTable("null_fact")
      spark
        .range(0, 600)
        .selectExpr(
          "id as k",
          "case when id % 17 = 0 then null else " +
            "concat('padded_dimension_name_', cast(id as string)) end as name")
        .write
        .saveAsTable("null_dim")

      withSQLConf(
        ("spark.sql.autoBroadcastJoinThreshold", "10MB"),
        ("spark.sql.adaptive.enabled", "false"),
        (VeloxConfig.VELOX_DRIVER_SIDE_BROADCAST_HASH_TABLE_BUILD.key, "true"),
        (VeloxConfig.VELOX_BROADCAST_BUILD_RELATION_USE_OFFHEAP.key, "false"),
        (VeloxConfig.VELOX_MIN_TABLE_ROWS_FOR_PARALLEL_JOIN_BUILD.key, "0")
      ) {
        val query =
          """
            |SELECT /*+ BROADCAST(d) */ f.id, f.fk, d.name
            |FROM null_fact f
            |LEFT JOIN null_dim d
            |ON f.fk = d.k
            |ORDER BY f.id
            |""".stripMargin
        runQueryAndCompare(query)(df => assertFannedOut(df))
      }
    }
  }
}
