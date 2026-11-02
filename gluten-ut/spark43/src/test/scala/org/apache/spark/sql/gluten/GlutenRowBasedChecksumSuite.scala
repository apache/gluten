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
package org.apache.spark.sql.gluten

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.{MapOutputTrackerMaster, SparkConf}
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

/**
 * End-to-end tests for the row-based checksum (SPARK-51756) computed by Gluten's
 * ColumnarShuffleWriter. Verifies that `MapStatus.checksumValue` is propagated, deterministic for
 * identical input, and changes when row data changes.
 */
class GlutenRowBasedChecksumSuite extends GlutenSQLTestsTrait {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key, "5")
      .set(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key, "false")
      // Disable ANSI fallback to force Gluten's ColumnarShuffleWriter path.
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
  }

  private def getLatestShuffleChecksumValues(): Array[Long] = {
    val tracker = spark.sparkContext.env.mapOutputTracker
      .asInstanceOf[MapOutputTrackerMaster]
    val latestShuffleId = tracker.shuffleStatuses.keys.max
    tracker.shuffleStatuses(latestShuffleId).mapStatuses.map(_.checksumValue)
  }

  test("Gluten row-based checksum is deterministic") {
    withSQLConf(
      SQLConf.SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED.key -> "true",
      SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key -> "false") {
      withTable("t_det1", "t_det2") {
        spark.range(500).repartition(5, col("id")).write.mode("overwrite").saveAsTable("t_det1")
        val checksums1 = getLatestShuffleChecksumValues()

        spark.range(500).repartition(5, col("id")).write.mode("overwrite").saveAsTable("t_det2")
        val checksums2 = getLatestShuffleChecksumValues()

        // Same input -> same checksumValue (deterministic)
        assert(
          checksums1.zip(checksums2).forall { case (a, b) => a == b },
          s"Checksums not deterministic: ${checksums1.toSeq} vs ${checksums2.toSeq}")
      }
    }
  }

  test("Gluten row-based checksum detects data change") {
    withSQLConf(
      SQLConf.SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED.key -> "true",
      SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key -> "false") {
      withTable("t_diff1", "t_diff2") {
        spark.range(500).repartition(5, col("id")).write.mode("overwrite").saveAsTable("t_diff1")
        val checksums1 = getLatestShuffleChecksumValues()

        // Different data
        spark
          .range(500, 1000)
          .repartition(5, col("id"))
          .write
          .mode("overwrite")
          .saveAsTable("t_diff2")
        val checksums2 = getLatestShuffleChecksumValues()

        // Different input -> different checksumValue
        assert(
          checksums1.zip(checksums2).exists { case (a, b) => a != b },
          s"Checksums should differ for different data: ${checksums1.toSeq} vs ${checksums2.toSeq}")
      }
    }
  }

  test("Gluten row-based checksum covers the first data column") {
    withSQLConf(
      SQLConf.SHUFFLE_ORDER_INDEPENDENT_CHECKSUM_ENABLED.key -> "true",
      SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key -> "false") {
      withTable("t_first1", "t_first2") {
        // Only the first column differs. Rows are partitioned by the second column, so each
        // row goes to the same partition in both writes.
        spark
          .range(500)
          .selectExpr("id % 7 AS a", "id AS b")
          .repartition(5, col("b"))
          .write
          .mode("overwrite")
          .saveAsTable("t_first1")
        val checksums1 = getLatestShuffleChecksumValues()

        spark
          .range(500)
          .selectExpr("id % 7 + 1 AS a", "id AS b")
          .repartition(5, col("b"))
          .write
          .mode("overwrite")
          .saveAsTable("t_first2")
        val checksums2 = getLatestShuffleChecksumValues()

        assert(
          checksums1.zip(checksums2).exists { case (a, b) => a != b },
          "Checksums should differ when only the first column changes: " +
            s"${checksums1.toSeq} vs ${checksums2.toSeq}"
        )
      }
    }
  }
}
