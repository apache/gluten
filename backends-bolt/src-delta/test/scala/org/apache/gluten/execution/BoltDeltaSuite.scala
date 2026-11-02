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

import org.apache.gluten.extension.BoltOffloadDeltaScan
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.FileSourceScanExec

import org.apache.hadoop.fs.Path

class BoltDeltaSuite extends DeltaSuite {
  test("Bolt does not list generic Delta indexes during offload") {
    withSQLConf("spark.gluten.enabled" -> "false") {
      Seq(false, true).foreach {
        enableDvs =>
          withTempPath {
            directory =>
              import testImplicits._
              val path = directory.getCanonicalPath
              Seq(1, 2).toDF("id").write.format("delta").save(path)
              if (enableDvs) {
                spark.sql(
                  s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
              }
              val log = DeltaLog.forTable(spark, new Path(path))
              val scan = spark.read.format("delta").load(path).queryExecution.executedPlan.collect {
                case fileScan: FileSourceScanExec => fileScan
              }.head
              var listings = 0
              val snapshot = log.update()
              val index = new TahoeFileIndex(spark, log, new Path(path)) {
                override def version: Long = snapshot.version
                override def metadata = snapshot.metadata
                override def protocol = snapshot.protocol
                // Delta 3.3+ asks for these optional statistics; Delta 2.4 has no such members.
                def numOfFilesIfKnown: Option[Long] = None
                def sizeInBytesIfKnown: Option[Long] = None
                override def inputFiles: Array[String] = scan.relation.location.inputFiles
                override def sizeInBytes: Long = scan.relation.location.sizeInBytes
                override def refresh(): Unit = ()
                override def matchingFiles(
                    partitionFilters: Seq[Expression],
                    dataFilters: Seq[Expression]): Seq[AddFile] = {
                  listings += 1
                  snapshot.filesForScan(partitionFilters ++ dataFilters).files
                }
              }
              val genericScan = scan.copy(relation = scan.relation.copy(location = index)(spark))
              val offloaded = BoltOffloadDeltaScan().offload(genericScan)
              if (enableDvs) {
                assert(offloaded eq genericScan, "Unknown DV-capable indexes must stay on Spark")
              } else {
                assert(offloaded.isInstanceOf[DeltaScanTransformer])
              }
              assert(
                listings == 0,
                "The DV guard must not repeat Delta's driver-side file selection")
          }
      }
    }
  }

  testWithMinSparkVersion("Bolt does not reuse prepared Delta files for different filters", "3.5") {
    withSQLConf("spark.gluten.enabled" -> "false") {
      withTempPath {
        directory =>
          import testImplicits._
          val path = directory.getCanonicalPath
          Seq((1, "dv"), (2, "dv"), (3, "plain")).toDF("id", "region").coalesce(1)
            .write.format("delta").partitionBy("region").save(path)
          spark.sql(
            s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
          spark.sql(s"DELETE FROM delta.`$path` WHERE id = 2")
          val scan = spark.read.format("delta").load(path).where("region = 'plain'")
            .queryExecution.executedPlan.collect {
              case fileScan: FileSourceScanExec => fileScan
            }.head
          val prepared = scan.relation.location.asInstanceOf[PreparedDeltaFileIndex]
          assert(prepared.preparedScan.files.nonEmpty)
          assert(prepared.preparedScan.files.forall(_.deletionVector == null))
          assert(
            DeltaLog.forTable(spark, new Path(path)).update().allFiles.collect()
              .exists(_.deletionVector != null))
          assert(
            BoltOffloadDeltaScan().offload(scan).isInstanceOf[DeltaScanTransformer],
            "Delta's injected deleted-row filter must not invalidate the prepared partition selection"
          )
          Seq(Nil, scan.dataFilters).foreach {
            dataFilters =>
              val unpruned = scan.copy(partitionFilters = Nil, dataFilters = dataFilters)
              assert(
                BoltOffloadDeltaScan().offload(unpruned) eq unpruned,
                "Dropping partition filters must not reuse a stale DV-free selection, even with the injected row filter"
              )
          }
      }
    }
  }

  testWithMinSparkVersion(
    "Bolt keeps DV-free scans native and reads existing deletion vectors through Spark",
    "3.5") {
    withTempPath {
      directory =>
        import testImplicits._
        val path = directory.getCanonicalPath
        val original = Seq((1, "dv"), (2, "dv"), (3, "plain"), (4, "plain"))
        original.toDF("id", "region").coalesce(1)
          .write.format("delta").partitionBy("region").save(path)
        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")

        val before = spark.read.format("delta").load(path)
        checkAnswer(before, original.map { case (id, region) => Row(id, region) })
        assert(
          collect(before.queryExecution.executedPlan) {
            case _: DeltaScanTransformer => true
          }.nonEmpty,
          "Enabling deletion vectors must not disable DV-free native scans"
        )

        spark.sql(s"DELETE FROM delta.`$path` WHERE id = 2")
        assert(
          DeltaLog.forTable(spark, new Path(path)).update().allFiles.collect()
            .exists(_.deletionVector != null),
          "The DELETE must leave an actual deletion vector for this regression"
        )

        // This property controls future DV writes; existing descriptors remain authoritative.
        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)")
        val after = spark.read.format("delta").load(path)
        checkAnswer(after, Seq(Row(1, "dv"), Row(3, "plain"), Row(4, "plain")))
        assert(
          collect(after.queryExecution.executedPlan) {
            case _: DeltaScanTransformer => true
          }.isEmpty,
          "Bolt cannot interpret the Delta deletion-vector split protocol"
        )
        assert(
          collect(after.queryExecution.executedPlan) {
            case _: FileSourceScanExec => true
          }.nonEmpty,
          "Spark must apply the deletion vector instead of returning deleted rows"
        )

        val pruned = spark.read.format("delta").load(path).where("region = 'plain'")
        checkAnswer(pruned, Seq(Row(3, "plain"), Row(4, "plain")))
        // Delta can retain row-index metadata after pruning every file with a DV. Bolt's native
        // ROWINDEX_COL validation is independently unsupported, so check the guard in isolation.
        val prunedScans = collect(pruned.queryExecution.sparkPlan) {
          case scan: FileSourceScanExec => scan
        }
        assert(prunedScans.size == 1)
        val prunedScan = prunedScans.head.copy()
        FallbackTags.untag(prunedScan)
        assert(
          BoltOffloadDeltaScan().offload(prunedScan).isInstanceOf[DeltaScanTransformer],
          "A DV in another partition must not make the Bolt DV guard reject the selected files"
        )
    }
  }
}
