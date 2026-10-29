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

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.FileSourceScanExec

import org.apache.hadoop.fs.Path

class BoltDeltaSuite extends DeltaSuite {
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
        assert(
          collect(pruned.queryExecution.executedPlan) {
            case _: DeltaScanTransformer => true
          }.nonEmpty,
          "A DV in another partition must not disable a DV-free native scan"
        )
    }
  }
}
