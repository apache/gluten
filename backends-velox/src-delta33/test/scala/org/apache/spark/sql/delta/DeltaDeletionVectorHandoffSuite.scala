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
package org.apache.spark.sql.delta

import org.apache.gluten.execution.DeltaScanTransformer

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

import org.apache.hadoop.fs.Path

@ExtendedSQLTest
class DeltaDeletionVectorHandoffSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  test("Spark 3.5 Delta DV scan handoff should filter deleted rows") {
    withTempDir {
      tempDir =>
        val path = tempDir.getCanonicalPath
        Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"))
          .toDF("id", "value")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)

        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (3, 4)")

        val log = DeltaLog.forTable(spark, new Path(path))
        val addFileWithDv = log.update().allFiles.collect().find(_.deletionVector != null)
        assert(addFileWithDv.nonEmpty)

        val dataFile = addFileWithDv.get
        assert(dataFile.deletionVector.cardinality == 2L)

        val df = spark.read.format("delta").load(path)
        val executedPlan = df.queryExecution.executedPlan
        assert(executedPlan.collect { case _: DeltaScanTransformer => true }.nonEmpty)
        val planText = executedPlan.toString()
        assert(!planText.contains("__delta_internal_is_row_deleted"))
        assert(!planText.contains("__delta_internal_row_index"))
        checkAnswer(df, Seq((1, "a"), (2, "b")).toDF())
    }
  }
}
