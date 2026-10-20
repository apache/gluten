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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

class VeloxDelta4ReadSuite extends QueryTest with DeltaSQLCommandTest with AdaptiveSparkPlanHelper {

  test("a struct whose fields are all VOID is read by vanilla Spark") {
    // Delta 4.4 keeps VOID columns in the read schema from Spark 4.1 on.
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    withTable("t") {
      withSQLConf(DeltaSQLConf.DELTA_CREATE_DATAFRAME_DROP_NULL_COLUMNS.key -> "false") {
        val data = spark.createDataFrame(
          Seq(Row(1, Row(1, null))).asJava,
          new StructType()
            .add("a", IntegerType)
            .add("b", new StructType().add("c", IntegerType).add("d", NullType)))
        sql(
          s"CREATE TABLE t (${data.schema.toDDL}) USING delta " +
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        data.write.format("delta").mode("append").saveAsTable("t")
        // Leaves b with only its VOID field.
        sql("ALTER TABLE t DROP COLUMN b.c")

        val df = spark.table("t").withColumn("isnull", col("b").isNull)
        checkAnswer(df, Row(1, Row(null), false))
        val nativeScans =
          collect(df.queryExecution.executedPlan) { case scan: DeltaScanTransformer => scan }
        assert(nativeScans.isEmpty)
      }
    }
  }
}
