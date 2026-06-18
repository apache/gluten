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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.{ColumnarToRowExecBase => GlutenC2R}

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.internal.SQLConf

class GlutenSparkPlanSuite extends SparkPlanSuite with GlutenSQLTestsTrait {

  testGluten(
    "SPARK-37779: ColumnarToRowExec should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath {
        path =>
          spark.range(1).write.parquet(path.getAbsolutePath)
          val df = spark.read.parquet(path.getAbsolutePath)
          // Gluten replaces ColumnarToRowExec with VeloxColumnarToRowExec
          val c2r = df.queryExecution.executedPlan
            .collectFirst { case p: GlutenC2R => p }
            .orElse(df.queryExecution.executedPlan
              .collectFirst { case p: ColumnarToRowExec => p })
            .get
          try {
            spark.range(1).foreach {
              _ =>
                c2r.canonicalized
                ()
            }
          } catch {
            case e: Throwable =>
              fail("ColumnarToRow was not canonicalizable", e)
          }
      }
    }
  }
}
