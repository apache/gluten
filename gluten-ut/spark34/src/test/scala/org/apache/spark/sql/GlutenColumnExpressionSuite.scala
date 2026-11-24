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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.functions.{expr, input_file_name}

class GlutenColumnExpressionSuite extends ColumnExpressionSuite with GlutenSQLTestsTrait {
  import testImplicits._
  testGluten(
    "input_file_name, input_file_block_start and input_file_block_length " +
      "should fall back if scan falls back") {
    withSQLConf((GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key, "false")) {
      withTempPath {
        dir =>
          val data = sparkContext.parallelize(0 to 10).toDF("id")
          data.write.parquet(dir.getCanonicalPath)

          val q =
            spark.read
              .parquet(dir.getCanonicalPath)
              .select(
                input_file_name(),
                expr("input_file_block_start()"),
                expr("input_file_block_length()"))
          val firstRow = q.head()
          assert(firstRow.getString(0).contains(dir.toURI.getPath))
          assert(firstRow.getLong(1) == 0)
          assert(firstRow.getLong(2) > 0)
          val project = q.queryExecution.executedPlan.collect { case p: ProjectExec => p }
          assert(project.size == 1)
      }
    }
  }
}
