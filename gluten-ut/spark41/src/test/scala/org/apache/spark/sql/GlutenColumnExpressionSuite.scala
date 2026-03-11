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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.functions.{assert_true, expr, input_file_name, lit}

class GlutenColumnExpressionSuite extends ColumnExpressionSuite with GlutenSQLTestsTrait {
  import testImplicits._

  // TODO: fix in Spark-4.0
  // ignore("raise_error") {
  //   val strDf = Seq(("hello")).toDF("a")

  //   val e1 = intercept[SparkRuntimeException] {
  //     strDf.select(raise_error(lit(null.asInstanceOf[String]))).collect()
  //   }
  //   assert(e1.getCause.isInstanceOf[RuntimeException])

  //   val e2 = intercept[SparkRuntimeException] {
  //     strDf.select(raise_error($"a")).collect()
  //   }
  //   assert(e2.getCause.isInstanceOf[RuntimeException])
  //   assert(e2.getCause.getMessage.contains("hello"))
  // }

  // TODO: fix in Spark-4.0
  ignoreGluten("assert_true") {
    // assert_true(condition, errMsgCol)
    val booleanDf = Seq((true), (false)).toDF("cond")
    checkAnswer(
      booleanDf.filter("cond = true").select(assert_true($"cond")),
      Row(null) :: Nil
    )
    val e1 = intercept[SparkRuntimeException] {
      booleanDf.select(assert_true($"cond", lit(null.asInstanceOf[String]))).collect()
    }
    assert(e1.getCause.isInstanceOf[RuntimeException])

    val nullDf = Seq(("first row", None), ("second row", Some(true))).toDF("n", "cond")
    checkAnswer(
      nullDf.filter("cond = true").select(assert_true($"cond", $"cond")),
      Row(null) :: Nil
    )
    val e2 = intercept[SparkRuntimeException] {
      nullDf.select(assert_true($"cond", $"n")).collect()
    }
    assert(e2.getCause.isInstanceOf[RuntimeException])
    assert(e2.getCause.getMessage.contains("first row"))

    // assert_true(condition)
    val intDf = Seq((0, 1)).toDF("a", "b")
    checkAnswer(intDf.select(assert_true($"a" < $"b")), Row(null) :: Nil)
    val e3 = intercept[SparkRuntimeException] {
      intDf.select(assert_true($"a" > $"b")).collect()
    }
    assert(e3.getCause.isInstanceOf[RuntimeException])
    assert(e3.getCause.getMessage.contains("'('a > 'b)' is not true!"))
  }

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
