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

import org.apache.gluten.exception.GlutenException

import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class GlutenDataFrameFunctionsSuite extends DataFrameFunctionsSuite with GlutenSQLTestsTrait {
  import testImplicits._

  testGluten("map_zip_with function - map of primitive types") {
    val df = Seq(
      (Map(8 -> 6L, 3 -> 5L, 6 -> 2L), Map[Integer, Integer]((6, 4), (8, 2), (3, 2))),
      (Map(10 -> 6L, 8 -> 3L), Map[Integer, Integer]((8, 4), (4, null))),
      (Map.empty[Int, Long], Map[Integer, Integer]((5, 1))),
      (Map(5 -> 1L), null)
    ).toDF("m1", "m2")

    GlutenQueryTestUtil.sameRows(
      df.selectExpr("map_zip_with(m1, m2, (k, v1, v2) -> k == v1 + v2)").collect.toSeq,
      Seq(
        Row(Map(8 -> true, 3 -> false, 6 -> true)),
        Row(Map(10 -> null, 8 -> false, 4 -> null)),
        Row(Map(5 -> null)),
        Row(null)),
      false
    )

    GlutenQueryTestUtil.sameRows(
      df.select(map_zip_with(df("m1"), df("m2"), (k, v1, v2) => k === v1 + v2)).collect.toSeq,
      Seq(
        Row(Map(8 -> true, 3 -> false, 6 -> true)),
        Row(Map(10 -> null, 8 -> false, 4 -> null)),
        Row(Map(5 -> null)),
        Row(null)),
      false
    )
  }

  testGluten("array_insert functions") {
    val fiveShort: Short = 5

    val df1 = Seq((Array[Integer](3, 2, 5, 1, 2), 6, 3)).toDF("a", "b", "c")
    val df2 = Seq((Array[Short](1, 2, 3, 4), 5, fiveShort)).toDF("a", "b", "c")
    val df3 = Seq((Array[Double](3.0, 2.0, 5.0, 1.0, 2.0), 2, 3.0)).toDF("a", "b", "c")
    val df4 = Seq((Array[Boolean](true, false), 3, false)).toDF("a", "b", "c")
    val df5 = Seq((Array[String]("a", "b", "c"), 0, "d")).toDF("a", "b", "c")
    val df6 = Seq((Array[String]("a", null, "b", "c"), 5, "d")).toDF("a", "b", "c")

    checkAnswer(df1.selectExpr("array_insert(a, b, c)"), Seq(Row(Seq(3, 2, 5, 1, 2, 3))))
    checkAnswer(df2.selectExpr("array_insert(a, b, c)"), Seq(Row(Seq[Short](1, 2, 3, 4, 5))))
    checkAnswer(
      df3.selectExpr("array_insert(a, b, c)"),
      Seq(Row(Seq[Double](3.0, 3.0, 2.0, 5.0, 1.0, 2.0)))
    )
    checkAnswer(df4.selectExpr("array_insert(a, b, c)"), Seq(Row(Seq(true, false, false))))

    val e1 = intercept[SparkException] {
      df5.selectExpr("array_insert(a, b, c)").show()
    }
    assert(e1.getCause.isInstanceOf[GlutenException])
//    checkError(
//      exception = e1.getCause.asInstanceOf[SparkRuntimeException],
//      errorClass = "INVALID_INDEX_OF_ZERO",
//      parameters = Map.empty,
//      context = ExpectedContext(
//        fragment = "array_insert(a, b, c)",
//        start = 0,
//        stop = 20)
//    )

    checkAnswer(
      df5.select(array_insert(col("a"), lit(1), col("c"))),
      Seq(Row(Seq("d", "a", "b", "c")))
    )
    // null checks
    checkAnswer(df6.selectExpr("array_insert(a, b, c)"), Seq(Row(Seq("a", null, "b", "c", "d"))))
    checkAnswer(
      df6.select(array_insert(col("a"), col("b"), lit(null).cast("string"))),
      Seq(Row(Seq("a", null, "b", "c", null)))
    )
    checkAnswer(
      df5.select(array_insert(col("a"), lit(null).cast("integer"), col("c"))),
      Seq(Row(null))
    )
    checkAnswer(
      df5.select(array_insert(lit(null).cast("array<string>"), col("b"), col("c"))),
      Seq(Row(null))
    )
    checkAnswer(df1.selectExpr("array_insert(a, 7, c)"), Seq(Row(Seq(3, 2, 5, 1, 2, null, 3))))
    checkAnswer(df1.selectExpr("array_insert(a, -6, c)"), Seq(Row(Seq(3, 3, 2, 5, 1, 2))))

    withSQLConf(SQLConf.LEGACY_NEGATIVE_INDEX_IN_ARRAY_INSERT.key -> "true") {
      checkAnswer(df1.selectExpr("array_insert(a, -6, c)"), Seq(Row(Seq(3, null, 3, 2, 5, 1, 2))))
    }
  }
}
