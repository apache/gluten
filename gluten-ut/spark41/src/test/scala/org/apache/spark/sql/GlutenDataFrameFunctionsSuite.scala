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

import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType}

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

  testGluten("map_concat function") {
    val df1 = Seq(
      (Map[Int, Int](1 -> 100, 2 -> 200), Map[Int, Int](3 -> 300, 4 -> 400)),
      (Map[Int, Int](1 -> 100, 2 -> 200), Map[Int, Int](3 -> 300, 1 -> 400)),
      (null, Map[Int, Int](3 -> 300, 4 -> 400))
    ).toDF("map1", "map2")

    val expected1a = Seq(
      Row(Map(1 -> 100, 2 -> 200, 3 -> 300, 4 -> 400)),
      Row(Map(1 -> 400, 2 -> 200, 3 -> 300)),
      Row(null)
    )

    intercept[SparkException](df1.selectExpr("map_concat(map1, map2)").collect())
    intercept[SparkException](df1.select(map_concat($"map1", $"map2")).collect())
    withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      checkAnswer(df1.selectExpr("map_concat(map1, map2)"), expected1a)
      checkAnswer(df1.select(map_concat($"map1", $"map2")), expected1a)
    }

    val expected1b = Seq(
      Row(Map(1 -> 100, 2 -> 200)),
      Row(Map(1 -> 100, 2 -> 200)),
      Row(null)
    )

    checkAnswer(df1.selectExpr("map_concat(map1)"), expected1b)
    checkAnswer(df1.select(map_concat($"map1")), expected1b)

    val df2 = Seq(
      (
        Map[Array[Int], Int](Array(1) -> 100, Array(2) -> 200),
        Map[String, Int]("3" -> 300, "4" -> 400)
      )
    ).toDF("map1", "map2")

    val expected2 = Seq(Row(Map()))

    checkAnswer(df2.selectExpr("map_concat()"), expected2)
    checkAnswer(df2.select(map_concat()), expected2)

    val df3 = {
      val schema = StructType(
        StructField("map1", MapType(StringType, IntegerType, true), false) ::
          StructField("map2", MapType(StringType, IntegerType, false), false) :: Nil
      )
      val data = Seq(
        Row(Map[String, Any]("a" -> 1, "b" -> null), Map[String, Any]("c" -> 3, "d" -> 4)),
        Row(Map[String, Any]("a" -> 1, "b" -> 2), Map[String, Any]("c" -> 3, "d" -> 4))
      )
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }

    val expected3 = Seq(
      Row(Map[String, Any]("a" -> 1, "b" -> null, "c" -> 3, "d" -> 4)),
      Row(Map[String, Any]("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4))
    )

    checkAnswer(df3.selectExpr("map_concat(map1, map2)"), expected3)
    checkAnswer(df3.select(map_concat($"map1", $"map2")), expected3)

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("map_concat(map1, map2)").collect()
      },
      condition = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, map2)\"",
        "dataType" -> "(\"MAP<ARRAY<INT>, INT>\" or \"MAP<STRING, INT>\")",
        "functionName" -> "`map_concat`"),
      context = ExpectedContext(fragment = "map_concat(map1, map2)", start = 0, stop = 21)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.select(map_concat($"map1", $"map2")).collect()
      },
      condition = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, map2)\"",
        "dataType" -> "(\"MAP<ARRAY<INT>, INT>\" or \"MAP<STRING, INT>\")",
        "functionName" -> "`map_concat`"),
      context =
        ExpectedContext(fragment = "map_concat", callSitePattern = getCurrentClassCallSitePattern)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.selectExpr("map_concat(map1, 12)").collect()
      },
      condition = "DATATYPE_MISMATCH.MAP_CONCAT_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, 12)\"",
        "dataType" -> "[\"MAP<ARRAY<INT>, INT>\", \"INT\"]",
        "functionName" -> "`map_concat`"),
      context = ExpectedContext(fragment = "map_concat(map1, 12)", start = 0, stop = 19)
    )

    checkError(
      exception = intercept[AnalysisException] {
        df2.select(map_concat($"map1", lit(12))).collect()
      },
      condition = "DATATYPE_MISMATCH.MAP_CONCAT_DIFF_TYPES",
      sqlState = None,
      parameters = Map(
        "sqlExpr" -> "\"map_concat(map1, 12)\"",
        "dataType" -> "[\"MAP<ARRAY<INT>, INT>\", \"INT\"]",
        "functionName" -> "`map_concat`"),
      context =
        ExpectedContext(fragment = "map_concat", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  testGluten("transform keys function - primitive data types") {
    val dfExample1 = Seq(
      Map[Int, Int](1 -> 1, 9 -> 9, 8 -> 8, 7 -> 7)
    ).toDF("i")

    val dfExample2 = Seq(
      Map[Int, Double](1 -> 1.0, 2 -> 1.40, 3 -> 1.70)
    ).toDF("j")

    val dfExample3 = Seq(
      Map[Int, Boolean](25 -> true, 26 -> false)
    ).toDF("x")

    val dfExample4 = Seq(
      Map[Array[Int], Boolean](Array(1, 2) -> false)
    ).toDF("y")

    def testMapOfPrimitiveTypesCombination(): Unit = {
      checkAnswer(
        dfExample1.selectExpr("transform_keys(i, (k, v) -> k + v)"),
        Seq(Row(Map(2 -> 1, 18 -> 9, 16 -> 8, 14 -> 7))))

      checkAnswer(
        dfExample1.select(transform_keys(col("i"), (k, v) => k + v)),
        Seq(Row(Map(2 -> 1, 18 -> 9, 16 -> 8, 14 -> 7))))

      checkAnswer(
        dfExample2.selectExpr(
          "transform_keys(j, " +
            "(k, v) -> map_from_arrays(ARRAY(1, 2, 3), ARRAY('one', 'two', 'three'))[k])"),
        Seq(Row(Map("one" -> 1.0, "two" -> 1.4, "three" -> 1.7)))
      )

      checkAnswer(
        dfExample2.select(
          transform_keys(
            col("j"),
            (k, v) =>
              element_at(
                map_from_arrays(
                  array(lit(1), lit(2), lit(3)),
                  array(lit("one"), lit("two"), lit("three"))
                ),
                k
              )
          )
        ),
        Seq(Row(Map("one" -> 1.0, "two" -> 1.4, "three" -> 1.7)))
      )

      checkAnswer(
        dfExample2.selectExpr("transform_keys(j, (k, v) -> CAST(v * 2 AS BIGINT) + k)"),
        Seq(Row(Map(3 -> 1.0, 4 -> 1.4, 6 -> 1.7))))

      checkAnswer(
        dfExample2.select(transform_keys(col("j"), (k, v) => (v * 2).cast("bigint") + k)),
        Seq(Row(Map(3 -> 1.0, 4 -> 1.4, 6 -> 1.7))))

      checkAnswer(
        dfExample2.selectExpr("transform_keys(j, (k, v) -> k + v)"),
        Seq(Row(Map(2.0 -> 1.0, 3.4 -> 1.4, 4.7 -> 1.7))))

      checkAnswer(
        dfExample2.select(transform_keys(col("j"), (k, v) => k + v)),
        Seq(Row(Map(2.0 -> 1.0, 3.4 -> 1.4, 4.7 -> 1.7))))

      intercept[SparkException] {
        dfExample3.selectExpr("transform_keys(x, (k, v) ->  k % 2 = 0 OR v)").collect()
      }
      intercept[SparkException] {
        dfExample3.select(transform_keys(col("x"), (k, v) => k % 2 === 0 || v)).collect()
      }
      withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
        checkAnswer(
          dfExample3.selectExpr("transform_keys(x, (k, v) ->  k % 2 = 0 OR v)"),
          Seq(Row(Map(true -> true, true -> false))))

        checkAnswer(
          dfExample3.select(transform_keys(col("x"), (k, v) => k % 2 === 0 || v)),
          Seq(Row(Map(true -> true, true -> false))))
      }

      checkAnswer(
        dfExample3.selectExpr("transform_keys(x, (k, v) -> if(v, 2 * k, 3 * k))"),
        Seq(Row(Map(50 -> true, 78 -> false))))

      checkAnswer(
        dfExample3.select(transform_keys(col("x"), (k, v) => when(v, k * 2).otherwise(k * 3))),
        Seq(Row(Map(50 -> true, 78 -> false))))

      checkAnswer(
        dfExample4.selectExpr("transform_keys(y, (k, v) -> array_contains(k, 3) AND v)"),
        Seq(Row(Map(false -> false))))

      checkAnswer(
        dfExample4.select(transform_keys(col("y"), (k, v) => array_contains(k, lit(3)) && v)),
        Seq(Row(Map(false -> false))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testMapOfPrimitiveTypesCombination()
    dfExample1.cache()
    dfExample2.cache()
    dfExample3.cache()
    dfExample4.cache()
    // Test with cached relation, the Project will be evaluated with codegen
    testMapOfPrimitiveTypesCombination()
  }

}
