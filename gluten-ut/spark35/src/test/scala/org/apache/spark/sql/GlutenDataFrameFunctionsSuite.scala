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

import org.apache.spark.sql.functions._

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

  testGluten("flatten function") {
    // Test cases with a primitive type
    val intDF = Seq(
      (Seq(Seq(1, 2, 3), Seq(4, 5), Seq(6))),
      (Seq(Seq(1, 2))),
      (Seq(Seq(1), Seq.empty)),
      (Seq(Seq.empty, Seq(1)))
    ).toDF("i")

    val intDFResult = Seq(Row(Seq(1, 2, 3, 4, 5, 6)), Row(Seq(1, 2)), Row(Seq(1)), Row(Seq(1)))

    def testInt(): Unit = {
      checkAnswer(intDF.select(flatten($"i")), intDFResult)
      checkAnswer(intDF.selectExpr("flatten(i)"), intDFResult)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testInt()
    // Test with cached relation, the Project will be evaluated with codegen
    intDF.cache()
    testInt()

    // Test cases with non-primitive types
    val strDF = Seq(
      (Seq(Seq("a", "b"), Seq("c"), Seq("d", "e", "f"))),
      (Seq(Seq("a", "b"))),
      (Seq(Seq("a", null), Seq(null, "b"), Seq(null, null))),
      (Seq(Seq("a"), Seq.empty)),
      (Seq(Seq.empty, Seq("a")))
    ).toDF("s")

    val strDFResult = Seq(
      Row(Seq("a", "b", "c", "d", "e", "f")),
      Row(Seq("a", "b")),
      Row(Seq("a", null, null, "b", null, null)),
      Row(Seq("a")),
      Row(Seq("a")))

    def testString(): Unit = {
      checkAnswer(strDF.select(flatten($"s")), strDFResult)
      checkAnswer(strDF.selectExpr("flatten(s)"), strDFResult)
    }

    // Test with local relation, the Project will be evaluated without codegen
    testString()
    // Test with cached relation, the Project will be evaluated with codegen
    strDF.cache()
    testString()

    val arrDF = Seq((1, "a", Seq(1, 2, 3))).toDF("i", "s", "arr")

    def testArray(): Unit = {
      checkAnswer(
        arrDF.selectExpr("flatten(array(arr, array(null, 5), array(6, null)))"),
        Seq(Row(Seq(1, 2, 3, null, 5, 6, null))))
      checkAnswer(
        arrDF.selectExpr("flatten(array(array(arr, arr), array(arr)))"),
        Seq(Row(Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3)))))
    }

    // Test with local relation, the Project will be evaluated without codegen
    testArray()
    // Test with cached relation, the Project will be evaluated with codegen
    arrDF.cache()
    testArray()

    // Error test cases
    val oneRowDF = Seq((1, "a", Seq(1, 2, 3))).toDF("i", "s", "arr")
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"arr"))
    }
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"i"))
    }
    intercept[AnalysisException] {
      oneRowDF.select(flatten($"s"))
    }
    intercept[AnalysisException] {
      oneRowDF.selectExpr("flatten(null)")
    }
  }
}
