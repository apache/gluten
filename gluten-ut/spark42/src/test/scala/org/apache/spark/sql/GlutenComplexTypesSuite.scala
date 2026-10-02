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

class GlutenComplexTypesSuite extends ComplexTypesSuite with GlutenSQLTestsTrait {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .range(10)
      .selectExpr(
        "(id % 2 = 0) as bool",
        "cast(id as BYTE) as i8",
        "cast(id as SHORT) as i16",
        "cast(id as FLOAT) as fp32",
        "cast(id as DOUBLE) as fp64",
        "cast(id as DECIMAL(4, 2)) as dec",
        "cast(cast(id as BYTE) as BINARY) as vbin",
        "binary(id) as vbin1",
        "map_from_arrays(array(id),array(id+2)) as map",
        "array(id, id+1, id+2) as list",
        "struct(cast(id as LONG) as a, cast(id+1 as STRING) as b) as struct"
      )
      .write
      .saveAsTable("tab_types")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS tab_types")
    } finally {
      super.afterAll()
    }
  }

  testGluten("types bool/byte/short/float/double/decimal/binary/map/array/struct") {
    val df = spark
      .table("tab_types")
      .selectExpr(
        "bool",
        "i8",
        "i16",
        "fp32",
        "fp64",
        "dec",
        "vbin",
        "length(vbin)",
        "vbin1",
        "length(vbin1)",
        "struct",
        "struct.a",
        "list",
        "map"
      )
      .sort("i8")
      .limit(1)

    checkAnswer(
      df,
      Seq(
        Row(
          true,
          0.toByte,
          0.toShort,
          0.toFloat,
          0.toDouble,
          BigDecimal(0),
          Array.fill[Byte](1)(0.toByte),
          1.toInt,
          Array.fill[Byte](8)(0.toByte),
          8.toInt,
          Row(0.toLong, "1"),
          0.toLong,
          Array(0, 1, 2),
          Map(0 -> 2)
        ))
    )

    checkNamedStruct(df.queryExecution.optimizedPlan, expectedCount = 0)
  }
}
