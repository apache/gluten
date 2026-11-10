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

import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec

class GlutenDatasetSuite extends DatasetSuite with GlutenSQLTestsTrait {
  import testImplicits._

  testGluten("dropDuplicates: columns with same column name") {
    val ds1 = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    val ds2 = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    // The dataset joined has two columns of the same name "_2".
    val joined = ds1.join(ds2, "_1").select(ds1("_2").as[Int], ds2("_2").as[Int])
    // Using the checkDatasetUnorderly method to sort the result in Gluten.
    checkDatasetUnorderly(joined.dropDuplicates(), (1, 2), (1, 1), (2, 1), (2, 2))
  }

  testGluten("groupBy.as") {
    val df1 = Seq(DoubleData(1, "one"), DoubleData(2, "two"), DoubleData(3, "three"))
      .toDS()
      .repartition($"id")
      .sortWithinPartitions("id")
    val df2 = Seq(DoubleData(5, "one"), DoubleData(1, "two"), DoubleData(3, "three"))
      .toDS()
      .repartition($"id")
      .sortWithinPartitions("id")

    val df3 = df1
      .groupBy("id")
      .as[Int, DoubleData]
      .cogroup(df2.groupBy("id").as[Int, DoubleData]) {
        case (key, data1, data2) =>
          if (key == 1) {
            Iterator(DoubleData(key, (data1 ++ data2).foldLeft("")((cur, next) => cur + next.val1)))
          } else Iterator.empty
      }
    checkDataset(df3, DoubleData(1, "onetwo"))

    // Assert that no extra shuffle introduced by cogroup.
    val exchanges = collect(df3.queryExecution.executedPlan) {
      case h: ColumnarShuffleExchangeExec => h
    }
    // Assert the number of ColumnarShuffleExchangeExec
    // instead of ShuffleExchangeExec in Gluten.
    assert(exchanges.size == 2)
  }
}
